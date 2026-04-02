from collections import defaultdict, deque
import json
import os


class SchemaInfere:
    def __init__(self, unique_fields, global_key, output_dir="."):
        self.unique_fields = set(unique_fields)
        self.global_key    = global_key
        self.output_dir    = output_dir

        self.buffer_400  = []
        self.buffer_1000 = []

        self.entities     = defaultdict(set)
        self.fd           = defaultdict(set)
        self.foreign_keys = set()
        self.m2m          = set()

        self.all_records  = []   # full copy kept for operation generation

        self._schema_snapshot = {}   #{table_name: set(columns)}

        #Tracks which Primary Key(PK) have already been INSERTed per table.
        #1st time a PK value is seen → INSERT, after that → UPDATE.
        self._seen_pks = defaultdict(set)
        self._all_ops_log = []

    #conflict resolver, resolves ambigiuty of column in which table

    def resolve_conflicts(self):
        column_owner = {}

        for entity, attrs in self.entities.items():
            for attr in attrs:
                if attr not in column_owner:
                    column_owner[attr] = entity
                else:
                    current_owner = column_owner[attr]
                    if len(self.entities[entity]) > len(self.entities[current_owner]):
                        column_owner[attr] = entity

        clean_entities = {e: set() for e in self.entities}
        for col, owner in column_owner.items():
            clean_entities[owner].add(col)

        return clean_entities

    #Data ingestion

    def queue_reader(self, rec_queue):
        # create/add-> schema learning + INSERT
        # change-> UPDATE
        # remove-> DELETE
        # get-> SELECT
        while rec_queue:
            item = rec_queue.popleft()
            if isinstance(item, tuple):
                event, record = item
            else:
                event, record = 'create', item
            event = event.lower().strip()
            if event in ('create', 'add'):
                self.add_record(record)
            else:
                self.all_records.append(('__event__', event, record))
        self.flush()

        schema = self.build_schema()
        self._log_create_tables(schema)

        for item in self.all_records:
            if isinstance(item, tuple) and item[0] == '__event__':
                _, event, record = item
                self._handle_crud_event(event, record, schema)
            else:
                self.generate_operations(item, schema)

        # Write operations_sql.json after all ops are generated
        self._save_ops_json()

        return schema

    def add_record(self, record):
        self.all_records.append(record)
        self.buffer_400.append(record)
        self.buffer_1000.append(record)

        if len(self.buffer_400) >= 400:
            self.process_400()
            self._check_schema_changes()   # emit ALTER TABLE if anything changed
            self.buffer_400.clear()

        if len(self.buffer_1000) >= 1000:
            self.process_1000()
            self._check_schema_changes()   # emit ALTER TABLE if anything changed
            self.buffer_1000.clear()

    def flush(self):
        if self.buffer_400:
            self.process_400()
            self.buffer_400.clear()
        if self.buffer_1000:
            self.process_1000()
            self.buffer_1000.clear()

    #Functional Dependencies

    def is_dependent(self, A, B, recs):
        mapping = {}
        for rec in recs:
            if A not in rec or B not in rec:
                continue
            a_val = rec[A]
            b_val = rec[B]
            if isinstance(a_val, list) or isinstance(b_val, list):
                continue
            if a_val not in mapping:
                mapping[a_val] = b_val
            else:
                if mapping[a_val] != b_val:
                    return False
        return len(mapping) > 0

    #Entity detection and update 

    def process_400(self):
        col_vals = defaultdict(list)
        for rec in self.buffer_400:
            for col, val in rec.items():
                if col == self.global_key or isinstance(val, list):
                    continue
                col_vals[col].append(val)

        for key in self.unique_fields:
            for col in col_vals:
                if col == key:
                    continue
                if self.is_dependent(key, col, self.buffer_400):
                    self.entities[key].add(col)

    #Relations detection and update

    def process_1000(self):
        col_vals = defaultdict(list)
        is_list  = defaultdict(bool)

        for rec in self.buffer_1000:
            for col, val in rec.items():
                if col == self.global_key:
                    continue
                if isinstance(val, list):
                    is_list[col] = True
                    col_vals[col].extend(val)
                else:
                    col_vals[col].append(val)

        for key in self.unique_fields:
            if key not in col_vals:
                continue
            for col in col_vals:
                if col == key:
                    continue
                if self.is_dependent(key, col, self.buffer_1000):
                    self.fd[key].add(col)

        for A in col_vals:
            if is_list[A] or A in self.unique_fields:
                continue
            for B in self.unique_fields:
                if A == B or B not in col_vals:
                    continue
                set_A = set(v for v in col_vals[A] if not isinstance(v, list))
                set_B = set(v for v in col_vals[B] if not isinstance(v, list))
                if set_A and set_A.issubset(set_B):
                    self.foreign_keys.add((A, B))

        for col, flag in is_list.items():
            if flag:
                self.m2m.add(col)

    #Schema change detection

    def _current_table_snapshot(self):
        clean     = self.resolve_conflicts()
        all_keys  = self.unique_fields | set(clean.keys())
        snapshot  = {}

        for key in all_keys:
            attrs      = clean.get(key, set())
            table_name = key.replace("_id", "").lower()
            snapshot[table_name] = set([key] + list(attrs))

        # Junction tables
        pk_to_tbl = {key.replace("_id", "").lower(): key for key in all_keys}
        # reverse: table_name -> pk
        tbl_to_pk = {v.replace("_id", "").lower(): v for v in all_keys}
        col_to_tbl = {}
        for tname, cols in snapshot.items():
            for c in cols:
                col_to_tbl[c] = tname

        for list_col in self.m2m:
            if list_col not in self.unique_fields:
                continue
            owner_key = self._find_owner_of_list(list_col)
            if owner_key is None:
                continue
            owner_tbl = owner_key.replace("_id", "").lower()
            ref_tbl   = list_col.replace("_id", "").lower()
            jname = "_".join(sorted([owner_tbl, ref_tbl]))
            snapshot[jname] = set([owner_key, list_col])

        return snapshot

    def _check_schema_changes(self):
        current  = self._current_table_snapshot()
        previous = self._schema_snapshot
        ops      = []

        for table_name, cols in current.items():
            if table_name not in previous:
                # new table detection
                ops.append({
                    "type":       "CREATE",
                    "table_name": table_name,
                    "columns":    sorted(cols),
                    "sql":        self._render_create_sql(table_name, cols),
                })
            else:
                #check for new columns
                new_cols = cols - previous[table_name]
                for col in sorted(new_cols):
                    ops.append({
                        "type":       "ALTER_TABLE",
                        "table_name": table_name,
                        "action":     "ADD_COLUMN",
                        "column":     col,
                        "sql":        f"ALTER TABLE {table_name} ADD COLUMN {col};",
                    })

        if ops:
            self._log_operations(ops)
        self._schema_snapshot = {t: set(c) for t, c in current.items()}

    #Final Schema Building

    def build_schema(self):
        clean_entities = self.resolve_conflicts()
        tables         = self.build_tables(clean_entities)
        junction_tables = self.build_junction_tables(tables)
        tables          = self.attach_foreign_keys(tables, junction_tables)
        tables.update(junction_tables)

        schema = {
            "tables": tables,
            "functional_dependencies": {k: list(v) for k, v in self.fd.items()},
            "foreign_keys": list(self.foreign_keys),
            "many_to_many": list(self.m2m),
        }

        self._save_schema(schema)
        return schema

    def build_tables(self, clean_entities):
        tables   = {}
        all_keys = self.unique_fields | set(clean_entities.keys())

        for key in all_keys:
            attrs      = clean_entities.get(key, set())
            table_name = key.replace("_id", "").lower()
            tables[table_name] = {
                "primary_key":  key,
                "columns":      [key] + list(attrs),
                "foreign_keys": [],
            }
        return tables

    def build_junction_tables(self, tables):
        junction_tables = {}
        col_to_table    = {td["primary_key"]: tn for tn, td in tables.items()}

        for list_col in self.m2m:
            if list_col not in self.unique_fields:
                continue
            owner_key = self._find_owner_of_list(list_col)
            if owner_key is None:
                continue

            owner_table = col_to_table.get(owner_key)
            ref_table   = col_to_table.get(list_col)
            if owner_table is None or ref_table is None:
                continue

            jname = "_".join(sorted([owner_table, ref_table]))
            junction_tables[jname] = {
                "primary_key":  None,
                "columns":      [owner_key, list_col],
                "foreign_keys": [
                    {"column": owner_key, "references_table": owner_table,
                     "references_column": owner_key},
                    {"column": list_col,  "references_table": ref_table,
                     "references_column": list_col},
                ],
                "is_junction": True,
            }
        return junction_tables

    def _find_owner_of_list(self, list_col):
        for key in self.unique_fields:
            if key == list_col:
                continue
            if self.fd[key] or self.entities[key]:
                return key
        return None

    #Foriegn Keys

    def attach_foreign_keys(self, tables, junction_tables):
        col_to_table = {}
        for tname, tdef in tables.items():
            for col in tdef["columns"]:
                col_to_table[col] = tname
        pk_to_table = {td["primary_key"]: tn for tn, td in tables.items()}

        for (fk_col, ref_key) in self.foreign_keys:
            owner = col_to_table.get(fk_col)
            ref_t = pk_to_table.get(ref_key)
            if owner and ref_t:
                tables[owner]["foreign_keys"].append({
                    "column":            fk_col,
                    "references_table":  ref_t,
                    "references_column": ref_key,
                })
        return tables

    #Data operations

    def generate_operations(self, record, schema):
        tables= schema["tables"]
        operations= []

        scalar_record= {
            k: v for k, v in record.items()
            if not isinstance(v, list) and k != self.global_key
        }
        global_key_val = record.get(self.global_key)

        for table_name, table_def in tables.items():
            if table_def.get("is_junction", False):
                ops = self._generate_junction_ops(record, table_name, table_def)
                operations.extend(ops)
            else:
                op = self._generate_entity_op(
                    scalar_record, table_name, table_def, global_key_val
                )
                if op:
                    operations.append(op)

        self._log_operations(operations)
        return operations

#CRUD Events
    def _handle_crud_event(self, event, record, schema):
        global_key_val = record.get(self.global_key)
        tables         = schema['tables']
        ops            = []

        columns_hint = record.get('COLUMNS')   # list of cols requested in GET
        scalar_record = {
            k: v for k, v in record.items()
            if k not in (self.global_key, 'COLUMNS') and not isinstance(v, list)
        }

        for table_name, table_def in tables.items():
            pk = table_def.get('primary_key')
            if not pk or pk not in scalar_record:
                continue
            pk_val = scalar_record[pk]

            if event == 'add':
                columns = [c for c in table_def['columns'] if c in scalar_record]
                values  = [scalar_record[c] for c in columns]
                if columns:
                    if pk_val not in self._seen_pks[table_name]:
                        self._seen_pks[table_name].add(pk_val)
                        op_type = 'INSERT'
                    else:
                        op_type = 'UPDATE'
                    op = {'type': op_type, 'table_name': table_name,
                          'columns': columns, 'values': values}
                    if global_key_val is not None:
                        op[self.global_key] = global_key_val
                    ops.append(op)

            elif event == 'change':
                update_cols = [c for c in table_def['columns']
                               if c in scalar_record and c != pk]
                update_vals = [scalar_record[c] for c in update_cols]
                if update_cols:
                    op = {'type': 'UPDATE', 'table_name': table_name,
                          'columns': update_cols, 'values': update_vals,
                          'where': {pk: pk_val}}
                    if global_key_val is not None:
                        op[self.global_key] = global_key_val
                    ops.append(op)

            elif event == 'remove':
                op = {'type': 'DELETE', 'table_name': table_name,
                      'where': {pk: pk_val}}
                if global_key_val is not None:
                    op[self.global_key] = global_key_val
                self._seen_pks[table_name].discard(pk_val)
                ops.append(op)

            elif event == 'get':
                if columns_hint:
                    select_cols = [c for c in columns_hint
                                   if c in table_def['columns']]
                else:
                    select_cols = table_def['columns']
                op = {'type': 'SELECT', 'table_name': table_name,
                      'columns': select_cols, 'where': {pk: pk_val}}
                if global_key_val is not None:
                    op[self.global_key] = global_key_val
                ops.append(op)

        self._log_operations(ops)
        return ops

    def _generate_entity_op(self, scalar_record, table_name, table_def, global_key_val=None):
        pk = table_def["primary_key"]
        if pk not in scalar_record:
            return None

        pk_val  = scalar_record[pk]
        columns = [c for c in table_def["columns"] if c in scalar_record]
        values  = [scalar_record[c] for c in columns]

        if not columns:
            return None

        # First time we see this PK value → INSERT, subsequent → UPDATE
        if pk_val not in self._seen_pks[table_name]:
            self._seen_pks[table_name].add(pk_val)
            op_type = "INSERT"
        else:
            op_type = "UPDATE"

        op = {
            "type":       op_type,
            "table_name": table_name,
            "columns":    columns,
            "values":     values,
        }
        if global_key_val is not None:
            op[self.global_key] = global_key_val
        return op

    def _generate_junction_ops(self, record, table_name, table_def):
        ops        = []
        list_col   = None
        scalar_col = None

        for col in table_def["columns"]:
            val = record.get(col)
            if isinstance(val, list):
                list_col = col
            elif val is not None:
                scalar_col = col

        if not list_col or not scalar_col:
            return ops

        gk_val = record.get(self.global_key)
        for item in record[list_col]:
            op = {
                "type":       "INSERT",
                "table_name": table_name,
                "columns":    [scalar_col, list_col],
                "values":     [record[scalar_col], item],
            }
            if gk_val is not None:
                op[self.global_key] = gk_val
            ops.append(op)
        return ops

    #Data Defining Helpers

    def _log_create_tables(self, schema):
        ops = []
        for table_name, tdef in schema["tables"].items():
            cols = set(tdef["columns"])
            ops.append({
                "type":       "CREATE",
                "table_name": table_name,
                "columns":    tdef["columns"],
                "sql":        self._render_create_sql(table_name, cols, tdef),
            })
        self._log_operations(ops)

    def _render_create_sql(self, table_name, cols, tdef=None):
        pk = None
        if tdef:
            pk = tdef.get("primary_key")

        col_lines = []
        for col in sorted(cols):
            if col == pk:
                col_lines.append(f"  {col} VARCHAR(255) PRIMARY KEY")
            else:
                col_lines.append(f"  {col} VARCHAR(255)")

        # Add FK constraints if available
        if tdef:
            for fk in tdef.get("foreign_keys", []):
                col_lines.append(
                    f"  FOREIGN KEY ({fk['column']}) "
                    f"REFERENCES {fk['references_table']}({fk['references_column']})"
                )

        body = ",\n".join(col_lines)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{body}\n);"

    def _render_sql_for_op(self, op):
        t    = op.get('type', '').upper()
        tbl  = op.get('table_name', '')
        cols = op.get('columns', [])
        vals = op.get('values', [])
        whr  = op.get('where', {})

        def q(v):
            return str(v) if isinstance(v, (int, float)) else f"'{v}'"

        def where_clause(w):
            return ' AND '.join(f"{k} = {q(v)}" for k, v in w.items())

        if t == 'CREATE':
            return op.get('sql', '')

        if t == 'INSERT':
            col_str = ', '.join(cols)
            val_str = ', '.join(q(v) for v in vals)
            return f'INSERT INTO {tbl} ({col_str}) VALUES ({val_str});'

        if t == 'UPDATE':
            set_str = ', '.join(f'{c} = {q(v)}' for c, v in zip(cols, vals))
            sql = f'UPDATE {tbl} SET {set_str}'
            if whr:
                sql += f' WHERE {where_clause(whr)}'
            return sql + ';'

        if t == 'DELETE':
            sql = f'DELETE FROM {tbl}'
            if whr:
                sql += f' WHERE {where_clause(whr)}'
            return sql + ';'

        if t == 'SELECT':
            col_str = ', '.join(cols) if cols else '*'
            sql = f'SELECT {col_str} FROM {tbl}'
            if whr:
                sql += f' WHERE {where_clause(whr)}'
            return sql + ';'

        if t == 'ALTER_TABLE':
            return op.get('sql', '')

        return ''

    def _save_ops_json(self):
        from datetime import datetime
        ops   = self._all_ops_log
        total = len(ops)

        # Count by type for metadata
        from collections import Counter
        counts = Counter(o.get('type','').upper() for o in ops)

        generated_queries = []
        for idx, op in enumerate(ops, start=1):
            t   = op.get('type', '').upper()
            tbl = op.get('table_name', '')
            sql = self._render_sql_for_op(op)

            entry = {
                'type':        t.lower(),
                'entity':      tbl,
                'sql':         sql,
                'description': f'{t.capitalize()} on {tbl}',
                'query_index': idx,
            }
            # Carry through the global key if present
            gk = op.get(self.global_key)
            if gk is not None:
                entry[self.global_key] = gk

            generated_queries.append(entry)

        output = {
            'metadata': {
                'generated_at':           datetime.now().isoformat(),
                'total_queries':          total,
                'valid_queries':          total,
                'generated_sql_statements': total,
                'by_type':                dict(counts),
                'errors':                 0,
                'warnings':               0,
            },
            'errors':            [],
            'warnings':          [],
            'generated_queries': generated_queries,
        }

        path = os.path.join(self.output_dir, 'operations_sql.json')
        with open(path, 'w') as f:
            json.dump(output, f, indent=2, default=list)
        print(f'[SchemaInfere] operations_sql.json saved → {path}')

    def _save_schema(self, schema):
        path = os.path.join(self.output_dir, "schema.json")
        with open(path, "w") as f:
            json.dump(schema, f, indent=2, default=list)
        print(f"[SchemaInfere] Schema saved → {path}")

    def _log_operations(self, operations):
        if not operations:
            return
        # Append to flat log file
        path = os.path.join(self.output_dir, "operations.log.")
        with open(path, "a") as f:
            for op in operations:
                f.write(json.dumps(op) + "\n")
        # Keep in-memory copy for operations_sql.json
        self._all_ops_log.extend(operations)
        print(f"[SchemaInfere] {len(operations)} operation(s) logged → {path}")

#Testing

if __name__ == "__main__":
    records = deque([
        {"student_id": "S1", "name": "Alice",  "course_id": ["CS101", "CS102"],
         "dept_name": "CSE", "username": "alice"},
        {"student_id": "S2", "name": "Bob",    "course_id": ["CS101"],
         "dept_name": "ECE", "username": "bob"},
        {"student_id": "S1", "name": "Alice",  "course_id": ["CS103"],
         "dept_name": "CSE", "username": "alice"},
    ])

    engine = SchemaInfere(
        unique_fields=["student_id", "course_id"],
        global_key="username",
        output_dir=".",
    )
    schema = engine.queue_reader(records)