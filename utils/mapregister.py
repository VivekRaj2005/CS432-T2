import json
import os
from utils.log import logger
from utils.resolve import Metadata
from tabulate import tabulate
from pickle import dumps, loads


class MapRegister:
    def __init__(self, table_name="root", updateOrder=None, save_file_name='map_register.pkl'):
        self.table_name = table_name
        self.map = {"table_autogen_id": Metadata(type_="int", auto=True)}
        self.request_count = 0
        self._created_sql = False
        self._created_nosql = False
        self.save_file_name = save_file_name
        # Emit CREATE placeholders as soon as the register is initialized.
        self._emit_create_placeholders(updateOrder)

    @staticmethod
    def _metadata_type_name(meta: Metadata):
        if meta.type == "list":
            sub = meta.subtype.type if meta.subtype else "UNK"
            return f"list<{sub}>"
        return meta.type

    def _emit_create_placeholders(self, updateOrder):
        if updateOrder is None:
            return
        if not self._created_sql:
            updateOrder.append({
                "type": "CREATE",
                "table_name": self.table_name,
                "columns": ["table_autogen_id"],
                "Executer": "SQL"
            })
            self._created_sql = True
        if not self._created_nosql:
            updateOrder.append({
                "type": "CREATE",
                "table_name": self.table_name,
                "columns": ["table_autogen_id"],
                "Executer": "NoSQL"
            })
            self._created_nosql = True

    def _emit_storage_migration_placeholders(self, updateOrder, column_name, old_storage, new_storage):
        if updateOrder is None or old_storage == new_storage:
            return
        updateOrder.append({
            "type": "ALTER",
            "table_name": self.table_name,
            "column_name": column_name,
            "old_storage": old_storage,
            "new_storage": new_storage,
            "Executer": new_storage
        })
        # Placeholder transfer command to copy historical values to the new storage path.
        updateOrder.append({
            "type": "INSERT",
            "table_name": self.table_name,
            "columns": ["table_autogen_id", column_name],
            "values": ["<TRANSFER_ALL_IDS>", f"<COPY:{old_storage}->{new_storage}:{column_name}>"],
            "Executer": new_storage,
            "migration": True
        })

    def _recalc_all_storages(self, updateOrder=None):
        for key, meta in self.map.items():
            if not isinstance(meta, Metadata) or key == "table_autogen_id":
                continue
            old_storage = meta.storage
            meta.reCalcStorage()
            if old_storage != meta.storage:
                self._emit_storage_migration_placeholders(updateOrder, key, old_storage, meta.storage)

    def _normalize_request(self, request):
        if isinstance(request, str):
            try:
                return json.loads(request)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Unable to parse request payload: {request}") from exc
        if isinstance(request, dict):
            return request
        raise TypeError(f"Unsupported request type: {type(request)}")

    def _coerce_delete_value(self, key, value):
        meta = self.map.get(key)
        if not isinstance(meta, Metadata):
            return value

        if meta.type == "int":
            return meta.convert_scalar("int", value)
        if meta.type == "float":
            return meta.convert_scalar("float", value)
        if meta.type == "bool":
            return meta.convert_scalar("bool", value)
        if meta.type == "str":
            return meta.convert_scalar("str", value)
        if meta.type == "list":
            subtype = meta.subtype.type if meta.subtype and meta.subtype.type != "UNK" else None
            if subtype is None:
                return meta.normalize_list(value) if isinstance(value, str) else value
            return meta.convert_list(subtype, value)
        if meta.type == "dict":
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
        return value

    def __getitem__(self, key):
        return self.map[key]
    
    def __contains__(self, item):
        return item in self.map
    
    def __iter__(self):
        return iter(self.map)

    def ResolveRequest(self, request, updateOrder=None):
        request = self._normalize_request(request)
        self._emit_create_placeholders(updateOrder)
        table_autogen_id = self.map['table_autogen_id'].resolveValue() # Increment the auto ID for each request
        self.request_count += 1
        
        # Collect resolved values for INSERT split by storage path.
        sql_columns = ["table_autogen_id"]
        sql_values = [table_autogen_id]
        nosql_columns = ["table_autogen_id"]
        nosql_values = [table_autogen_id]
        
        for key in request:
            value = request[key]
            # Dict/list are kept as single columns and resolved by Metadata.
            if key in self.map and isinstance(self.map[key], Metadata):
                old_type = self._metadata_type_name(self.map[key])
                old_storage = self.map[key].storage
                resolved_val = self.map[key].resolveValue(value)
                new_type = self._metadata_type_name(self.map[key])
                new_storage = self.map[key].storage

                if old_type != new_type and updateOrder is not None:
                    updateOrder.append({
                        "type": "ALTER",
                        "table_name": self.table_name,
                        "column_name": key,
                        "old_type": old_type,
                        "new_type": new_type,
                        "Executer": new_storage
                    })
                if old_storage != new_storage:
                    self._emit_storage_migration_placeholders(updateOrder, key, old_storage, new_storage)
            else:
                if key in self.map and not isinstance(self.map[key], Metadata):
                    logger.warning(
                        "Column '%s' had legacy nested MapRegister metadata. Replacing with Metadata(UNK).",
                        key
                    )
                self.map[key] = Metadata(type_="UNK")
                resolved_val = self.map[key].resolveValue(value)
                if updateOrder is not None:
                    updateOrder.append({
                        "type": "ALTER",
                        "table_name": self.table_name,
                        "column_name": key,
                        "old_type": None,
                        "new_type": self._metadata_type_name(self.map[key]),
                        "Executer": self.map[key].storage
                    })

            if self.map[key].storage == "NoSQL":
                nosql_columns.append(key)
                nosql_values.append(resolved_val)
            else:
                sql_columns.append(key)
                sql_values.append(resolved_val)

        # Every 1000 requests, recalculate storage paths and emit migration placeholders.
        if self.request_count % 1000 == 0:
            self._recalc_all_storages(updateOrder=updateOrder)
        if self.request_count %100 == 0:
            self.Save(self.save_file_name)
        
        # Emit INSERT placeholders in the existing order after ALTER/CREATE.
        if updateOrder is not None:
            updateOrder.append({
                "type": "INSERT",
                "table_name": self.table_name,
                "columns": sql_columns,
                "values": sql_values,
                "Executer": "SQL"
            })
            updateOrder.append({
                "type": "INSERT",
                "table_name": self.table_name,
                "columns": nosql_columns,
                "values": nosql_values,
                "Executer": "NoSQL"
            })
        
        return table_autogen_id

    def DeleteRequest(self, request, updateOrder=None):
        request = self._normalize_request(request)
        self._emit_create_placeholders(updateOrder)
        self.request_count += 1

        if not request:
            logger.warning("DeleteRequest received an empty request payload; skipping")
            return None

        criteria = {}
        for key, value in request.items():
            criteria[key] = self._coerce_delete_value(key, value)

        if updateOrder is not None:
            for executer in ("SQL", "NoSQL"):
                updateOrder.append({
                    "type": "DELETE",
                    "table_name": self.table_name,
                    "criteria": criteria,
                    "columns": list(criteria.keys()),
                    "values": list(criteria.values()),
                    "Executer": executer
                })

        return criteria

    RemoveRequest = DeleteRequest
    
    def __repr__(self):
        # print("MapRegister __repr__ called; preparing tabulated output")
        # print(f"Current map contents: {self.map}")
        return tabulate([[k, v] for k, v in self.map.items()], headers=["Key", "Metadata"], tablefmt="grid")

    def Save(self, filename=None):
        if filename is None:
            logger.warning("No filename provided for Save; using default 'map_register.pkl'")
            filename = "map_register.pkl"
        state = {
            "map": self.map,
            "request_count": self.request_count,
            "created_sql": self._created_sql,
            "created_nosql": self._created_nosql,
            "table_name": self.table_name
        }
        with open(filename, "wb") as f:
            f.write(dumps(state))
        logger.info(f"MapRegister saved to {filename}")
    
    def Load(self, filename=None):
        if filename is None:
            logger.warning("No filename provided for Load; using default 'map_register.pkl'")
            filename = "map_register.pkl"
        if not os.path.exists(filename):
            logger.error(f"File {filename} does not exist; cannot load MapRegister")
            return
        with open(filename, "rb") as f:
            data = loads(f.read())
        # Backward compatibility for older map-only checkpoints.
        if isinstance(data, dict) and "map" in data:
            self.map = data.get("map", self.map)
            self.request_count = data.get("request_count", self.request_count)
            self._created_sql = data.get("created_sql", self._created_sql)
            self._created_nosql = data.get("created_nosql", self._created_nosql)
            self.table_name = data.get("table_name", self.table_name)
        else:
            self.map = data
        logger.info(f"MapRegister loaded from {filename}")
        logger.info(f"Loaded Data: \n{self}")
        
