import json
import os
from typing import Dict, Tuple, Optional
from utils.log import mapregister_logger as logger
from utils.resolve import Metadata
from utils.Classify import FieldClassifier
from tabulate import tabulate
from pickle import dumps, loads


class MapRegister:
    def __init__(self, table_name="root", updateOrder=None, save_file_name='map_register.pkl', schema_manager=None):
        self.table_name = table_name
        self.map = {"table_autogen_id": Metadata(type_="int", auto=True)}
        # Child registers per nested field for depth-1 nested data persisted in SQL.
        self.nested_registers = {}
        # Foreign-key reference metadata per nested field.
        self.foreign_key_refs = {}
        self.request_count = 0
        self._created_sql = False
        self._created_nosql = False
        self.save_file_name = save_file_name
        # Initialize the field classifier for dynamic SQL/NoSQL decisions
        self.field_classifier = FieldClassifier()
        # Optional schema manager for schema-aware operations and conflict resolution
        self.schema_manager = schema_manager
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
        column_meta = self.map.get(column_name)
        column_data_type = "UNK"
        if isinstance(column_meta, Metadata):
            column_data_type = self._metadata_type_name(column_meta)

        updateOrder.append({
            "type": "ALTER",
            "table_name": self.table_name,
            "column_name": column_name,
            "old_storage": old_storage,
            "new_storage": new_storage,
            "new_type": column_data_type,
            "Executer": new_storage
        })
        # Placeholder transfer command to copy historical values to the new storage path.
        updateOrder.append({
            "type": "INSERT",
            "table_name": self.table_name,
            "columns": ["table_autogen_id", column_name],
            "values": ["<TRANSFER_ALL_IDS>", f"<COPY:{old_storage}->{new_storage}:{column_name}>"],
            "column_data_type": column_data_type,
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

    def _recalc_sql_storages(self, updateOrder=None):
        for key, meta in self.map.items():
            if not isinstance(meta, Metadata) or key == "table_autogen_id":
                continue
            if meta.storage != "SQL":
                continue
            old_storage = meta.storage
            meta.reCalcStorage()
            if old_storage != meta.storage:
                self._emit_storage_migration_placeholders(updateOrder, key, old_storage, meta.storage)

    def _apply_classifier_storage_decisions(self, classifications: Dict[str, str], updateOrder=None) -> None:
        """
        Apply classifier decisions to update field storage paths.
        Overrides the default storage calculation with classifier-based decisions.
        
        Args:
            classifications: Dict mapping field names to "sql" or "mongodb"
            updateOrder: Optional list to append migration commands to
        """
        for field, classified_storage in classifications.items():
            if field not in self.map or not isinstance(self.map[field], Metadata):
                continue
            
            meta = self.map[field]
            # Map classifier output ("sql"/"mongodb") to our storage terminology ("SQL"/"NoSQL")
            new_storage = "SQL" if classified_storage == "sql" else "NoSQL"
            old_storage = meta.storage
            
            if old_storage != new_storage:
                logger.info(
                    "FieldClassifier: '%s' storage changed %s -> %s",
                    field, old_storage, new_storage
                )
                meta.storage = new_storage
                self._emit_storage_migration_placeholders(updateOrder, field, old_storage, new_storage)

    def _recalculate_field_classifiers(self, updateOrder=None) -> None:
        """
        Periodically recompute classifier decisions and apply resulting storage updates.
        """
        if self.request_count % 10 != 0:
            return
        classifications = self.field_classifier.recalculate_classifications()
        self._apply_classifier_storage_decisions(classifications, updateOrder)

    @staticmethod
    def _nesting_depth(value) -> int:
        if isinstance(value, dict):
            if not value:
                return 1
            return 1 + max(MapRegister._nesting_depth(v) for v in value.values())
        if isinstance(value, list):
            if not value:
                return 1
            return 1 + max(MapRegister._nesting_depth(v) for v in value)
        return 0

    def _is_sql_shallow_nested(self, value) -> bool:
        return isinstance(value, (dict, list)) and self._nesting_depth(value) <= 1

    def _child_table_name(self, field_name: str) -> str:
        return f"{self.table_name}__{field_name}"

    @staticmethod
    def _foreign_key_field_name(field_name: str) -> str:
        return f"{field_name}_fk"

    def _prune_legacy_fk_fields(self):
        known_fk_fields = {
            ref.get("fk_field")
            for ref in self.foreign_key_refs.values()
            if isinstance(ref, dict) and isinstance(ref.get("fk_field"), str)
        }
        stale_keys = [
            key for key in self.map.keys()
            if key != "table_autogen_id" and key.endswith("_fk")
        ]
        for key in stale_keys:
            if key in known_fk_fields or not known_fk_fields:
                self.map.pop(key, None)

    def _get_or_create_nested_register(self, field_name: str, updateOrder=None):
        child_table = self._child_table_name(field_name)
        register = self.nested_registers.get(field_name)
        if register is None:
            register = MapRegister(
                table_name=child_table,
                updateOrder=None,
                save_file_name=f"{self.save_file_name}.{field_name}",
                schema_manager=self.schema_manager,
            )
            self.nested_registers[field_name] = register
        register._emit_create_placeholders(updateOrder)
        return register

    @staticmethod
    def _fk_storage_from_classifier(classified_storage: str, fallback: str = "SQL") -> str:
        if classified_storage == "sql":
            return "SQL"
        if classified_storage == "mongodb":
            return "NoSQL"
        return fallback

    def _ensure_fk_reference(self, field_name: str, parent_id, updateOrder=None):
        fk_field = self._foreign_key_field_name(field_name)
        child_table = self._child_table_name(field_name)

        classified = self.field_classifier.classify_record({fk_field: parent_id})
        classified_storage = classified.get(fk_field) or self.field_classifier.get_classification(fk_field)
        desired_storage = self._fk_storage_from_classifier(classified_storage, fallback="SQL")

        existing = self.foreign_key_refs.get(field_name)
        old_storage = existing.get("storage") if isinstance(existing, dict) else None
        if existing is None:
            self.foreign_key_refs[field_name] = {
                "fk_field": fk_field,
                "child_table": child_table,
                "storage": desired_storage,
            }
            if updateOrder is not None:
                updateOrder.append({
                    "type": "ALTER",
                    "table_name": self.table_name,
                    "column_name": fk_field,
                    "old_type": None,
                    "new_type": "int",
                    "Executer": desired_storage,
                })
        else:
            existing["fk_field"] = fk_field
            existing["child_table"] = child_table
            existing["storage"] = desired_storage
            if old_storage and old_storage != desired_storage and updateOrder is not None:
                updateOrder.append({
                    "type": "ALTER",
                    "table_name": self.table_name,
                    "column_name": fk_field,
                    "old_storage": old_storage,
                    "new_storage": desired_storage,
                    "new_type": "int",
                    "Executer": desired_storage,
                })
                updateOrder.append({
                    "type": "INSERT",
                    "table_name": self.table_name,
                    "columns": ["table_autogen_id", fk_field],
                    "values": ["<TRANSFER_ALL_IDS>", f"<COPY:{old_storage}->{desired_storage}:{fk_field}>"],
                    "column_data_type": "int",
                    "Executer": desired_storage,
                    "migration": True,
                })

        return fk_field, desired_storage

    def _resolve_child_payload_split(self, register, payload, updateOrder=None):
        classifications = register.field_classifier.classify_record(payload)
        register._apply_classifier_storage_decisions(classifications, updateOrder)
        if updateOrder is not None:
            register.field_classifier.ingest_alter_events(updateOrder)
        register.request_count += 1
        register._recalculate_field_classifiers(updateOrder)

        sql_updates = {}
        nosql_updates = {}
        for nested_key, nested_value in payload.items():
            if nested_key in register.map and isinstance(register.map[nested_key], Metadata):
                old_type = self._metadata_type_name(register.map[nested_key])
                old_storage = register.map[nested_key].storage
                resolved_val = register.map[nested_key].resolveValue(nested_value)
                new_type = self._metadata_type_name(register.map[nested_key])
                new_storage = register.map[nested_key].storage
                if old_type != new_type and updateOrder is not None:
                    updateOrder.append({
                        "type": "ALTER",
                        "table_name": register.table_name,
                        "column_name": nested_key,
                        "old_type": old_type,
                        "new_type": new_type,
                        "Executer": new_storage
                    })
                if old_storage != new_storage:
                    register._emit_storage_migration_placeholders(updateOrder, nested_key, old_storage, new_storage)
            else:
                register.map[nested_key] = Metadata(type_="UNK")
                resolved_val = register.map[nested_key].resolveValue(nested_value)
                if updateOrder is not None:
                    updateOrder.append({
                        "type": "ALTER",
                        "table_name": register.table_name,
                        "column_name": nested_key,
                        "old_type": None,
                        "new_type": self._metadata_type_name(register.map[nested_key]),
                        "Executer": register.map[nested_key].storage
                    })

            if register.map[nested_key].storage == "NoSQL":
                nosql_updates[nested_key] = resolved_val
            else:
                sql_updates[nested_key] = resolved_val

        if register.request_count % 1000 == 0:
            register._recalc_all_storages(updateOrder=updateOrder)
        if register.request_count % 100 == 0:
            register.Save(register.save_file_name)

        return sql_updates, nosql_updates

    def _store_shallow_nested_insert(self, field_name: str, value, parent_id, updateOrder=None):
        payload = value if isinstance(value, dict) else {"value": value}
        register = self._get_or_create_nested_register(field_name, updateOrder)
        sql_updates, nosql_updates = self._resolve_child_payload_split(register, payload, updateOrder)
        if updateOrder is not None:
            if sql_updates:
                columns = ["table_autogen_id"] + list(sql_updates.keys())
                values = [parent_id] + list(sql_updates.values())
                updateOrder.append({
                    "type": "INSERT",
                    "table_name": register.table_name,
                    "columns": columns,
                    "values": values,
                    "Executer": "SQL"
                })
            if nosql_updates:
                columns = ["table_autogen_id"] + list(nosql_updates.keys())
                values = [parent_id] + list(nosql_updates.values())
                updateOrder.append({
                    "type": "INSERT",
                    "table_name": register.table_name,
                    "columns": columns,
                    "values": values,
                    "Executer": "NoSQL"
                })

    def _store_shallow_nested_update(self, field_name: str, value, parent_criteria, updateOrder=None) -> bool:
        parent_id = parent_criteria.get("table_autogen_id") if isinstance(parent_criteria, dict) else None
        if parent_id is None:
            logger.warning(
                "Skipping shallow nested SQL update for '%s': criteria missing table_autogen_id",
                field_name
            )
            return False

        payload = value if isinstance(value, dict) else {"value": value}
        register = self._get_or_create_nested_register(field_name, updateOrder)
        sql_updates, nosql_updates = self._resolve_child_payload_split(register, payload, updateOrder)
        if updateOrder is not None:
            if sql_updates:
                updateOrder.append({
                    "type": "UPDATE",
                    "table_name": register.table_name,
                    "criteria": {"table_autogen_id": parent_id},
                    "set_fields": sql_updates,
                    "Executer": "SQL"
                })
            if nosql_updates:
                updateOrder.append({
                    "type": "UPDATE",
                    "table_name": register.table_name,
                    "criteria": {"table_autogen_id": parent_id},
                    "set_fields": nosql_updates,
                    "Executer": "NoSQL"
                })
        return True

    def _resolve_field_value(self, key, value, updateOrder=None):
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

        return resolved_val, self.map[key].storage

    def ingest_into_schema(self, record: Dict[str, any]) -> None:
        """
        Optionally ingest a record into the schema manager for schema inference.
        
        Args:
            record: Record to ingest
        """
        if self.schema_manager:
            self.schema_manager.ingest_record(record)

    def resolve_field_ownership(self, field: str) -> Optional[str]:
        """
        Resolve which table a field belongs to using schema manager.
        
        Args:
            field: Field name
            
        Returns:
            Table name or None if unresolved
        """
        if self.schema_manager:
            return self.schema_manager.get_table_for_field(field)
        return None

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

    @staticmethod
    def _is_identifier_key(key):
        return key in {"table_autogen_id", "record_id", "dept_name"} or key.endswith("_id")

    def _split_update_payload(self, request: Dict) -> Tuple[Dict, Dict]:
        criteria = {}
        updates = {}

        criteria_payload = request.get("criteria") or request.get("filter") or request.get("where")
        updates_payload = request.get("set") or request.get("updates") or request.get("changes") or request.get("values")

        if isinstance(criteria_payload, dict):
            criteria = dict(criteria_payload)
        if isinstance(updates_payload, dict):
            updates = dict(updates_payload)

        if criteria or updates:
            if not updates:
                reserved = {"criteria", "filter", "where", "set", "updates", "changes", "values"}
                updates = {k: v for k, v in request.items() if k not in reserved}
            return criteria, updates

        id_keys = [k for k in request.keys() if self._is_identifier_key(k)]
        if id_keys:
            criteria = {k: request[k] for k in id_keys}
            updates = {k: v for k, v in request.items() if k not in criteria}

        return criteria, updates

    def __getitem__(self, key):
        return self.map[key]
    
    def __contains__(self, item):
        return item in self.map
    
    def __iter__(self):
        return iter(self.map)

    def ResolveRequest(self, request, updateOrder=None):
        request = self._normalize_request(request)
        self._emit_create_placeholders(updateOrder)
        
        # Optionally ingest into schema manager for schema inference
        self.ingest_into_schema(request)
        
        table_autogen_id = self.map['table_autogen_id'].resolveValue() # Increment the auto ID for each request
        self.request_count += 1
        
        # Classify the record to determine SQL vs NoSQL storage for each field
        classifications = self.field_classifier.classify_record(request)
        self._apply_classifier_storage_decisions(classifications, updateOrder)
        
        # Feed any ALTER events from prior requests to the classifier for stability tracking
        if updateOrder is not None:
            self.field_classifier.ingest_alter_events(updateOrder)

        self._recalculate_field_classifiers(updateOrder)
        
        # Collect resolved values for INSERT split by storage path.
        sql_columns = ["table_autogen_id"]
        sql_values = [table_autogen_id]
        nosql_columns = ["table_autogen_id"]
        nosql_values = [table_autogen_id]
        
        for key in request:
            value = request[key]
            if self._is_sql_shallow_nested(value):
                self._store_shallow_nested_insert(key, value, table_autogen_id, updateOrder)
                fk_field, fk_storage = self._ensure_fk_reference(key, table_autogen_id, updateOrder)
                fk_value = table_autogen_id
                if fk_storage == "NoSQL":
                    nosql_columns.append(fk_field)
                    nosql_values.append(fk_value)
                else:
                    sql_columns.append(fk_field)
                    sql_values.append(fk_value)
                continue
            resolved_val, storage = self._resolve_field_value(key, value, updateOrder)

            if storage == "NoSQL":
                nosql_columns.append(key)
                nosql_values.append(resolved_val)
            else:
                sql_columns.append(key)
                sql_values.append(resolved_val)

        # Recalculate storage each ResolveRequest for fields currently on SQL.
        self._recalc_sql_storages(updateOrder=updateOrder)

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
        self._recalculate_field_classifiers(updateOrder)

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

            # Cascade delete shallow nested SQL rows by shared table_autogen_id.
            parent_id = criteria.get("table_autogen_id")
            if parent_id is not None:
                for nested_field, nested_register in self.nested_registers.items():
                    nested_register._emit_create_placeholders(updateOrder)
                    for executer in ("SQL", "NoSQL"):
                        updateOrder.append({
                            "type": "DELETE",
                            "table_name": nested_register.table_name,
                            "criteria": {"table_autogen_id": parent_id},
                            "columns": ["table_autogen_id"],
                            "values": [parent_id],
                            "Executer": executer
                        })

        # Recalculate storage after every delete so remaining SQL/NoSQL assignments stay current.
        self._recalc_all_storages(updateOrder=updateOrder)

        return criteria

    def UpdateRequest(self, request, updateOrder=None):
        request = self._normalize_request(request)
        self._emit_create_placeholders(updateOrder)
        self.request_count += 1

        if not request:
            logger.warning("UpdateRequest received an empty request payload; skipping")
            return None

        criteria, updates = self._split_update_payload(request)
        if not criteria:
            logger.warning("UpdateRequest could not infer criteria; expected identifier fields or explicit criteria")
            return None
        if not updates:
            logger.warning("UpdateRequest has no fields to update; skipping")
            return {"criteria": criteria, "updates": updates}

        # Optionally ingest into schema manager for schema inference
        self.ingest_into_schema(updates)
        
        # Classify the update payload to determine storage for updated fields
        classifications = self.field_classifier.classify_record(updates)
        self._apply_classifier_storage_decisions(classifications, updateOrder)
        
        # Feed any ALTER events from prior requests to the classifier for stability tracking
        if updateOrder is not None:
            self.field_classifier.ingest_alter_events(updateOrder)

        self._recalculate_field_classifiers(updateOrder)

        resolved_criteria = {}
        for key, value in criteria.items():
            resolved_criteria[key] = self._coerce_delete_value(key, value)

        sql_updates = {}
        nosql_updates = {}

        for key, value in updates.items():
            if self._is_sql_shallow_nested(value):
                updated_nested = self._store_shallow_nested_update(
                    key,
                    value,
                    resolved_criteria,
                    updateOrder
                )
                if updated_nested:
                    parent_id = resolved_criteria.get("table_autogen_id")
                    fk_field, fk_storage = self._ensure_fk_reference(key, parent_id, updateOrder)
                    fk_value = parent_id
                    if fk_storage == "NoSQL":
                        nosql_updates[fk_field] = fk_value
                    else:
                        sql_updates[fk_field] = fk_value
                    continue

            resolved_val, storage = self._resolve_field_value(key, value, updateOrder)

            if storage == "NoSQL":
                nosql_updates[key] = resolved_val
            else:
                sql_updates[key] = resolved_val

        if self.request_count % 1000 == 0:
            self._recalc_all_storages(updateOrder=updateOrder)
        if self.request_count % 100 == 0:
            self.Save(self.save_file_name)

        if updateOrder is not None:
            if sql_updates:
                updateOrder.append({
                    "type": "UPDATE",
                    "table_name": self.table_name,
                    "criteria": resolved_criteria,
                    "set_fields": sql_updates,
                    "Executer": "SQL"
                })
            if nosql_updates:
                updateOrder.append({
                    "type": "UPDATE",
                    "table_name": self.table_name,
                    "criteria": resolved_criteria,
                    "set_fields": nosql_updates,
                    "Executer": "NoSQL"
                })

        return {"criteria": resolved_criteria, "updates": {**sql_updates, **nosql_updates}}

    RemoveRequest = DeleteRequest
    ChangeRequest = UpdateRequest
    
    def get_field_classifications(self) -> Dict[str, str]:
        """Get all field classifications from the FieldClassifier."""
        return self.field_classifier.classifications.copy()
    
    def get_cardinality_report(self) -> list:
        """Get cardinality analysis report from FieldClassifier."""
        return self.field_classifier.cardinality_report()
    
    def get_stability_report(self) -> list:
        """Get stability analysis report from FieldClassifier."""
        return self.field_classifier.stability_report()
    
    def get_length_variance_report(self) -> list:
        """Get length variance analysis report from FieldClassifier."""
        return self.field_classifier.length_variance_report()
    
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
            "nested_registers": self.nested_registers,
            "foreign_key_refs": self.foreign_key_refs,
            "request_count": self.request_count,
            "created_sql": self._created_sql,
            "created_nosql": self._created_nosql,
            "table_name": self.table_name,
            "field_classifier": self.field_classifier,
            "schema_manager": self.schema_manager
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
            self.nested_registers = data.get("nested_registers", self.nested_registers)
            self.foreign_key_refs = data.get("foreign_key_refs", self.foreign_key_refs)
            self._prune_legacy_fk_fields()
            self.request_count = data.get("request_count", self.request_count)
            self._created_sql = data.get("created_sql", self._created_sql)
            self._created_nosql = data.get("created_nosql", self._created_nosql)
            self.table_name = data.get("table_name", self.table_name)
            # Restore field classifier if available, otherwise use fresh instance
            self.field_classifier = data.get("field_classifier", FieldClassifier())
            # Restore schema manager if available
            if "schema_manager" in data and data["schema_manager"]:
                self.schema_manager = data["schema_manager"]
        else:
            self.map = data
        logger.info(f"MapRegister loaded from {filename}")
        logger.info(f"Loaded Data: \n{self}")
        
