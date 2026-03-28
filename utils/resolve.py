from utils.log import logger
from json import loads

from utils.settings import PERSISTANCE, STABILITY

def CheckBool(value):
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        if value.lower() in ['true', '1', 'yes']:
            return True
        elif value.lower() in ['false', '0', 'no']:
            return True
    if isinstance(value, int):
        if value == 0 or value == 1:
            return True
    return False

def ResolveBool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ['true', '1', 'yes']:
            return True
        elif value.lower() in ['false', '0', 'no']:
            return False
    if isinstance(value, int):
        if value == 0:
            return False
        elif value == 1:
            return True
    raise ValueError(f"Cannot convert {value} to bool")

class Metadata:
    
    def __init__(self, type_="UNK", auto = False):
        assert type_ in ["int", "str", "float", "bool", "list", "dict", "UNK"], "Unsupported type"
        assert not auto or type_ == "int", "AUTO can only be set for int type"
        if type_ == "UNK":
            logger.warning("Type UNK is not recommended for Metadata")
        
        self.type = type_
        self.subtype = None
        self.auto = auto
        self.current_value = 0 if auto else None
        self.stabiltiy = 1
        self.persistance = 1
        self.length = [None, None] # for strings and lists
        self.storage = "SQL"
        self.idX = 0
        # Once a field transitions from an established type, lock it permanently.
        self.type_locked = False
        
    
    def __repr__(self):
        if self.type == "list":
            sub = self.subtype if self.subtype else "UNK"
            return f"Metadata(array<{sub}>{', LOCKED' if self.type_locked else ''})\nStorage:{self.storage}"
        return f"Metadata(type={self.type}{', AUTO' if self.auto else ''}{', LOCKED' if self.type_locked else ''})\nStorage:{self.storage}"

    def reset_to_unk(self):
        self.type = "UNK"
        self.subtype = None
        self.auto = False
        self.type_locked = False

    def re_resolve_as_unk(self, value, reason):
        logger.warning(reason)
        self.reset_to_unk()
        return self.resolveValue(value)

    def normalize_list(self, value):
        if isinstance(value, str):
            try:
                value = loads(value)
            except Exception:
                import ast
                value = ast.literal_eval(value)
        if not isinstance(value, list):
            raise ValueError(f"Expected list, got {type(value)}")
        return value

    def convert_scalar(self, target_type, value):
        if target_type == "int":
            if isinstance(value, float) and not value.is_integer():
                raise ValueError(f"Cannot convert non-integer float {value} to int")
            return int(value)
        if target_type == "float":
            return float(value)
        if target_type == "bool":
            return ResolveBool(value)
        if target_type == "str":
            return str(value)
        raise ValueError(f"Unsupported scalar type {target_type}")

    def convert_list(self, subtype, value):
        value = self.normalize_list(value)
        return [self.convert_scalar(subtype, x) for x in value]

    @staticmethod
    def _executer_for_type(type_name):
        if type_name == "dict" or type_name.startswith("list"):
            return "NoSQL"
        return "SQL"

    def try_allowed_transitions(self, value, allowed_targets, queue=None, column_name=None):
        if self.type_locked:
            raise ValueError(
                f"Type for column '{column_name}' is locked as {self.type}; transitions are not allowed"
            )
        last_error = None
        for target in allowed_targets:
            try:
                if target.startswith("list:"):
                    subtype = target.split(":", 1)[1]
                    converted = self.convert_list(subtype, value)
                    old_type = self.type
                    self.type = "list"
                    self.subtype = Metadata(type_=subtype)
                    self.auto = False
                    if old_type != "UNK":
                        self.type_locked = True
                    if queue is not None and column_name is not None:
                        queue.append({
                            "type": "ALTER",
                            "column_name": column_name,
                            "old_type": old_type,
                            "new_type": f"list<{subtype}>",
                            "Executer": "NoSQL"
                        })
                    return converted
                converted = self.convert_scalar(target, value)
                old_type = self.type
                self.type = target
                self.subtype = None
                self.auto = False
                if old_type != "UNK":
                    self.type_locked = True
                if queue is not None and column_name is not None:
                    queue.append({
                        "type": "ALTER",
                        "column_name": column_name,
                        "old_type": old_type,
                        "new_type": target,
                        "Executer": self._executer_for_type(target)
                    })
                return converted
            except Exception as e:
                last_error = e
        raise ValueError(f"Type change not allowed or failed: {last_error}")

    def get_allowed_list_subtypes(self):
        if self.subtype is None:
            return ["int", "float", "bool", "str"]
        if self.subtype.type == "int":
            return ["int", "float", "str"]
        if self.subtype.type == "float":
            return ["float", "str"]
        if self.subtype.type == "bool":
            return ["bool", "int", "float", "str"]
        if self.subtype.type == "str":
            return ["str"]
        return ["int", "float", "bool", "str"]

    def reCalcStorage(self):
        if self.type == "list" or self.type=="dict":
          self.storage = "NoSQL"
        else:
            curPersistence = (self.idX - self.persistance) /self.idX if self.idX > 0 else 1
            curStability = (self.idX - self.stabiltiy) /self.idX if self.idX > 0 else 1
            if curPersistence <= PERSISTANCE:
                self.storage = "NoSQL"
            elif curStability <= STABILITY:
                self.storage = "NoSQL"
            else:
                self.storage = "SQL"
            

    def resolveValue(self, value=None, queue=None, column_name=None):
        self.idX += 1
        try:
            if self.auto:
                self.current_value += 1
                return self.current_value
            if value is None and self.type != "UNK":
                logger.warning(f"No value provided for non-UNK type {self.type}; returning None")
                self.persistance -=1
                return None
            if self.type == "dict":
                if isinstance(value, dict):
                    return value
                if isinstance(value, str):
                    parsed = loads(value)
                    if isinstance(parsed, dict):
                        return parsed
                raise ValueError(f"Cannot convert value to dict: {value}")
            if self.type == "UNK":
                logger.debug("Resolving value for UNK type")
                if value is None:
                    logger.warning("Value is None for UNK type; returning None")
                    self.persistance -= 1
                    return None
                if isinstance(value, bool):
                    self.type = "bool"
                    self.auto = False
                    logger.info("Resolved UNK type to bool based on provided value")
                    return value
                if isinstance(value, int):
                    self.type = "int"
                    self.auto = False
                    logger.info("Resolved UNK type to int based on provided value")
                    return value
                if isinstance(value, float):
                    self.type = "float"
                    self.auto = False
                    logger.info("Resolved UNK type to float based on provided value")
                    return value
                if isinstance(value, list):
                    self.type = "list"
                    self.subtype = Metadata(type_="UNK")
                    return_list_data = []
                    if len(value) > 0:
                        for x in value:
                            return_list_data.append(self.subtype.resolveValue(x))
                    else:
                        logger.warning("Empty list provided for UNK type; unable to determine subtype")
                    logger.info("Resolved UNK type to list based on provided value")
                    return return_list_data
                if isinstance(value, dict):
                    self.type = "dict"
                    self.subtype = None
                    self.auto = False
                    logger.info("Resolved UNK type to dict based on provided value")
                    return value

                logger.warning(f"Unsupported value type {type(value)} for UNK Metadata; Trying all combinations of basic types")
                try:
                    val = int(value)
                    self.type = "int"
                    self.auto = False
                    logger.info("Resolved UNK type to int based on provided value")
                    return val
                except ValueError:
                    logger.debug("Failed to resolve UNK type as int")
                try:
                    val = float(value)
                    self.type = "float"
                    self.auto = False
                    logger.info("Resolved UNK type to float based on provided value")
                    return val
                except ValueError:
                    logger.debug("Failed to resolve UNK type as float")

                if CheckBool(value):
                    value = ResolveBool(value)
                    self.type = "bool"
                    self.auto = False
                    logger.info("Resolved UNK type to bool based on provided value")
                    return value

                try:
                    val = loads(value)
                    if isinstance(val, list):
                        self.type = "list"
                        self.subtype = Metadata(type_="UNK")
                        return_list_data = []
                        if len(val) > 0:
                            for x in val:
                                return_list_data.append(self.subtype.resolveValue(x))
                        else:
                            logger.warning("Empty list provided for UNK type; unable to determine subtype")
                        logger.info("Resolved UNK type to list based on provided value")
                        return return_list_data
                    if isinstance(val, dict):
                        self.type = "dict"
                        self.subtype = None
                        self.auto = False
                        logger.info("Resolved UNK type to dict based on provided value")
                        return val
                except (ValueError, SyntaxError, TypeError):
                    logger.debug("Failed to resolve UNK type as list/dict")

                if isinstance(value, str):
                    self.type = "str"
                    self.auto = False
                    logger.info("Resolved UNK type to str based on provided value")
                    return value

                logger.error(f"Unable to resolve UNK type for value: {value} of type {type(value)}; returning None")
                raise ValueError(f"Unable to resolve UNK type for value: {value} of type {type(value)}")

            elif self.type == "int":
                try:
                    return int(value)
                except (ValueError, TypeError):
                    if self.type_locked:
                        raise ValueError(f"Type is locked as int; cannot resolve value: {value}")
                    logger.warning(
                        f"Type change detected: cannot convert {value} to int; trying allowed transitions"
                    )
                    return self.try_allowed_transitions(
                        value, ["float", "list:int", "list:float", "str"], queue=queue, column_name=column_name
                    )

            elif self.type == "str":
                try:
                    return str(value)
                except (ValueError, TypeError):
                    if self.type_locked:
                        raise ValueError(f"Type is locked as str; cannot resolve value: {value}")
                    logger.warning(
                        f"Type change detected: cannot convert {value} to str; trying allowed transitions"
                    )
                    return self.try_allowed_transitions(value, ["list:str"], queue=queue, column_name=column_name)

            elif self.type == "float":
                try:
                    return float(value)
                except (ValueError, TypeError):
                    if self.type_locked:
                        raise ValueError(f"Type is locked as float; cannot resolve value: {value}")
                    logger.warning(
                        f"Type change detected: cannot convert {value} to float; trying allowed transitions"
                    )
                    return self.try_allowed_transitions(value, ["list:float", "str"], queue=queue, column_name=column_name)

            elif self.type == "bool":
                try:
                    return ResolveBool(value)
                except (ValueError, TypeError):
                    if self.type_locked:
                        raise ValueError(f"Type is locked as bool; cannot resolve value: {value}")
                    logger.warning(
                        f"Type change detected: cannot convert {value} to bool; trying allowed transitions"
                    )
                    return self.try_allowed_transitions(
                        value, ["int", "float", "list:int", "list:float", "str"], queue=queue, column_name=column_name
                    )

            elif self.type == "list":
                try:
                    if self.subtype is None:
                        self.subtype = Metadata(type_="UNK")
                    if self.subtype.type == "UNK":
                        value = self.normalize_list(value)
                        return_list_data = []
                        if len(value) > 0:
                            self.subtype = Metadata(type_="UNK")
                            for x in value:
                                return_list_data.append(self.subtype.resolveValue(x))
                        return return_list_data

                    return self.convert_list(self.subtype.type, value)

                except Exception as e:
                    if self.type_locked:
                        # Keep list type permanent, but coerce scalars into a one-item list.
                        if self.subtype is not None and self.subtype.type != "UNK":
                            if isinstance(value, str):
                                return [value]
                            try:
                                if isinstance(value, list):
                                    return [self.convert_scalar(self.subtype.type, x) for x in value]
                                return [self.convert_scalar(self.subtype.type, value)]
                            except Exception:
                                logger.warning(
                                    "Locked list<%s> received incompatible value %s; using subtype default",
                                    self.subtype.type,
                                    value
                                )
                                default_scalar = {
                                    "int": 0,
                                    "float": 0.0,
                                    "bool": False,
                                    "str": ""
                                }.get(self.subtype.type, None)
                                return [default_scalar]
                        if isinstance(value, list):
                            return value
                        return [value]
                    logger.warning(
                        f"Type change detected while resolving list: {e}; trying allowed subtype transitions"
                    )
                    try:
                        value = self.normalize_list(value)
                        for subtype in self.get_allowed_list_subtypes():
                            try:
                                return_list_data = [self.convert_scalar(subtype, x) for x in value]
                                old_subtype = self.subtype.type if self.subtype else "UNK"
                                self.subtype = Metadata(type_=subtype)
                                if old_subtype != "UNK" and old_subtype != subtype:
                                    self.type_locked = True
                                if queue is not None and column_name is not None and old_subtype != subtype:
                                    queue.append({
                                        "type": "ALTER",
                                        "column_name": column_name,
                                        "old_type": f"list<{old_subtype}>",
                                        "new_type": f"list<{subtype}>",
                                        "Executer": "NoSQL"
                                    })
                                return return_list_data
                            except Exception:
                                continue
                        raise ValueError(f"Cannot convert list elements with allowed subtypes")
                    except Exception as inner:
                        logger.error(f"List resolution failed: {inner}")
                        return [self.try_allowed_transitions(value, ["int", "float", "bool", "str"])]

            else:
                logger.error(f"Unsupported Metadata type {self.type}")
                raise ValueError(f"Unsupported Metadata type {self.type}, {value}")

            return value
        finally:
            self.reCalcStorage()