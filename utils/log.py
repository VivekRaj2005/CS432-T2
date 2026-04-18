import logging
import os
from datetime import datetime

# Create logs directory if it doesn't exist
LOGS_DIR = "logs"
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# Global logger (deprecated, kept for backward compatibility)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s')

file_handler = logging.FileHandler(os.path.join(LOGS_DIR, 'logs.log'))
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_module_logger(module_name: str) -> logging.Logger:
    """
    Get a module-specific logger that writes to a separate log file.
    
    Args:
        module_name: Name of the module (e.g., 'scheduler', 'sql', 'mongodb')
        
    Returns:
        Configured logger instance
    """
    log_filename = os.path.join(LOGS_DIR, f"{module_name}.log")
    module_logger = logging.getLogger(module_name)
    module_logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers to avoid duplicates
    module_logger.handlers = []
    
    # File handler for module-specific log
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        f'[%(levelname)s] %(asctime)s [{module_name}] : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    module_logger.addHandler(file_handler)
    
    # Console handler for stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        f'[%(levelname)s] [{module_name}] : %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    module_logger.addHandler(console_handler)
    
    # Prevent propagation to root logger to avoid duplication
    module_logger.propagate = False
    
    return module_logger


# Pre-configured loggers for each module
scheduler_logger = get_module_logger("scheduler")
sql_logger = get_module_logger("sql")
mongodb_logger = get_module_logger("mongodb")
mapregister_logger = get_module_logger("mapregister")
classify_logger = get_module_logger("classify")
resolve_logger = get_module_logger("resolve")
schema_maker_logger = get_module_logger("schema_maker")
schema_manager_logger = get_module_logger("schema_manager")
network_logger = get_module_logger("network")