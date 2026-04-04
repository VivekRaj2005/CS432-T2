# Logs Directory

This directory contains comprehensive logging output from all modules in the CS432-T2 system.

## Log Files

Each module generates its own log file for easier debugging and monitoring:

### Core Processing
- **scheduler.log** - CRUD operation handling, record processing, async task coordination
  - Record processing pipeline
  - CrudOperationGenerator operations
  - SQL/NoSQL update dispatching
  - Async processor status

- **mapregister.log** - Schema registration and metadata tracking
  - Field classification decisions
  - Storage migration tracking
  - Record request processing
  - Schema manager integration

### Data Storage
- **sql.log** - MySQL/SQL database operations
  - Connection management
  - CREATE/ALTER/INSERT/UPDATE/DELETE executions
  - Primary key tracking
  - Transaction management

- **mongodb.log** - MongoDB/NoSQL database operations
  - Collection management
  - Document upserts and updates
  - Primary key tracking
  - Schema-less operations

### Schema & Classification
- **schema_maker.log** - Automatic schema inference
  - Record ingestion and buffering
  - Entity detection (functional dependencies)
  - Relationship detection (foreign keys, M2M)
  - Schema conflict resolution
  - SQL operation generation

- **schema_manager.log** - Schema management wrapper
  - Schema building coordination
  - Field ownership resolution
  - PK tracking across tables
  - Operation type discrimination

- **classify.log** - Field classification and storage routing
  - Presence ratio tracking
  - Cardinality analysis
  - Stability tracking (ALTER frequency)
  - Length variance detection
  - SQL vs NoSQL classification decisions

### Type & Resolution System
- **resolve.log** - Type resolution and metadata management
  - Type inference from values
  - Type locking and transitions
  - Scalar and list conversions
  - Storage calculation

### Network & Utilities
- **network.log** - Network-related operations
- **logs.log** - Legacy consolidated log (backward compatibility)

## Log Format

Each log entry includes:
- **Level**: DEBUG, INFO, WARNING, ERROR
- **Timestamp**: ISO format with microseconds
- **Module**: Source module name
- **Message**: Detailed log message

Example:
```
[INFO] 2026-04-04 14:32:15.123456 [scheduler] : Processing record 1 with event 'add'
[DEBUG] 2026-04-04 14:32:15.234567 [mapregister] : FieldClassifier: 'student_id' storage changed SQL -> NoSQL
[ERROR] 2026-04-04 14:32:15.345678 [sql] : Error executing SQL INSERT command: Connection refused
```

## Viewing Logs

### Real-time Monitoring
```bash
# Follow scheduler updates
tail -f logs/scheduler.log

# Monitor SQL operations
tail -f logs/sql.log

# Watch schema inference
tail -f logs/schema_maker.log
```

### Analysis
```bash
# Count operations by type in scheduler
grep "Created" logs/scheduler.log | cut -d' ' -f4 | sort | uniq -c

# Find all errors
grep ERROR logs/*.log

# Trace a specific record
grep "record 42" logs/*.log

# Monitor performance
grep "complete" logs/*.log
```

## Log Levels

- **DEBUG**: Detailed operational information (high volume)
- **INFO**: General informational messages (normal operation)
- **WARNING**: Warning messages about potential issues
- **ERROR**: Error conditions and exceptions

## Configuration

Log files are automatically created in this directory when the application starts. No manual configuration required.

The logging system is configured in `utils/log.py` with module-specific loggers accessible via:
```python
from utils.log import scheduler_logger, sql_logger, mongodb_logger, etc.
```
