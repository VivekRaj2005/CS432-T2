"""Quick test to verify history_store is working."""
import sys
import asyncio
from pathlib import Path
from utils.query_executor import QueryExecutor, QueryExecution
from utils.query_history_store import QueryHistoryStore


def test_history_store():
    """Test that history store logs and retrieves queries correctly."""
    
    # Create history store
    history_store = QueryHistoryStore(history_dir="logs", filename="query_execution_history.jsonl")
    print(f"Initial records in cache: {len(history_store.records)}")
    
    # Create a fake execution
    exec_data = {
        "query_id": "test-123",
        "session_id": None,
        "entity_name": "User",
        "operation_type": "SELECT",
        "filters": {"limit": 10},
        "started_at": "2026-04-16T10:00:00",
        "completed_at": "2026-04-16T10:00:01",
        "execution_ms": 100,
        "result_count": 5,
        "rows_affected": 0,
        "status": "SUCCESS",
        "error_message": None,
        "source": "HYBRID",
    }
    
    # Log it
    print("\nLogging test query...")
    result = history_store.log_query(exec_data)
    print(f"log_query returned: {result}")
    print(f"Records after logging: {len(history_store.records)}")
    
    # Retrieve it
    print("\nRetrieving history...")
    history = history_store.get_history(limit=10)
    print(f"Retrieved {len(history)} records")
    if history:
        print(f"First record: {history[0]}")
    
    # Check pickle file
    pickle_path = Path("logs") / "query_execution_history.pkl"
    print(f"\nPickle file exists: {pickle_path.exists()}")
    
    if pickle_path.exists():
        print(f"Pickle file size: {pickle_path.stat().st_size} bytes")
    
    # Check JSONL file
    jsonl_path = Path("logs") / "query_execution_history.jsonl"
    print(f"JSONL file exists: {jsonl_path.exists()}")
    
    if jsonl_path.exists():
        print(f"JSONL file size: {jsonl_path.stat().st_size} bytes")
        with open(jsonl_path, "r") as f:
            lines = f.readlines()
            print(f"JSONL line count: {len(lines)}")


if __name__ == "__main__":
    test_history_store()
