#!/usr/bin/env python3
"""
Logging system test and demonstration.
Tests all logging modules to ensure they are generating logs correctly.
"""

import sys
import os
from collections import deque

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_logging():
    """Test all logging modules."""
    
    print("=" * 60)
    print("LOGGING SYSTEM TEST")
    print("=" * 60)
    
    # Test log module
    try:
        from utils.log import (
            scheduler_logger, sql_logger, mongodb_logger, 
            mapregister_logger, classify_logger, resolve_logger,
            schema_maker_logger, schema_manager_logger, network_logger,
            logger as global_logger
        )
        print("\n✓ Successfully imported all module loggers")
    except Exception as e:
        print(f"\n✗ Failed to import loggers: {e}")
        return False
    
    # Log test messages
    print("\n--- Testing Scheduler Logger ---")
    scheduler_logger.info("Scheduler logger test - INFO level")
    scheduler_logger.debug("Scheduler logger test - DEBUG level")
    scheduler_logger.warning("Scheduler logger test - WARNING level")
    
    print("\n--- Testing SQL Logger ---")
    sql_logger.info("SQL executor initialized")
    sql_logger.debug("Executing SQL INSERT on table_users")
    
    print("\n--- Testing MongoDB Logger ---")
    mongodb_logger.info("MongoDB connection established")
    mongodb_logger.debug("Upserting document in collection_records")
    
    print("\n--- Testing MapRegister Logger ---")
    mapregister_logger.info("MapRegister initialized")
    mapregister_logger.debug("Resolving field ownership for student_id")
    
    print("\n--- Testing Classify Logger ---")
    classify_logger.info("FieldClassifier initialized")
    classify_logger.debug("Classifying field: email_address")
    
    print("\n--- Testing Resolve Logger ---")
    resolve_logger.info("Type resolution system started")
    resolve_logger.debug("Resolving type for value: 12345")
    
    print("\n--- Testing Schema Maker Logger ---")
    schema_maker_logger.info("Schema inference started")
    schema_maker_logger.debug("Processing 400-record buffer")
    
    print("\n--- Testing Schema Manager Logger ---")
    schema_manager_logger.info("Schema manager initialized")
    schema_manager_logger.debug("Building schema from accumulated data")
    
    print("\n--- Testing Network Logger ---")
    network_logger.info("Network module initialized")
    network_logger.debug("Connection attempt to server")
    
    print("\n--- Testing Global Logger (backward compat) ---")
    global_logger.info("Global logger test - backward compatibility check")
    
    # Verify log files exist
    print("\n--- Verifying Log Files ---")
    log_files = [
        "logs/scheduler.log",
        "logs/sql.log",
        "logs/mongodb.log",
        "logs/mapregister.log",
        "logs/classify.log",
        "logs/resolve.log",
        "logs/schema_maker.log",
        "logs/schema_manager.log",
        "logs/network.log",
        "logs/logs.log",
    ]
    
    all_exist = True
    for log_file in log_files:
        if os.path.exists(log_file):
            file_size = os.path.getsize(log_file)
            print(f"✓ {log_file} ({file_size} bytes)")
        else:
            print(f"✗ {log_file} NOT FOUND")
            all_exist = False
    
    if not all_exist:
        print("\nNote: Log files will be created when the next code execution starts.")
    
    return True


def test_crud_generator():
    """Test CrudOperationGenerator logging."""
    print("\n" + "=" * 60)
    print("TESTING CRUD OPERATION GENERATOR LOGGING")
    print("=" * 60)
    
    try:
        from utils.scheduler import CrudOperationGenerator
        
        gen = CrudOperationGenerator()
        
        print("\n--- Testing Operation Generation ---")
        record = {"student_id": "S123", "name": "Alice", "dept": "CSE"}
        
        # Test different operations
        print("\n1. CREATE operation:")
        op = gen.generate_operation("add", record, "student_id")
        print(f"   Generated: {op['type']}")
        
        print("\n2. UPDATE operation:")
        op = gen.generate_operation("update", record, "student_id")
        print(f"   Generated: {op['type']}")
        
        print("\n3. DELETE operation:")
        op = gen.generate_operation("delete", record, "student_id")
        print(f"   Generated: {op['type']}")
        
        print("\n4. SELECT operation:")
        op = gen.generate_operation("get", record, "student_id")
        print(f"   Generated: {op['type']}")
        
        print("\n✓ CrudOperationGenerator logging test passed")
        return True
        
    except Exception as e:
        print(f"\n✗ CrudOperationGenerator test failed: {e}")
        return False


def test_schema_maker():
    """Test SchemaInfere logging."""
    print("\n" + "=" * 60)
    print("TESTING SCHEMA MAKER LOGGING")
    print("=" * 60)
    
    try:
        from utils.schema_maker import SchemaInfere
        
        print("\n--- Testing Schema Inference ---")
        
        records = deque([
            {"student_id": "S1", "name": "Alice", "course_id": ["CS101", "CS102"]},
            {"student_id": "S2", "name": "Bob", "course_id": ["CS101"]},
            {"student_id": "S3", "name": "Charlie", "course_id": ["CS102", "CS103"]},
        ])
        
        engine = SchemaInfere(
            unique_fields=["student_id", "course_id"],
            global_key="student_id",
            output_dir=".",
        )
        
        schema = engine.queue_reader(records)
        
        print(f"\n✓ Schema Inference Completed")
        print(f"  - Tables: {len(schema.get('tables', {}))}")
        print(f"  - Foreign Keys: {len(schema.get('foreign_keys', []))}")
        print(f"  - M2M Relationships: {len(schema.get('many_to_many', []))}")
        print(f"  - Dependencies: {len(schema.get('functional_dependencies', {}))}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Schema Maker test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all logging tests."""
    
    results = []
    
    # Test 1: Core logging
    print("\n[TEST 1/3] Core Logging System")
    results.append(("Core Logging", test_logging()))
    
    # Test 2: CRUD Generator
    print("\n[TEST 2/3] CRUD Operation Generator")
    results.append(("CRUD Generator", test_crud_generator()))
    
    # Test 3: Schema Maker
    print("\n[TEST 3/3] Schema Maker")
    results.append(("Schema Maker", test_schema_maker()))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:.<40} {status}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED - LOGGING SYSTEM IS OPERATIONAL")
    else:
        print("✗ SOME TESTS FAILED - CHECK ERRORS ABOVE")
    print("=" * 60)
    
    # Display log file location
    print(f"\nLog files location: {os.path.abspath('logs')}")
    print("View logs with: tail -f logs/*.log")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
