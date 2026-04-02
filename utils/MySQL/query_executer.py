
import json
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Tuple, Any
from datetime import datetime


class MySQLQueryExecutor:
    """Execute generated MySQL queries on actual database."""

    def __init__(self, host: str, user: str, password: str, database: str):
        """Initialize database connection."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        self.execution_log = []
        self.errors = []

    def connect(self) -> bool:
        """Establish MySQL connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"Connected to MySQL database '{self.database}'\n")
            return True
        except Error as e:
            self.errors.append(f"Connection failed: {e}")
            print(f"Connection failed: {e}\n")
            return False

    def disconnect(self):
        """Close MySQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from MySQL\n")

    def execute_generated_queries(self, generated_file: str, stop_on_error: bool = False) -> Dict:
        """
        Execute all generated queries from JSON file.
        
        Args:
            generated_file: Path to generated_queries.json
            stop_on_error: Stop execution on first error
        
        Returns:
            Execution report dictionary
        """
        print("\n" + "="*70)
        print("EXECUTING GENERATED QUERIES")
        print("="*70 + "\n")
        
        try:
            with open(generated_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            self.errors.append(f"Failed to load {generated_file}: {e}")
            return self._generate_report()

        queries = data.get('generated_queries', [])
        executed = 0
        failed = 0

        for idx, query_obj in enumerate(queries, 1):
            query_type = query_obj.get('type')
            entity = query_obj.get('entity')
            sql = query_obj.get('sql')
            description = query_obj.get('description', '')

            print(f"[{idx}/{len(queries)}] {query_type.upper():<6} | {entity:<12} | ", end="")

            try:
                self.cursor.execute(sql)
                
                # Fetch results for SELECT queries
                result = None
                if query_type == 'select':
                    result = self.cursor.fetchall()
                    rows_affected = len(result)
                    print(f"{rows_affected} row(s) saved")
                else:
                    rows_affected = self.cursor.rowcount
                    self.connection.commit()
                    print(f"{rows_affected} row(s) affected")
                
                executed += 1
                self.execution_log.append({
                    "index": idx,
                    "type": query_type,
                    "entity": entity,
                    "sql": sql,
                    "status": "success",
                    "rows_affected": rows_affected,
                    "result": result if query_type == 'select' else None
                })

            except Error as e:
                failed += 1
                error_msg = str(e)
                print(f"✗ ERROR: {error_msg}")

                self.errors.append(f"Query {idx}: {error_msg}")
                self.execution_log.append({
                    "index": idx,
                    "type": query_type,
                    "entity": entity,
                    "sql": sql,
                    "status": "failed",
                    "error": error_msg
                })

                if stop_on_error:
                    print(f"\n Execution stopped at error.\n")
                    break

        print(f"\n{'='*70}")
        print(f"EXECUTION COMPLETE: {executed} succeeded, {failed} failed")
        print(f"{'='*70}\n")

        return self._generate_report()

    def _generate_report(self) -> Dict:
        """Generate execution report."""
        return {
            "executed_at": datetime.now().isoformat(),
            "total_queries": len(self.execution_log),
            "succeeded": len([q for q in self.execution_log if q.get('status') == 'success']),
            "failed": len([q for q in self.execution_log if q.get('status') == 'failed']),
            "errors": self.errors,
            "execution_log": self.execution_log
        }

    def save_execution_report(self, output_file: str = "execution_report.json"):
        """Save execution report to JSON file."""
        report = self._generate_report()
        try:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"Execution report saved to: {output_file}\n")
            return True
        except Exception as e:
            print(f"Failed to save report: {e}\n")
            return False

    def save_query_results(self, output_file: str = "query_results.json"):
        """
        Save all query results to a JSON file.
        This extracts just the data/results from queries, organized by entity.
        """
        results_by_entity = {}
        
        # Organize results by entity type
        for log_entry in self.execution_log:
            entity = log_entry.get('entity', 'unknown')
            query_type = log_entry.get('type')
            
            if entity not in results_by_entity:
                results_by_entity[entity] = {
                    "select_results": [],
                    "other_operations": []
                }
            
            if query_type == 'select':
                # Save SELECT query results
                results_by_entity[entity]["select_results"].append({
                    "index": log_entry.get('index'),
                    "sql": log_entry.get('sql'),
                    "status": log_entry.get('status'),
                    "rows_found": log_entry.get('rows_affected'),
                    "data": log_entry.get('result', [])
                })
            else:
                # Save INSERT/UPDATE/DELETE operations
                results_by_entity[entity]["other_operations"].append({
                    "index": log_entry.get('index'),
                    "type": query_type.upper(),
                    "sql": log_entry.get('sql'),
                    "status": log_entry.get('status'),
                    "rows_affected": log_entry.get('rows_affected'),
                    "error": log_entry.get('error')
                })
        
        # Create final output structure
        output_data = {
            "saved_at": datetime.now().isoformat(),
            "total_queries": len(self.execution_log),
            "results_by_entity": results_by_entity
        }
        
        try:
            with open(output_file, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
            print(f"Query results saved to: {output_file}\n")
            return True
        except Exception as e:
            print(f"Failed to save results: {e}\n")
            return False


def main():
    """Execute generated queries."""
    # Configure MySQL credentials
    executor = MySQLQueryExecutor(
        host='localhost',      # Change as needed
        user='root',           # Change as needed
        password='p',   # Change as needed
        database='university'  # Change as needed
    )

    if not executor.connect():
        return

    try:
        # Execute generated queries
        executor.execute_generated_queries(
            'generated_queries.json',
            stop_on_error=False  # Set to True to stop on first error
        )

        # Save execution report
        executor.save_execution_report('execution_report.json')
        
        # Save query results to JSON file
        executor.save_query_results('query_results.json')

    except Exception as e:
        print(f"Error: {e}\n")
    finally:
        executor.disconnect()


if __name__ == "__main__":
    main()
