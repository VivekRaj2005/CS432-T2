"""
QueryHistoryStore: Persists query execution history to JSON Lines format.
Enables audit trails and historical analysis of query patterns.
"""
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from utils.log import logger


class QueryHistoryStore:
    """
    Persistent store for query execution history.
    Uses JSON Lines format (one JSON object per line) for efficient streaming.
    """
    
    def __init__(self, history_dir: str = "logs", filename: str = "query_execution_history.jsonl"):
        """
        Initialize query history store.
        
        Args:
            history_dir: Directory to store history files
            filename: Name of the history file
        """
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.history_dir / filename
        self.max_file_size = 10 * 1024 * 1024  # 10 MB
        logger.info(f"QueryHistoryStore initialized: {self.history_file}")
    
    def log_query(self, execution_data: Dict[str, Any]) -> bool:
        """
        Append a query execution record to history.
        
        Args:
            execution_data: Dictionary from QueryExecution.to_dict()
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure history file exists
            if not self.history_file.exists():
                self.history_file.touch()
            
            # Append to JSON Lines file
            with open(self.history_file, "a") as f:
                f.write(json.dumps(execution_data) + "\n")
            
            # Check if rotation needed
            self._rotate_if_needed()
            
            logger.debug(f"Logged query: {execution_data.get('query_id')}")
            return True
        except Exception as e:
            logger.error(f"Failed to log query: {e}")
            return False
    
    def get_history(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve recent query history.
        
        Args:
            limit: Maximum number of records to return
            offset: Skip first N records
        
        Returns:
            List of query execution records, ordered by most recent first
        """
        if not self.history_file.exists():
            return []
        
        records = []
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to read history: {e}")
            return []
        
        # Reverse to get most recent first
        records.reverse()
        return records[offset : offset + limit]
    
    def get_session_history(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get query history for a specific session.
        
        Args:
            session_id: Session ID to filter by
            limit: Maximum records to return
        
        Returns:
            List of query records for the session
        """
        if not self.history_file.exists():
            return []
        
        records = []
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if record.get("session_id") == session_id:
                            records.append(record)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to read session history: {e}")
            return []
        
        # Reverse to get most recent first
        records.reverse()
        return records[:limit]
    
    def get_entity_history(self, entity_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get query history for a specific entity.
        
        Args:
            entity_name: Entity name to filter by
            limit: Maximum records to return
        
        Returns:
            List of query records for the entity
        """
        if not self.history_file.exists():
            return []
        
        records = []
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if record.get("entity_name") == entity_name:
                            records.append(record)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to read entity history: {e}")
            return []
        
        # Reverse to get most recent first
        records.reverse()
        return records[:limit]
    
    def get_history_by_date_range(
        self, start_date: datetime, end_date: datetime, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get query history within a date range.
        
        Args:
            start_date: Start datetime (inclusive)
            end_date: End datetime (inclusive)
            limit: Maximum records to return
        
        Returns:
            List of query records within date range
        """
        if not self.history_file.exists():
            return []
        
        records = []
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if "started_at" in record:
                            record_date = datetime.fromisoformat(record["started_at"])
                            if start_date <= record_date <= end_date:
                                records.append(record)
                    except (json.JSONDecodeError, ValueError):
                        continue
        except Exception as e:
            logger.error(f"Failed to read date range history: {e}")
            return []
        
        # Reverse to get most recent first
        records.reverse()
        return records[:limit]
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Generate statistics from query history.
        
        Returns:
            Dictionary with aggregated statistics
        """
        if not self.history_file.exists():
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "avg_execution_ms": 0,
                "operations_breakdown": {},
                "entities_queried": [],
            }
        
        stats = {
            "total_queries": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "total_execution_ms": 0,
            "avg_execution_ms": 0,
            "operations_breakdown": {},
            "entities_queried": set(),
        }
        
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        stats["total_queries"] += 1
                        
                        if record.get("status") == "SUCCESS":
                            stats["successful_queries"] += 1
                        elif record.get("status") == "ERROR":
                            stats["failed_queries"] += 1
                        
                        op_type = record.get("operation_type", "UNKNOWN")
                        stats["operations_breakdown"][op_type] = (
                            stats["operations_breakdown"].get(op_type, 0) + 1
                        )
                        
                        stats["total_execution_ms"] += record.get("execution_ms", 0)
                        
                        entity = record.get("entity_name")
                        if entity:
                            stats["entities_queried"].add(entity)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to generate statistics: {e}")
        
        if stats["total_queries"] > 0:
            stats["avg_execution_ms"] = round(stats["total_execution_ms"] / stats["total_queries"], 2)
        
        stats["entities_queried"] = list(stats["entities_queried"])
        del stats["total_execution_ms"]
        
        return stats
    
    def clear_old_records(self, days: int = 30) -> int:
        """
        Remove query history records older than N days.
        
        Args:
            days: Delete records older than this many days
        
        Returns:
            Number of records deleted
        """
        if not self.history_file.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=days)
        records_to_keep = []
        deleted_count = 0
        
        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if "started_at" in record:
                            record_date = datetime.fromisoformat(record["started_at"])
                            if record_date > cutoff_date:
                                records_to_keep.append(record)
                            else:
                                deleted_count += 1
                        else:
                            records_to_keep.append(record)
                    except json.JSONDecodeError:
                        continue
            
            # Rewrite file with kept records
            with open(self.history_file, "w") as f:
                for record in records_to_keep:
                    f.write(json.dumps(record) + "\n")
            
            logger.info(f"Cleared {deleted_count} records older than {days} days")
        except Exception as e:
            logger.error(f"Failed to clear old records: {e}")
        
        return deleted_count
    
    def _rotate_if_needed(self) -> None:
        """Rotate file if it exceeds max size."""
        try:
            file_size = self.history_file.stat().st_size
            if file_size > self.max_file_size:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_file = self.history_dir / f"query_execution_history_{timestamp}.jsonl"
                self.history_file.rename(archive_file)
                logger.info(f"Rotated history file: {archive_file}")
        except Exception as e:
            logger.error(f"Failed to rotate history file: {e}")
