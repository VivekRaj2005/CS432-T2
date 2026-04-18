"""
QueryHistoryStore: Persists query execution history to JSON Lines format.
Enables audit trails and historical analysis of query patterns.
Includes real-time ingest queue tracking with websocket broadcast capability.
"""
import json
import os
import pickle
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from utils.log import logger


class QueryHistoryStore:
    """
    Persistent store for query execution history.
    Uses JSON Lines format (one JSON object per line) for efficient streaming.
    Tracks ingest queue commands with real-time updates.
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
        
        # In-memory cache for fast access
        self.records: List[Dict[str, Any]] = []
        self.pickle_file = self.history_dir / "query_execution_history.pkl"
        
        # Ingest queue command tracking
        self.ingest_records: List[Dict[str, Any]] = []
        self.ingest_queue_file = self.history_dir / "ingest_queue_history.jsonl"
        self.ingest_pickle_file = self.history_dir / "ingest_queue_history.pkl"
        
        # Websocket clients for real-time broadcast
        self.ingest_subscribers: Set[Any] = set()
        
        self._load_from_pickle()
        self._load_ingest_from_pickle()
        
        logger.info(f"QueryHistoryStore initialized: {self.history_file}")
        logger.info(f"QueryHistoryStore ingest tracking: {len(self.ingest_records)} records")
    
    def _load_from_pickle(self) -> None:
        """Load cached records from pickle file."""
        try:
            if self.pickle_file.exists():
                with open(self.pickle_file, "rb") as f:
                    self.records = pickle.load(f)
                logger.info(f"Loaded {len(self.records)} dashboard query records from cache")
            else:
                self._load_from_jsonl()
        except Exception as e:
            logger.error(f"Failed to load from pickle: {e}")
            self.records = []
    
    def _save_to_pickle(self) -> None:
        """Save cached records to pickle file."""
        try:
            with open(self.pickle_file, "wb") as f:
                pickle.dump(self.records, f)
        except Exception as e:
            logger.error(f"Failed to save to pickle: {e}")
    
    def _load_from_jsonl(self) -> None:
        """Load records from JSONL file into memory cache."""
        if not self.history_file.exists():
            return
        
        try:
            records = []
            with open(self.history_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError:
                        continue
            records.reverse()
            self.records = records
            self._save_to_pickle()
        except Exception as e:
            logger.error(f"Failed to load from JSONL: {e}")
    
    def _load_ingest_from_pickle(self) -> None:
        """Load ingest queue records from pickle file."""
        try:
            if self.ingest_pickle_file.exists():
                with open(self.ingest_pickle_file, "rb") as f:
                    self.ingest_records = pickle.load(f)
                logger.info(f"Loaded {len(self.ingest_records)} ingest records from cache")
            else:
                self._load_ingest_from_jsonl()
        except Exception as e:
            logger.error(f"Failed to load ingest records from pickle: {e}")
            self.ingest_records = []
    
    def _load_ingest_from_jsonl(self) -> None:
        """Load ingest records from JSONL file into memory cache."""
        if not self.ingest_queue_file.exists():
            return
        
        try:
            records = []
            with open(self.ingest_queue_file, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError:
                        continue
            records.reverse()
            self.ingest_records = records
            self._save_ingest_to_pickle()
        except Exception as e:
            logger.error(f"Failed to load ingest records from JSONL: {e}")
    
    def _save_ingest_to_pickle(self) -> None:
        """Save ingest queue records to pickle file."""
        try:
            with open(self.ingest_pickle_file, "wb") as f:
                pickle.dump(self.ingest_records, f)
        except Exception as e:
            logger.error(f"Failed to save ingest records to pickle: {e}")
    
    def register_ingest_subscriber(self, websocket_client: Any) -> None:
        """Register a websocket client to receive ingest updates."""
        self.ingest_subscribers.add(websocket_client)
        logger.debug(f"Registered ingest subscriber. Total: {len(self.ingest_subscribers)}")
    
    def unregister_ingest_subscriber(self, websocket_client: Any) -> None:
        """Unregister a websocket client."""
        self.ingest_subscribers.discard(websocket_client)
        logger.debug(f"Unregistered ingest subscriber. Total: {len(self.ingest_subscribers)}")
    
    async def broadcast_ingest_update(self, update: Dict[str, Any]) -> None:
        """Broadcast ingest status update to all connected websocket clients."""
        if not self.ingest_subscribers:
            return
        
        for client in list(self.ingest_subscribers):
            try:
                await client.send_json({
                    "type": "ingest_update",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "data": update
                })
            except Exception as e:
                logger.debug(f"Failed to broadcast to client: {e}")
                self.ingest_subscribers.discard(client)
    
    def log_query(self, execution_data: Dict[str, Any]) -> bool:
        """
        Append a query execution record to history.
        
        Args:
            execution_data: Dictionary from QueryExecution.to_dict()
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add to in-memory cache
            self.records.insert(0, execution_data)
            if len(self.records) > 10000:
                self.records = self.records[:10000]
            self._save_to_pickle()
            
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
    
    def log_ingest_command(self, event_type: str, data: Dict[str, Any]) -> str:
        """
        Log an ingest queue command (add/update/delete).
        
        Args:
            event_type: Type of event (add, update, delete)
            data: Command data
        
        Returns:
            Command ID for tracking
        """
        try:
            from uuid import uuid4
            command_id = str(uuid4())
            
            record = {
                "command_id": command_id,
                "event_type": event_type,
                "data": data,
                "queued_at": datetime.utcnow().isoformat() + "Z",
                "status": "QUEUED",
                "completed_at": None,
                "execution_ms": 0,
                "error_message": None,
            }
            
            # Add to in-memory cache (most recent first)
            self.ingest_records.insert(0, record)
            if len(self.ingest_records) > 10000:
                self.ingest_records = self.ingest_records[:10000]
            self._save_ingest_to_pickle()
            
            # Ensure ingest file exists
            if not self.ingest_queue_file.exists():
                self.ingest_queue_file.touch()
            
            # Append to JSON Lines file
            with open(self.ingest_queue_file, "a") as f:
                f.write(json.dumps(record) + "\n")
            
            return command_id
        except Exception as e:
            logger.error(f"Failed to log ingest command: {e}")
            return ""
    
    def update_ingest_status(self, command_id: str, status: str, execution_ms: int = 0, error: Optional[str] = None) -> bool:
        """
        Update the status of an ingest command and broadcast to websocket clients.
        
        Args:
            command_id: Command ID from log_ingest_command()
            status: New status (PROCESSING, SUCCESS, ERROR)
            execution_ms: Execution time in milliseconds
            error: Error message if failed
        
        Returns:
            True if updated successfully
        """
        try:
            for record in self.ingest_records:
                if record["command_id"] == command_id:
                    record["status"] = status
                    record["execution_ms"] = execution_ms
                    record["error_message"] = error
                    record["completed_at"] = datetime.utcnow().isoformat() + "Z" if status in ["SUCCESS", "ERROR"] else None
                    
                    # Save updated cache
                    self._save_ingest_to_pickle()
                    
                    # Rewrite ingest file
                    with open(self.ingest_queue_file, "w") as f:
                        for r in reversed(self.ingest_records):
                            f.write(json.dumps(r) + "\n")
                    
                    logger.debug(f"Updated ingest command {command_id} status: {status}")
                    return True
            
            logger.warning(f"Ingest command {command_id} not found")
            return False
        except Exception as e:
            logger.error(f"Failed to update ingest command status: {e}")
            return False
    
    def get_ingest_history(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get ingest queue command history with execution status.
        
        Args:
            limit: Maximum number of records to return
            offset: Skip first N records
        
        Returns:
            List of ingest command records, ordered by most recent first
        """
        return self.ingest_records[offset : offset + limit]
    
    def get_ingest_status_summary(self) -> Dict[str, Any]:
        """
        Get summary of ingest command execution status.
        
        Returns:
            Dictionary with aggregated statistics
        """
        stats = {
            "total_commands": len(self.ingest_records),
            "queued": 0,
            "processing": 0,
            "successful": 0,
            "failed": 0,
            "total_execution_ms": 0,
            "avg_execution_ms": 0,
            "event_type_breakdown": {},
        }
        
        for record in self.ingest_records:
            status = record.get("status", "UNKNOWN")
            if status == "QUEUED":
                stats["queued"] += 1
            elif status == "PROCESSING":
                stats["processing"] += 1
            elif status == "SUCCESS":
                stats["successful"] += 1
            elif status == "ERROR":
                stats["failed"] += 1
            
            event_type = record.get("event_type", "UNKNOWN")
            stats["event_type_breakdown"][event_type] = (
                stats["event_type_breakdown"].get(event_type, 0) + 1
            )
            
            stats["total_execution_ms"] += record.get("execution_ms", 0)
        
        if stats["total_commands"] > 0:
            stats["avg_execution_ms"] = round(stats["total_execution_ms"] / stats["total_commands"], 2)
        
        return stats
    
    def get_history(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve recent query history from cache.
        
        Args:
            limit: Maximum number of records to return
            offset: Skip first N records
        
        Returns:
            List of query execution records, ordered by most recent first
        """
        return self.records[offset : offset + limit]
    
    def get_session_history(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get query history for a specific session.
        
        Args:
            session_id: Session ID to filter by
            limit: Maximum records to return
        
        Returns:
            List of query records for the session
        """
        records = [r for r in self.records if r.get("session_id") == session_id]
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
        records = [r for r in self.records if r.get("entity_name") == entity_name]
        return records[:limit]
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Generate statistics from query history.
        
        Returns:
            Dictionary with aggregated statistics
        """
        stats = {
            "total_queries": len(self.records),
            "successful_queries": 0,
            "failed_queries": 0,
            "total_execution_ms": 0,
            "avg_execution_ms": 0,
            "operations_breakdown": {},
            "entities_queried": set(),
        }
        
        for record in self.records:
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
        
        if stats["total_queries"] > 0:
            stats["avg_execution_ms"] = round(stats["total_execution_ms"] / stats["total_queries"], 2)
        
        stats["entities_queried"] = list(stats["entities_queried"])
        del stats["total_execution_ms"]
        
        return stats
        
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
