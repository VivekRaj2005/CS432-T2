"""
QueryExecutor: Wraps query operations with execution tracking and metadata enrichment.
Captures query execution details for audit trail and performance analysis.
"""
from datetime import datetime
from typing import Dict, Any, Optional, List
from uuid import uuid4
import time
from utils.log import logger


class QueryExecution:
    """Represents a single query execution record."""
    
    def __init__(self, query_id: str, session_id: Optional[str] = None):
        self.query_id = query_id
        self.session_id = session_id
        self.entity_name: Optional[str] = None
        self.operation_type: str = "SELECT"  # SELECT, INSERT, UPDATE, DELETE
        self.filters: Dict[str, Any] = {}
        self.started_at = datetime.now()
        self.completed_at: Optional[datetime] = None
        self.execution_ms = 0
        self.result_count = 0
        self.rows_affected = 0
        self.status = "RUNNING"  # RUNNING, SUCCESS, ERROR
        self.error_message: Optional[str] = None
        self.source: str = "UNKNOWN"  # SQL, NOSQL, HYBRID
    
    def mark_complete(self, status: str = "SUCCESS", error: Optional[str] = None) -> None:
        """Mark query as completed."""
        self.completed_at = datetime.now()
        self.execution_ms = int((self.completed_at - self.started_at).total_seconds() * 1000)
        self.status = status
        self.error_message = error
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize execution to dictionary."""
        return {
            "query_id": self.query_id,
            "session_id": self.session_id,
            "entity_name": self.entity_name,
            "operation_type": self.operation_type,
            "filters": self.filters,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_ms": self.execution_ms,
            "result_count": self.result_count,
            "rows_affected": self.rows_affected,
            "status": self.status,
            "error_message": self.error_message,
            "source": self.source,
        }


class QueryExecutor:
    """
    Wraps query operations with execution tracking and history.
    Provides query execution context for audit trails and analytics.
    """
    
    def __init__(self):
        self.executions: Dict[str, QueryExecution] = {}
        self.max_history = 10000  # Keep last N queries in memory
        logger.info("QueryExecutor initialized")
    
    def start_query_execution(
        self,
        session_id: Optional[str] = None,
        entity_name: Optional[str] = None,
        operation_type: str = "SELECT",
        filters: Optional[Dict[str, Any]] = None,
        source: str = "HYBRID",
    ) -> str:
        """
        Start a new query execution context.
        
        Args:
            session_id: Associated session ID
            entity_name: Name of the entity being queried
            operation_type: Type of operation (SELECT, INSERT, UPDATE, DELETE)
            filters: Query filters applied
            source: Data source (SQL, NOSQL, HYBRID)
        
        Returns:
            Query ID for tracking
        """
        query_id = str(uuid4())
        execution = QueryExecution(query_id, session_id)
        execution.entity_name = entity_name
        execution.operation_type = operation_type
        execution.filters = filters or {}
        execution.source = source
        
        self.executions[query_id] = execution
        logger.debug(f"Query {query_id} started: {operation_type} on {entity_name}")
        
        return query_id
    
    def complete_query_execution(
        self,
        query_id: str,
        result_count: int = 0,
        rows_affected: int = 0,
        status: str = "SUCCESS",
        error: Optional[str] = None,
    ) -> Optional[QueryExecution]:
        """
        Mark query execution as complete.
        
        Args:
            query_id: Query ID to complete
            result_count: Number of results returned
            rows_affected: Number of rows modified
            status: Execution status (SUCCESS, ERROR)
            error: Error message if failed
        
        Returns:
            Completed QueryExecution or None
        """
        if query_id not in self.executions:
            logger.warning(f"Query {query_id} not found in tracking")
            return None
        
        execution = self.executions[query_id]
        execution.result_count = result_count
        execution.rows_affected = rows_affected
        execution.mark_complete(status, error)
        
        logger.debug(f"Query {query_id} completed: {status} ({execution.execution_ms}ms)")
        
        # Keep memory bounded
        if len(self.executions) > self.max_history:
            oldest_key = next(iter(self.executions))
            del self.executions[oldest_key]
        
        return execution
    
    def get_execution(self, query_id: str) -> Optional[QueryExecution]:
        """Get execution details for a query."""
        return self.executions.get(query_id)
    
    def get_session_executions(self, session_id: str) -> List[Dict[str, Any]]:
        """Get all executions for a session."""
        return [
            exec.to_dict()
            for exec in self.executions.values()
            if exec.session_id == session_id
        ]
    
    def get_entity_executions(self, entity_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent executions for a specific entity."""
        executions = [
            exec.to_dict()
            for exec in self.executions.values()
            if exec.entity_name == entity_name
        ]
        return sorted(executions, key=lambda x: x["started_at"], reverse=True)[:limit]
    
    def get_all_executions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent query executions."""
        executions = [exec.to_dict() for exec in self.executions.values()]
        return sorted(executions, key=lambda x: x["started_at"], reverse=True)[:limit]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query execution statistics."""
        if not self.executions:
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "avg_execution_ms": 0,
                "operations_breakdown": {},
            }
        
        successful = [e for e in self.executions.values() if e.status == "SUCCESS"]
        failed = [e for e in self.executions.values() if e.status == "ERROR"]
        
        avg_ms = (
            sum(e.execution_ms for e in successful) / len(successful)
            if successful
            else 0
        )
        
        ops = {}
        for exec in self.executions.values():
            ops[exec.operation_type] = ops.get(exec.operation_type, 0) + 1
        
        return {
            "total_queries": len(self.executions),
            "successful_queries": len(successful),
            "failed_queries": len(failed),
            "avg_execution_ms": round(avg_ms, 2),
            "operations_breakdown": ops,
        }
