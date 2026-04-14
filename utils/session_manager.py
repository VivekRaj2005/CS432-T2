"""
SessionManager: Tracks active user sessions and entity access patterns.
Provides session lifecycle management and query tracking context.
"""
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, Any
from uuid import uuid4
import json
import asyncio
from utils.log import logger


class Session:
    """Represents a user session."""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = datetime.now()
        self.last_activity = datetime.now()
        self.entities_accessed: Set[str] = set()
        self.query_count = 0
        self.is_active = True
    
    def mark_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.now()
    
    def add_entity_access(self, entity_name: str) -> None:
        """Record that an entity was accessed."""
        self.entities_accessed.add(entity_name)
        self.mark_activity()
    
    def increment_query_count(self) -> None:
        """Increment query count."""
        self.query_count += 1
        self.mark_activity()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize session to dictionary."""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "entities_accessed": list(self.entities_accessed),
            "query_count": self.query_count,
            "is_active": self.is_active,
            "duration_seconds": (self.last_activity - self.created_at).total_seconds(),
        }


class SessionManager:
    """
    Manages active user sessions with automatic cleanup of stale sessions.
    Thread-safe session lifecycle management.
    """
    
    def __init__(self, inactive_timeout_minutes: int = 30, cleanup_interval_minutes: int = 5):
        """
        Initialize session manager.
        
        Args:
            inactive_timeout_minutes: Mark session inactive after this duration
            cleanup_interval_minutes: Run cleanup task every N minutes
        """
        self.sessions: Dict[str, Session] = {}
        self.inactive_timeout = timedelta(minutes=inactive_timeout_minutes)
        self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)
        self.last_cleanup = datetime.now()
        logger.info(f"SessionManager initialized (timeout={inactive_timeout_minutes}min)")
    
    def create_session(self) -> str:
        """Create a new session."""
        session_id = str(uuid4())
        self.sessions[session_id] = Session(session_id)
        logger.info(f"Session created: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Retrieve a session by ID."""
        return self.sessions.get(session_id)
    
    def end_session(self, session_id: str) -> bool:
        """Mark a session as ended."""
        if session_id in self.sessions:
            self.sessions[session_id].is_active = False
            logger.info(f"Session ended: {session_id}")
            return True
        return False
    
    def record_entity_access(self, session_id: str, entity_name: str) -> bool:
        """Record entity access for a session."""
        session = self.get_session(session_id)
        if session and session.is_active:
            session.add_entity_access(entity_name)
            return True
        return False
    
    def record_query(self, session_id: str) -> bool:
        """Record query execution for a session."""
        session = self.get_session(session_id)
        if session and session.is_active:
            session.increment_query_count()
            return True
        return False
    
    def get_active_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions as serialized data."""
        self._cleanup_stale_sessions()
        return {
            sid: session.to_dict()
            for sid, session in self.sessions.items()
            if session.is_active
        }
    
    def get_session_details(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific session."""
        session = self.get_session(session_id)
        if session:
            return session.to_dict()
        return None
    
    def _cleanup_stale_sessions(self) -> None:
        """Remove stale inactive sessions (runs periodically)."""
        now = datetime.now()
        if (now - self.last_cleanup) < self.cleanup_interval:
            return
        
        stale_sessions = []
        for session_id, session in list(self.sessions.items()):
            if (now - session.last_activity) > self.inactive_timeout:
                session.is_active = False
                stale_sessions.append(session_id)
        
        if stale_sessions:
            logger.info(f"Cleaned up {len(stale_sessions)} stale sessions")
        
        self.last_cleanup = now
    
    def get_session_statistics(self) -> Dict[str, Any]:
        """Get aggregate statistics about all sessions."""
        self._cleanup_stale_sessions()
        active = self.get_active_sessions()
        total_queries = sum(s["query_count"] for s in active.values())
        unique_entities = set()
        for session in active.values():
            unique_entities.update(session["entities_accessed"])
        
        return {
            "total_active_sessions": len(active),
            "total_queries_across_sessions": total_queries,
            "unique_entities_accessed": list(unique_entities),
            "avg_queries_per_session": total_queries / len(active) if active else 0,
        }
