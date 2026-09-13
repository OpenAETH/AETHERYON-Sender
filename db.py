from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import logging

logger = logging.getLogger(__name__)

# Global session management
engine = None
SessionLocal = None

@asynccontextmanager
def get_db_session():
    """Provide a transactional database session"""
    if SessionLocal is None:
        raise RuntimeError("Database not initialized")
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def db_select_one(query: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
    """Execute SELECT query and return first row as dict"""
    try:
        with get_db_session() as session:
            result = session.execute(text(query), params or {})
            row = result.fetchone()
            return dict(row._mapping) if row else None
    except Exception as e:
        logger.error(f"db_select_one error: {e}")
        raise


def db_select_all(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Execute SELECT query and return all rows as list of dicts"""
    try:
        with get_db_session() as session:
            result = session.execute(text(query), params or {})
            return [dict(r._mapping) for r in result.fetchall()]
    except Exception as e:
        logger.error(f"db_select_all error: {e}")
        raise


def db_insert(query: str, params: Dict[str, Any]) -> Any:
    """Execute INSERT/UPDATE/DELETE and return affected rows"""
    try:
        with get_db_session() as session:
            result = session.execute(text(query), params)
            return result.rowcount
    except Exception as e:
        logger.error(f"db_insert error: {e}")
        raise
