import os
import sqlite3
from typing import Any, Dict, Optional

_CURRENT_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
TASK_CACHE_DB_PATH = os.path.join(_CURRENT_MODULE_DIR, "task_cache.db")


def init_task_cache_db() -> None:
    """Initializes the immutable metadata staging and tracking table schema safely."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_tasks (
                routing_key TEXT PRIMARY KEY,
                interrupt_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE'
            )
            """
        )
        conn.commit()


def save_active_task_token(routing_key: str, interrupt_id: str, thread_id: str) -> None:
    """Inserts or overwrites a live task tracking token row along with its permanent thread handle."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO active_tasks (routing_key, interrupt_id, thread_id, status)
            VALUES (?, ?, ?, 'ACTIVE')
            """,
            (routing_key, interrupt_id, thread_id),
        )
        conn.commit()


def get_active_task_token(routing_key: str) -> Optional[str]:
    """Retrieves an active task execution interrupt identifier from the staging tier."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT interrupt_id FROM active_tasks WHERE routing_key = ? AND status = 'ACTIVE'",
            (routing_key,),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def get_thread_id_by_routing_key(routing_key: str) -> Optional[str]:
    """Recovers the permanent thread identifier mapping for a given business routing token."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT thread_id FROM active_tasks WHERE routing_key = ?",
            (routing_key,),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def consume_active_task_token(routing_key: str) -> None:
    """Flips an active tracking row status to CONSUMED rather than hard deleting it from disk."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute(
            "UPDATE active_tasks SET status = 'CONSUMED' WHERE routing_key = ?",
            (routing_key,),
        )
        conn.commit()
