import os
import sqlite3
from typing import Optional

_DB_DIR = os.path.dirname(os.path.abspath(__file__))
TASK_CACHE_DB_PATH = os.path.join(_DB_DIR, "task_cache.db")


def init_task_cache_db() -> None:
    """Initializes the flat, high-performance global task token routing cache table."""
    with sqlite3.connect(TASK_CACHE_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_tasks (
                routing_key TEXT PRIMARY KEY,
                task_token TEXT NOT NULL
            )
            """
        )
        conn.commit()


def save_active_task_token(routing_key: str, task_token: str) -> None:
    """Binds a unique business routing key directly to its active LangGraph task token."""
    with sqlite3.connect(TASK_CACHE_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO active_tasks (routing_key, task_token) VALUES (?, ?)",
            (routing_key, task_token),
        )
        conn.commit()


def get_active_task_token(routing_key: str) -> Optional[str]:
    """Retrieves the live framework task token matching the unique routing key handle."""
    with sqlite3.connect(TASK_CACHE_DB_PATH, timeout=30.0) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT task_token FROM active_tasks WHERE routing_key = ?", (routing_key,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


def delete_active_task_token(routing_key: str) -> None:
    """Purges the single-use routing token from the cache once its resumption turn finishes."""
    with sqlite3.connect(TASK_CACHE_DB_PATH, timeout=30.0) as conn:
        conn.execute("DELETE FROM active_tasks WHERE routing_key = ?", (routing_key,))
        conn.commit()
