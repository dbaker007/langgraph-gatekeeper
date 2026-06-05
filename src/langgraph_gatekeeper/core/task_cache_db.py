import os
import sqlite3
from typing import Any, Dict, Optional

_CURRENT_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
TASK_CACHE_DB_PATH = os.path.join(_CURRENT_MODULE_DIR, "task_cache.db")


def init_task_cache_db() -> None:
    """Initializes the metadata staging and tracking table schema safely.

    Enforces absolute data integrity via a Partial Unique Index that prevents
    concurrent duplicate active tasks within a thread while preserving history.
    """
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                routing_key TEXT PRIMARY KEY,
                interrupt_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                business_context TEXT NOT NULL,
                required_claim TEXT NOT NULL, -- FIXED: Added unified security metadata column!
                status TEXT NOT NULL DEFAULT 'ACTIVE'
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_unique_active_context 
            ON tasks (thread_id, business_context) 
            WHERE status = 'ACTIVE'
            """
        )
        conn.commit()


def save_active_task_token(
    routing_key: str,
    interrupt_id: str,
    thread_id: str,
    business_context: str,
    required_claim: str,
) -> None:
    """Inserts a task tracking token row along with its business context and claim metrics.

    Uses standard INSERT semantics to guarantee that any Partial Unique Index violations
    cause a hard sqlite3.IntegrityError to bubble up, rather than silently deleting old data.
    """
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        try:
            conn.execute(
                """
                INSERT INTO tasks (routing_key, interrupt_id, thread_id, business_context, required_claim, status)
                VALUES (?, ?, ?, ?, ?, 'ACTIVE')
                """,
                (
                    routing_key,
                    interrupt_id,
                    thread_id,
                    business_context,
                    required_claim,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError as err:
            if "UNIQUE constraint failed: tasks.routing_key" in str(err):
                conn.execute(
                    """
                    UPDATE tasks 
                    SET interrupt_id = ?, thread_id = ?, business_context = ?, required_claim = ?, status = 'ACTIVE'
                    WHERE routing_key = ?
                    """,
                    (
                        interrupt_id,
                        thread_id,
                        business_context,
                        required_claim,
                        routing_key,
                    ),
                )
                conn.commit()
            else:
                raise err


def get_active_task_token(routing_key: str) -> Optional[str]:
    """Retrieves an active task execution interrupt identifier from the staging tier."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT interrupt_id FROM tasks WHERE routing_key = ? AND status = 'ACTIVE'",
            (routing_key,),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def get_token_by_business_context(
    thread_id: str, business_context: str
) -> Optional[Dict[str, str]]:
    """COMPOSITE KEY LOOKUP: Matches the unique active task utilizing composite primary indicators.

    Returns a dictionary holding 'token_id', 'routing_key', and 'required_claim' safely.
    """
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT interrupt_id, routing_key, required_claim FROM tasks 
            WHERE thread_id = ? AND business_context = ? AND status = 'ACTIVE'
            """,
            (thread_id, business_context),
        )
        row = cursor.fetchone()
        if row:
            # FIXED: Package the newly extracted required_claim element safely!
            return {
                "token_id": str(row[0]),
                "routing_key": str(row[1]),
                "required_claim": str(row[2]),
            }
        return None


def get_thread_id_by_routing_key(routing_key: str) -> Optional[str]:
    """Recovers the permanent thread identifier mapping for a given business routing token."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT thread_id FROM tasks WHERE routing_key = ?",
            (routing_key,),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def consume_active_task_token(routing_key: str) -> None:
    """Flips an active tracking row status to CONSUMED rather than hard deleting it from disk."""
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute(
            "UPDATE tasks SET status = 'CONSUMED' WHERE routing_key = ?",
            (routing_key,),
        )
        conn.commit()
