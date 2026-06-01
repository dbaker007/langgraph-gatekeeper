import os
import sqlite3
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------------
# ABSOLUTE PATH ANCHORING (Production Scalable Pattern)
# -----------------------------------------------------------------
# Resolves the database path relative to the physical directory where this code file lives.
# This ensures that no matter where pytest or a web container is executed, it hooks
# to the exact same tracking file location!
_DB_DIR = os.path.dirname(os.path.abspath(__file__))
SLA_DB_PATH = os.path.join(_DB_DIR, "sla_timers.db")


def init_sla_db() -> None:
    """Initializes the persistent schema tables for background tracking."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_registry (
                graph_key TEXT PRIMARY KEY,
                module_path TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sla_timers (
                thread_id TEXT PRIMARY KEY,
                graph_key TEXT NOT NULL,
                expiration_timestamp TEXT NOT NULL,
                is_complete INTEGER DEFAULT 0,
                FOREIGN KEY (graph_key) REFERENCES graph_registry(graph_key)
            )
            """
        )
        conn.commit()


def insert_graph_registry(graph_key: str, module_path: str) -> None:
    """Persists a graph's module path mapping blindly down to the registry table."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO graph_registry (graph_key, module_path) VALUES (?, ?)",
            (graph_key, module_path),
        )
        conn.commit()


def get_graph_module_path(graph_key: str) -> Optional[str]:
    """Retrieves the target physical module path string matching a specific graph key."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT module_path FROM graph_registry WHERE graph_key = ?", (graph_key,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


def insert_sla_timer(thread_id: str, graph_key: str, expiration_timestamp: str) -> None:
    """Logs a fresh thread ID and its target expiration date for daemon tracking."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sla_timers (thread_id, graph_key, expiration_timestamp) VALUES (?, ?, ?)",
            (thread_id, graph_key, expiration_timestamp),
        )
        conn.commit()


def get_expired_sla_rows() -> List[Dict[str, Any]]:
    """Queries and returns all active thread records that have breached their deadline."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT thread_id, graph_key FROM sla_timers WHERE is_complete = 0 AND expiration_timestamp <= datetime('now')"
        )
        return [dict(row) for row in cursor.fetchall()]


def mark_sla_thread_complete(thread_id: str) -> None:
    """Flags a thread tracking row as successfully terminated out-of-band."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute(
            "UPDATE sla_timers SET is_complete = 1 WHERE thread_id = ?", (thread_id,)
        )
        conn.commit()


def purge_completed_sla_rows() -> None:
    """Clean-up utility that deletes finalized rows out-of-band to prevent database bloat."""
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute("DELETE FROM sla_timers WHERE is_complete = 1")
        conn.commit()
