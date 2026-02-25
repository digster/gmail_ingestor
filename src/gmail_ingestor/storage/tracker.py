"""SQLite-based state tracking for email fetch/conversion pipeline."""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Status state machine: pending → fetched → converted (or → failed → pending on retry)
VALID_STATUSES = {"pending", "fetched", "converted", "failed"}


class FetchTracker:
    """Tracks email processing state in SQLite.

    Tables:
    - messages: per-message state with status, paths, timestamps
    - fetch_runs: audit log of ingestion runs
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Open database connection and ensure schema exists."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> FetchTracker:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._conn

    def _create_tables(self) -> None:
        """Create tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                label_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                subject TEXT DEFAULT '',
                sender TEXT DEFAULT '',
                date TEXT DEFAULT '',
                raw_text_path TEXT DEFAULT '',
                raw_html_path TEXT DEFAULT '',
                markdown_path TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
            CREATE INDEX IF NOT EXISTS idx_messages_label ON messages(label_id);

            CREATE TABLE IF NOT EXISTS labels (
                label_id TEXT PRIMARY KEY,
                label_name TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_labels (
                message_id TEXT NOT NULL,
                label_id TEXT NOT NULL,
                PRIMARY KEY (message_id, label_id),
                FOREIGN KEY (message_id) REFERENCES messages(message_id),
                FOREIGN KEY (label_id) REFERENCES labels(label_id)
            );

            CREATE TABLE IF NOT EXISTS fetch_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                label_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                ids_discovered INTEGER DEFAULT 0,
                messages_fetched INTEGER DEFAULT 0,
                messages_converted INTEGER DEFAULT 0,
                messages_failed INTEGER DEFAULT 0
            );
        """)

    def insert_pending(self, message_id: str, thread_id: str, label_id: str) -> bool:
        """Insert a message as 'pending' if not already tracked.

        Returns True if inserted, False if already exists.
        """
        now = datetime.now(UTC).isoformat()
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO messages
                   (message_id, thread_id, label_id, status, created_at, updated_at)
                   VALUES (?, ?, ?, 'pending', ?, ?)""",
                (message_id, thread_id, label_id, now, now),
            )
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error("Failed to insert pending message %s: %s", message_id, e)
            return False

    def bulk_insert_pending(self, stubs: list[tuple[str, str]], label_id: str) -> int:
        """Bulk insert message stubs as 'pending', skipping existing ones.

        Args:
            stubs: List of (message_id, thread_id) tuples.
            label_id: The label being fetched.

        Returns:
            Number of newly inserted rows.
        """
        now = datetime.now(UTC).isoformat()
        rows = [(msg_id, thread_id, label_id, "pending", now, now) for msg_id, thread_id in stubs]
        cursor = self.conn.executemany(
            """INSERT OR IGNORE INTO messages
               (message_id, thread_id, label_id, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()
        return cursor.rowcount

    def update_status(
        self,
        message_id: str,
        status: str,
        *,
        subject: str = "",
        sender: str = "",
        date: str = "",
        raw_text_path: str = "",
        raw_html_path: str = "",
        markdown_path: str = "",
        error_message: str = "",
    ) -> None:
        """Update the status and metadata of a tracked message."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}")

        now = datetime.now(UTC).isoformat()
        sets = ["status = ?", "updated_at = ?"]
        params: list[str] = [status, now]

        if subject:
            sets.append("subject = ?")
            params.append(subject)
        if sender:
            sets.append("sender = ?")
            params.append(sender)
        if date:
            sets.append("date = ?")
            params.append(date)
        if raw_text_path:
            sets.append("raw_text_path = ?")
            params.append(raw_text_path)
        if raw_html_path:
            sets.append("raw_html_path = ?")
            params.append(raw_html_path)
        if markdown_path:
            sets.append("markdown_path = ?")
            params.append(markdown_path)
        if error_message:
            sets.append("error_message = ?")
            params.append(error_message)

        params.append(message_id)
        self.conn.execute(
            f"UPDATE messages SET {', '.join(sets)} WHERE message_id = ?",
            params,
        )
        self.conn.commit()

    def get_pending_ids(self, limit: int = 100, offset: int = 0) -> list[str]:
        """Get message IDs with 'pending' status."""
        rows = self.conn.execute(
            "SELECT message_id FROM messages WHERE status = 'pending' "
            "ORDER BY created_at LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [row["message_id"] for row in rows]

    def get_fetched_ids(self, limit: int = 100, offset: int = 0) -> list[str]:
        """Get message IDs with 'fetched' status (ready for conversion)."""
        rows = self.conn.execute(
            "SELECT message_id FROM messages WHERE status = 'fetched' "
            "ORDER BY created_at LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [row["message_id"] for row in rows]

    def get_message(self, message_id: str) -> dict | None:
        """Get full message record by ID."""
        row = self.conn.execute(
            "SELECT * FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        return dict(row) if row else None

    def count_by_status(self) -> dict[str, int]:
        """Get count of messages grouped by status."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM messages GROUP BY status"
        ).fetchall()
        return {row["status"]: row["cnt"] for row in rows}

    def is_tracked(self, message_id: str) -> bool:
        """Check if a message ID is already tracked."""
        row = self.conn.execute(
            "SELECT 1 FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        return row is not None

    def start_run(self, label_id: str) -> int:
        """Record the start of a fetch run. Returns the run_id."""
        now = datetime.now(UTC).isoformat()
        cursor = self.conn.execute(
            "INSERT INTO fetch_runs (label_id, started_at) VALUES (?, ?)",
            (label_id, now),
        )
        self.conn.commit()
        return cursor.lastrowid or 0

    def complete_run(
        self,
        run_id: int,
        ids_discovered: int = 0,
        messages_fetched: int = 0,
        messages_converted: int = 0,
        messages_failed: int = 0,
    ) -> None:
        """Record the completion of a fetch run."""
        now = datetime.now(UTC).isoformat()
        self.conn.execute(
            """UPDATE fetch_runs SET
               completed_at = ?, ids_discovered = ?, messages_fetched = ?,
               messages_converted = ?, messages_failed = ?
               WHERE run_id = ?""",
            (now, ids_discovered, messages_fetched, messages_converted, messages_failed, run_id),
        )
        self.conn.commit()

    def upsert_labels(self, labels: list[dict[str, str]]) -> int:
        """Bulk upsert label ID → name mappings from Gmail API.

        Args:
            labels: List of dicts with 'id' and 'name' keys.

        Returns:
            Number of labels upserted.
        """
        now = datetime.now(UTC).isoformat()
        rows = [(lbl["id"], lbl["name"], now) for lbl in labels]
        self.conn.executemany(
            """INSERT INTO labels (label_id, label_name, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(label_id) DO UPDATE SET
                   label_name = excluded.label_name,
                   updated_at = excluded.updated_at""",
            rows,
        )
        self.conn.commit()
        return len(rows)

    def insert_message_labels(self, message_id: str, label_ids: tuple[str, ...]) -> None:
        """Populate the message_labels junction table for a message.

        Args:
            message_id: Gmail message ID.
            label_ids: Tuple of label IDs associated with the message.
        """
        rows = [(message_id, lid) for lid in label_ids]
        self.conn.executemany(
            "INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES (?, ?)",
            rows,
        )
        self.conn.commit()

    def get_message_labels(self, message_id: str) -> list[dict[str, str]]:
        """Get label IDs and names for a message via JOIN.

        Args:
            message_id: Gmail message ID.

        Returns:
            List of dicts with 'id' and 'name' keys, sorted by label name.
        """
        rows = self.conn.execute(
            """SELECT ml.label_id, COALESCE(l.label_name, ml.label_id) as label_name
               FROM message_labels ml
               LEFT JOIN labels l ON ml.label_id = l.label_id
               WHERE ml.message_id = ?
               ORDER BY label_name""",
            (message_id,),
        ).fetchall()
        return [{"id": row["label_id"], "name": row["label_name"]} for row in rows]

    def retry_failed(self) -> int:
        """Reset all 'failed' messages back to 'pending' for retry.

        Returns the number of messages reset.
        """
        now = datetime.now(UTC).isoformat()
        cursor = self.conn.execute(
            "UPDATE messages SET status = 'pending', error_message = '', updated_at = ? "
            "WHERE status = 'failed'",
            (now,),
        )
        self.conn.commit()
        return cursor.rowcount
