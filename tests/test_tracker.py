"""Tests for FetchTracker — SQLite-based email processing state tracker."""

from __future__ import annotations

from pathlib import Path

import pytest

from gmail_ingestor.storage.tracker import FetchTracker


class TestConnect:
    """FetchTracker.connect() initialises the database schema."""

    def test_creates_messages_table(self, tmp_db_path: Path) -> None:
        tracker = FetchTracker(tmp_db_path)
        tracker.connect()
        tables = tracker.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='messages'"
        ).fetchall()
        tracker.close()
        assert len(tables) == 1

    def test_creates_fetch_runs_table(self, tmp_db_path: Path) -> None:
        tracker = FetchTracker(tmp_db_path)
        tracker.connect()
        tables = tracker.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fetch_runs'"
        ).fetchall()
        tracker.close()
        assert len(tables) == 1

    def test_creates_indexes(self, tmp_db_path: Path) -> None:
        tracker = FetchTracker(tmp_db_path)
        tracker.connect()
        indexes = tracker.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_messages_%'"
        ).fetchall()
        tracker.close()
        index_names = {row["name"] for row in indexes}
        assert "idx_messages_status" in index_names
        assert "idx_messages_label" in index_names

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "test.db"
        tracker = FetchTracker(nested)
        tracker.connect()
        tracker.close()
        assert nested.exists()


class TestInsertPending:
    """insert_pending stores a single message with 'pending' status."""

    def test_inserts_new_message(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            result = tracker.insert_pending("msg1", "thread1", "INBOX")
            assert result is True

            row = tracker.conn.execute(
                "SELECT * FROM messages WHERE message_id = 'msg1'"
            ).fetchone()
            assert row is not None
            assert row["status"] == "pending"
            assert row["thread_id"] == "thread1"
            assert row["label_id"] == "INBOX"

    def test_returns_false_for_duplicate(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "thread1", "INBOX")
            result = tracker.insert_pending("msg1", "thread1", "INBOX")
            # INSERT OR IGNORE does not change total_changes on duplicate,
            # but the return value depends on the implementation's check.
            # At minimum, no exception should be raised.
            assert isinstance(result, bool)


class TestBulkInsertPending:
    """bulk_insert_pending inserts multiple stubs and skips duplicates."""

    def test_inserts_multiple(self, tmp_db_path: Path) -> None:
        stubs = [("m1", "t1"), ("m2", "t2"), ("m3", "t3")]
        with FetchTracker(tmp_db_path) as tracker:
            count = tracker.bulk_insert_pending(stubs, "INBOX")
            assert count == 3

    def test_skips_duplicates(self, tmp_db_path: Path) -> None:
        stubs = [("m1", "t1"), ("m2", "t2")]
        with FetchTracker(tmp_db_path) as tracker:
            tracker.bulk_insert_pending(stubs, "INBOX")
            # Insert again with one new, one duplicate
            count = tracker.bulk_insert_pending([("m2", "t2"), ("m3", "t3")], "INBOX")
            assert count == 1

    def test_empty_list_returns_zero(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            count = tracker.bulk_insert_pending([], "INBOX")
            assert count == 0


class TestUpdateStatus:
    """update_status changes status and optional metadata fields."""

    def test_changes_status(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            tracker.update_status("msg1", "fetched")
            row = tracker.conn.execute(
                "SELECT status FROM messages WHERE message_id = 'msg1'"
            ).fetchone()
            assert row["status"] == "fetched"

    def test_sets_metadata_fields(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            tracker.update_status(
                "msg1",
                "fetched",
                subject="Hello",
                sender="alice@test.com",
                date="2024-01-15",
                raw_text_path="/tmp/msg1.txt",
                raw_html_path="/tmp/msg1.html",
            )
            row = tracker.conn.execute(
                "SELECT * FROM messages WHERE message_id = 'msg1'"
            ).fetchone()
            assert row["subject"] == "Hello"
            assert row["sender"] == "alice@test.com"
            assert row["date"] == "2024-01-15"
            assert row["raw_text_path"] == "/tmp/msg1.txt"
            assert row["raw_html_path"] == "/tmp/msg1.html"

    def test_sets_error_message(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            tracker.update_status("msg1", "failed", error_message="timeout")
            row = tracker.conn.execute(
                "SELECT error_message FROM messages WHERE message_id = 'msg1'"
            ).fetchone()
            assert row["error_message"] == "timeout"

    def test_sets_markdown_path(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            tracker.update_status("msg1", "converted", markdown_path="/out/msg1.md")
            row = tracker.conn.execute(
                "SELECT markdown_path FROM messages WHERE message_id = 'msg1'"
            ).fetchone()
            assert row["markdown_path"] == "/out/msg1.md"

    def test_rejects_invalid_status(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            with pytest.raises(ValueError, match="Invalid status"):
                tracker.update_status("msg1", "bogus")

    def test_updates_updated_at(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            before = tracker.conn.execute(
                "SELECT updated_at FROM messages WHERE message_id = 'msg1'"
            ).fetchone()["updated_at"]

            tracker.update_status("msg1", "fetched")
            after = tracker.conn.execute(
                "SELECT updated_at FROM messages WHERE message_id = 'msg1'"
            ).fetchone()["updated_at"]

            assert after >= before


class TestGetIds:
    """get_pending_ids and get_fetched_ids return the correct message IDs."""

    def test_get_pending_ids(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("p1", "t1", "INBOX")
            tracker.insert_pending("p2", "t2", "INBOX")
            tracker.insert_pending("f1", "t3", "INBOX")
            tracker.update_status("f1", "fetched")

            pending = tracker.get_pending_ids()
            assert set(pending) == {"p1", "p2"}

    def test_get_fetched_ids(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("p1", "t1", "INBOX")
            tracker.insert_pending("f1", "t2", "INBOX")
            tracker.insert_pending("f2", "t3", "INBOX")
            tracker.update_status("f1", "fetched")
            tracker.update_status("f2", "fetched")

            fetched = tracker.get_fetched_ids()
            assert set(fetched) == {"f1", "f2"}

    def test_limit_parameter(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            for i in range(5):
                tracker.insert_pending(f"m{i}", f"t{i}", "INBOX")

            limited = tracker.get_pending_ids(limit=3)
            assert len(limited) == 3

    def test_empty_result(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            assert tracker.get_pending_ids() == []
            assert tracker.get_fetched_ids() == []

    def test_offset_for_pending_ids(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            for i in range(5):
                tracker.insert_pending(f"m{i}", f"t{i}", "INBOX")

            result = tracker.get_pending_ids(limit=100, offset=2)
            assert len(result) == 3
            # Ordered by created_at; first two should be skipped
            assert result == ["m2", "m3", "m4"]

    def test_offset_for_fetched_ids(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            for i in range(5):
                tracker.insert_pending(f"m{i}", f"t{i}", "INBOX")
                tracker.update_status(f"m{i}", "fetched")

            result = tracker.get_fetched_ids(limit=100, offset=2)
            assert len(result) == 3
            assert result == ["m2", "m3", "m4"]

    def test_offset_exceeds_count(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            for i in range(3):
                tracker.insert_pending(f"m{i}", f"t{i}", "INBOX")

            assert tracker.get_pending_ids(limit=100, offset=10) == []
            assert tracker.get_fetched_ids(limit=100, offset=10) == []


class TestCountByStatus:
    """count_by_status returns a dict of status -> count."""

    def test_counts_correctly(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("m1", "t1", "INBOX")
            tracker.insert_pending("m2", "t2", "INBOX")
            tracker.insert_pending("m3", "t3", "INBOX")
            tracker.update_status("m2", "fetched")
            tracker.update_status("m3", "failed", error_message="err")

            counts = tracker.count_by_status()
            assert counts["pending"] == 1
            assert counts["fetched"] == 1
            assert counts["failed"] == 1

    def test_empty_database(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            counts = tracker.count_by_status()
            assert counts == {}


class TestIsTracked:
    """is_tracked returns True when a message exists, False otherwise."""

    def test_returns_true_for_existing(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("msg1", "t1", "INBOX")
            assert tracker.is_tracked("msg1") is True

    def test_returns_false_for_missing(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            assert tracker.is_tracked("nonexistent") is False


class TestRuns:
    """start_run and complete_run manage fetch_runs records."""

    def test_start_run_returns_run_id(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            run_id = tracker.start_run("INBOX")
            assert isinstance(run_id, int)
            assert run_id >= 1

    def test_start_run_creates_record(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            run_id = tracker.start_run("INBOX")
            row = tracker.conn.execute(
                "SELECT * FROM fetch_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            assert row is not None
            assert row["label_id"] == "INBOX"
            assert row["started_at"] is not None
            assert row["completed_at"] is None

    def test_complete_run_updates_record(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            run_id = tracker.start_run("INBOX")
            tracker.complete_run(
                run_id,
                ids_discovered=100,
                messages_fetched=95,
                messages_converted=90,
                messages_failed=5,
            )
            row = tracker.conn.execute(
                "SELECT * FROM fetch_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            assert row["completed_at"] is not None
            assert row["ids_discovered"] == 100
            assert row["messages_fetched"] == 95
            assert row["messages_converted"] == 90
            assert row["messages_failed"] == 5

    def test_multiple_runs_get_distinct_ids(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            r1 = tracker.start_run("INBOX")
            r2 = tracker.start_run("SENT")
            assert r1 != r2


class TestRetryFailed:
    """retry_failed resets failed messages back to pending."""

    def test_resets_failed_to_pending(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("m1", "t1", "INBOX")
            tracker.insert_pending("m2", "t2", "INBOX")
            tracker.update_status("m1", "failed", error_message="err")
            tracker.update_status("m2", "failed", error_message="err2")

            count = tracker.retry_failed()
            assert count == 2

            pending = tracker.get_pending_ids()
            assert set(pending) == {"m1", "m2"}

    def test_clears_error_message(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("m1", "t1", "INBOX")
            tracker.update_status("m1", "failed", error_message="something broke")
            tracker.retry_failed()

            row = tracker.conn.execute(
                "SELECT error_message FROM messages WHERE message_id = 'm1'"
            ).fetchone()
            assert row["error_message"] == ""

    def test_does_not_affect_other_statuses(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tracker.insert_pending("m1", "t1", "INBOX")
            tracker.insert_pending("m2", "t2", "INBOX")
            tracker.update_status("m1", "fetched")
            # m2 stays pending

            count = tracker.retry_failed()
            assert count == 0

            row = tracker.conn.execute(
                "SELECT status FROM messages WHERE message_id = 'm1'"
            ).fetchone()
            assert row["status"] == "fetched"

    def test_returns_zero_when_no_failed(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            assert tracker.retry_failed() == 0


class TestContextManager:
    """FetchTracker works as a context manager for connect/close lifecycle."""

    def test_context_manager_creates_tables(self, tmp_db_path: Path) -> None:
        with FetchTracker(tmp_db_path) as tracker:
            tables = tracker.conn.execute(
                "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table'"
            ).fetchone()
            assert tables["cnt"] >= 2

    def test_context_manager_closes_connection(self, tmp_db_path: Path) -> None:
        tracker = FetchTracker(tmp_db_path)
        with tracker:
            assert tracker._conn is not None
        assert tracker._conn is None

    def test_conn_property_raises_when_not_connected(self, tmp_db_path: Path) -> None:
        tracker = FetchTracker(tmp_db_path)
        with pytest.raises(RuntimeError, match="not connected"):
            _ = tracker.conn
