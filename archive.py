"""Durable append-only event journal for the continuous relay."""

from __future__ import annotations

import json
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class EventArchive:
    """Write market events to SQLite without blocking socket readers."""

    def __init__(self, path: Path, *, maximum_queue: int = 100_000) -> None:
        self.path = path.resolve()
        self.queue: queue.Queue[tuple[Any, ...] | None] = queue.Queue(
            maxsize=maximum_queue
        )
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.ready = False
        self.written = 0
        self.dropped = 0
        self.last_write_at: str | None = None
        self.last_error: str | None = None

    def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.thread = threading.Thread(
            target=self._run,
            name="event-archive",
            daemon=True,
        )
        self.thread.start()

    def append(
        self,
        *,
        source: str,
        market_id: str | None,
        event_type: str | None,
        sequence: int | None,
        payload: dict[str, Any],
        received_at: str | None = None,
    ) -> bool:
        row = (
            received_at or utc_now(),
            source,
            market_id,
            event_type,
            sequence,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
        )
        try:
            self.queue.put_nowait(row)
        except queue.Full:
            with self.lock:
                self.dropped += 1
            return False
        return True

    def flush(self, timeout: float = 5) -> bool:
        complete = threading.Event()

        def wait_for_queue() -> None:
            self.queue.join()
            complete.set()

        threading.Thread(target=wait_for_queue, daemon=True).start()
        return complete.wait(timeout)

    def stop(self, timeout: float = 10) -> None:
        if self.thread is None:
            return
        self.queue.put(None)
        self.thread.join(timeout=timeout)

    def status(self) -> dict[str, Any]:
        with self.lock:
            return {
                "configured": True,
                "healthy": (
                    self.ready
                    and self.last_error is None
                    and self.dropped == 0
                    and self.thread is not None
                    and self.thread.is_alive()
                ),
                "path": str(self.path),
                "queueDepth": self.queue.qsize(),
                "written": self.written,
                "dropped": self.dropped,
                "lastWriteAt": self.last_write_at,
                "lastError": self.last_error,
            }

    def read_events(
        self, *, after_id: int = 0, limit: int = 1000
    ) -> dict[str, Any]:
        connection = sqlite3.connect(
            f"file:{self.path}?mode=ro",
            uri=True,
            timeout=5,
        )
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """SELECT id, received_at, source, market_id, event_type,
                      sequence, payload_json
               FROM relay_events
               WHERE id > ?
               ORDER BY id
               LIMIT ?""",
            (max(0, after_id), min(max(1, limit), 5000)),
        ).fetchall()
        max_id = connection.execute(
            "SELECT COALESCE(MAX(id), 0) FROM relay_events"
        ).fetchone()[0]
        connection.close()
        return {
            "afterId": max(0, after_id),
            "nextAfterId": int(rows[-1]["id"]) if rows else max(0, after_id),
            "maximumId": int(max_id),
            "events": [
                {
                    "id": int(row["id"]),
                    "receivedAt": row["received_at"],
                    "source": row["source"],
                    "marketId": row["market_id"],
                    "eventType": row["event_type"],
                    "sequence": row["sequence"],
                    "payload": json.loads(row["payload_json"]),
                }
                for row in rows
            ],
        }

    def _run(self) -> None:
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self.path, timeout=30)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS relay_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    market_id TEXT,
                    event_type TEXT,
                    sequence INTEGER,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_relay_events_source_id
                    ON relay_events(source, id);
                """
            )
            connection.commit()
            with self.lock:
                self.ready = True
            while True:
                item = self.queue.get()
                if item is None:
                    self.queue.task_done()
                    break
                batch = [item]
                stop_after_batch = False
                while len(batch) < 500:
                    try:
                        next_item = self.queue.get_nowait()
                    except queue.Empty:
                        break
                    if next_item is None:
                        self.queue.task_done()
                        stop_after_batch = True
                        break
                    batch.append(next_item)
                connection.executemany(
                    """INSERT INTO relay_events
                       (received_at, source, market_id, event_type,
                        sequence, payload_json)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    batch,
                )
                connection.commit()
                with self.lock:
                    self.written += len(batch)
                    self.last_write_at = utc_now()
                for _ in batch:
                    self.queue.task_done()
                if stop_after_batch:
                    break
        except Exception as exc:
            with self.lock:
                self.last_error = f"{type(exc).__name__}: {exc}"
        finally:
            if connection is not None:
                connection.close()
