"""Durable, locally owned chat history for the standalone SIWC server.

The remote SIWC Responses route is stateless. This store persists only local
conversation items, never tokens, and only commits a turn after response.completed.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid
from typing import Any

from .auth import AuthError, CredentialStore


class SessionError(Exception):
    pass


class SessionStore:
    def __init__(self, root: Path | None = None):
        self.root = (root or CredentialStore().root).expanduser()
        self.path = self.root / "sessions.sqlite3"
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.is_symlink() or self.root.stat().st_mode & 0o077:
            raise SessionError("Session directory must be private (0700)")
        if self.path.is_symlink():
            raise SessionError("Session database must not be a symlink")
        if self.path.exists():
            meta = self.path.stat()
            if not stat.S_ISREG(meta.st_mode) or meta.st_mode & 0o077:
                raise SessionError("Session database must be a private regular file")
        # SQLite's temporary journals remain inside the 0700 directory.
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY, model TEXT NOT NULL, instructions TEXT NOT NULL,
                history TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL,
                created REAL NOT NULL, updated REAL NOT NULL
            )""")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            os.chmod(self.path, 0o600)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=5000")
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _public(row: sqlite3.Row, full: bool = False):
        fields = {k: row[k] for k in ("id", "model", "state", "revision", "created", "updated")}
        if full:
            fields.update(instructions=row["instructions"], history=json.loads(row["history"]))
        return fields

    def create(self, model: str, instructions: str = "") -> dict:
        if not isinstance(model, str) or not model or len(model) > 160 or not all(c.isalnum() or c in "_.-" for c in model):
            raise SessionError("Invalid model slug")
        if not isinstance(instructions, str) or len(instructions) > 10000:
            raise SessionError("Invalid instructions")
        sid = str(uuid.uuid4()); now = time.time()
        with self._db() as db:
            db.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, 'idle', 0, ?, ?)", (sid, model, instructions, '[]', now, now))
            return self._public(db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone())

    def list(self) -> list[dict]:
        with self._db() as db:
            return [self._public(row) for row in db.execute("SELECT * FROM sessions ORDER BY updated DESC LIMIT 100")]

    def get(self, sid: str, full: bool = True) -> dict:
        with self._db() as db:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
            if row is None:
                raise SessionError("Unknown local session")
            return self._public(row, full)

    def delete(self, sid: str) -> None:
        with self._db() as db:
            result = db.execute("DELETE FROM sessions WHERE id=? AND state != 'running'", (sid,))
            if result.rowcount != 1:
                raise SessionError("Session not found or currently running")

    def recover_interrupted(self) -> None:
        """One-worker server startup: a former running stream did not complete."""
        with self._db() as db:
            db.execute("UPDATE sessions SET state='interrupted',updated=? WHERE state='running'", (time.time(),))

    def begin(self, sid: str, items: list[dict], expected_revision: int | None = None) -> dict:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
            if row is None:
                raise SessionError("Unknown local session")
            if row["state"] == "running":
                raise SessionError("A turn is already running in this session")
            if expected_revision is not None and row["revision"] != expected_revision:
                raise SessionError("Session changed; reload its history before retrying")
            old = json.loads(row["history"])
            all_calls = {item["call_id"] for item in old if isinstance(item, dict) and item.get("type") in ("function_call", "custom_tool_call")}
            resolved = {item["call_id"] for item in old if isinstance(item, dict) and item.get("type") in ("function_call_output", "custom_tool_call_output")}
            outstanding = all_calls - resolved
            if row["state"] == "waiting_tool":
                returned = {item.get("call_id") for item in items if item.get("type") in ("function_call_output", "custom_tool_call_output")}
                if returned != outstanding or len(items) != len(outstanding):
                    raise SessionError("Supply one tool output for every pending call before continuing")
            elif not any(item.get("role") in ("user", "developer") for item in items):
                raise SessionError("New turns require a user message")
            if len(old) + len(items) > 200:
                raise SessionError("Conversation is too long; start a new local session")
            rev = row["revision"] + 1
            db.execute("UPDATE sessions SET state='running',revision=?,updated=? WHERE id=?", (rev, time.time(), sid))
            return {**self._public(row, True), "revision": rev, "incoming": items}

    def finish(self, sid: str, revision: int, incoming: list, output: list, success: bool, cancelled: bool = False) -> None:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
            if row is None or row["revision"] != revision or row["state"] != "running":
                raise SessionError("Session changed while inference was running")
            history = json.loads(row["history"])
            if success:
                if not output:
                    raise SessionError("Successful response did not contain replayable output")
                history.extend(incoming + output)
                state = "waiting_tool" if any(x.get("type") in ("function_call", "custom_tool_call") for x in output) else "idle"
            else:
                state = "interrupted" if cancelled else "failed"
            db.execute("UPDATE sessions SET history=?,state=?,updated=? WHERE id=?", (json.dumps(history), state, time.time(), sid))
