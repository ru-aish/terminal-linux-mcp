"""Durable T3 bindings and event-driven inactivity recovery. No ChatGPT polling."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
import math
import re
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


class ThreadState:
    def __init__(self, path: Path, inactivity_seconds: float = 300):
        self.path = path
        self.inactivity_seconds = inactivity_seconds
        self.timers: dict[str, asyncio.TimerHandle] = {}
        self.automation_timers: dict[str, asyncio.TimerHandle] = {}
        self.changed = asyncio.Event()
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS bindings (
                thread_id TEXT PRIMARY KEY, terminal_id TEXT UNIQUE NOT NULL,
                environment_id TEXT NOT NULL, provider_id TEXT NOT NULL,
                conversation_id TEXT UNIQUE, cwd TEXT NOT NULL, model TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'idle', paused INTEGER NOT NULL DEFAULT 0,
                epoch INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
                last_activity REAL NOT NULL, deadline REAL
              );
              CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, action TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS operations (
                operation_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL,
                kind TEXT NOT NULL, pid INTEGER, start_ticks INTEGER,
                started REAL NOT NULL, finished REAL, result TEXT
              );
              CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
                thread_id TEXT NOT NULL, kind TEXT NOT NULL, epoch INTEGER NOT NULL,
                revision INTEGER NOT NULL, payload TEXT NOT NULL, created REAL NOT NULL,
                claimed INTEGER NOT NULL DEFAULT 0, acknowledged INTEGER NOT NULL DEFAULT 0
              );
              CREATE TABLE IF NOT EXISTS automations (
                automation_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL,
                epoch INTEGER NOT NULL, kind TEXT NOT NULL, due_at REAL,
                operation_id TEXT, status TEXT NOT NULL,
                idempotency_key TEXT, event_id TEXT UNIQUE,
                created REAL NOT NULL, updated REAL NOT NULL
              );
              CREATE UNIQUE INDEX IF NOT EXISTS automations_idempotency
                ON automations(thread_id, kind, idempotency_key);
              CREATE INDEX IF NOT EXISTS automations_thread ON automations(thread_id, status);
              CREATE INDEX IF NOT EXISTS automations_operation ON automations(operation_id, status);
              CREATE INDEX IF NOT EXISTS operations_active ON operations(thread_id, finished);
              CREATE INDEX IF NOT EXISTS events_thread ON events(thread_id, seq);
            ''')
            if 'inactivity_seconds' not in {r[1] for r in db.execute('PRAGMA table_info(bindings)')}:
                db.execute('ALTER TABLE bindings ADD COLUMN inactivity_seconds REAL')
            if 'process_id' not in {r[1] for r in db.execute('PRAGMA table_info(operations)')}:
                db.execute('ALTER TABLE operations ADD COLUMN process_id TEXT')
            db.execute('CREATE UNIQUE INDEX IF NOT EXISTS operations_process ON operations(thread_id, process_id)')

    @contextmanager
    def connect(self):
        # sqlite3.Connection.__exit__ commits or rolls back, but it does NOT
        # close the connection. Explicitly close every per-operation handle.
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA journal_mode=WAL')
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def identifier(value: Any) -> str:
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value) or value == 'default':
            raise ValueError('A non-default registered thread identifier is required')
        return value

    def get(self, thread_id: str) -> dict:
        with self.connect() as db:
            row = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
        if row is None:
            raise ValueError('Unknown T3 thread')
        return dict(row)

    def require_terminal(self, terminal_id: str) -> dict:
        self.identifier(terminal_id)
        with self.connect() as db:
            row = db.execute('SELECT * FROM bindings WHERE terminal_id=?', (terminal_id,)).fetchone()
        if row is None:
            raise ValueError('Thread ID was not registered by T3; tool execution refused')
        return dict(row)

    def register(self, *, thread_id: str, environment_id: str, provider_id: str, cwd: str, model: str, terminal_id: str | None = None, inactivity_seconds: float | None = None) -> dict:
        thread_id = self.identifier(thread_id)
        terminal_id = self.identifier(terminal_id or thread_id)
        environment_id = self.identifier(environment_id)
        provider_id = self.identifier(provider_id)
        if inactivity_seconds is not None and not 1 <= float(inactivity_seconds) <= 3600:
            raise ValueError('Inactivity interval must be between 1 and 3600 seconds')
        directory = Path(cwd).expanduser().resolve(strict=True)
        if not directory.is_dir() or not isinstance(model, str) or not model.strip():
            raise ValueError('Existing workspace directory and model are required')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if row is not None:
                if (row['terminal_id'], row['environment_id'], row['provider_id'], row['cwd']) != (terminal_id, environment_id, provider_id, str(directory)):
                    raise ValueError('T3 thread binding is immutable')
                db.execute('UPDATE bindings SET model=? WHERE thread_id=?', (model, thread_id))
            else:
                db.execute('INSERT INTO bindings(thread_id,terminal_id,environment_id,provider_id,cwd,model,last_activity) VALUES(?,?,?,?,?,?,?)',
                           (thread_id, terminal_id, environment_id, provider_id, str(directory), model, time.time()))
            if inactivity_seconds is not None:
                db.execute('UPDATE bindings SET inactivity_seconds=? WHERE thread_id=?', (float(inactivity_seconds), thread_id))
        return self.get(thread_id)

    def bind(self, thread_id: str, conversation_id: str) -> dict:
        conversation_id = str(uuid.UUID(conversation_id))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT conversation_id FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if row is None or row['conversation_id'] not in (None, conversation_id):
                raise ValueError('Conversation binding is unknown or immutable')
            db.execute('UPDATE bindings SET conversation_id=? WHERE thread_id=?', (conversation_id, thread_id))
        return self.get(thread_id)

    def _event(self, db, thread_id: str, kind: str, payload: dict) -> str:
        row = db.execute('SELECT epoch,revision FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
        event_id = str(uuid.uuid4())
        db.execute('INSERT INTO events(event_id,thread_id,kind,epoch,revision,payload,created) VALUES(?,?,?,?,?,?,?)',
                   (event_id, thread_id, kind, row['epoch'], row['revision'], json.dumps(payload), time.time()))
        self.changed.set()
        return event_id

    def _request(self, db, thread_id: str, request_id: str, action: str) -> bool:
        self.identifier(request_id)
        row = db.execute('SELECT * FROM requests WHERE request_id=?', (request_id,)).fetchone()
        if row:
            if (row['thread_id'], row['action']) != (thread_id, action):
                raise ValueError('Request ID belongs to another operation')
            return False
        db.execute('INSERT INTO requests VALUES(?,?,?)', (request_id, thread_id, action))
        return True

    def activate(self, thread_id: str, request_id: str) -> dict:
        self.get(thread_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._request(db, thread_id, request_id, 'activate'):
                self._stale_automations(db, thread_id)
                db.execute("UPDATE bindings SET state='working',paused=0,epoch=epoch+1,revision=revision+1,last_activity=?,deadline=NULL WHERE thread_id=?", (time.time(), thread_id))
                self._event(db, thread_id, 'state', {'state': 'working'})
        self.arm(thread_id)
        return self.get(thread_id)

    def pause(self, thread_id: str, request_id: str) -> dict:
        self.get(thread_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            applied = self._request(db, thread_id, request_id, 'pause')
            if applied:
                self._stale_automations(db, thread_id)
                db.execute('UPDATE bindings SET paused=1,revision=revision+1,deadline=NULL WHERE thread_id=?', (thread_id,))
                self._event(db, thread_id, 'state', {'state': self.get(thread_id)['state'], 'paused': True})
        if applied:
            self.cancel_timer(thread_id)
            self.cancel_automation_timer(thread_id)
        return self.get(thread_id)

    def begin(self, thread_id: str, operation_id: str, kind: str, pid: int | None = None, start_ticks: int | None = None, process_id: str | None = None):
        self.get(thread_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO operations(operation_id,thread_id,kind,pid,start_ticks,started,process_id) VALUES(?,?,?,?,?,?,?)',
                       (operation_id, thread_id, kind, pid, start_ticks, time.time(), process_id))
            db.execute('UPDATE bindings SET revision=revision+1,last_activity=?,deadline=NULL WHERE thread_id=?', (time.time(), thread_id))
        self.cancel_timer(thread_id)

    def finish(self, operation_id: str, result: str):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT thread_id,finished FROM operations WHERE operation_id=?', (operation_id,)).fetchone()
            if row is None or row['finished'] is not None:
                return
            thread_id = row['thread_id']
            db.execute('UPDATE operations SET finished=?,result=? WHERE operation_id=?', (time.time(), result[:1000], operation_id))
            db.execute("UPDATE automations SET status='due',updated=? WHERE operation_id=? AND status='pending'", (time.time(), operation_id))
            db.execute('UPDATE bindings SET revision=revision+1,last_activity=? WHERE thread_id=?', (time.time(), thread_id))
        self.arm(thread_id)

    def active(self, thread_id: str, excluding: str | None = None) -> list[dict]:
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM operations WHERE thread_id=? AND finished IS NULL AND operation_id!=?', (thread_id, excluding or ''))]

    def complete(self, thread_id: str, evidence: list[str], excluding: str | None = None):
        if not isinstance(evidence, list) or not evidence or any(not isinstance(e, str) or not e.strip() for e in evidence):
            raise ValueError('Completion requires nonempty evidence entries')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if row is None or row['state'] != 'working':
                raise ValueError('Thread has no active task')
            if db.execute('SELECT 1 FROM operations WHERE thread_id=? AND finished IS NULL AND operation_id!=?', (thread_id, excluding or '')).fetchone():
                raise ValueError('Tools or processes are still active')
            db.execute("UPDATE bindings SET state='done',revision=revision+1,deadline=NULL WHERE thread_id=?", (thread_id,))
            self._stale_automations(db, thread_id)
            self._event(db, thread_id, 'state', {'state': 'done', 'evidence': evidence})
        self.cancel_timer(thread_id)
        self.cancel_automation_timer(thread_id)
        return self.get(thread_id)

    @staticmethod
    def _stale_automations(db, thread_id: str):
        db.execute("UPDATE automations SET status='stale',updated=? WHERE thread_id=? AND status!='stale'", (time.time(), thread_id))

    def schedule_continue(self, thread_id: str, after_seconds: float | None = None, wake_at: float | str | None = None, idempotency_key: str | None = None) -> dict:
        if (after_seconds is None) == (wake_at is None):
            raise ValueError('Provide exactly one of after_seconds or wake_at')
        if after_seconds is not None:
            delay = float(after_seconds)
            if not math.isfinite(delay) or delay <= 0:
                raise ValueError('after_seconds must be positive and finite')
            due_at = time.time() + delay
        else:
            try:
                due_at = float(wake_at)
            except (ValueError, TypeError):
                parsed = datetime.fromisoformat(wake_at)
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError('wake_at must include a timezone')
                due_at = parsed.timestamp()
        if not math.isfinite(due_at):
            raise ValueError('wake_at must be finite')
        return self._schedule(thread_id, 'timer', due_at=due_at, idempotency_key=idempotency_key)

    def continue_after_process(self, thread_id: str, process_id: str, idempotency_key: str | None = None) -> dict:
        return self._schedule(thread_id, 'process', process_id=process_id, idempotency_key=idempotency_key)

    def _schedule(self, thread_id: str, kind: str, *, due_at: float | None = None, process_id: str | None = None, idempotency_key: str | None = None) -> dict:
        key = (idempotency_key or '').strip() or None
        automation_id = str(uuid.uuid4())
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            binding = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if binding is None:
                raise ValueError('Unknown T3 thread')
            operation = None
            if kind == 'process':
                operation = db.execute("SELECT * FROM operations WHERE thread_id=? AND process_id=? AND kind='process'", (thread_id, process_id)).fetchone()
                if operation is None:
                    raise ValueError('process_id is unknown or not owned by this Terminal MCP session')
            existing = db.execute('SELECT * FROM automations WHERE thread_id=? AND kind=? AND idempotency_key=?', (thread_id, kind, key)).fetchone()
            if existing:
                return dict(existing)
            if binding['state'] != 'working' or binding['paused']:
                raise ValueError('Thread must be working and unpaused')
            now = time.time()
            due = operation['finished'] is not None if operation else due_at <= now
            db.execute('''INSERT INTO automations(automation_id,thread_id,epoch,kind,due_at,operation_id,status,idempotency_key,created,updated)
              VALUES(?,?,?,?,?,?,?,?,?,?)''', (automation_id, thread_id, binding['epoch'], kind, due_at,
              operation['operation_id'] if operation else None, 'due' if due else 'pending', key, now, now))
        self.arm(thread_id)
        with self.connect() as db:
            return dict(db.execute('SELECT * FROM automations WHERE automation_id=?', (automation_id,)).fetchone())

    def cancel_automation_timer(self, thread_id: str):
        timer = self.automation_timers.pop(thread_id, None)
        if timer:
            timer.cancel()

    def _arm_automations(self, thread_id: str):
        self.cancel_automation_timer(thread_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if row['state'] != 'working' or row['paused']:
                self._stale_automations(db, thread_id)
                return
            now = time.time()
            db.execute("UPDATE automations SET status='stale',updated=? WHERE thread_id=? AND epoch!=? AND status!='stale'", (now, thread_id, row['epoch']))
            db.execute("UPDATE automations SET status='due',updated=? WHERE thread_id=? AND status='pending' AND kind='timer' AND due_at<=?", (now, thread_id, now))
            if not db.execute('SELECT 1 FROM operations WHERE thread_id=? AND finished IS NULL', (thread_id,)).fetchone():
                for automation in db.execute("SELECT * FROM automations WHERE thread_id=? AND status='due'", (thread_id,)).fetchall():
                    event_id = self._event(db, thread_id, 'continue', {'prompt': 'continue', 'reason': 'scheduled_' + automation['kind'], 'stopFirst': True, 'automationId': automation['automation_id']})
                    db.execute("UPDATE automations SET status='delivered',event_id=?,updated=? WHERE automation_id=?", (event_id, now, automation['automation_id']))
            deadline = db.execute("SELECT MIN(due_at) FROM automations WHERE thread_id=? AND status='pending' AND kind='timer'", (thread_id,)).fetchone()[0]
        if deadline is not None:
            self.automation_timers[thread_id] = asyncio.get_running_loop().call_later(max(0, deadline-time.time()), self.arm, thread_id)

    def link_pr(self, thread_id: str, url: str):
        if not re.fullmatch(r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[1-9][0-9]*', url):
            raise ValueError('A full GitHub pull request URL is required')
        with self.connect() as db:
            return self._event(db, thread_id, 'link_pr', {'url': url})

    def cancel_timer(self, thread_id: str):
        timer = self.timers.pop(thread_id, None)
        if timer:
            timer.cancel()

    @staticmethod
    def _pending_continue(db, thread_id: str, epoch: int, revision: int) -> bool:
        # Explicit wakes survive ordinary activity; inactivity wakes do not.
        return db.execute('''SELECT 1 FROM events e
          WHERE e.thread_id=? AND e.kind='continue' AND e.epoch=? AND e.acknowledged=0
          AND (e.revision=? OR EXISTS (
            SELECT 1 FROM automations a WHERE a.event_id=e.event_id
            AND a.epoch=e.epoch AND a.status='delivered'))''', (thread_id, epoch, revision)).fetchone() is not None

    def arm(self, thread_id: str):
        self.cancel_timer(thread_id)
        self._arm_automations(thread_id)
        row = self.get(thread_id)
        with self.connect() as db:
            pending = self._pending_continue(db, thread_id, row['epoch'], row['revision'])
        if row['state'] != 'working' or row['paused'] or self.active(thread_id) or pending:
            return
        deadline = row['last_activity'] + (row['inactivity_seconds'] or self.inactivity_seconds)
        with self.connect() as db:
            db.execute('UPDATE bindings SET deadline=? WHERE thread_id=?', (deadline, thread_id))
        self.timers[thread_id] = asyncio.get_running_loop().call_later(max(0, deadline-time.time()), self.expire, thread_id, row['epoch'], row['revision'])

    def expire(self, thread_id: str, epoch: int, revision: int):
        self.timers.pop(thread_id, None)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if row is None or row['state'] != 'working' or row['paused'] or (row['epoch'], row['revision']) != (epoch, revision):
                return
            if db.execute('SELECT 1 FROM operations WHERE thread_id=? AND finished IS NULL', (thread_id,)).fetchone():
                return
            if self._pending_continue(db, thread_id, epoch, revision):
                return
            self._event(db, thread_id, 'continue', {'prompt': 'continue', 'reason': 'terminal_inactivity', 'stopFirst': True})
            db.execute('UPDATE bindings SET deadline=NULL WHERE thread_id=?', (thread_id,))

    def events(self, environment_id: str, provider_id: str, after_seq: int = 0) -> list[dict]:
        with self.connect() as db:
            rows = db.execute('''SELECT e.*,b.terminal_id,b.conversation_id FROM events e JOIN bindings b USING(thread_id)
              WHERE b.environment_id=? AND b.provider_id=? AND e.seq>? ORDER BY e.seq LIMIT 100''', (environment_id, provider_id, after_seq))
            return [{**dict(r), 'payload': json.loads(r['payload'])} for r in rows]

    def claim(self, event_id: str, environment_id: str, provider_id: str) -> dict:
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT e.*,b.state,b.paused,b.epoch AS current_epoch,b.revision AS current_revision,
              b.environment_id,b.provider_id,a.automation_id,a.status AS automation_status FROM events e
              JOIN bindings b USING(thread_id) LEFT JOIN automations a ON a.event_id=e.event_id WHERE e.event_id=?''', (event_id,)).fetchone()
            if row is None or (row['environment_id'], row['provider_id']) != (environment_id, provider_id):
                raise ValueError('Unknown event')
            valid = not row['acknowledged']
            if row['kind'] == 'continue':
                valid = valid and row['state'] == 'working' and not row['paused'] and row['epoch'] == row['current_epoch']
                if row['automation_id']:
                    valid = valid and row['automation_status'] == 'delivered'
                else:
                    valid = valid and row['revision'] == row['current_revision']
                if valid and db.execute('SELECT 1 FROM operations WHERE thread_id=? AND finished IS NULL', (row['thread_id'],)).fetchone():
                    if row['automation_id'] and row['claimed'] == 0:
                        # The adapter acknowledges even rejected claims. Retry
                        # only unclaimed wakes; claimed replays must retain the
                        # event ID used by adapter dispatch dedupe.
                        db.execute('UPDATE events SET acknowledged=1 WHERE event_id=?', (event_id,))
                        db.execute("UPDATE automations SET status='due',event_id=NULL,updated=? WHERE automation_id=?", (time.time(), row['automation_id']))
                    valid = False
            if valid:
                db.execute('UPDATE events SET claimed=1 WHERE event_id=?', (event_id,))
            return {'valid': bool(valid), 'eventId': event_id}

    def acknowledge(self, event_id: str, environment_id: str, provider_id: str):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT e.* FROM events e JOIN bindings b USING(thread_id)
              WHERE event_id=? AND b.environment_id=? AND b.provider_id=?''', (event_id, environment_id, provider_id)).fetchone()
            if row is None:
                raise ValueError('Unknown event')
            db.execute('UPDATE events SET acknowledged=1 WHERE event_id=?', (event_id,))
            if row['kind'] == 'continue' and not row['acknowledged']:
                db.execute('UPDATE bindings SET last_activity=? WHERE thread_id=?', (time.time(), row['thread_id']))
        self.arm(row['thread_id'])

    def close(self):
        for thread_id in list(self.timers):
            self.cancel_timer(thread_id)
        for thread_id in list(self.automation_timers):
            self.cancel_automation_timer(thread_id)
