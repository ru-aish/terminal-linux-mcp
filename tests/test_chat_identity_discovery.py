import asyncio
import json
from pathlib import Path
import sys
from dataclasses import replace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from chat_identity_discovery import ChatIdentityDiscovery, CodexIdentityMatcher
from chat_gateway import (ChatGateway, FakeBackend, FakeClock, SQLiteLedger, ThreadSnapshot,
                          TurnSnapshot, InfrastructureError, OperationState)
from gpt_thread_store import GPTThreadStore
from test_gateway_agent_orchestrator import fast_config

MESSAGE = "Please find the trained PrivacyAI model location and compare the recorded benchmark results with the base model."


class ListingBackend(FakeBackend):
    def __init__(self):
        super().__init__()
        self.pages = {None: {"items": [], "cursor": None}}

    async def list_discovery_threads(self, **kwargs):
        self._record("list_discovery_threads", kwargs)
        value = self.pages[kwargs.get("cursor") if kwargs.get("project_id") else kwargs.get("offset", 0)]
        if isinstance(value, Exception):
            raise value
        return value


def make(tmp_path, monkeypatch, matcher=None, *, scopes=("project",)):
    monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
    clock = FakeClock(1000)
    store = GPTThreadStore(lambda: tmp_path)
    backend = ListingBackend()
    ledger = SQLiteLedger(tmp_path / "gateway.db")
    gateway = ChatGateway(ledger, backend, fast_config(tmp_path / "gateway.db"), clock=clock)
    managed = {"ids": set(), "pending": False}
    async def deterministic_matcher(events, candidates):
        return [{"event_id": e["event_id"], "conversation_id": c["conversation_id"], "confidence": 1}
                for e in events for c in candidates if e["exact_user_message"] == c["snippet"]]
    discovery = ChatIdentityDiscovery(store, gateway, scopes=list(scopes),
                managed=lambda: (managed["ids"], managed["pending"]), matcher=matcher, clock=clock.now)
    if matcher is None:
        discovery.matcher = deterministic_matcher
    return discovery, store, gateway, backend, clock, managed


def item(cid="new-chat", message=MESSAGE):
    return {"conversation_id": cid, "project_id": "project", "title": "New chat", "snippet": message[:90]}


def install_chat(backend, cid="new-chat", message=MESSAGE):
    backend.set_snapshot(ThreadSnapshot(cid, True, True, (
        TurnSnapshot("user-" + cid, "user", "finished_successfully", message),
        TurnSnapshot("assistant-" + cid, "assistant", "in_progress", "", False),
    ), "New chat", "assistant-" + cid), project_id="project")


def token(response):
    return response.splitlines()[0].split(": ", 1)[1]


async def steps(discovery, clock, count=25):
    for _ in range(count):
        await discovery.run_once()
        clock.advance(2)


def run(coroutine):
    return asyncio.run(coroutine)


def test_cached_new_chat_complete_handoff_and_restart_no_second_message(tmp_path, monkeypatch):
    d, store, gateway, backend, clock, managed = make(tmp_path, monkeypatch)
    d.seed_known("project", ["old-chat"])
    d.observe("project", [item()])
    install_chat(backend)
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock))
    assert d.event(eid)["state"] == "confirmed"
    assert store.binding_for_conversation("new-chat")["thread_id"] == GPTThreadStore.derive_thread_id("new-chat")
    assert [c.method for c in backend.calls].count("list_discovery_threads") == 0
    assert [c.method for c in backend.calls].count("cancel_thread") == 1
    assert [c.method for c in backend.calls].count("continue_thread") == 1
    assert [c.method for c in backend.calls].count("get_thread") == 2  # stopped parent + handoff verification
    assert not gateway.ledger.list_operations(state=OperationState.PENDING)
    handoff = backend.threads["new-chat"].turns[-2].text
    assert "get_thread_context" in handoff and "Bootstrap is forbidden" in handoff
    d2 = ChatIdentityDiscovery(store, gateway, scopes=["project"], managed=lambda: (set(), False), clock=clock.now)
    assert "permanent" in d2.submit(MESSAGE, event_id=eid, scope="project")
    run(steps(d2, clock, 3))
    assert d2.event(eid)["state"] == "confirmed"
    assert [c.method for c in backend.calls].count("continue_thread") == 1


def test_cache_miss_scans_once_and_exact_preview_avoids_match_reads(tmp_path, monkeypatch):
    d, store, gateway, backend, clock, _ = make(tmp_path, monkeypatch)
    d.seed_known("project", ["old-chat"])
    backend.pages[None] = {"items": [item()], "cursor": None}
    install_chat(backend)
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock))
    assert d.event(eid)["state"] == "confirmed"
    assert [c.method for c in backend.calls].count("list_discovery_threads") == 1
    assert [c.method for c in backend.calls].count("get_thread") == 2


def test_older_match_returns_saved_id_without_scan_stop_or_message(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    d.observe("project", [item("older")])  # first complete listing is a baseline
    bound = store.ensure_chat_binding("older", source="desktop_preview")
    eid = token(d.submit(MESSAGE, scope="project"))
    assert bound["thread_id"] in d.response(eid)
    run(steps(d, clock, 2))
    assert d.event(eid)["state"] == "old"
    assert not backend.calls


def test_first_baseline_and_no_new_chat_never_message_existing_chats(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    backend.pages[None] = {"items": [item("existing")], "cursor": None}
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 5))
    assert d.event(eid)["state"] == "old"
    assert store.binding_for_conversation("existing") is None
    assert [c.method for c in backend.calls] == ["list_discovery_threads"]


def test_missing_preview_no_new_chat_returns_exact_message_guidance(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    d.seed_known("project", ["old-chat"])
    eid = token(d.submit("Find something unrelated", scope="project"))
    run(steps(d, clock, 5))
    assert d.event(eid)["state"] == "unmatched"
    assert "copied verbatim" in d.response(eid)
    assert [c.method for c in backend.calls] == ["list_discovery_threads"]


def test_identical_requests_in_two_chats_are_not_guessed(tmp_path, monkeypatch):
    calls = []
    async def matcher(events, candidates):
        calls.append((events, candidates))
        return [{"event_id": events[0]["event_id"], "conversation_id": candidates[0]["conversation_id"], "confidence": 1}]
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch, matcher)
    d.seed_known("project", [])
    d.observe("project", [item("chat-a"), item("chat-b")])
    install_chat(backend, "chat-a")
    install_chat(backend, "chat-b")
    e1 = token(d.submit(MESSAGE, scope="project"))
    e2 = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 10))
    assert len(calls) == 1
    assert d.event(e1)["state"] == d.event(e2)["state"] == "matcher_failed"
    assert not store.binding_for_conversation("chat-a")
    assert not store.binding_for_conversation("chat-b")
    assert [c.method for c in backend.calls] == ["get_thread", "get_thread"]


def test_full_candidate_reads_disambiguate_truncated_common_preview(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    prefix = "Please analyze the new benchmark results for the trained model carefully and thoroughly. "
    a, b = prefix + "Compare the red model.", prefix + "Compare the blue model."
    d.seed_known("project", [])
    d.observe("project", [item("a", prefix), item("b", prefix)])
    install_chat(backend, "a", a)
    install_chat(backend, "b", b)
    eid = token(d.submit(a, scope="project"))
    run(steps(d, clock))
    assert d.event(eid)["state"] == "confirmed"
    assert store.binding_for_conversation("a")
    assert store.binding_for_conversation("b") is None
    assert [c.method for c in backend.calls].count("get_thread") == 4


def test_managed_children_and_pending_creates_excluded(tmp_path, monkeypatch):
    d, store, _, backend, clock, managed = make(tmp_path, monkeypatch)
    d.seed_known("project", [])
    managed["ids"] = {"child"}
    d.observe("project", [item("child")])
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 3))
    assert d.event(eid)["state"] == "unmatched"
    assert not store.binding_for_conversation("child")
    assert not backend.calls
    managed["pending"] = True
    eid2 = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 3))
    assert d.event(eid2)["state"] == "pending"
    assert not backend.calls


def test_partial_failed_scan_does_not_replace_baseline(tmp_path, monkeypatch):
    d, store, gateway, backend, clock, _ = make(tmp_path, monkeypatch)
    backend.pages[None] = {"items": [item()], "cursor": "next"}
    backend.pages["next"] = ValueError("bad page")
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 12))
    assert d.event(eid)["state"] == "scan_failed"
    assert d._rows("SELECT initialized FROM identity_scopes")[0]["initialized"] == 0
    assert not d._rows("SELECT * FROM identity_previews")
    assert not store.binding_for_conversation("new-chat")


def test_changed_latest_user_message_keeps_id_but_does_not_send(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    d.seed_known("project", [])
    d.observe("project", [item()])
    install_chat(backend, message="The user changed the task after discovery.")
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 10))
    assert d.event(eid)["state"] == "target_changed"
    assert store.binding_for_conversation("new-chat")
    assert not any(c.method == "continue_thread" for c in backend.calls)


def test_uncertain_handoff_outage_and_expired_claim_never_resubmit(tmp_path, monkeypatch):
    d, store, gateway, backend, clock, _ = make(tmp_path, monkeypatch)
    d.seed_known("project", [])
    d.observe("project", [item()])
    install_chat(backend)
    backend.queue("continue_thread", InfrastructureError("submission response lost"))
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 12))
    assert d.event(eid)["state"] == "uncertain"
    assert [c.method for c in backend.calls].count("continue_thread") == 1
    aid = gateway.register_existing(project_id="project", conversation_id="another")
    oid = gateway.enqueue_continue(agent_id=aid, message="identity", maximum_attempts=1)
    with gateway.ledger.transaction() as db:
        gateway.ledger.claim_operation(db, operation_id=oid, claim_token="test", claim_expires_at=clock.now()+1, now=clock.now())
    clock.advance(2)
    with gateway.ledger.transaction() as db:
        gateway.ledger.recover_expired_claims(db, clock.now())
    assert gateway.ledger.get_operation(oid).state is OperationState.FAILED


def test_retry_token_cannot_change_a_pending_or_completed_request(tmp_path, monkeypatch):
    d, *_ = make(tmp_path, monkeypatch)
    eid = token(d.submit(MESSAGE, scope="project"))
    assert "cannot be reused" in d.submit("different", event_id=eid, scope="project")
    assert "unknown" in d.submit(MESSAGE, event_id="invented", scope="project")
    assert "configured" in d.submit(MESSAGE, scope="wrong-project")


def test_matcher_output_invented_duplicate_and_uncertain_ids_rejected():
    events = [{"event_id": "e", "exact_message": MESSAGE, "scope": "project"}]
    candidates = [dict(item(), scope="project")]
    for match in [
        {"event_id": "invented", "conversation_id": "new-chat", "confidence": 1},
        {"event_id": "e", "conversation_id": "new-chat", "confidence": .8},
        {"event_id": "e", "conversation_id": "new-chat", "confidence": True},
    ]:
        try:
            ChatIdentityDiscovery._validate_matches([match], events, candidates)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid matcher result was accepted")


def test_desktop_listing_preserves_preview_and_iso_time_without_thread_reads(monkeypatch):
    from chat_gateway.adapters.codex_renderer import CodexRendererBackend
    backend = CodexRendererBackend()
    expressions = []
    def evaluate(expression):
        expressions.append(expression)
        return {"items": [{"id": "chat", "title": "New chat", "snippet": MESSAGE, "create_time": "2026-10-07T00:00:00Z", "update_time": "2026-10-08T00:00:00Z"}], "cursor": None}
    monkeypatch.setattr(backend, "_evaluate", evaluate)
    value = run(backend.list_discovery_threads(project_id="project"))
    assert value["items"][0]["snippet"] == MESSAGE
    assert value["items"][0]["update_time"] == "2026-10-08T00:00:00Z"
    assert len(expressions) == 1 and "listProjectConversations" in expressions[0]
    assert "client.get(" not in expressions[0]


def test_watchdog_prefix_can_update_initialized_cache_but_cannot_set_baseline(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    d.observe("project", [item()], complete=False)
    assert not d._rows("SELECT * FROM identity_previews")
    d.seed_known("project", ["old-chat"])
    d.observe("project", [item()], complete=False)
    assert d._rows("SELECT is_new FROM identity_previews WHERE conversation_id='new-chat'")[0]["is_new"] == 1
    assert d._rows("SELECT last_scan FROM identity_scopes")[0]["last_scan"] == 0


def test_new_bootstrap_during_luna_inference_stays_for_next_batch(tmp_path, monkeypatch):
    seen = []
    second_message = MESSAGE + " Then export the blue model results."
    async def matcher(events, candidates):
        seen.append([e["event_id"] for e in events])
        if len(seen) == 1:
            second_event.append(token(d.submit(second_message, scope="project")))
        return [{"event_id": e["event_id"], "conversation_id": c["conversation_id"], "confidence": 1}
                for e in events for c in candidates if e["exact_user_message"] == c["snippet"]]
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch, matcher)
    d.seed_known("project", [])
    d.observe("project", [item("a", ""), item("b", "")])
    install_chat(backend, "a", MESSAGE)
    install_chat(backend, "b", second_message)
    first = token(d.submit(MESSAGE, scope="project"))
    second_event = []
    async def drive():
        for _ in range(8):
            await d.run_once()
            clock.advance(2)
            if seen:
                break
    run(drive())
    assert seen == [[first]]
    assert d.event(second_event[0])["state"] == "pending"
    assert store.binding_for_conversation("a")
    assert store.binding_for_conversation("b") is None
    run(steps(d, clock))
    assert len(seen) == 2 and seen[1] == second_event
    assert d.event(first)["state"] == d.event(second_event[0])["state"] == "confirmed"


def test_repeated_pagination_cursor_fails_without_committing_any_previews(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    backend.pages[None] = {"items": [item()], "cursor": "repeat"}
    backend.pages["repeat"] = {"items": [item("second")], "cursor": "repeat"}
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 8))
    assert d.event(eid)["state"] == "scan_failed"
    assert not d._rows("SELECT * FROM identity_previews")
    assert not store.binding_for_conversation("new-chat")


def test_very_short_common_message_is_not_identity_evidence():
    from chat_identity_discovery import preview_matches
    assert not preview_matches("continue", "continue")
    assert preview_matches(MESSAGE, MESSAGE[:90])


def test_tool_turn_after_running_assistant_does_not_look_stopped():
    from chat_gateway.adapters.codex_renderer import _normalize_conversation
    raw = {"id": "chat", "current_node": "tool", "mapping": {
        "user": {"parent": None, "message": {"id": "user", "author": {"role": "user"}, "content": {"parts": [MESSAGE]}, "status": "finished_successfully"}},
        "assistant": {"parent": "user", "message": {"id": "assistant", "author": {"role": "assistant"}, "content": {"parts": [""]}, "status": "in_progress", "end_turn": False}},
        "tool": {"parent": "assistant", "message": {"id": "tool", "author": {"role": "tool"}, "content": {"parts": ["bootstrap requested"]}, "status": "finished_successfully"}},
    }}
    assert _normalize_conversation(raw, "chat").running


def test_general_listing_pagination_uses_offsets_with_null_cursor(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch, scopes=[""])
    d.seed_known("", ["old"])
    new = dict(item(), project_id="")
    backend.pages[0] = {"items": [new], "cursor": None, "next_offset": 50}
    backend.pages[50] = {"items": [], "cursor": None, "next_offset": None}
    install_chat(backend)
    eid = token(d.submit(MESSAGE, scope=""))
    run(steps(d, clock))
    assert d.event(eid)["state"] == "confirmed"
    assert [c.arguments['offset'] for c in backend.calls if c.method=='list_discovery_threads'] == [0, 50]


def test_non_adjacent_cursor_cycle_is_rejected_after_three_pages(tmp_path, monkeypatch):
    d, store, _, backend, clock, _ = make(tmp_path, monkeypatch)
    backend.pages[None] = {"items": [item()], "cursor": "a"}
    backend.pages["a"] = {"items": [], "cursor": "b"}
    backend.pages["b"] = {"items": [], "cursor": "a"}
    eid = token(d.submit(MESSAGE, scope="project"))
    run(steps(d, clock, 10))
    assert d.event(eid)["state"] == "scan_failed"
    assert [c.method for c in backend.calls].count('list_discovery_threads') == 3
    assert not d._rows('SELECT * FROM identity_previews')
