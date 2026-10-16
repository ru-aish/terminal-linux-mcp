from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from starlette.applications import Starlette
from starlette.testclient import TestClient

import terminal_mcp
from gpt_thread_store import GPTThreadStore
from usage_dashboard import install_usage_dashboard


class DashboardCoordinator:
    def dashboard_snapshot(self):
        return {
            "generated_at": 1000.0,
            "capacity": {"active_children": 2, "maximum_active_children": 5},
            "counts": {
                "total": 3,
                "active": 2,
                "terminal": 1,
                "queued_operations": 1,
                "active_automations": 2,
                "scheduled_wakeups": 1,
                "completion_triggers": 1,
            },
            "next_eligible_at": 1020.0,
            "next_wakeup_at": 1010.0,
            "reasoning": {
                "model": "gpt-5.6-terra",
                "thinking_effort": "extended",
                "require_high_reasoning": True,
            },
            "circuits": [
                {
                    "scope": "conversation",
                    "state": "CLOSED",
                    "retry_at": None,
                    "probe_failures": 0,
                    "half_open_successes": 0,
                }
            ],
            "operations": [
                {
                    "id": "op-1",
                    "agent_id": "agent-child",
                    "type": "INSPECT",
                    "lane": "conversation_read",
                    "state": "PENDING",
                    "due_at": 1020.0,
                    "attempts": 0,
                    "last_error": None,
                }
            ],
            "agents": [
                {
                    "agent_id": "agent-root",
                    "parent_agent_id": None,
                    "title": "Root",
                    "status": "registered",
                    "gateway_state": "UNKNOWN",
                    "task": None,
                    "pending_mailbox": 0,
                    "depth": 0,
                    "path": ["agent-root"],
                    "children_count": 1,
                },
                {
                    "agent_id": "agent-child",
                    "parent_agent_id": "agent-root",
                    "title": "Child",
                    "status": "running",
                    "gateway_state": "RUNNING",
                    "task": {"status": "running"},
                    "pending_mailbox": 1,
                    "depth": 1,
                    "path": ["agent-root", "agent-child"],
                    "children_count": 0,
                },
            ],
            "automations": [
                {
                    "automation_id": "auto-wake",
                    "kind": "wakeup",
                    "target_agent_id": "agent-root",
                    "target_title": "Root",
                    "message": "Review children",
                    "due_at": 1010.0,
                    "status": "scheduled",
                },
                {
                    "automation_id": "auto-gate",
                    "kind": "after_completion",
                    "source_agent_id": "agent-child",
                    "source_title": "Child",
                    "target_agent_id": "agent-root",
                    "target_title": "Root",
                    "message": "Synthesize",
                    "completion_marker": "DONE",
                    "status": "waiting",
                },
            ],
            "request_summary": [],
            "requests": [],
        }


class DashboardService:
    state = {
        "enabled": True,
        "running": True,
        "last_sync_at": 999.0,
        "last_result": {"physical_requests": 1},
        "last_error": "",
    }


def make_store(tmp_path: Path, monkeypatch) -> GPTThreadStore:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True)
    home = tmp_path / "gpt-home"
    monkeypatch.setenv("TEST_AGENT_DASHBOARD_HOME", str(home))
    store = GPTThreadStore(lambda: workspace, home_env="TEST_AGENT_DASHBOARD_HOME")
    store.ensure_layout()
    return store


def test_agent_dashboard_page_api_assets_and_auth(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_DASHBOARD_TOKEN", raising=False)
    app = Starlette()
    install_usage_dashboard(
        app,
        make_store(tmp_path, monkeypatch),
        agent_coordinator=DashboardCoordinator(),
        agent_service=DashboardService(),
    )
    with TestClient(app) as client:
        page = client.get("/dashboard/agents")
        assert page.status_code == 200
        assert "See who is working, sleeping, and next in line" in page.text
        assert "width=device-width" in page.text
        script = client.get("/dashboard/assets/agents.js").text
        assert "/dashboard/agents/api" in script
        assert "renderAutomations" in script
        assert "next_wakeup_at" in script
        css = client.get("/dashboard/assets/agents.css")
        assert css.status_code == 200
        assert "min-width: 320px" in css.text
        assert "@media (min-width: 700px)" in css.text
        payload = client.get("/dashboard/agents/api").json()
        assert payload["capacity"]["maximum_active_children"] == 5
        assert payload["operations"][0]["type"] == "INSPECT"
        assert payload["sync_service"]["running"] is True
        assert payload["reasoning"]["thinking_effort"] == "extended"
        assert payload["automations"][1]["completion_marker"] == "DONE"

    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "agent-secret")
    protected = Starlette()
    install_usage_dashboard(
        protected,
        make_store(tmp_path / "protected", monkeypatch),
        agent_coordinator=DashboardCoordinator(),
        agent_service=DashboardService(),
    )
    with TestClient(protected) as client:
        assert client.get("/dashboard/agents").status_code == 401
        assert client.get("/dashboard/agents/api").status_code == 401
        assert (
            client.get(
                "/dashboard/agents/api",
                headers={"authorization": "Bearer agent-secret"},
            ).status_code
            == 200
        )


def test_core_behavior_precedes_agents_and_mentions_ui_skills():
    startup = terminal_mcp._build_startup_instructions()
    assert startup.index("## Terminal MCP core behavior") < startup.index(
        "## Mandatory .GPT instructions"
    )
    behavior = terminal_mcp._core_behavior_prompt()
    assert "frontend-design" in behavior
    assert "user-html-ui-preference" in behavior
    assert "Investigate before changing code" in behavior
    assert "sub-agent" not in behavior.lower()
