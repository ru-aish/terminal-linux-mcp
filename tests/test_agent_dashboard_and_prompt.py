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
            },
            "next_eligible_at": 1020.0,
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
                },
                {
                    "agent_id": "agent-child",
                    "parent_agent_id": "agent-root",
                    "title": "Child",
                    "status": "running",
                    "gateway_state": "RUNNING",
                    "task": {"status": "running"},
                    "pending_mailbox": 1,
                },
            ],
            "request_summary": [],
            "requests": [],
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
    )
    with TestClient(app) as client:
        page = client.get("/dashboard/agents")
        assert page.status_code == 200
        assert "Every request has a place in line" in page.text
        assert "width=device-width" in page.text
        script = client.get("/dashboard/assets/agents.js").text
        assert "/dashboard/agents/api" in script
        assert "requestSummary" in script
        assert ' : "idle"' in script
        css = client.get("/dashboard/assets/agents.css")
        assert css.status_code == 200
        assert "min-width: 320px" in css.text
        assert "@media (min-width: 700px)" in css.text
        payload = client.get("/dashboard/agents/api").json()
        assert payload["capacity"]["maximum_active_children"] == 5
        assert payload["operations"][0]["type"] == "INSPECT"

    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "agent-secret")
    protected = Starlette()
    install_usage_dashboard(
        protected,
        make_store(tmp_path / "protected", monkeypatch),
        agent_coordinator=DashboardCoordinator(),
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
