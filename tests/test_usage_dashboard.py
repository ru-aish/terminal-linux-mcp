from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from starlette.applications import Starlette
from starlette.testclient import TestClient

from gpt_thread_store import GPTThreadStore
from usage_dashboard import install_usage_dashboard


def make_store(tmp_path: Path, monkeypatch) -> GPTThreadStore:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    home = tmp_path / "gpt-home"
    monkeypatch.setenv("TEST_DASHBOARD_GPT_HOME", str(home))
    store = GPTThreadStore(lambda: workspace, home_env="TEST_DASHBOARD_GPT_HOME")
    store.ensure_layout()
    return store


def test_dashboard_snapshot_separates_exact_estimates_and_counts_tools(tmp_path, monkeypatch):
    store = make_store(tmp_path, monkeypatch)
    store.record_usage(
        "thread-alpha",
        event_type="mcp_tool_loop",
        source="proxy_estimate",
        input_tokens=120,
        output_tokens=30,
        metadata={"tool_name": "run_command"},
    )
    store.record_usage(
        "thread-alpha",
        event_type="mcp_tool_loop",
        source="proxy_estimate",
        input_tokens=80,
        output_tokens=20,
        metadata={"tool_name": "run_command"},
    )
    store.record_usage(
        "thread-beta",
        event_type="mcp_tool_loop",
        source="proxy_estimate",
        input_tokens=40,
        output_tokens=10,
        metadata={"tool_name": "read_file"},
    )
    store.record_usage(
        "thread-beta",
        event_type="model_usage",
        source="provider_reported",
        input_tokens=1000,
        output_tokens=250,
        cached_input_tokens=200,
        is_exact=True,
        model="test-model",
    )

    snapshot = store.dashboard_snapshot(hours=24, event_limit=20)

    assert store.dashboard_version() == snapshot["version"]
    assert snapshot["totals"]["tool_calls"] == 3
    assert snapshot["totals"]["exact_tokens"] == 1250
    assert snapshot["totals"]["estimated_tokens"] == 300
    assert snapshot["totals"]["exact_cached_input_tokens"] == 200
    assert snapshot["totals"]["threads"] == 2
    assert snapshot["window"]["tool_calls"] == 3
    assert snapshot["tools"][0]["name"] == "run_command"
    assert snapshot["tools"][0]["calls"] == 2
    assert {row["thread_id"] for row in snapshot["threads"]} == {
        "thread-alpha",
        "thread-beta",
    }
    assert any(point["estimated_tokens"] == 300 for point in snapshot["series"])
    assert "shown separately" in snapshot["accounting_note"]


def test_dashboard_routes_assets_api_and_cookie_auth(tmp_path, monkeypatch):
    store = make_store(tmp_path, monkeypatch)
    store.record_usage(
        "live-thread",
        event_type="mcp_tool_loop",
        source="proxy_estimate",
        input_tokens=12,
        output_tokens=3,
        metadata={"tool_name": "bootstrap_thread"},
    )
    app = Starlette()
    install_usage_dashboard(app, store)

    with TestClient(app) as client:
        page = client.get("/dashboard")
        assert page.status_code == 200
        assert "Usage ledger" in page.text
        assert page.headers["cache-control"] == "no-store"

        css = client.get("/dashboard/assets/dashboard.css")
        js = client.get("/dashboard/assets/dashboard.js")
        assert css.status_code == 200
        assert "--exact" in css.text
        assert js.status_code == 200
        assert "EventSource" in js.text

        api = client.get("/dashboard/api?hours=6")
        assert api.status_code == 200
        assert api.json()["window"]["hours"] == 6
        assert api.json()["tools"][0]["name"] == "bootstrap_thread"

    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "dashboard-secret")
    protected = Starlette()
    install_usage_dashboard(protected, store)
    with TestClient(protected) as client:
        assert client.get("/dashboard").status_code == 401
        assert client.get("/dashboard/api").status_code == 401
        rejected = client.post(
            "/dashboard/login",
            content="token=wrong",
            headers={"content-type": "application/x-www-form-urlencoded"},
        )
        assert rejected.status_code == 401
        login = client.post(
            "/dashboard/login",
            content="token=dashboard-secret",
            headers={"content-type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert login.status_code == 303
        assert "terminal_usage_dashboard" in login.headers["set-cookie"]
        assert client.get("/dashboard/api").status_code == 200


def test_install_dashboard_is_idempotent(tmp_path, monkeypatch):
    store = make_store(tmp_path, monkeypatch)
    app = Starlette()
    install_usage_dashboard(app, store)
    first_count = len(app.routes)
    install_usage_dashboard(app, store)
    assert len(app.routes) == first_count
