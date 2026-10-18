import asyncio
import json
import uuid
from pathlib import Path

import pytest

from t3_thread_state import ThreadState


def register(state, cwd, thread='thread-one'):
    return state.register(thread_id=thread, environment_id='test-env', provider_id='chatgpt', cwd=str(cwd), model='test-model')


def test_sqlite_connections_are_closed_on_success_and_failure(tmp_path, monkeypatch):
    import sqlite3

    original_connect = sqlite3.connect
    connections = []

    def track_connect(*args, **kwargs):
        db = original_connect(*args, **kwargs)
        connections.append(db)
        return db

    monkeypatch.setattr(sqlite3, 'connect', track_connect)
    state = ThreadState(tmp_path / 'state.db')
    register(state, tmp_path)
    for _ in range(32):
        state.get('thread-one')
        state.require_terminal('thread-one')
    with pytest.raises(RuntimeError, match='probe'):
        with state.connect() as db:
            db.execute('SELECT 1')
            raise RuntimeError('probe')
    assert len(connections) > 60
    for db in connections:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            db.execute('SELECT 1')
    state.close()


def test_immutable_bindings_and_allowlist(tmp_path):
    state = ThreadState(tmp_path/'state.db')
    register(state, tmp_path)
    assert state.require_terminal('thread-one')['conversation_id'] is None
    with pytest.raises(ValueError):
        state.require_terminal('unknown')
    with pytest.raises(ValueError):
        state.require_terminal('default')
    with pytest.raises(ValueError):
        state.register(thread_id='thread-one', terminal_id='other', environment_id='test-env', provider_id='chatgpt', cwd=str(tmp_path), model='test-model')
    cid = str(uuid.uuid4())
    state.bind('thread-one', cid)
    assert state.bind('thread-one', cid)['conversation_id'] == cid
    with pytest.raises(ValueError):
        state.bind('thread-one', str(uuid.uuid4()))
    register(state, tmp_path, 'thread-two')
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        state.bind('thread-two', cid)


def test_final_process_exit_starts_one_deadline(tmp_path):
    async def run():
        state = ThreadState(tmp_path/'state.db', .035)
        register(state, tmp_path)
        state.activate('thread-one', 'request-one')
        state.begin('thread-one', 'short', 'process')
        state.begin('thread-one', 'long', 'process')
        state.finish('short', 'exit=0')
        await asyncio.sleep(.06)
        assert not [e for e in state.events('test-env','chatgpt') if e['kind']=='continue']
        state.finish('long', 'exit=0')
        await asyncio.sleep(.06)
        events = [e for e in state.events('test-env','chatgpt') if e['kind']=='continue']
        assert len(events)==1
        state.expire('thread-one', state.get('thread-one')['epoch'], state.get('thread-one')['revision'])
        assert len([e for e in state.events('test-env','chatgpt') if e['kind']=='continue'])==1
        assert state.claim(events[0]['event_id'],'test-env','chatgpt')['valid']
        state.close()
    asyncio.run(run())


def test_new_activity_invalidates_queued_continue(tmp_path):
    async def run():
        state = ThreadState(tmp_path/'state.db', .015)
        register(state, tmp_path)
        state.activate('thread-one', 'request-one')
        await asyncio.sleep(.03)
        event = [e for e in state.events('test-env','chatgpt') if e['kind']=='continue'][0]
        state.begin('thread-one', 'new-tool', 'tool')
        assert not state.claim(event['event_id'],'test-env','chatgpt')['valid']
        state.finish('new-tool','returned')
        state.close()
    asyncio.run(run())


def test_stop_and_done_disable_recovery_and_done_requires_no_work(tmp_path):
    async def run():
        state = ThreadState(tmp_path/'state.db', .015)
        register(state, tmp_path)
        state.activate('thread-one', 'request-one')
        state.pause('thread-one', 'user-stop')
        await asyncio.sleep(.03)
        assert state.get('thread-one')['state']=='working'
        assert not [e for e in state.events('test-env','chatgpt') if e['kind']=='continue']
        state.activate('thread-one', 'request-two')
        state.begin('thread-one', 'running', 'process')
        with pytest.raises(ValueError, match='still active'):
            state.complete('thread-one',['tested'])
        state.finish('running','exit=0')
        with pytest.raises(ValueError, match='evidence'):
            state.complete('thread-one',[])
        state.complete('thread-one',['tested'])
        await asyncio.sleep(.03)
        assert state.get('thread-one')['state']=='done'
        assert not [e for e in state.events('test-env','chatgpt') if e['kind']=='continue']
        state.close()
    asyncio.run(run())


def test_replay_and_request_idempotency(tmp_path):
    async def run():
        path=tmp_path/'state.db'
        state=ThreadState(path,.015)
        register(state,tmp_path)
        state.activate('thread-one','request-one')
        state.activate('thread-one','request-one')
        assert state.get('thread-one')['epoch']==1
        with pytest.raises(ValueError):
            state.pause('thread-one','request-one')
        await asyncio.sleep(.03)
        state.close()
        restarted=ThreadState(path,.015)
        assert len(restarted.events('test-env','chatgpt'))==2
        event=restarted.events('test-env','chatgpt')[-1]
        assert not restarted.events('other-env','chatgpt')
        with pytest.raises(ValueError):
            restarted.claim(event['event_id'],'other-env','chatgpt')
        restarted.acknowledge(event['event_id'],'test-env','chatgpt')
        assert not restarted.claim(event['event_id'],'test-env','chatgpt')['valid']
        restarted.close()
    asyncio.run(run())


def test_real_tools_are_gated_and_background_exit_is_observed(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv('MCP_GPT_HOME',str(tmp_path/'gpt-home'))
        import terminal_mcp as core
        from t3_terminal_server import build
        for name in ('mcp', '_spawn_workload', '_set_startup_instructions', '_core_behavior_prompt'):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, 'WORKSPACE_DIR', tmp_path)
        state=ThreadState(tmp_path/'state.db', .03)
        mcp,private=build(core,state,'test-control-token')
        names={t.name for t in await mcp.list_tools()}
        assert 'bootstrap_thread' in names and 'thread_task' in names
        assert not {'agent_register_parent','agent_spawn','chat_runtime_status'} & names
        denied=await mcp.call_tool('run_command',{'command':'touch forbidden','session_id':'unknown'})
        assert not (tmp_path/'forbidden').exists()
        assert 'refused' in str(denied)
        register(state,tmp_path)
        state.activate('thread-one','request-one')
        await mcp.call_tool('bootstrap_thread',{'thread_id':'thread-one','cwd':str(tmp_path),'max_chars':0})
        result=await mcp.call_tool('start_process',{'session_id':'thread-one','command':'sleep 0.07','cwd':str(tmp_path)})
        assert 'Process ID' in str(result)
        assert state.active('thread-one')
        process = next(p.process for p in core.processes.values() if p.session_id == 'thread-one')
        await asyncio.wait_for(process.wait(), 10)
        await asyncio.sleep(.07)
        assert not state.active('thread-one')
        assert any(e['kind']=='continue' for e in state.events('test-env','chatgpt'))
        await mcp.call_tool('thread_task',{'session_id':'thread-one','action':'complete','evidence':['process exited']})
        assert state.get('thread-one')['state']=='done'
        state.close()
    asyncio.run(run())


def test_t3_public_app_mounts_usage_dashboard_without_legacy_features(tmp_path, monkeypatch):
    monkeypatch.setenv('MCP_GPT_HOME', str(tmp_path/'gpt-home'))
    import terminal_mcp as core
    from t3_terminal_server import build, build_public_app

    for name in ('mcp', '_spawn_workload', '_set_startup_instructions', '_core_behavior_prompt'):
        monkeypatch.setattr(core, name, getattr(core, name))
    monkeypatch.setattr(core, 'WORKSPACE_DIR', tmp_path)
    state = ThreadState(tmp_path/'state.db')
    mcp, _private = build(core, state, 'test-control-token')
    app = build_public_app(core, mcp)
    paths = {getattr(route, 'path', '') for route in app.routes}

    assert '/dashboard' in paths
    assert '/dashboard/api' in paths
    assert '/dashboard/events' in paths
    assert '/dashboard/assets/{name:str}' in paths
    state.close()


def test_t3_mcp_credentials_are_private_durable_and_replaceable(tmp_path):
    path = tmp_path / 'state.db'
    state = ThreadState(path)
    state.register(
        thread_id='thread-one', environment_id='test-env', provider_id='chatgpt',
        cwd=str(tmp_path), model='test-model',
        t3_mcp_endpoint='http://127.0.0.1:43123/mcp',
        t3_mcp_authorization='Bearer first-secret',
    )
    assert 'first-secret' not in json.dumps(state.get('thread-one'))
    assert state.t3_mcp_credentials('thread-one') == {
        'endpoint': 'http://127.0.0.1:43123/mcp',
        'authorization': 'Bearer first-secret',
    }
    state.close()

    restarted = ThreadState(path)
    assert restarted.t3_mcp_credentials('thread-one')['authorization'] == 'Bearer first-secret'
    restarted.register(
        thread_id='thread-one', environment_id='test-env', provider_id='chatgpt',
        cwd=str(tmp_path), model='test-model',
        t3_mcp_endpoint='http://localhost:43124/mcp',
        t3_mcp_authorization='Bearer second-secret',
    )
    assert restarted.t3_mcp_credentials('thread-one')['authorization'] == 'Bearer second-secret'
    with pytest.raises(ValueError, match='loopback'):
        restarted.register(
            thread_id='thread-one', environment_id='test-env', provider_id='chatgpt',
            cwd=str(tmp_path), model='test-model',
            t3_mcp_endpoint='https://example.com/mcp',
            t3_mcp_authorization='Bearer nope',
        )
    restarted.register(
        thread_id='thread-one', environment_id='test-env', provider_id='chatgpt',
        cwd=str(tmp_path), model='test-model',
    )
    assert restarted.t3_mcp_credentials('thread-one') is None
    restarted.close()


def test_t3_owned_subagent_tools_are_stateless_proxies(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv('MCP_GPT_HOME', str(tmp_path / 'gpt-home'))
        import terminal_mcp as core
        from t3_terminal_server import build

        for name in ('mcp', '_spawn_workload', '_set_startup_instructions', '_core_behavior_prompt', 'PUBLIC_TOOL_ORDER'):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, 'WORKSPACE_DIR', tmp_path)
        calls = []

        async def fake_with_mcp_session(session_id, server_name, config, operation, tool_name='', arguments=None, meta=None, elicitation_callback=None):
            calls.append((session_id, server_name, config, operation, tool_name, arguments))
            payloads = {
                'orchestrator_capabilities': {'features': {'appOwnedSubagents': True}, 'providers': []},
                'delegate_task': {
                    'taskId': 'task-one',
                    'childThreadId': 'child-one',
                    'status': 'running',
                    'providerInstanceId': 'chatgpt',
                    'model': 'gpt-test',
                },
                'task_status': {
                    'taskId': 'task-one',
                    'childThreadId': 'child-one',
                    'status': 'running',
                    'summary': None,
                },
                't3_thread_send': {
                    'threadId': 'child-one',
                    'taskId': 'task-two',
                    'status': 'running',
                    'delivery': 'started',
                },
                'task_cancel': {'taskId': 'task-one', 'status': 'cancel_requested'},
                't3_thread_read': {'thread': {'id': 'thread-one'}, 'items': []},
            }
            return core.mcp_types.CallToolResult(
                content=[core.mcp_types.TextContent(type='text', text=json.dumps(payloads[tool_name]))]
            )

        monkeypatch.setattr(core, '_with_mcp_session', fake_with_mcp_session)
        state = ThreadState(tmp_path / 'state.db')
        state.register(
            thread_id='thread-one', environment_id='test-env', provider_id='chatgpt',
            cwd=str(tmp_path), model='test-model',
            t3_mcp_endpoint='http://127.0.0.1:43123/mcp',
            t3_mcp_authorization='Bearer private-token',
        )
        mcp, _private = build(core, state, 'test-control-token')
        names = {tool.name for tool in await mcp.list_tools()}
        assert {'agents_available','spawn_agent','agent_status','send_input','close_agent','list_agents'} <= names
        assert 'agent_spawn' not in names and 'agent_register_parent' not in names

        def payload(result):
            content = result[0] if isinstance(result, tuple) else result.content
            text = next(item.text for item in content if getattr(item, 'text', None))
            return json.loads(text)

        available = payload(await mcp.call_tool('agents_available', {'session_id': 'thread-one'}))
        assert available['features']['appOwnedSubagents'] is True

        spawned = payload(await mcp.call_tool('spawn_agent', {
            'session_id': 'thread-one',
            'task': 'review this',
            'provider_instance_id': 'chatgpt',
            'model': 'gpt-test',
            'client_request_id': 'spawn-one',
        }))
        assert spawned == {
            'agent_id': 'task-one',
            'task_id': 'task-one',
            'child_thread_id': 'child-one',
            'status': 'running',
            'provider_instance_id': 'chatgpt',
            'model': 'gpt-test',
        }
        delegate = next(call for call in calls if call[4] == 'delegate_task')
        assert delegate[5]['mode'] == 'async'
        assert delegate[5]['target'] == {'providerInstanceId': 'chatgpt', 'model': 'gpt-test'}
        assert delegate[2]['headers']['Authorization'] == 'Bearer private-token'

        status = payload(await mcp.call_tool('agent_status', {
            'session_id': 'thread-one', 'agent_id': 'task-one',
        }))
        assert status['childThreadId'] == 'child-one'

        sent = payload(await mcp.call_tool('send_input', {
            'session_id': 'thread-one', 'agent_id': 'task-one', 'message': 'also test edge cases',
        }))
        assert sent['agent_id'] == 'task-two'
        send_call = next(call for call in calls if call[4] == 't3_thread_send')
        assert send_call[5]['threadId'] == 'child-one'
        assert send_call[5]['message'] == 'also test edge cases'

        cancelled = payload(await mcp.call_tool('close_agent', {
            'session_id': 'thread-one', 'agent_id': 'task-one',
        }))
        assert cancelled['status'] == 'cancel_requested'
        listing = payload(await mcp.call_tool('list_agents', {'session_id': 'thread-one'}))
        assert listing['thread']['id'] == 'thread-one'

        with state.connect() as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert 'subagent_links' not in tables
        state.close()

    asyncio.run(run())
