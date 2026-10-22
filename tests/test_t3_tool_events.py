"""Tool observability is part of the deployed T3 protocol, not a local patch."""
import asyncio

import pytest

from test_t3_durable_run_command import setup


def tool_events(state):
    return [event for event in state.events('test-env', 'chatgpt')
            if event['kind'] in ('tool_started', 'tool_completed')]


def test_bootstrap_and_tool_results_emit_durable_correlated_events(tmp_path, monkeypatch):
    async def scenario():
        _, _, state, mcp, session_id = await setup(tmp_path, monkeypatch)
        bootstrap = tool_events(state)
        assert [event['kind'] for event in bootstrap] == ['tool_started', 'tool_completed']
        assert bootstrap[0]['payload']['toolName'] == 'fedora_t3code.bootstrap_thread'

        async def observable_probe(session_id: str, value: str):
            return 'verified ' + value

        mcp.add_tool(observable_probe)
        result = await mcp.call_tool('observable_probe',
                                     {'session_id': session_id, 'value': 'result'})
        assert 'verified result' in result[0].text
        started, completed = tool_events(state)[-2:]
        assert started['payload']['operationId'] == completed['payload']['operationId']
        assert started['payload']['input']['value'] == 'result'
        assert completed['payload']['status'] == 'completed'
        assert 'verified result' in str(completed['payload']['output'])
        from t3_thread_state import ThreadState
        reopened = ThreadState(state.path)
        assert tool_events(reopened)[-1]['event_id'] == completed['event_id']
        reopened.close()
        state.close()
    asyncio.run(scenario())


def test_tool_failure_and_cancellation_emit_terminal_events(tmp_path, monkeypatch):
    async def scenario():
        _, _, state, mcp, session_id = await setup(tmp_path, monkeypatch)
        entered = asyncio.Event()

        async def failed_probe(session_id: str):
            raise ValueError('deliberate probe failure')

        async def cancelled_probe(session_id: str):
            entered.set()
            await asyncio.Future()

        mcp.add_tool(failed_probe)
        mcp.add_tool(cancelled_probe)
        with pytest.raises(Exception, match='deliberate probe failure'):
            await mcp.call_tool('failed_probe', {'session_id': session_id})
        assert tool_events(state)[-1]['payload']['status'] == 'failed'
        pending = asyncio.create_task(mcp.call_tool('cancelled_probe', {'session_id': session_id}))
        await entered.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert tool_events(state)[-1]['payload']['status'] == 'cancelled'
        assert not state.blocking_active(session_id)
        state.close()
    asyncio.run(scenario())


def test_tool_events_redact_environment_values(tmp_path, monkeypatch):
    async def scenario():
        _, _, state, mcp, session_id = await setup(tmp_path, monkeypatch)

        async def redaction_probe(session_id: str, env: dict[str, str]):
            return 'updated'

        mcp.add_tool(redaction_probe)
        await mcp.call_tool('redaction_probe', {'session_id': session_id,
                                              'env': {'TOKEN': 'private-value'}})
        started = tool_events(state)[-2]
        assert started['payload']['input']['env'] == {'TOKEN': '[redacted]'}
        assert 'private-value' not in str(tool_events(state)[-2:])
        state.close()
    asyncio.run(scenario())
