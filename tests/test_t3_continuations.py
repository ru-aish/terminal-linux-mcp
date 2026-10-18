import asyncio
import sys
import time
from datetime import datetime, timezone

import pytest

from t3_thread_state import ThreadState


@pytest.fixture
def state(tmp_path):
    state = ThreadState(tmp_path / 'state.db', inactivity_seconds=3600)
    state.register(thread_id='thread-one', terminal_id='terminal-one', environment_id='test-env',
                   provider_id='chatgpt', cwd=str(tmp_path), model='test-model')
    yield state
    state.close()


@pytest.fixture
def core(tmp_path, monkeypatch):
    monkeypatch.setenv('MCP_GPT_HOME', str(tmp_path / 'gpt-home'))
    import terminal_mcp as core
    for name in ('mcp', '_spawn_workload', '_set_startup_instructions', '_core_behavior_prompt'):
        monkeypatch.setattr(core, name, getattr(core, name))
    monkeypatch.setattr(core, 'WORKSPACE_DIR', tmp_path)
    monkeypatch.setattr(core, 'WORKLOAD_ISOLATION', 'off')
    return core


def automations(state):
    with state.connect() as db:
        return [dict(r) for r in db.execute('SELECT * FROM automations ORDER BY created')]


def continues(state):
    return [e for e in state.events('test-env', 'chatgpt') if e['kind'] == 'continue']


def test_timer_timing_exactly_once_and_revision_independence(state):
    async def run():
        state.activate('thread-one', 'activate')
        scheduled = state.schedule_continue('thread-one', after_seconds=.08)
        state.begin('thread-one', 'tool', 'tool')
        state.finish('tool', 'returned')
        await asyncio.sleep(.02)
        assert not continues(state)
        await asyncio.sleep(.1)
        event, = continues(state)
        assert event['created'] >= scheduled['due_at']
        assert event['payload']['automationId'] == scheduled['automation_id']
        state.begin('thread-one', 'another-tool', 'tool')
        state.finish('another-tool', 'returned')
        assert state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        state.acknowledge(event['event_id'], 'test-env', 'chatgpt')
        assert not state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        for _ in range(3):
            state.arm('thread-one')
        assert len(continues(state)) == 1
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['timer', 'process'])
@pytest.mark.parametrize('ack_after_finish', [False, True])
def test_claimed_explicit_event_is_idempotent(state, kind, ack_after_finish):
    async def run():
        state.activate('thread-one', 'activate')
        if kind == 'process':
            state.begin('thread-one', 'process', 'process', process_id='proc-one')
            scheduled = state.continue_after_process('thread-one', 'proc-one')
            state.finish('process', 'exit_code=0')
        else:
            scheduled = state.schedule_continue('thread-one', wake_at=0)
        event, = continues(state)
        event_id = event['event_id']
        assert event['payload']['automationId'] == scheduled['automation_id']
        assert state.claim(event_id, 'test-env', 'chatgpt')['valid']
        claimed, = continues(state)
        assert claimed['claimed'] == 1
        assert claimed['acknowledged'] == 0
        delivered, = automations(state)
        assert delivered['status'] == 'delivered'
        assert delivered['event_id'] == event_id

        state.begin('thread-one', 'tool', 'tool')
        for _ in range(2):
            assert not state.claim(event_id, 'test-env', 'chatgpt')['valid']
            state.arm('thread-one')
            assert automations(state) == [delivered]
            assert continues(state) == [claimed]

        if not ack_after_finish:
            state.acknowledge(event_id, 'test-env', 'chatgpt')
            assert not state.claim(event_id, 'test-env', 'chatgpt')['valid']
        state.finish('tool', 'returned')
        if ack_after_finish:
            assert automations(state) == [delivered]
            assert continues(state) == [claimed]
            # Replays keep the stable event ID used by adapter dispatch dedupe.
            assert state.claim(event_id, 'test-env', 'chatgpt')['valid']
            assert continues(state)[0]['event_id'] == event_id
            state.acknowledge(event_id, 'test-env', 'chatgpt')
        assert not state.claim(event_id, 'test-env', 'chatgpt')['valid']
        state.acknowledge(event_id, 'test-env', 'chatgpt')
        for _ in range(3):
            state.arm('thread-one')
        assert automations(state) == [delivered]
        assert continues(state) == [{**claimed, 'acknowledged': 1}]
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['timer', 'process'])
def test_claimed_explicit_event_reclaims_same_id_after_restart(tmp_path, kind):
    async def run():
        path = tmp_path / 'state.db'
        state = ThreadState(path, inactivity_seconds=3600)
        state.register(thread_id='thread-one', terminal_id='terminal-one', environment_id='test-env',
                       provider_id='chatgpt', cwd=str(tmp_path), model='test-model')
        state.activate('thread-one', 'activate')
        if kind == 'process':
            state.begin('thread-one', 'process', 'process', process_id='proc-one')
            state.continue_after_process('thread-one', 'proc-one')
            state.finish('process', 'exit_code=0')
        else:
            state.schedule_continue('thread-one', wake_at=0)
        event, = continues(state)
        event_id = event['event_id']
        assert state.claim(event_id, 'test-env', 'chatgpt')['valid']
        state.close()

        recovered = ThreadState(path, inactivity_seconds=3600)
        try:
            replay, = continues(recovered)
            assert replay['event_id'] == event_id
            assert replay['claimed'] == 1
            assert replay['acknowledged'] == 0
            assert recovered.claim(event_id, 'test-env', 'chatgpt')['valid']
            assert continues(recovered)[0]['event_id'] == event_id
            recovered.acknowledge(event_id, 'test-env', 'chatgpt')
            assert not recovered.claim(event_id, 'test-env', 'chatgpt')['valid']
            assert len(continues(recovered)) == 1
        finally:
            recovered.close()
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['timer', 'process'])
@pytest.mark.parametrize('ack_after_finish', [False, True])
def test_blocked_explicit_claim_requeues_once_and_tolerates_late_ack(state, kind, ack_after_finish):
    async def run():
        state.activate('thread-one', 'activate')
        if kind == 'process':
            state.begin('thread-one', 'process', 'process', process_id='proc-one')
            scheduled = state.continue_after_process('thread-one', 'proc-one')
            state.finish('process', 'exit_code=0')
        else:
            scheduled = state.schedule_continue('thread-one', wake_at=time.time() - 1)
        old, = continues(state)
        assert old['claimed'] == 0
        state.begin('thread-one', 'tool', 'tool')
        state.begin('thread-one', 'other-tool', 'tool')
        assert not state.claim(old['event_id'], 'test-env', 'chatgpt')['valid']
        retired, = continues(state)
        assert retired['claimed'] == 0
        assert retired['acknowledged'] == 1
        automation, = automations(state)
        assert automation['status'] == 'due'
        assert automation['event_id'] is None
        assert not state.claim(old['event_id'], 'test-env', 'chatgpt')['valid']
        if not ack_after_finish:
            state.acknowledge(old['event_id'], 'test-env', 'chatgpt')
        state.arm('thread-one')
        state.finish('tool', 'returned')
        assert len(continues(state)) == 1
        state.finish('other-tool', 'returned')
        retired, fresh = continues(state)
        assert fresh['event_id'] != old['event_id']
        assert fresh['seq'] > old['seq']
        assert fresh['payload']['automationId'] == scheduled['automation_id']
        assert fresh['acknowledged'] == 0
        before_ack = state.get('thread-one')['last_activity']
        state.acknowledge(old['event_id'], 'test-env', 'chatgpt')
        state.acknowledge(old['event_id'], 'test-env', 'chatgpt')
        assert state.get('thread-one')['last_activity'] == before_ack
        automation, = automations(state)
        assert automation['status'] == 'delivered'
        assert automation['event_id'] == fresh['event_id']
        assert state.claim(fresh['event_id'], 'test-env', 'chatgpt')['valid']
        state.acknowledge(fresh['event_id'], 'test-env', 'chatgpt')
        assert not state.claim(fresh['event_id'], 'test-env', 'chatgpt')['valid']
        for _ in range(3):
            state.arm('thread-one')
        assert len(continues(state)) == 2
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['timer', 'process'])
@pytest.mark.parametrize('transition', ['activate', 'pause', 'complete'])
@pytest.mark.parametrize('requeue_before_transition', [False, True])
def test_task_transition_prevents_busy_claim_retry(state, kind, transition, requeue_before_transition):
    async def run():
        state.activate('thread-one', 'activate')
        if kind == 'process':
            state.begin('thread-one', 'process', 'process', process_id='proc-one')
            state.continue_after_process('thread-one', 'proc-one')
            state.finish('process', 'exit_code=0')
        else:
            state.schedule_continue('thread-one', wake_at=time.time() - 1)
        old, = continues(state)
        state.begin('thread-one', 'tool', 'tool')
        if requeue_before_transition:
            assert not state.claim(old['event_id'], 'test-env', 'chatgpt')['valid']
            assert automations(state)[0]['status'] == 'due'
        if transition == 'complete':
            state.complete('thread-one', ['verified'], excluding='tool')
        else:
            getattr(state, transition)('thread-one', 'transition')
        assert not state.claim(old['event_id'], 'test-env', 'chatgpt')['valid']
        assert automations(state)[0]['status'] == 'stale'
        state.acknowledge(old['event_id'], 'test-env', 'chatgpt')
        state.finish('tool', 'returned')
        state.arm('thread-one')
        assert len(continues(state)) == 1
        assert automations(state)[0]['status'] == 'stale'
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['timer', 'process'])
def test_explicit_event_blocks_inactivity_across_revisions(state, kind):
    async def run():
        state.activate('thread-one', 'activate')
        if kind == 'process':
            state.begin('thread-one', 'process', 'process', process_id='proc-one')
            state.continue_after_process('thread-one', 'proc-one')
            state.finish('process', 'exit_code=0')
        else:
            state.schedule_continue('thread-one', wake_at=time.time() - 1)
        event, = continues(state)
        state.begin('thread-one', 'tool', 'tool')
        state.finish('tool', 'returned')
        binding = state.get('thread-one')
        assert event['revision'] != binding['revision']
        assert 'thread-one' not in state.timers
        state.expire('thread-one', binding['epoch'], binding['revision'])
        assert len(continues(state)) == 1
        assert state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        state.acknowledge(event['event_id'], 'test-env', 'chatgpt')
        assert 'thread-one' in state.timers
    asyncio.run(run())


def test_stale_inactivity_event_does_not_block_new_inactivity(state):
    async def run():
        state.activate('thread-one', 'activate')
        binding = state.get('thread-one')
        state.expire('thread-one', binding['epoch'], binding['revision'])
        old, = continues(state)
        state.begin('thread-one', 'tool', 'tool')
        state.finish('tool', 'returned')
        assert 'thread-one' in state.timers
        binding = state.get('thread-one')
        state.expire('thread-one', binding['epoch'], binding['revision'])
        assert len(continues(state)) == 2
        assert not state.claim(old['event_id'], 'test-env', 'chatgpt')['valid']
        assert state.claim(continues(state)[1]['event_id'], 'test-env', 'chatgpt')['valid']
    asyncio.run(run())


def test_replayed_pause_preserves_current_epoch_timers(state, monkeypatch):
    async def run():
        state.activate('thread-one', 'activate')
        state.pause('thread-one', 'pause')
        state.activate('thread-one', 'new-task')
        scheduled = state.schedule_continue('thread-one', after_seconds=100)
        inactivity_timer = state.timers['thread-one']
        automation_timer = state.automation_timers['thread-one']
        binding = state.get('thread-one')
        assert state.pause('thread-one', 'pause') == binding
        assert not inactivity_timer.cancelled()
        assert not automation_timer.cancelled()
        assert state.timers['thread-one'] is inactivity_timer
        assert state.automation_timers['thread-one'] is automation_timer
        assert automations(state)[0]['status'] == 'pending'
        monkeypatch.setattr('t3_thread_state.time.time', lambda: scheduled['due_at'] + 1)
        # Invoke the preserved callback without relying on wall-clock sleeps.
        automation_timer._run()
        event, = continues(state)
        assert event['payload']['automationId'] == scheduled['automation_id']
        assert state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
    asyncio.run(run())


def test_timer_due_during_active_operation(state):
    async def run():
        state.activate('thread-one', 'activate')
        state.schedule_continue('thread-one', after_seconds=.02)
        state.begin('thread-one', 'tool', 'tool')
        state.begin('thread-one', 'process', 'process')
        await asyncio.sleep(.05)
        assert automations(state)[0]['status'] == 'due'
        assert not continues(state)
        state.finish('tool', 'returned')
        assert not continues(state)
        state.finish('process', 'exit_code=1')
        assert len(continues(state)) == 1
    asyncio.run(run())


@pytest.mark.parametrize('transition', ['activate', 'pause', 'complete'])
def test_task_transition_stales_pending_and_delivered_automations(state, transition):
    async def run():
        state.activate('thread-one', 'activate')
        state.schedule_continue('thread-one', wake_at=time.time() - 1)
        event, = continues(state)
        state.schedule_continue('thread-one', after_seconds=.02)
        state.begin('thread-one', 'transition-tool', 'tool')
        if transition == 'complete':
            state.complete('thread-one', ['verified'], excluding='transition-tool')
        else:
            getattr(state, transition)('thread-one', 'transition')
        assert not state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        assert automations(state)[0]['status'] == 'stale'
        assert automations(state)[0]['event_id'] == event['event_id']
        state.acknowledge(event['event_id'], 'test-env', 'chatgpt')
        state.finish('transition-tool', 'returned')
        if transition == 'activate':
            assert 'thread-one' in state.timers
        await asyncio.sleep(.05)
        assert all(a['status'] == 'stale' for a in automations(state))
        assert not state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        assert len(continues(state)) == 1
    asyncio.run(run())


@pytest.mark.parametrize('wake_at', [1234567890, '1234567890', '2009-02-13T23:31:30Z', '2009-02-14T05:01:30+05:30'])
def test_absolute_timer_formats(state, wake_at):
    async def run():
        state.activate('thread-one', 'activate')
        scheduled = state.schedule_continue('thread-one', wake_at=wake_at)
        assert scheduled['due_at'] == 1234567890
        assert scheduled['status'] == 'delivered'
    asyncio.run(run())


@pytest.mark.parametrize('arguments', [
    {}, {'after_seconds': 1, 'wake_at': 1234567890}, {'after_seconds': 0},
    {'after_seconds': -1}, {'after_seconds': float('nan')}, {'after_seconds': float('inf')},
    {'wake_at': '2026-10-18T12:00:00'}, {'wake_at': 'bad'}, {'wake_at': float('nan')},
])
def test_invalid_timer_inputs(state, arguments):
    with pytest.raises(ValueError):
        state.schedule_continue('thread-one', **arguments)
    assert not automations(state)


def test_timer_restart_reconstructs_future_and_overdue_callbacks(state):
    async def run():
        state.activate('thread-one', 'activate')
        state.schedule_continue('thread-one', after_seconds=.02)
        state.schedule_continue('thread-one', wake_at=datetime.fromtimestamp(time.time() + .15, timezone.utc).isoformat())
        state.close()
        await asyncio.sleep(.04)
        restarted = ThreadState(state.path, inactivity_seconds=3600)
        try:
            restarted.arm('thread-one')
            assert len(continues(restarted)) == 1
            await asyncio.sleep(.18)
            assert len(continues(restarted)) == 2
            restarted.arm('thread-one')
            assert len(continues(restarted)) == 2
        finally:
            restarted.close()
    asyncio.run(run())


@pytest.mark.parametrize('already_finished', [False, True])
def test_process_exit_any_code_waits_for_other_operations(state, already_finished):
    async def run():
        state.activate('thread-one', 'activate')
        state.begin('thread-one', 'operation', 'process', process_id='proc-one')
        state.begin('thread-one', 'tool', 'tool')
        if already_finished:
            state.finish('operation', 'exit_code=7')
        scheduled = state.continue_after_process('thread-one', 'proc-one')
        assert scheduled['operation_id'] == 'operation'
        assert scheduled['status'] == ('due' if already_finished else 'pending')
        if not already_finished:
            state.finish('operation', 'exit_code=7')
        assert automations(state)[0]['status'] == 'due'
        assert not continues(state)
        state.finish('tool', 'returned')
        assert len(continues(state)) == 1
        state.finish('operation', 'exit_code=7')
        assert len(continues(state)) == 1
    asyncio.run(run())


def test_idempotency_is_durable_and_scoped_to_thread_and_kind(state, tmp_path):
    async def run():
        state.activate('thread-one', 'activate')
        state.begin('thread-one', 'operation', 'process', process_id='proc-one')
        first = state.schedule_continue('thread-one', wake_at=time.time() - 1, idempotency_key='key')
        assert state.schedule_continue('thread-one', after_seconds=100, idempotency_key='key') == first
        process = state.continue_after_process('thread-one', 'proc-one', idempotency_key='key')
        assert process['automation_id'] != first['automation_id']
        assert state.continue_after_process('thread-one', 'proc-one', idempotency_key='key') == process
        state.register(thread_id='thread-two', environment_id='test-env', provider_id='chatgpt', cwd=str(tmp_path), model='test-model')
        state.activate('thread-two', 'activate-two')
        second = state.schedule_continue('thread-two', after_seconds=100, idempotency_key='key')
        assert second['automation_id'] != first['automation_id']
        state.finish('operation', 'exit_code=0')
        state.close()
        restarted = ThreadState(state.path, inactivity_seconds=3600)
        try:
            assert restarted.schedule_continue('thread-one', after_seconds=100, idempotency_key='key')['automation_id'] == first['automation_id']
            assert restarted.continue_after_process('thread-one', 'proc-one', idempotency_key='key')['automation_id'] == process['automation_id']
            restarted.activate('thread-one', 'new-epoch')
            assert restarted.schedule_continue('thread-one', after_seconds=100, idempotency_key='key')['status'] == 'stale'
            assert len(continues(restarted)) == 2
        finally:
            restarted.close()
    asyncio.run(run())


def test_process_must_belong_to_terminal_session(state, tmp_path):
    async def run():
        state.activate('thread-one', 'activate')
        state.register(thread_id='thread-two', environment_id='test-env', provider_id='chatgpt', cwd=str(tmp_path), model='test-model')
        state.begin('thread-two', 'operation', 'process', process_id='foreign-process')
        for process_id in ('unknown', 'foreign-process'):
            with pytest.raises(ValueError, match='not owned'):
                state.continue_after_process('thread-one', process_id)
        assert not automations(state)
    asyncio.run(run())


def test_public_tools_target_only_injected_terminal_and_observe_process_exit(state, core, tmp_path):
    from t3_terminal_server import build

    async def run():
        mcp, _ = build(core, state, 'test-token')
        tools = {t.name: t for t in await mcp.list_tools()}
        assert not any(n.startswith(('agent_', 'chat_runtime_')) for n in tools)
        for name in ('schedule_continue', 'continue_after_process'):
            assert 'thread_id' not in tools[name].inputSchema['properties']
            assert 'session_id' in tools[name].inputSchema['required']
        state.activate('thread-one', 'activate')
        denied = await mcp.call_tool('schedule_continue', {'session_id': 'thread-one', 'after_seconds': 1})
        assert 'refused' in str(denied)
        await mcp.call_tool('schedule_continue', {'session_id': 'terminal-one', 'wake_at': time.time() - 1, 'idempotency_key': 'timer'})
        event, = continues(state)
        assert state.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
        await mcp.call_tool('bootstrap_thread', {'thread_id': 'terminal-one', 'cwd': str(tmp_path), 'max_chars': 0})
        result = await mcp.call_tool('start_process', {'session_id': 'terminal-one', 'command': 'sleep 0.15; exit 9', 'cwd': str(tmp_path)})
        assert 'Process ID' in str(result)
        process = next(p for p in core.processes.values() if p.session_id == 'terminal-one')
        await mcp.call_tool('continue_after_process', {'session_id': 'terminal-one', 'process_id': process.process_id, 'idempotency_key': 'process'})
        assert automations(state)[1]['status'] == 'pending'
        await asyncio.wait_for(process.process.wait(), 10)
        await asyncio.sleep(.03)
        assert len(continues(state)) == 2
        await mcp.call_tool('continue_after_process', {'session_id': 'terminal-one', 'process_id': process.process_id, 'idempotency_key': 'process'})
        assert len(continues(state)) == 2
        await mcp.call_tool('continue_after_process', {'session_id': 'terminal-one', 'process_id': process.process_id})
        assert len(continues(state)) == 3
    asyncio.run(run())


@pytest.mark.parametrize('live', [False, True])
def test_startup_process_recovery_delivers_attached_continuation(state, core, live):
    from t3_terminal_server import build

    async def run():
        state.activate('thread-one', 'activate')
        process = await asyncio.create_subprocess_exec(sys.executable, '-c', 'import sys; sys.stdin.read(); sys.exit(6)', stdin=asyncio.subprocess.PIPE)
        try:
            state.begin('thread-one', 'operation', 'process', process.pid, core.read_process_start_ticks(process.pid), process_id='persisted-process')
            state.continue_after_process('thread-one', 'persisted-process', idempotency_key='key')
            state.close()
            if not live:
                process.stdin.close()
                await process.wait()
            restarted = ThreadState(state.path, inactivity_seconds=3600)
            mcp, _ = build(core, restarted, 'test-token')
            async with mcp.settings.lifespan(mcp):
                if live:
                    assert not continues(restarted)
                    process.stdin.close()
                    await asyncio.wait_for(process.wait(), 10)
                    await asyncio.sleep(.03)
                event, = continues(restarted)
                assert restarted.claim(event['event_id'], 'test-env', 'chatgpt')['valid']
                assert restarted.continue_after_process('thread-one', 'persisted-process', idempotency_key='key')['event_id'] == event['event_id']
                assert len(continues(restarted)) == 1
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()
    asyncio.run(run())
