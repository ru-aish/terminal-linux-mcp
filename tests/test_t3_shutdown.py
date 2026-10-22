import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


def _free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_http_tool_events_and_sigterm_stops_both_servers(tmp_path):
    public_port, private_port = _free_port(), _free_port()
    token_file = tmp_path / 'token'
    token_file.write_text('isolated-test-token')
    env = {**os.environ,
           'T3_TERMINAL_DB': str(tmp_path / 'state.sqlite'),
           'T3_TERMINAL_CONTROL_TOKEN_FILE': str(token_file),
           'MCP_GPT_HOME': str(tmp_path / 'gpt'),
           'MCP_LOG_DIR': str(tmp_path / 'logs'),
           'MCP_CHAT_AGENT_ENABLED': '0',
           'MCP_CHAT_WATCHDOG_ENABLED': '0',
           'MCP_WORKLOAD_ISOLATION': 'off',
           'MCP_WORKSPACE': str(tmp_path),
           'PYTHONDONTWRITEBYTECODE': '1'}
    runtime = Path(os.environ.get('T3_TEST_RUNTIME', Path(__file__).resolve().parents[1]))
    server = runtime / 't3_terminal_server.py'
    manifest = runtime / 'RELEASE.json'
    commit = json.loads(manifest.read_text())['commit'] if manifest.exists() else None
    with (tmp_path / 'server.log').open('w') as log:
        process = subprocess.Popen(
            [sys.executable, str(server), '--port', str(public_port),
             '--control-port', str(private_port)],
            cwd=server.parent, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                assert process.poll() is None, 'T3 server exited before becoming ready'
                try:
                    with urlopen(f'http://127.0.0.1:{public_port}/healthz', timeout=.2) as response:
                        if response.status == 200:
                            health = json.load(response)
                            assert health['capabilities'] == ['tool-events-v1']
                            assert health['releaseCommit'] == commit
                            break
                except OSError:
                    time.sleep(.1)
            else:
                raise AssertionError('T3 server did not become ready')
            import httpx
            from mcp import ClientSession
            from mcp.client.streamable_http import streamable_http_client

            headers = {'Authorization': 'Bearer isolated-test-token'}
            control = f'http://127.0.0.1:{private_port}/v1'
            binding = {'threadId': 'release-probe', 'environmentId': 'release-test',
                       'providerId': 'chatgpt', 'cwd': str(tmp_path), 'model': 'test'}
            response = httpx.post(control + '/register', headers=headers, json=binding)
            response.raise_for_status()
            response = httpx.post(control + '/activate', headers=headers,
                                  json={**binding, 'requestId': 'release-activation'})
            response.raise_for_status()

            async def invoke():
                async with streamable_http_client(f'http://127.0.0.1:{public_port}/mcp') as (read, write, _):
                    async with ClientSession(read, write) as client:
                        await client.initialize()
                        result = await client.call_tool('bootstrap_thread', {
                            'thread_id': 'release-probe', 'cwd': str(tmp_path), 'max_chars': 1000})
                        assert not result.isError
                        assert any(part.type == 'text' and part.text for part in result.content)
            asyncio.run(invoke())
            response = httpx.get(control + '/events', headers=headers,
                                 params={'environmentId': 'release-test', 'providerId': 'chatgpt'})
            response.raise_for_status()
            traces = [event for event in response.json()['events']
                      if event['kind'] in ('tool_started', 'tool_completed')]
            assert [event['kind'] for event in traces] == ['tool_started', 'tool_completed']
            started, completed = [event['payload'] for event in traces]
            assert started['operationId'] == completed['operationId']
            assert started['toolName'] == 'fedora_t3code.bootstrap_thread'
            assert completed['status'] == 'completed'
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=15) == 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
