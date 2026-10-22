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


def test_sigterm_stops_both_http_servers(tmp_path):
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
           'MCP_WORKLOAD_ISOLATION': 'off'}
    server = Path(__file__).resolve().parents[1] / 't3_terminal_server.py'
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
                            assert health['releaseCommit'] is None
                            break
                except OSError:
                    time.sleep(.1)
            else:
                raise AssertionError('T3 server did not become ready')
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=15) == 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
