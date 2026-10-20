"""Real user-systemd containment: mocked subprocesses cannot establish a cgroup boundary."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest
from durable_process_supervisor import DurableProcessSupervisor


def _has_user_systemd():
    if not shutil.which('systemd-run'):
        return False
    try:
        return subprocess.run(['systemd-run', '--user', '--quiet', '--wait', '--pipe',
                               '--property=Delegate=yes', '--', '/bin/true'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=8).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


@pytest.mark.skipif(not _has_user_systemd(), reason='user systemd and cgroup delegation required')
@pytest.mark.parametrize('mode', ['natural', 'timeout', 'stop'])
def test_real_systemd_contains_separate_session_descendant(tmp_path, mode):
    manager = DurableProcessSupervisor(tmp_path / 'state', launch_mode='systemd')
    marker = tmp_path / 'escaped-write'
    ready = tmp_path / 'ready'
    python = ("import os,signal,time;os.setsid();"
              "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
              f"open({str(ready)!r},'w').write('ready');"
              "time.sleep(2.5);"
              f"open({str(marker)!r},'w').write('escaped')")
    if mode == 'natural':
        command = 'printf done'
    else:
        command = f'{sys.executable} -c {python!r} & exit 0'
    job = manager.start(command, owner='test', cwd=tmp_path,
                        hard_timeout=.7 if mode == 'timeout' else None)
    if mode == 'stop':
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert ready.exists()
        manager.stop(job['job_id'], owner='test')
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        result = manager.status(job['job_id'], owner='test')['result']
        if result:
            break
        time.sleep(.025)
    assert result is not None
    assert result['group_verified'] is True
    assert result['status'] == {'natural': 'exited', 'timeout': 'timed_out',
                                'stop': 'descendants_stopped'}[mode]
    time.sleep(2.7 if mode != 'natural' else .05)
    assert not marker.exists(), 'An escaped descendant wrote after the terminal receipt'
