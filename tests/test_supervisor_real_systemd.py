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


@pytest.mark.skipif(not _has_user_systemd(), reason='user systemd and cgroup delegation required')
def test_natural_parent_exit_waits_for_escaped_child_before_receipt(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / 'state', launch_mode='systemd')
    marker = tmp_path / 'late-natural'
    ready = tmp_path / 'child-started'
    script = ("import os,time;os.setsid();"
              f"open({str(ready)!r},'w').write('1');"
              "time.sleep(.8);"
              f"open({str(marker)!r},'w').write('finished')")
    job = manager.start(f'{sys.executable} -c {script!r} & exit 0',
                        owner='test', cwd=tmp_path)
    deadline = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(.025)
    assert ready.exists()
    assert manager.status(job['job_id'], owner='test')['result'] is None
    deadline = time.monotonic() + 5
    result = None
    while time.monotonic() < deadline:
        result = manager.status(job['job_id'], owner='test')['result']
        if result:
            break
        time.sleep(.025)
    assert result and result['status'] == 'exited' and result['group_verified']
    assert marker.read_text() == 'finished'
