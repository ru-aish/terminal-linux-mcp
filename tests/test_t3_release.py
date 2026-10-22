"""A runtime must be an exact committed snapshot, including file permissions."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


spec = importlib.util.spec_from_file_location(
    't3_release', Path(__file__).resolve().parents[1] / 'scripts' / 't3_release.py')
release_tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_tool)


def repository(tmp_path):
    repo = tmp_path / 'source'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    subprocess.run(['git', '-C', str(repo), 'config', 'user.email', 'test@example.invalid'], check=True)
    subprocess.run(['git', '-C', str(repo), 'config', 'user.name', 'Test'], check=True)
    (repo / 'server.py').write_text('print("committed runtime")\n')
    subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(repo), 'commit', '-qm', 'test runtime'], check=True)
    return repo


@pytest.mark.parametrize('change', ['tracked', 'untracked'])
def test_dirty_source_cannot_be_staged(tmp_path, change):
    repo = repository(tmp_path)
    (repo / ('server.py' if change == 'tracked' else 'missing-patch.py')).write_text('local patch')
    with pytest.raises(ValueError, match='before staging'):
        release_tool.stage(repo, 'HEAD', tmp_path / 'releases')


def test_stage_is_reproducible_and_verifies_against_git(tmp_path):
    repo = repository(tmp_path)
    root = tmp_path / 'releases'
    staged = release_tool.stage(repo, 'HEAD', root)
    assert staged.name == release_tool.verify(staged, repo)
    assert release_tool.stage(repo, 'HEAD', root) == staged
    assert (staged / 'server.py').read_bytes() == (repo / 'server.py').read_bytes()
    assert not (staged / 'server.py').stat().st_mode & 0o222
    # Changing the working checkout cannot change a staged snapshot or its provenance.
    (repo / 'server.py').write_text('different local patch')
    assert release_tool.verify(staged, repo) == staged.name


@pytest.mark.parametrize('change', ['modified', 'missing', 'extra', 'manifest', 'symlink'])
def test_damaged_or_mislabeled_release_is_rejected(tmp_path, change):
    repo = repository(tmp_path)
    staged = release_tool.stage(repo, 'HEAD', tmp_path / 'releases')
    staged.chmod(0o755)
    source = staged / 'server.py'
    if change == 'modified':
        source.chmod(0o644)
        source.write_text('modified runtime')
        source.chmod(0o444)
    elif change == 'missing':
        source.unlink()
    elif change == 'extra':
        (staged / 'uncommitted.py').write_text('forgotten overlay')
    elif change == 'manifest':
        manifest = staged / 'RELEASE.json'
        manifest.chmod(0o644)
        value = json.loads(manifest.read_text())
        value['files']['server.py']['sha256'] = '0' * 64
        manifest.write_text(json.dumps(value))
        manifest.chmod(0o444)
    else:
        source.unlink()
        source.symlink_to(repo / 'server.py')
    with pytest.raises(ValueError):
        release_tool.verify(staged, repo)


def test_systemd_startup_always_verifies_the_recorded_commit(tmp_path):
    repo = repository(tmp_path)
    staged = release_tool.stage(repo, 'HEAD', tmp_path / 'releases')
    unit = release_tool.systemd_unit(staged, repo, Path(sys.executable))
    assert 'Environment=PYTHONDONTWRITEBYTECODE=1' in unit
    assert f'"{staged}/scripts/t3_release.py" "verify"' in unit
    assert f'"--repo" "{repo}"' in unit
    assert f'"{staged}/t3_terminal_server.py"' in unit
    (staged / 'server.py').chmod(0o644)
    with pytest.raises(ValueError, match='writable'):
        release_tool.systemd_unit(staged, repo, Path(sys.executable))


def test_release_root_symlinks_are_rejected(tmp_path):
    repo = repository(tmp_path)
    staged = release_tool.stage(repo, 'HEAD', tmp_path / 'releases')
    alias = tmp_path / 'current'
    alias.symlink_to(staged, target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        release_tool.verify(alias, repo)
