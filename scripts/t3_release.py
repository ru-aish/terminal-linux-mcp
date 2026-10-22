"""Stage and verify immutable T3 runtimes from committed Git objects only."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import tempfile


MANIFEST = 'RELEASE.json'


def git(repo: Path, *args: str) -> bytes:
    return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE)


def committed_files(repo: Path, ref: str):
    commit = git(repo, 'rev-parse', '--verify', ref + '^{commit}').decode().strip()
    tree = git(repo, 'rev-parse', commit + '^{tree}').decode().strip()
    files = {}
    with tarfile.open(fileobj=io.BytesIO(git(repo, 'archive', commit))) as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or '..' in path.parts:
                raise ValueError('Unsafe archive path: ' + member.name)
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError('Runtime sources must be regular files: ' + member.name)
            if member.name == MANIFEST:
                raise ValueError('Release manifests must be generated, not committed')
            files[member.name] = (archive.extractfile(member).read(), bool(member.mode & 0o111))
    return commit, tree, files


def fingerprints(files):
    return {name: {'sha256': hashlib.sha256(data).hexdigest(), 'executable': executable}
            for name, (data, executable) in sorted(files.items())}


def verify(release: Path, repo: Path):
    if release.is_symlink():
        raise ValueError('Release directory must not be a symlink')
    manifest_path = release / MANIFEST
    if manifest_path.is_symlink():
        raise ValueError('Release manifest must not be a symlink')
    manifest = json.loads(manifest_path.read_text())
    if manifest.get('version') != 1:
        raise ValueError('Unsupported or missing release manifest version')
    commit, tree, files = committed_files(repo, manifest['commit'])
    if manifest.get('tree') != tree or manifest.get('files') != fingerprints(files):
        raise ValueError('Release manifest does not match its recorded Git commit')
    actual = set()
    for path in release.rglob('*'):
        if path.is_symlink():
            raise ValueError('Unexpected symlink in release: ' + str(path))
        if path.is_dir():
            continue
        name = path.relative_to(release).as_posix()
        actual.add(name)
        if path.stat().st_mode & 0o222:
            raise ValueError('Release file is writable: ' + name)
        if name == MANIFEST:
            continue
        expected = manifest['files'].get(name)
        if expected is None or hashlib.sha256(path.read_bytes()).hexdigest() != expected['sha256']:
            raise ValueError('Release file differs from Git: ' + name)
        if bool(path.stat().st_mode & 0o111) != expected['executable']:
            raise ValueError('Release executable mode differs from Git: ' + name)
    if actual != set(files) | {MANIFEST}:
        raise ValueError('Release contains missing or unexpected files')
    return commit


def stage(repo: Path, ref: str, root: Path):
    if git(repo, 'status', '--porcelain', '--untracked-files=all').strip():
        raise ValueError('Commit or remove tracked and untracked changes before staging a release')
    commit, tree, files = committed_files(repo, ref)
    root.mkdir(parents=True, exist_ok=True)
    target = root / commit
    if target.exists():
        verify(target, repo)
        return target
    temporary = Path(tempfile.mkdtemp(prefix='.stage-', dir=root))
    try:
        for name, (data, executable) in files.items():
            path = temporary / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o555 if executable else 0o444)
        (temporary / MANIFEST).write_text(json.dumps({
            'version': 1, 'commit': commit, 'tree': tree, 'files': fingerprints(files),
        }, sort_keys=True, indent=2) + '\n')
        (temporary / MANIFEST).chmod(0o444)
        verify(temporary, repo)
        for path in sorted(temporary.rglob('*'), key=lambda p: len(p.parts), reverse=True):
            if path.is_dir():
                path.chmod(0o555)
        temporary.chmod(0o555)
        os.rename(temporary, target)
    except BaseException:
        for path in temporary.rglob('*'):
            if path.is_dir():
                path.chmod(0o755)
        temporary.chmod(0o755)
        shutil.rmtree(temporary)
        raise
    return target


def systemd_unit(release: Path, repo: Path, python: Path, port=8021, control_port=8022):
    """Render a drop-in only after verifying the exact runtime it will start."""
    release, repo, python = release.absolute(), repo.resolve(), python.absolute()
    verify(release, repo)
    if not python.is_file() or not os.access(python, os.X_OK):
        raise ValueError('Python executable is missing or not executable')
    if not (0 < port < 65536 and 0 < control_port < 65536) or port == control_port:
        raise ValueError('Public and control ports must be distinct valid ports')

    def quote(value):
        text = str(value)
        if any(char in text for char in ('\n', '\r', '\0')):
            raise ValueError('Invalid character in systemd argument')
        return '"' + text.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$') + '"'

    check = [python, release / 'scripts/t3_release.py', 'verify', '--release', release, '--repo', repo]
    start = [python, release / 't3_terminal_server.py', '--port', port, '--control-port', control_port]
    return ('[Service]\nEnvironment=PYTHONDONTWRITEBYTECODE=1\n'
            'ExecStartPre=\nExecStartPre=' + ' '.join(map(quote, check)) + '\n'
            'ExecStart=\nExecStart=' + ' '.join(map(quote, start)) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('stage')
    create.add_argument('--repo', type=Path, required=True)
    create.add_argument('--ref', default='HEAD')
    create.add_argument('--root', type=Path, required=True)
    check = commands.add_parser('verify')
    check.add_argument('--repo', type=Path, required=True)
    check.add_argument('--release', type=Path, required=True)
    unit = commands.add_parser('systemd-unit')
    unit.add_argument('--repo', type=Path, required=True)
    unit.add_argument('--release', type=Path, required=True)
    unit.add_argument('--python', type=Path, required=True)
    unit.add_argument('--port', type=int, default=8021)
    unit.add_argument('--control-port', type=int, default=8022)
    args = parser.parse_args()
    try:
        if args.command == 'stage':
            print(stage(args.repo.resolve(), args.ref, args.root.resolve()))
        elif args.command == 'systemd-unit':
            print(systemd_unit(args.release, args.repo, args.python, args.port, args.control_port), end='')
        else:
            print('Verified Git commit ' + verify(args.release.absolute(), args.repo.resolve()))
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        parser.exit(1, str(exc) + '\n')


if __name__ == '__main__':
    main()
