"""Helpers for inspecting the dispatcher process tree via /proc."""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

PROCFS_ROOT = Path('/proc')


def _parse_kib_value(raw_value: str) -> int | None:
    """Parse a value like '1234 kB' into a KiB integer."""
    raw_value = raw_value.strip()
    if not raw_value:
        return None
    parts = raw_value.split()
    if not parts:
        return None
    try:
        value = int(parts[0])
    except ValueError:
        return None
    return value


def _read_process_status(pid: int) -> tuple[str, int, str, dict] | None:
    """Read name, parent pid, state, and memory stats for pid from /proc."""
    status_path = PROCFS_ROOT / str(pid) / 'status'
    name = ''
    state = ''
    ppid: int | None = None
    rss_kib: int | None = None
    vms_kib: int | None = None
    try:
        with status_path.open('r', encoding='utf-8', errors='replace') as status_file:
            for line in status_file:
                if line.startswith('Name:'):
                    name = line.split(':', 1)[1].strip()
                elif line.startswith('PPid:'):
                    try:
                        ppid = int(line.split(':', 1)[1].strip())
                    except ValueError:
                        ppid = None
                elif line.startswith('State:'):
                    state = line.split(':', 1)[1].strip()
                elif line.startswith('VmRSS:'):
                    parsed = _parse_kib_value(line.split(':', 1)[1])
                    if parsed is not None:
                        rss_kib = parsed
                elif line.startswith('VmSize:'):
                    parsed = _parse_kib_value(line.split(':', 1)[1])
                    if parsed is not None:
                        vms_kib = parsed
                if name and state and ppid is not None and rss_kib is not None and vms_kib is not None:
                    break
    except FileNotFoundError:
        return None
    except PermissionError:
        return None
    except OSError:
        return None
    if ppid is None:
        ppid = 0
    memory: dict[str, int] = {}
    if rss_kib is not None:
        memory['rss_kib'] = rss_kib
        memory['rss_bytes'] = rss_kib * 1024
    if vms_kib is not None:
        memory['vmem_kib'] = vms_kib
        memory['vmem_bytes'] = vms_kib * 1024
    return name, ppid, state, memory


def _read_process_cmdline(pid: int) -> str:
    """Return the command line string for pid."""
    cmdline_path = PROCFS_ROOT / str(pid) / 'cmdline'
    try:
        raw = cmdline_path.read_bytes()
    except FileNotFoundError:
        return ''
    except PermissionError:
        return ''
    except OSError:
        return ''
    if not raw:
        return ''
    parts = [segment.decode('utf-8', 'replace') for segment in raw.rstrip(b'\x00').split(b'\x00') if segment]
    return ' '.join(parts)


def _collect_process_snapshot(target_pgid: int) -> tuple[dict[int, dict], dict[str, int]]:
    """Collect processes sharing target_pgid and gather metadata."""
    getpgid = getattr(os, 'getpgid', None)
    if getpgid is None:
        return {}, {'unsupported_platform': 1}

    processes: dict[int, dict] = {}
    errors: dict[str, int] = {}
    try:
        proc_entries = sorted(PROCFS_ROOT.iterdir(), key=lambda path: path.name)
    except FileNotFoundError:
        return {}, {'procfs_missing': 1}
    except PermissionError:
        return {}, {'procfs_permission_denied': 1}

    for entry in proc_entries:
        name = entry.name
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            pgid = getpgid(pid)
        except ProcessLookupError:
            errors['process_lookup_failed'] = errors.get('process_lookup_failed', 0) + 1
            continue
        except PermissionError:
            errors['pgid_permission_denied'] = errors.get('pgid_permission_denied', 0) + 1
            continue
        except OSError:
            errors['pgid_unknown_error'] = errors.get('pgid_unknown_error', 0) + 1
            continue

        if pgid != target_pgid:
            continue

        status_data = _read_process_status(pid)
        if not status_data:
            errors['status_unavailable'] = errors.get('status_unavailable', 0) + 1
            continue
        name_value, parent_pid, state, memory_info = status_data
        processes[pid] = {
            'pid': pid,
            'ppid': parent_pid,
            'pgid': pgid,
            'name': name_value,
            'state': state,
            'cmdline': _read_process_cmdline(pid),
        }
        if memory_info:
            processes[pid]['memory'] = memory_info
    return processes, errors


def _build_process_tree(processes: dict[int, dict]) -> list[dict]:
    """Convert a flat map keyed by pid into a nested tree."""
    children: defaultdict[int, list[int]] = defaultdict(list)
    for pid, info in processes.items():
        children[info['ppid']].append(pid)
    for child_list in children.values():
        child_list.sort()

    def _attach(pid: int) -> dict:
        node = {
            'pid': pid,
            'ppid': processes[pid]['ppid'],
            'name': processes[pid]['name'],
            'state': processes[pid]['state'],
            'cmdline': processes[pid]['cmdline'],
        }
        memory = processes[pid].get('memory')
        if memory:
            node['memory'] = memory
        child_nodes = children.get(pid, [])
        if child_nodes:
            node['children'] = [_attach(child_pid) for child_pid in child_nodes]
        return node

    roots = [pid for pid in processes if processes[pid]['ppid'] not in processes]
    return [_attach(pid) for pid in sorted(roots)]


def inspect_process_group(target_pgid: int) -> tuple[list[dict], int, dict[str, int]]:
    """Return (tree, process_count, errors) for the supplied process group."""
    snapshot, errors = _collect_process_snapshot(target_pgid)
    process_tree = _build_process_tree(snapshot) if snapshot else []
    return process_tree, len(snapshot), errors
