#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Bounded read-only Linux Docker diagnostics; never test/role/coverage evidence.

No Docker invocation is performed on import. All output belongs outside the SUT.
Docker creator/module/test ownership remains UNPROVEN without an observed witness.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import subprocess
import time

IMAGE = 'apache/kafka:3.9.1'
MODULE = 'pinot-plugins/pinot-stream-ingestion/pinot-kafka-3.0'
REFS = {'baseline': '1b552624f52a96926c67d340d9c27a8de390a4ef',
        'candidate': 'baed053b342a531196c43ccf018fbf830c60b902'}
SOURCE_PINS = {
    MODULE + '/src/main/java/org/apache/pinot/plugin/stream/kafka30/server/KafkaServerStartable.java':
        '2840e3391f3f16e0aac1d82d50179b0f06f31567453e89288b512e245f7503ef',
    MODULE + '/src/test/java/org/apache/pinot/plugin/stream/kafka30/server/KafkaServerStartableTest.java':
        '04159d057b925f9a665e94c245258260cb7d208b1f0931f76f882a82cc3950b0',
    MODULE + '/pom.xml': '65965a1cc806fdb1fe8bcc28adfc7b658343dc251e062f630f2e112d9243f151',
}
CID = re.compile(r'[0-9a-f]{64}')
NAME = re.compile(r'pinot-qs-kafka-[0-9]{13}')
ACTIONS = {'create', 'start', 'oom', 'die', 'destroy', 'stop', 'kill', 'restart'}
MAX_SECONDS = 7200
MAX_CONTAINERS = 8
MAX_EVENTS = 512
MAX_LINE = 8192
MAX_COMMAND_BYTES = 65536
MAX_LOG_BYTES = 16 * 1024 * 1024  # Combined original stdout/stderr per exact full CID.
EVENT_FORMAT = ('{"action":{{json .Action}},"id":{{json .Actor.ID}},'
                '"name":{{json (index .Actor.Attributes "name")}},'
                '"image":{{json (index .Actor.Attributes "image")}},'
                '"exitCode":{{json (index .Actor.Attributes "exitCode")}},'
                '"timeNano":{{json .TimeNano}}}')
INSPECT_FORMAT = ('{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Config.Image}},'
                  '"imageId":{{json .Image}},"created":{{json .Created}},'
                  '"status":{{json .State.Status}},"running":{{json .State.Running}},'
                  '"oomKilled":{{json .State.OOMKilled}},"dead":{{json .State.Dead}},'
                  '"pid":{{json .State.Pid}},"exitCode":{{json .State.ExitCode}},'
                  '"startedAt":{{json .State.StartedAt}},"finishedAt":{{json .State.FinishedAt}},'
                  '"ports":{{json .NetworkSettings.Ports}},"memory":{{json .HostConfig.Memory}},'
                  '"memorySwap":{{json .HostConfig.MemorySwap}},"nanoCpus":{{json .HostConfig.NanoCpus}}}')
IMAGE_FORMAT = ('{"id":{{json .Id}},"repoDigests":{{json .RepoDigests}},'
                '"created":{{json .Created}},"os":{{json .Os}},'
                '"architecture":{{json .Architecture}},"size":{{json .Size}}}')


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def proc_identity(pid):
    """Linux PID birth identity; never reads /proc/*/environ or persists argv."""
    raw = (Path('/proc') / str(pid) / 'stat').read_text()
    end = raw.rfind(')')
    fields = raw[end + 2:].split()
    return {'pid': pid, 'start_ticks': int(fields[19]), 'ppid': int(fields[1])}


def parent_matches(identity):
    try:
        return os.getppid() == identity['pid'] and proc_identity(identity['pid']) == identity
    except (OSError, ValueError, IndexError):
        return False


def direct_parent_owner(supplied_pid):
    """Bind the declared lifetime owner to this process's actual direct parent."""
    actual_parent = os.getppid()
    require(actual_parent > 1 and supplied_pid == actual_parent and supplied_pid != os.getpid(),
            'Observer owner must be the live actual direct parent, never self/foreign/reparented PID')
    identity = proc_identity(actual_parent)
    require(identity['pid'] == supplied_pid and parent_matches(identity),
            'Observer parent birth identity changed or process was already reparented')
    return identity


def owner_state(identity):
    """Record exact parent birth state without serializing argv or environment."""
    actual_parent = os.getppid()
    try:
        current = proc_identity(identity['pid'])
    except (OSError, ValueError, IndexError):
        current = None
    return {'actual_parent_pid': actual_parent, 'owner_birth_identity': current,
            'birth_unchanged': current == identity,
            'still_actual_parent': actual_parent == identity['pid'] and current == identity}


def output_root(output, runner_temp, workspace):
    output, temp, workspace = Path(output).absolute(), Path(runner_temp).resolve(), Path(workspace).resolve()
    require(output.parent == temp and not output.is_relative_to(workspace),
            'Diagnostic output must be a new private runner-temp child outside the SUT')
    require(output.resolve() == output and not any(p.is_symlink() for p in output.parents),
            'Diagnostic output ancestor is a symlink')
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    return output


def check_sources(workspace, role, expected):
    require(role in REFS and expected == REFS[role], 'Unreviewed source role/ref')
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=workspace, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5).stdout.decode().strip()
    require(head == expected, 'Actual source checkout is not the declared role')
    actual = {name: digest((workspace / name).read_bytes()) for name in SOURCE_PINS}
    require(actual == SOURCE_PINS, 'Kafka test/Main/POM source changed')
    return {'head': head, 'source_sha256': actual}


def command(binary, operation, start, end, identity=None):
    """Finite read-only command allowlist. Names never enter Docker command targets."""
    if operation == 'events':
        argv = [binary, 'events', '--since', str(int(start)), '--until', str(int(end)),
                '--filter', 'type=container', '--filter', 'image=' + IMAGE, '--format', EVENT_FORMAT]
        for action in sorted(ACTIONS):
            argv += ['--filter', 'event=' + action]
        return argv
    if operation == 'context':
        return [binary, 'context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}']
    if operation == 'inspect':
        require(isinstance(identity, str) and CID.fullmatch(identity), 'Inspect requires a full CID')
        return [binary, 'container', 'inspect', '--format', INSPECT_FORMAT, identity]
    if operation == 'logs':
        require(isinstance(identity, str) and CID.fullmatch(identity), 'Follow requires a full CID')
        return [binary, 'container', 'logs', '--follow', '--timestamps', '--tail', 'all',
                '--since', str(int(start)), '--until', str(int(end)), identity]
    if operation == 'image':
        require(isinstance(identity, str) and re.fullmatch(r'sha256:[0-9a-f]{64}', identity),
                'Image inspect requires an exact image ID')
        return [binary, 'image', 'inspect', '--format', IMAGE_FORMAT, identity]
    raise ValueError('Unapproved Docker operation')


def event_record(raw, start, end):
    require(len(raw) <= MAX_LINE, 'Oversized Docker event')
    value = json.loads(raw)
    require(set(value) == {'action', 'id', 'name', 'image', 'exitCode', 'timeNano'}, 'Unexpected event fields')
    require(value['action'] in ACTIONS and CID.fullmatch(value['id']) and NAME.fullmatch(value['name'])
            and value['image'] == IMAGE, 'Foreign event scope/name/image/CID')
    require(type(value['timeNano']) is int and int(start * 1e9) <= value['timeNano'] <= int(end * 1e9),
            'Stale or future event')
    require(isinstance(value['exitCode'], str) and re.fullmatch(r'[0-9]{0,3}', value['exitCode']),
            'Unexpected event exit code')
    return value


def container_record(raw, event):
    value = json.loads(raw)
    keys = {'id', 'name', 'image', 'imageId', 'created', 'status', 'running', 'oomKilled', 'dead',
            'pid', 'exitCode', 'startedAt', 'finishedAt', 'ports', 'memory', 'memorySwap', 'nanoCpus'}
    require(set(value) == keys, 'Unexpected inspect fields (Env/Cmd/labels/health output are never collected)')
    require(value['id'] == event['id'] and value['name'] == '/' + event['name']
            and value['image'] == IMAGE and re.fullmatch(r'sha256:[0-9a-f]{64}', value['imageId']),
            'Container CID/name/image changed')
    require(all(type(value[k]) is bool for k in ['running', 'oomKilled', 'dead'])
            and all(type(value[k]) is int for k in ['pid', 'exitCode', 'memory', 'memorySwap', 'nanoCpus']),
            'Unexpected state types')
    require(all(isinstance(value[k], str) and len(value[k]) < 100
                for k in ['created', 'status', 'startedAt', 'finishedAt']), 'Unbounded state strings')
    ports = value['ports'] or {}
    require(isinstance(ports, dict) and len(ports) <= 8, 'Unbounded port inventory')
    for port, bindings in ports.items():
        require(re.fullmatch(r'[0-9]{1,5}/(?:tcp|udp)', port), 'Invalid container port')
        if bindings is not None:
            require(isinstance(bindings, list) and len(bindings) <= 4, 'Unbounded port bindings')
            for binding in bindings:
                require(set(binding) == {'HostIp', 'HostPort'} and len(binding['HostIp']) <= 50
                        and re.fullmatch(r'[0-9]{1,5}', binding['HostPort']), 'Unexpected port fields')
    return value


class Child:
    """Signals only a pidfd for a read client created by this observer."""
    def __init__(self, process, pidfd):
        self.process, self.pidfd, self.closed = process, pidfd, False

    async def close(self):
        if self.closed:
            return
        try:
            if self.process.returncode is None:
                try:
                    signal.pidfd_send_signal(self.pidfd, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), 2)
                except asyncio.TimeoutError:
                    try:
                        signal.pidfd_send_signal(self.pidfd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await asyncio.wait_for(self.process.wait(), 2)
        finally:
            os.close(self.pidfd)
            self.closed = True


class Observer:
    def __init__(self, args, folder, binary, owner):
        self.args, self.folder, self.binary, self.owner = args, folder, binary, owner
        self.start = time.time()
        self.end = self.start + args.seconds
        self.deadline = time.monotonic() + args.seconds
        self.children, self.followers, self.tasks, self.containers = [], {}, [], {}
        self.event_count, self.rejected, self.truncated = 0, 0, False
        self.stop_requested = False
        source_bytes = Path(__file__).read_bytes()
        (folder / 'observer-source.py').write_bytes(source_bytes)
        self.records = {'schema_version': 1, 'purpose': 'DIAGNOSTIC_ONLY', 'formal_credit': False,
                        'root_cause': None, 'test_or_runtime_ownership_acceptance': False,
                        'creator_module_test_attribution': 'UNPROVEN_HOLD', 'owner_process': owner,
                        'owner_binding': 'SUPPLIED_PID_IS_ACTUAL_DIRECT_PARENT_WITH_PINNED_BIRTH',
                        'owner_before': owner_state(owner),
                        'declared_control': args.control, 'run_id': args.run_id, 'attempt': args.attempt,
                        'role': args.role, 'scope': 'unit2', 'start_epoch': self.start, 'expiry_epoch': self.end,
                        'observer_sha256': digest(source_bytes), 'observer_source_archive': 'observer-source.py',
                        'observer_source_size': len(source_bytes), 'issues': []}

    def write(self, name, value):
        target = self.folder / name
        require(target.parent == self.folder, 'Unapproved output member')
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

    async def spawn(self, operation, identity=None):
        require(parent_matches(self.owner), 'Actual direct parent ended or changed before Docker read spawn')
        argv = command(self.binary, operation, self.start, self.end, identity)
        # A private Python bootstrap waits for a byte before exec. Acquire the
        # pidfd while it is still waiting, so even very short read clients cannot
        # race PID registration. On registration failure EOF exits without Docker.
        gate_read, gate_write = os.pipe()
        bootstrap = ("import os,sys; f=int(sys.argv[1]); b=os.read(f,1); os.close(f); "
                     "b==b'R' or sys.exit(125); os.execv(sys.argv[2],sys.argv[2:])")
        try:
            process = await asyncio.create_subprocess_exec(sys.executable, '-c', bootstrap,
                        str(gate_read), *argv, stdin=asyncio.subprocess.DEVNULL,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        start_new_session=True, pass_fds=(gate_read,), limit=MAX_LINE)
        except Exception:
            os.close(gate_write)
            raise
        finally:
            os.close(gate_read)
        child = None
        try:
            child = Child(process, os.pidfd_open(process.pid))
            os.write(gate_write, b'R')
        except Exception:
            os.close(gate_write)
            if child is not None:
                await child.close()
            else:
                await asyncio.wait_for(process.wait(), 2)
            raise
        else:
            os.close(gate_write)
        self.children.append(child)
        return child

    async def bounded(self, reader, maximum):
        data = bytearray()
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return bytes(data)
            require(len(data) + len(chunk) <= maximum, 'Read-client output exceeds diagnostic bound')
            data.extend(chunk)

    async def snapshot(self, operation, identity=None):
        remaining = min(3, self.deadline - time.monotonic())
        if remaining <= 0:
            return None
        child = await self.spawn(operation, identity)
        try:
            data, errors, code = await asyncio.wait_for(asyncio.gather(
                self.bounded(child.process.stdout, MAX_COMMAND_BYTES),
                self.bounded(child.process.stderr, MAX_COMMAND_BYTES), child.process.wait()), remaining)
            return data if code == 0 else None
        except (ValueError, asyncio.TimeoutError):
            self.records['issues'].append(operation + '_unavailable_or_output_bound')
            return None
        finally:
            await child.close()
            self.children.remove(child)

    async def follow(self, event):
        cid = event['id']
        if cid in self.followers:
            return
        child = await self.spawn('logs', cid)
        self.followers[cid] = child
        budget = {'bytes': 0, 'truncated': False}
        async def copy(reader, suffix):
            with (self.folder / (cid + suffix)).open('xb') as output:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    left = MAX_LOG_BYTES - budget['bytes']
                    output.write(chunk[:left]); output.flush()
                    budget['bytes'] += min(left, len(chunk))
                    if len(chunk) > left:
                        budget['truncated'] = self.truncated = True
                        break
        self.containers[cid]['log_follow'] = {'command': 'docker container logs --follow --timestamps',
            'full_cid': cid, 'start_epoch': time.time(), 'limits': budget,
            'creator_module_test_attribution': 'UNPROVEN_HOLD'}
        self.tasks += [asyncio.create_task(copy(child.process.stdout, '.stdout.log')),
                       asyncio.create_task(copy(child.process.stderr, '.stderr.log'))]

    async def inspect_event(self, event):
        cid = event['id']
        raw = await self.snapshot('inspect', cid)
        if raw is None:
            self.containers[cid]['snapshots'].append({'event': event['action'], 'unavailable': True})
            return
        try:
            state = container_record(raw, event)
        except (ValueError, TypeError, KeyError):
            self.records['issues'].append('inspect_scope_mismatch'); return
        self.containers[cid]['snapshots'].append({'event': event['action'], 'state': state})
        if event['action'] in ('start', 'oom', 'die'):
            await self.follow(event)
        if 'image_snapshot' not in self.containers[cid]:
            raw = await self.snapshot('image', state['imageId'])
            if raw:
                image = json.loads(raw)
                require(set(image) == {'id', 'repoDigests', 'created', 'os', 'architecture', 'size'}
                        and image['id'] == state['imageId'], 'Unexpected image fields')
                require(type(image['size']) is int and image['size'] >= 0
                        and all(isinstance(image[k], str) and len(image[k]) <= 100
                                for k in ['created', 'os', 'architecture']), 'Invalid bounded image metadata')
                require(isinstance(image.get('repoDigests') or [], list)
                        and len(image.get('repoDigests') or []) <= 8
                        and all(isinstance(v, str) and len(v) <= 512
                                for v in image.get('repoDigests') or []), 'Unbounded image digest inventory')
                self.containers[cid]['image_snapshot'] = image

    async def run(self):
        self.records['source_before'] = check_sources(self.args.workspace, self.args.role, self.args.source)
        # Refuse remote contexts without serializing any host/context/credential value.
        require(os.environ.get('DOCKER_HOST', '') in ('', 'unix:///var/run/docker.sock')
                and os.environ.get('DOCKER_CONTEXT', '') in ('', 'default'), 'Nonlocal Docker context unsupported')
        context = await self.snapshot('context')
        require(context and json.loads(context) == 'unix:///var/run/docker.sock', 'Nonlocal default Docker endpoint')
        events = await self.spawn('events')
        self.tasks.append(asyncio.create_task(self.bounded(events.process.stderr, MAX_COMMAND_BYTES)))
        self.write('ready.json', {'pid': os.getpid(), 'identity': proc_identity(os.getpid()),
                   'purpose': 'diagnostic_only', 'end_epoch': self.end})
        try:
            while (time.monotonic() < self.deadline and parent_matches(self.owner)
                   and not (self.folder / 'stop.request').exists() and not self.truncated and not self.stop_requested):
                try:
                    raw = await asyncio.wait_for(events.process.stdout.readline(), 0.25)
                except asyncio.TimeoutError:
                    continue
                if not raw:
                    self.records['issues'].append('event_stream_ended'); break
                try:
                    event = event_record(raw, self.start, self.end)
                except (ValueError, TypeError, KeyError):
                    self.rejected += 1; continue
                cid = event['id']
                if cid not in self.containers:
                    if len(self.containers) == MAX_CONTAINERS:
                        self.truncated = True; break
                    self.containers[cid] = {'id': cid, 'name': event['name'], 'image': IMAGE,
                        'creator_module_test_attribution': 'UNPROVEN_HOLD', 'events': [], 'snapshots': []}
                require(self.containers[cid]['name'] == event['name'], 'CID renamed during diagnostic phase')
                if self.event_count == MAX_EVENTS:
                    self.truncated = True; break
                self.containers[cid]['events'].append(event); self.event_count += 1
                self.write('events-and-state.json', self.containers)
                actions = {v['event'] for v in self.containers[cid]['snapshots']}
                if event['action'] not in actions:
                    await self.inspect_event(event)
                self.write('events-and-state.json', self.containers)
        finally:
            if time.monotonic() >= self.deadline:
                reason = 'diagnostic_deadline'
            elif not parent_matches(self.owner):
                reason = 'owner_process_ended_or_changed'
            elif self.stop_requested:
                reason = 'observer_signal'
            elif (self.folder / 'stop.request').exists():
                reason = 'control_stop_request'
            elif self.truncated:
                reason = 'diagnostic_output_bound'
            else:
                reason = 'event_stream_end_or_error'
            self.records['stop_reason'] = reason
            await self.cleanup()

    async def cleanup(self):
        # Independent pidfds permit parallel bounded cleanup, regardless of PID reuse.
        outcomes = await asyncio.gather(*(child.close() for child in list(self.children)), return_exceptions=True)
        if any(isinstance(value, BaseException) for value in outcomes):
            self.records['issues'].append('private_read_child_cleanup_incomplete')
        self.children.clear()
        for cid, child in self.followers.items():
            self.containers[cid]['log_follow']['read_client_exit_code'] = child.process.returncode
        if self.tasks:
            try:
                outcomes = await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), 2)
                if any(isinstance(value, BaseException) for value in outcomes):
                    self.records['issues'].append('read_client_output_task_incomplete')
            except asyncio.TimeoutError:
                self.records['issues'].append('read_client_output_drain_incomplete')
                for task in self.tasks:
                    task.cancel()
                await asyncio.gather(*self.tasks, return_exceptions=True)
        self.write('events-and-state.json', self.containers)


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workspace', type=Path, required=True)
    p.add_argument('--runner-temp', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--owner-pid', type=int, required=True)
    p.add_argument('--source', required=True)
    p.add_argument('--control', required=True)
    p.add_argument('--role', choices=sorted(REFS), required=True)
    p.add_argument('--run-id', type=int, required=True)
    p.add_argument('--attempt', type=int, required=True)
    p.add_argument('--seconds', type=int, default=7200)
    a = p.parse_args(argv)
    require(1 <= a.seconds <= MAX_SECONDS and a.owner_pid > 1 and a.run_id > 0 and a.attempt == 1,
            'Unbounded expiry or invalid process/run identity')
    require(re.fullmatch(r'[0-9a-f]{40}', a.control) and a.source == REFS[a.role], 'Unreviewed role/control/source')
    return a


def main(argv=None):
    args = arguments(argv)
    owner = direct_parent_owner(args.owner_pid)
    require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal') and Path('/proc/self/stat').is_file(),
            'Linux pidfd support is required before launching any Docker read client')
    folder = output_root(args.output, args.runner_temp, args.workspace)
    binary = shutil.which('docker'); require(binary is not None, 'Docker CLI unavailable')
    observer = Observer(args, folder, str(Path(binary).resolve()), owner)
    try:
        async def execute():
            loop = asyncio.get_running_loop()
            for value in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(value, setattr, observer, 'stop_requested', True)
            try:
                await observer.run()
            except Exception as error:
                observer.records['issues'].append(type(error).__name__ + ':diagnostic_incomplete')
            finally:
                await observer.cleanup()
        asyncio.run(execute())
    except Exception as error:
        observer.records['issues'].append(type(error).__name__ + ':cleanup_or_loop_incomplete')
    finally:
        try:
            observer.records['source_after'] = check_sources(args.workspace, args.role, args.source)
        except Exception:
            observer.records['issues'].append('source_after_changed_or_unreadable')
        observer.records['owner_after'] = owner_state(owner)
        observer_source_after = digest(Path(__file__).read_bytes())
        if observer_source_after != observer.records['observer_sha256']:
            observer.records['issues'].append('observer_source_changed')
        observer.records.update(observer_sha256_after=observer_source_after,
                                observer_source_unchanged=observer_source_after == observer.records['observer_sha256'],
                                end_epoch=time.time(), rejected_events=observer.rejected,
                                truncated=observer.truncated, source_unchanged=observer.records.get('source_before') is not None and
                                observer.records.get('source_before') == observer.records.get('source_after'))
        observer.write('diagnostic.json', observer.records)
        files = {p.name: {'sha256': digest(p.read_bytes()), 'size': p.stat().st_size}
                 for p in folder.iterdir() if p.is_file() and p.name not in ('stop.request',)}
        observer.write('manifest.json', {'purpose': 'DIAGNOSTIC_ONLY_NO_FORMAL_CREDIT', 'files': files,
                                        'test_exit_code_owned_by_original_runner': True,
                                        'excluded_control_members': ['stop.request'],
                                        'manifest_self_hash_requires_external_artifact_receipt': True})
    return 0  # Diagnostic status never substitutes for the original runner's status.


if __name__ == '__main__':
    raise SystemExit(main())
