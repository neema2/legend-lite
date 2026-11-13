"""A THROWAWAY PROBE (2026-11-13, the orchestrator's (b), reverted in the commit after it): the Linux warehouse lane on
ubuntu-24.04 20261111.127.1 segfaults in mini-racer's MiniRacer() (_make_context) wherever a test makes a second
Headless in one process. Each scenario below runs in a process of its own under faulthandler, so one crash hides none
of the others; the report says each one's exit code (-11: SIGSEGV) and the tail of its stderr, and the host's facts.
It FAILS on purpose, so CI prints its report (--test_output=errors)."""

import os
import platform
import resource
import subprocess
import sys
import textwrap
import unittest

PRELUDE = textwrap.dedent('''
    import faulthandler, sys, threading
    faulthandler.enable()
    from py_mini_racer import MiniRacer
    held = []

    def on_thread(make, name):
        """``make`` run on a fresh thread, which then stays alive holding what it made (as Headless's engine thread)."""
        made, done, stop = {}, threading.Event(), threading.Event()
        def run():
            made['v'] = make()
            done.set()
            stop.wait()
        t = threading.Thread(target=run, name=name, daemon=True)
        t.start()
        if not done.wait(120):
            raise SystemExit('timed out: ' + name)
        held.append(stop)
        return made['v']

    def racer():
        m = MiniRacer()
        held.append(m)
        return m.eval('1 + 1')
''')

SCENARIOS = {
    'one MiniRacer, main thread': 'print(racer())',
    'one MiniRacer, fresh thread': "print(on_thread(racer, 't1'))",
    'two MiniRacers, SAME thread, sequential, first alive': 'print(racer(), racer())',
    'two MiniRacers, same thread, first closed first': textwrap.dedent('''
        a = MiniRacer(); print(a.eval('1')); a.close()
        b = MiniRacer(); print(b.eval('2'))
    '''),
    'two MiniRacers, two fresh threads, first alive': "print(on_thread(racer, 't1'), on_thread(racer, 't2'))",
    'two MiniRacers, two fresh threads, after a native library call': textwrap.dedent('''
        import legend_lite as ll
        ll.parse('|1')
        print(on_thread(racer, 't1'), on_thread(racer, 't2'))
    '''),
    'two MiniRacers, two fresh threads, a DuckDB connection open': textwrap.dedent('''
        import duckdb
        con = duckdb.connect()
        con.execute('SELECT 1').fetchall()
        print(on_thread(racer, 't1'), on_thread(racer, 't2'))
    '''),
    'one real Headless': textwrap.dedent('''
        from legend_lite import Frames
        from legend_lite._headless import Headless, bundle
        f1 = Frames(); h1 = Headless(bundle(), connection=f1.connection); print(h1.evaluate('1 + 1'))
    '''),
    'two real Headless, first alive': textwrap.dedent('''
        from legend_lite import Frames
        from legend_lite._headless import Headless, bundle
        f1 = Frames(); h1 = Headless(bundle(), connection=f1.connection); print(h1.evaluate('1 + 1'))
        f2 = Frames(); h2 = Headless(bundle(), connection=f2.connection); print(h2.evaluate('2 + 2'))
    '''),
    'two real Headless, first closed first': textwrap.dedent('''
        from legend_lite import Frames
        from legend_lite._headless import Headless, bundle
        f1 = Frames(); h1 = Headless(bundle(), connection=f1.connection); print(h1.evaluate('1 + 1')); h1.close()
        f2 = Frames(); h2 = Headless(bundle(), connection=f2.connection); print(h2.evaluate('2 + 2'))
    '''),
    'two MiniRacers, two fresh threads, each with Headless\'s hard memory limit and no WebAssembly': textwrap.dedent('''
        from legend_lite._headless import DEFAULT_MEMORY_LIMIT
        def limited():
            m = MiniRacer(); held.append(m)
            m.set_hard_memory_limit(DEFAULT_MEMORY_LIMIT)
            m.eval('delete globalThis.WebAssembly;')
            return m.eval('1 + 1')
        print(on_thread(limited, 't1'), on_thread(limited, 't2'))
    '''),
    'one real Headless alive, then a bare MiniRacer on a fresh thread': textwrap.dedent('''
        from legend_lite import Frames
        from legend_lite._headless import Headless, bundle
        f1 = Frames(); h1 = Headless(bundle(), connection=f1.connection); print(h1.evaluate('1 + 1'))
        print(on_thread(racer, 't2'))
    '''),
}


def _read(path: str) -> str:
    try:
        with open(path, encoding='utf-8') as f:
            return f.read().strip()
    except OSError as e:
        return f'unreadable: {e}'


def _host() -> list[str]:
    flags: set[str] = set()
    model = ''
    try:
        with open('/proc/cpuinfo', encoding='utf-8') as f:
            for line in f:
                if line.startswith('flags') and not flags:
                    flags = set(line.split(':', 1)[1].split())
                if line.startswith('model name') and not model:
                    model = line.split(':', 1)[1].strip()
    except OSError:
        pass
    try:
        from importlib.metadata import version
        racer = version('mini-racer')
    except Exception as e:  # noqa: BLE001 -- a fact the report says, whatever it is
        racer = f'? {e}'
    return [
        f'uname: {platform.uname()}',
        f'libc: {platform.libc_ver()}',
        f'cpu: {model}; cpus {os.cpu_count()}',
        f'cpu flags of interest: ' + ', '.join(f'{x}={x in flags}' for x in
                                              ('pku', 'ospke', 'la57', 'avx512f', 'shstk', 'ibt', 'user_shstk')),
        f'vm.max_map_count: {_read("/proc/sys/vm/max_map_count")}; vm.overcommit_memory: '
        f'{_read("/proc/sys/vm/overcommit_memory")}; kernel.randomize_va_space: {_read("/proc/sys/kernel/randomize_va_space")}',
        f'RLIMIT_AS: {resource.getrlimit(resource.RLIMIT_AS)}; RLIMIT_STACK: {resource.getrlimit(resource.RLIMIT_STACK)}',
        f'mini-racer: {racer}; python: {sys.version.split()[0]}',
    ]


class V8Probe(unittest.TestCase):
    def test_the_probes_report(self):
        report = ['V8 PROBE REPORT'] + _host()
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
        v8 = subprocess.run([sys.executable, '-X', 'faulthandler', '-c',
                             PRELUDE + "print(MiniRacer().v8_version)"], env=env, capture_output=True, text=True, timeout=300)
        report.append(f'V8: {v8.stdout.strip()} (exit {v8.returncode})')
        for name, body in SCENARIOS.items():
            try:
                ran = subprocess.run([sys.executable, '-X', 'faulthandler', '-c', PRELUDE + body], env=env,
                                     capture_output=True, text=True, timeout=300)
                code, out, err = ran.returncode, ran.stdout.strip(), ran.stderr.strip()
            except subprocess.TimeoutExpired as e:
                code, out, err = 'TIMEOUT', str(e.stdout or ''), str(e.stderr or '')
            verdict = 'OK' if code == 0 else ('SEGFAULT' if code == -11 else f'FAILED ({code})')
            report.append(f'--- {name}: {verdict}; stdout {out[-200:]!r}')
            if code != 0:
                report.append(textwrap.indent('\n'.join(err.splitlines()[-25:]), '    '))
        print('\n'.join(report))
        self.fail('a throwaway probe: it fails so CI prints its report\n' + '\n'.join(report))
