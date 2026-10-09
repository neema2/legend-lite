"""The wheel (//python:wheel) as a developer installs it: a fresh virtual environment of the repository's Python, the
wheel and its packages installed offline (Bazel's own downloads, the pinned versions), then show() and a query run in
isolated mode, from outside the repository, with no setting pointing back into it -- so the wheel stands alone: its
modules, the compiler's library and DataCube's page all come from inside it. And its label told true: the platform tag
the wheel declares against what its library needs, read from the library itself (its minimum macOS, or the newest
glibc it names)."""

import json
import os
import re
import struct
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

# resolved through the runfiles by main.py, as every suite's files are
WHEEL = Path(os.environ['LEGEND_LITE_WHEEL'])
DEPENDENCIES = [Path(p) for p in os.environ['LEGEND_LITE_DEPENDENCY_WHEELS'].split()]

# run in the installed environment: a frame shown, its cube asked for, a query executed as DataCube sends it
SCRIPT = r'''
import json, urllib.request, sys
import pandas as pd
import pyarrow as pa, pyarrow.ipc
import legend_lite as ll
from legend_lite import datacube
cube = ll.show(pd.DataFrame({"desk": ["FX", "EQ", "FX"], "qty": [1.5, 2.5, 4.0]}), name="trades", browser=False)
engine = datacube._session.engine()
table = datacube._session.frames["trades"]
headers = {"Authorization": engine.authorization}
page = urllib.request.urlopen(urllib.request.Request(engine.url + "/engine.html"), timeout=30).status
cube_json = json.loads(urllib.request.urlopen(urllib.request.Request(engine.url + "/cube.json?table=trades", headers=headers), timeout=30).read())
tree = ll.parse("|" + table.accessor + "->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])->sort(~desk->ascending())->from(" + table.runtime + ")")
body = json.dumps({"clientVersion": "vX_X_X", "function": tree, "model": {"_type": "text", "code": table.model},
                   "context": {"_type": "BaseExecutionContext"}}).encode()
answer = urllib.request.urlopen(urllib.request.Request(engine.url + "/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC",
                                body, dict(headers, **{"Content-Type": "application/json"})), timeout=30).read()
with pa.input_stream(pa.py_buffer(answer), compression="zstd") as stream:
    rows = pyarrow.ipc.open_stream(stream).read_all().to_pylist()
cube.close()
print(json.dumps({"module": ll.__file__, "library": __import__("legend_lite._library", fromlist=["x"]).library().path.as_posix(),
                  "page": page, "cube": sorted(cube_json), "rows": rows}))
'''


def wheel_tag(wheel: zipfile.ZipFile) -> str:
    info = next(n for n in wheel.namelist() if n.endswith('.dist-info/WHEEL'))
    return re.search(r'^Tag: (.+)$', wheel.read(info).decode(), re.M).group(1)


def macos_minimum(data: bytes) -> tuple[int, int]:
    """A Mach-O library's minimum macOS (its LC_BUILD_VERSION's minos)."""
    magic, _, _, _, ncmds = struct.unpack_from('<IiiII', data, 0)
    assert magic == 0xFEEDFACF, 'not a 64-bit Mach-O'
    offset = 32
    for _ in range(ncmds):
        cmd, size = struct.unpack_from('<II', data, offset)
        if cmd == 0x32:  # LC_BUILD_VERSION: platform, minos (xxxx.yy.zz), ...
            minos = struct.unpack_from('<I', data, offset + 12)[0]
            return minos >> 16, (minos >> 8) & 0xFF
        offset += size
    raise AssertionError('the library names no minimum macOS (no LC_BUILD_VERSION)')


def glibc_needed(data: bytes) -> tuple[int, int]:
    """The newest glibc version an ELF library names (its .gnu.version_r)."""
    assert data[:4] == b'\x7fELF' and data[4] == 2 and data[5] == 1, 'not a 64-bit little-endian ELF'
    shoff = struct.unpack_from('<Q', data, 0x28)[0]
    shentsize, shnum = struct.unpack_from('<HH', data, 0x3A)
    sections = [struct.unpack_from('<IIQQQQIIQQ', data, shoff + i * shentsize) for i in range(shnum)]
    needed = []
    for _, kind, _, _, offset, _, link, count, _, _ in sections:
        if kind != 0x6FFFFFFE:  # SHT_GNU_verneed
            continue
        strings = sections[link][4]
        for _ in range(count):
            _, aux_count, _, aux, following = struct.unpack_from('<HHIII', data, offset)
            at = offset + aux
            for _ in range(aux_count):
                _, _, _, name, aux_next = struct.unpack_from('<IHHII', data, at)
                text = data[strings + name:data.index(b'\0', strings + name)].decode()
                if text.startswith('GLIBC_'):
                    needed.append(tuple(int(n) for n in text[len('GLIBC_'):].split('.')[:2]))
                at += aux_next
            offset += following
    return max(needed)


class Label(unittest.TestCase):
    def test_the_platform_tag_is_what_the_library_needs(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            tag = wheel_tag(wheel)
            library = next(n for n in wheel.namelist() if '/_native/libcompiler' in n)
            data = wheel.read(library)
        self.assertRegex(tag, r'^py3-none-')
        if m := re.search(r'macosx_(\d+)_(\d+)_', tag):
            self.assertEqual(macos_minimum(data), (int(m.group(1)), int(m.group(2))),
                             f'{library}: its minimum macOS against the tag {tag}')
        elif m := re.search(r'manylinux_(\d+)_(\d+)_', tag):
            needed = glibc_needed(data)
            print(f'the library needs glibc {needed[0]}.{needed[1]}; the tag says {m.group(1)}.{m.group(2)}')
            self.assertLessEqual(needed, (int(m.group(1)), int(m.group(2))), f'{library} against the tag {tag}')
        else:
            self.fail(f'no platform this test reads: {tag}')

    def test_the_metadata_says_what_it_needs(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            info = next(n for n in wheel.namelist() if n.endswith('.dist-info/METADATA'))
            metadata = wheel.read(info).decode()
        self.assertIn('Requires-Python: >=3.12', metadata)
        for requirement in ('duckdb>=1.5.5', 'pyarrow>=23.0.1'):
            self.assertIn(f'Requires-Dist: {requirement}', metadata)


class Installed(unittest.TestCase):
    def test_installed_it_shows_a_frame_and_answers_a_query_on_its_own(self):
        root = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
        # nothing of the repository's or the machine's: no PYTHONPATH, no LEGEND_LITE_*, no PATH
        env = {'HOME': str(root), 'TMPDIR': str(root), 'PYTHONDONTWRITEBYTECODE': '1'}
        venv = root / 'venv'
        subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True, env=env, timeout=120)
        python = str(venv / 'bin' / 'python')
        subprocess.run([python, '-m', 'pip', 'install', '--no-index', '--no-deps', '--disable-pip-version-check', '-q',
                        str(WHEEL), *map(str, DEPENDENCIES)], check=True, env=env, timeout=240)
        ran = subprocess.run([python, '-I', '-c', SCRIPT], cwd=root, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
        out = json.loads(ran.stdout.strip().splitlines()[-1])
        self.assertTrue(out['module'].startswith(str(venv)), out['module'])
        self.assertTrue(out['library'].startswith(str(venv)), out['library'])
        self.assertEqual((out['page'], out['cube']), (200, ['model', 'runtime', 'source', 'title', 'version']))
        self.assertEqual(out['rows'], [{'desk': 'EQ', 'q': 2.5}, {'desk': 'FX', 'q': 5.5}])
