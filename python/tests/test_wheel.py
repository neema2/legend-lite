"""The wheel (//python:wheel) as a developer installs it: a fresh virtual environment of the repository's Python, the
wheel and its packages installed offline (Bazel's own downloads, the pinned versions, the notebook extra's with them),
then show(), a query and a notebook's cube run in isolated mode, from outside the repository, with no setting pointing
back into it -- so the wheel stands alone: its modules, the compiler's library, DataCube's page and its notebook module
all come from inside it. And its label told true: the platform tag
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
DEPENDENCIES = [Path(p) for p in os.environ['LEGEND_LITE_DEPENDENCY_WHEELS'].split(os.pathsep)]

# run in the installed environment: a frame shown, its cube asked for, a query executed as DataCube sends it
SCRIPT = r'''
import json, urllib.request, sys
import pandas as pd
import pyarrow as pa, pyarrow.ipc
import legend_lite as ll
from legend_lite import datacube
cube = ll.show(pd.DataFrame({"desk": ["FX", "EQ", "FX"], "qty": [1.5, 2.5, 4.0]}), name="trades", browser=False)
web = datacube._session.web()
table = datacube._session.frames["trades"]
headers = {"Authorization": web.authorization}
page = urllib.request.urlopen(urllib.request.Request(web.url + "/engine.html"), timeout=30).status
cube_json = json.loads(urllib.request.urlopen(urllib.request.Request(web.url + "/cube.json?table=trades", headers=headers), timeout=30).read())
tree = ll.parse("|" + table.accessor + "->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])->sort(~desk->ascending())->from(" + table.runtime + ")")
body = json.dumps({"clientVersion": "vX_X_X", "function": tree, "model": {"_type": "text", "code": table.model},
                   "context": {"_type": "BaseExecutionContext"}}).encode()
answer = urllib.request.urlopen(urllib.request.Request(web.url + "/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC",
                                body, dict(headers, **{"Content-Type": "application/json"})), timeout=30).read()
with pa.input_stream(pa.py_buffer(answer), compression="zstd") as stream:
    rows = pyarrow.ipc.open_stream(stream).read_all().to_pylist()
cube.close()
# the notebook extra: a notebook's cube (no kernel here), its script the loader, and DataCube's module answered over
# its channel as its page asks for it
widget = ll.DataCube(pd.DataFrame({"desk": ["FX"], "qty": [1.0]}), name="nb")
sent = []
widget.send = lambda content, buffers=None: sent.append((content, buffers))
for name in ("widget.js", "widget.css"):
    widget._answer({"kind": "call", "id": name, "method": "GET", "path": "/" + name, "query": "", "body": None})
notebook = {"loader": " as default" in widget._esm, "answers": [(c["status"], len(b[0])) for c, b in sent]}
widget.close()
print(json.dumps({"module": ll.__file__, "library": __import__("legend_lite._library", fromlist=["x"]).library().path.as_posix(),
                  "page": page, "cube": sorted(cube_json), "rows": rows, "notebook": notebook}))
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


# what a manylinux_2_17 library may link and name (PEP 599's manylinux2014 policy, which PEP 600's manylinux_2_17
# keeps): the system's own libraries, never one a machine may lack (libz, libssl, ...), and symbol versions no newer
# than these
MANYLINUX_VERSIONS = {'GLIBC': (2, 17), 'CXXABI': (1, 3, 7), 'GLIBCXX': (3, 4, 19), 'GCC': (4, 8, 0)}
MANYLINUX_LIBRARIES = {
    'libgcc_s.so.1', 'libstdc++.so.6', 'libm.so.6', 'libdl.so.2', 'librt.so.1', 'libc.so.6', 'libnsl.so.1',
    'libutil.so.1', 'libpthread.so.0', 'libresolv.so.2', 'libX11.so.6', 'libXext.so.6', 'libXrender.so.1',
    'libICE.so.6', 'libSM.so.6', 'libGL.so.1', 'libgobject-2.0.so.0', 'libgthread-2.0.so.0', 'libglib-2.0.so.0',
    # the dynamic loader itself
    'ld-linux-x86-64.so.2', 'ld-linux-aarch64.so.1',
}


def elf_sections(data: bytes) -> list[tuple[int, ...]]:
    """A 64-bit little-endian ELF file's section headers (Elf64_Shdr)."""
    assert data[:4] == b'\x7fELF' and data[4] == 2 and data[5] == 1, 'not a 64-bit little-endian ELF'
    shoff = struct.unpack_from('<Q', data, 0x28)[0]
    shentsize, shnum = struct.unpack_from('<HH', data, 0x3A)
    return [struct.unpack_from('<IIQQQQIIQQ', data, shoff + i * shentsize) for i in range(shnum)]


def elf_linked(data: bytes) -> set[str]:
    """The libraries an ELF library links (its .dynamic DT_NEEDED entries)."""
    sections = elf_sections(data)
    linked = set()
    for _, kind, _, _, offset, size, link, _, _, _ in sections:
        if kind != 6:  # SHT_DYNAMIC
            continue
        strings = sections[link][4]
        for at in range(offset, offset + size, 16):
            tag, value = struct.unpack_from('<qQ', data, at)
            if tag == 0:  # DT_NULL
                break
            if tag == 1:  # DT_NEEDED
                linked.add(data[strings + value:data.index(b'\0', strings + value)].decode())
    return linked


def elf_versions(data: bytes) -> dict[str, list[tuple[int, ...]]]:
    """The symbol versions an ELF library needs (its .gnu.version_r), by family: GLIBC, GLIBCXX, CXXABI, GCC;
    a version that is no number (GLIBC_PRIVATE) is the loader's own and not counted."""
    sections = elf_sections(data)
    needed: dict[str, list[tuple[int, ...]]] = {}
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
                family, _, version = text.partition('_')
                if version and all(part.isdigit() for part in version.split('.')):
                    needed.setdefault(family, []).append(tuple(int(n) for n in version.split('.')))
                at += aux_next
            offset += following
    return needed


# a library's architecture, against its tag's: Mach-O cputype, ELF e_machine
MACHO_CPU = {'arm64': 0x0100000C, 'x86_64': 0x01000007}
ELF_MACHINE = {'aarch64': 183, 'x86_64': 62}


class Label(unittest.TestCase):
    def test_the_platform_tag_is_what_the_library_needs(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            tag = wheel_tag(wheel)
            library = next(n for n in wheel.namelist() if '/_native/libcompiler' in n)
            data = wheel.read(library)
        self.assertRegex(tag, r'^py3-none-')
        if m := re.search(r'macosx_(\d+)_(\d+)_(\w+)$', tag):
            self.assertEqual(macos_minimum(data), (int(m.group(1)), int(m.group(2))),
                             f'{library}: its minimum macOS against the tag {tag}')
            self.assertEqual(struct.unpack_from('<i', data, 4)[0], MACHO_CPU[m.group(3)], f'{library}: its CPU')
        elif m := re.search(r'manylinux_(\d+)_(\d+)_(\w+)$', tag):
            versions = elf_versions(data)
            needed = max(versions['GLIBC'])[:2]
            linked = elf_linked(data)
            print(f'the library needs glibc {needed[0]}.{needed[1]}; the tag says {m.group(1)}.{m.group(2)}; '
                  f'it links {sorted(linked)}; it names {sorted((f, max(v)) for f, v in versions.items())}')
            # the tag IS the measurement: a library needing more, or less, says to change the tag on purpose
            self.assertEqual(needed, (int(m.group(1)), int(m.group(2))), f'{library} against the tag {tag}')
            self.assertLessEqual(linked, MANYLINUX_LIBRARIES, f'{library} links what a manylinux machine may lack')
            for family, newest in MANYLINUX_VERSIONS.items():
                self.assertLessEqual(max(versions.get(family, [()])), newest, f'{library}: its {family} versions')
            self.assertEqual(struct.unpack_from('<H', data, 0x12)[0], ELF_MACHINE[m.group(3)], f'{library}: its CPU')
        else:
            self.fail(f'no platform this test reads: {tag}')

    def test_the_licences_ship_inside_it(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            info = {n.rsplit('/', 1)[1]: n for n in wheel.namelist() if '.dist-info/' in n}
            notice = wheel.read(info['NOTICE']).decode()
            licence = wheel.read(info['LICENSE']).decode()
        self.assertIn('Apache License', licence)
        for credited in ('clean-sheet implementation', 'legend-pure', 'legend-engine', 'Copyright 2020 Goldman Sachs'):
            self.assertIn(credited, notice)
        for graalvm in ('GRAALVM-LICENSE.txt', 'GRAALVM-LICENSE-NATIVEIMAGE.txt', 'GRAALVM-THIRD-PARTY-LICENSE.txt'):
            self.assertIn(graalvm, info, 'the runtime compiled into the library brings its licences')

    def test_the_metadata_says_what_it_needs(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            info = next(n for n in wheel.namelist() if n.endswith('.dist-info/METADATA'))
            metadata = wheel.read(info).decode()
        self.assertIn('Requires-Python: >=3.12', metadata)
        self.assertIn('License: Apache-2.0', metadata)
        for requirement in ('duckdb>=1.5.5', 'pyarrow>=23.0.1', "anywidget>=0.11.0; extra == 'notebook'"):
            self.assertIn(f'Requires-Dist: {requirement}', metadata)


class Installed(unittest.TestCase):
    def test_installed_it_shows_a_frame_and_answers_a_query_on_its_own(self):
        place = tempfile.TemporaryDirectory(dir=os.environ.get('TEST_TMPDIR'))
        self.addCleanup(place.cleanup)
        root = Path(place.name)
        # nothing of the repository's or the machine's: no PYTHONPATH, no LEGEND_LITE_*, no PATH, no pip.conf
        env = {'HOME': str(root), 'TMPDIR': str(root), 'PYTHONDONTWRITEBYTECODE': '1', 'PIP_CONFIG_FILE': os.devnull}
        venv = root / 'venv'
        subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True, env=env, timeout=120)
        python = str(venv / 'bin' / 'python')
        subprocess.run([python, '-m', 'pip', 'install', '--no-index', '--no-deps', '--disable-pip-version-check', '-q',
                        str(WHEEL), *map(str, DEPENDENCIES)], check=True, env=env, timeout=240)
        # what each wheel says it needs (legend-lite's Requires-Dist, pandas' own) is what is installed
        checked = subprocess.run([python, '-m', 'pip', 'check', '--disable-pip-version-check'], env=env,
                                 capture_output=True, text=True, timeout=60)
        self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
        ran = subprocess.run([python, '-I', '-c', SCRIPT], cwd=root, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
        out = json.loads(ran.stdout.strip().splitlines()[-1])
        self.assertTrue(out['module'].startswith(str(venv)), out['module'])
        self.assertTrue(out['library'].startswith(str(venv)), out['library'])
        self.assertEqual((out['page'], out['cube']), (200, ['model', 'runtime', 'source', 'title', 'version']))
        self.assertEqual(out['rows'], [{'desk': 'EQ', 'q': 2.5}, {'desk': 'FX', 'q': 5.5}])
        # the notebook extra installed: the cube's loader, and DataCube's module (about 1.3 MB) and styles, from the wheel
        self.assertTrue(out['notebook']['loader'], out['notebook'])
        self.assertEqual([status for status, _ in out['notebook']['answers']], [200, 200], out['notebook'])
        self.assertGreater(out['notebook']['answers'][0][1], 500_000, out['notebook'])
