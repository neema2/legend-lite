"""The wheel (//python:wheel) as a developer installs it: a fresh virtual environment of the repository's Python, the
wheel and its packages installed offline (Bazel's own downloads, the pinned versions, the notebook extra's with them),
then show(), a query and a notebook's cube run in isolated mode -- and first the wheel alone, the compiler, which
reaches for DataCube only to be told to install its extra -- from outside the repository, with no setting pointing
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

# run with the wheel alone, before any package of its datacube extra is installed: the compiler works on Python's own
# library, and DataCube -- each way into it -- says, in one sentence, to install the extra (legend_lite/_extra.py)
CORE = r'''
import json
import legend_lite as ll
tree = ll.parse("|1 + 1")
refused = {}
for name in ("show", "Page", "Frames", "Engine", "DataCube"):
    try:
        getattr(ll, name)
        refused[name] = None
    except ImportError as failure:
        refused[name] = str(failure)
try:
    import legend_lite.page
    refused["legend_lite.page"] = None
except ImportError as failure:
    refused["legend_lite.page"] = str(failure)
# what reaches the native library beside the compiler needs no extra: every module the compiler-only install holds
# imports, and a model home answers, on the compiler's threads (an empty folder's projects: none)
import pathlib, tempfile
import legend_lite._home, legend_lite._pool, legend_lite._types, legend_lite.model
homed = list(legend_lite.model.home_call(pathlib.Path(tempfile.mkdtemp()), "sdlc", "GET", "/projects", None))
print(json.dumps({"parsed": sorted(tree), "refused": refused, "homed": homed}))
'''

# run in the installed environment: a frame shown, its cube asked for, a query executed as DataCube sends it
SCRIPT = r'''
import json, urllib.parse, urllib.request, sys
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
# and a lazy chunk of it, the chart's code, as the loader asks for it the first time a chart draws
chunks = __import__("pathlib").Path(ll.__file__).parent / "_site" / "widget-chunks"
for name in ("widget.js", "widget.css", "widget-chunks/" + next(f.name for f in chunks.iterdir() if f.name.startswith("chart-render-"))):
    widget._answer({"kind": "call", "id": name, "method": "GET", "path": "/" + name, "query": "", "body": None})
notebook = {"loader": " as default" in widget._esm, "answers": [(c["status"], len(b[0])) for c, b in sent]}
widget.close()
# the datacube extra: a page's grid run by DataCube's own code in V8, from the wheel alone (its bundle inside it)
grid = ll.Page("headless").grid(pd.DataFrame({"desk": ["FX", "EQ", "FX"], "qty": [1.5, 2.5, 4.0]}), name="hl",
                                rows=["desk"], measures={"qty": "sum"})
view = grid.view()
names = [c["name"] for c in view.columns]
headless = {"groups": sorted([r[names.index("__tree")], r[names.index("qty")]] for r in view.rows),
            "bundle": __import__("legend_lite._headless", fromlist=["x"])._BUNDLE.as_posix()}
# DuckDB's extensions the wheel carries: a page's remote file read through its own httpfs, from a server on this machine
# (nothing downloaded)
import functools, http.server, tempfile, threading
import pyarrow.parquet as pq
served = tempfile.mkdtemp()
pq.write_table(pa.table({"desk": ["FX", "EQ"]}), served + "/remote.parquet")
class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass
files = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Quiet, directory=served))
threading.Thread(target=files.serve_forever, daemon=True).start()
said = datacube._session.engine().answer("POST", "/remote.json", "", json.dumps(
    {"url": "http://127.0.0.1:%d/remote.parquet" % files.server_address[1], "name": "wheel_remote"}))
duck = datacube._session.frames.connection
remote = {"status": said.status, "answer": said.body.decode()[:300],
          "rows": duck.execute("SELECT count(*) FROM wheel_remote").fetchone()[0] if said.status == 200 else None,
          "httpfs": duck.execute("SELECT install_path FROM duckdb_extensions() WHERE extension_name = 'httpfs'").fetchone()[0]}
files.shutdown()
# and an Iceberg table (G5), served by the test, read through the iceberg and avro extensions inside the wheel
said = datacube._session.engine().answer("POST", "/remote.json", "", json.dumps({"url": sys.argv[1], "name": "wheel_iceberg"}))
iceberg = {"status": said.status, "answer": said.body.decode()[:300],
           "rows": duck.execute("SELECT count(*) FROM wheel_iceberg").fetchone()[0] if said.status == 200 else None,
           "extensions": [p for (p,) in duck.execute("SELECT install_path FROM duckdb_extensions() "
                                                     "WHERE extension_name IN ('iceberg', 'avro') AND loaded").fetchall()]}
# Query and Studio served from the wheel (E): each app's link traded for the cookie, its page, its config, and a call the
# page makes with the cookie alone
import http.client, pathlib
apps = {}
def get(link, path, headers=None):
    c = http.client.HTTPConnection(link.hostname, link.port, timeout=30)
    c.request("GET", path, headers=headers or {})
    r = c.getresponse()
    out = (r.status, r.getheader("Set-Cookie"), r.read())
    c.close()
    return out
for name, open_app in (("query", ll.query), ("studio", ll.studio)):
    link = urllib.parse.urlsplit(open_app(browser=False))
    traded, cookie, _ = get(link, link.path + "?" + link.query)
    # the page's own call, as the browser marks it
    jar = {"Cookie": cookie.split(";")[0], "Sec-Fetch-Site": "same-origin"}
    apps[name] = {"traded": traded, "page": get(link, link.path)[0],
                  "config": sorted(json.loads(get(link, f"/{name}/demo/config.json")[2])),
                  "user": get(link, "/api/server/v1/currentUser", jar)[:3:2],
                  "no_cookie": get(link, "/api/server/v1/currentUser")[0]}
shipped = pathlib.Path(ll.__file__).with_name("_apps")
apps["demo_projects"] = get(link, "/studio/demo/projects/manifest.json")[0]
apps["duckdb_wasm"] = sorted(p.name for p in shipped.rglob("duckdb-*.wasm"))
print(json.dumps({"module": ll.__file__, "library": __import__("legend_lite._library", fromlist=["x"]).library().path.as_posix(),
                  "page": page, "cube": sorted(cube_json), "rows": rows, "notebook": notebook, "headless": headless,
                  "remote": remote, "iceberg": iceberg, "apps": {k: v if not isinstance(v, dict) else
                                                                  {**v, "user": [v["user"][0], v["user"][1].decode()]}
                                                                  for k, v in apps.items()}}))
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


def pe_imports(data: bytes) -> tuple[int, int, set[str]]:
    """A PE32+ library's machine, its characteristics, and the DLLs it imports: its import directory's and its
    delay-load directory's."""
    assert data[:2] == b'MZ', 'not a PE file'
    pe = struct.unpack_from('<I', data, 0x3C)[0]
    assert data[pe:pe + 4] == b'PE\0\0', 'not a PE file'
    machine, sections, _, _, _, optional_size, characteristics = struct.unpack_from('<HHIIIHH', data, pe + 4)
    optional = pe + 24
    assert struct.unpack_from('<H', data, optional)[0] == 0x20B, 'not PE32+'
    # the data directories: 1, the import table (20-byte descriptors, the name at +12); 13, the delay-load table
    # (32-byte descriptors, the name at +4)
    imports_rva = struct.unpack_from('<I', data, optional + 112 + 1 * 8)[0]
    delayed_rva = struct.unpack_from('<I', data, optional + 112 + 13 * 8)[0]
    table = [struct.unpack_from('<IIII', data, optional + optional_size + i * 40 + 8) for i in range(sections)]

    def offset(rva: int) -> int:
        for size, address, raw_size, raw in table:
            if address <= rva < address + max(size, raw_size):
                return raw + rva - address
        raise AssertionError(f'RVA {rva:#x} is in no section')
    linked = set()
    for rva, size, name_at in ((imports_rva, 20, 12), (delayed_rva, 32, 4)):
        if rva == 0:  # no such table
            continue
        at = offset(rva)
        while (name := struct.unpack_from('<I', data, at + name_at)[0]) != 0:
            start = offset(name)
            linked.add(data[start:data.index(b'\0', start)].decode())
            at += size
    return machine, characteristics, linked


# what a win_amd64 library may import (measured: the first Windows run, 2026-10-09): Windows' own libraries, the
# Universal C Runtime's API sets (part of Windows since 10), and the Visual C++ runtime CPython itself ships beside
# python.exe -- never a DLL a machine may lack. Names compared as Windows compares them, without case
WINDOWS_LIBRARIES = {name.lower() for name in (
    'ADVAPI32.dll', 'CRYPT32.dll', 'IPHLPAPI.DLL', 'KERNEL32.dll', 'MSWSOCK.dll', 'ncrypt.dll', 'USERENV.dll',
    'VERSION.dll', 'WS2_32.dll', 'VCRUNTIME140.dll', 'VCRUNTIME140_1.dll',
)}
WINDOWS_API_SETS = 'api-ms-win-crt-'

# a library's architecture, against its tag's: Mach-O cputype, ELF e_machine, PE machine
MACHO_CPU = {'arm64': 0x0100000C, 'x86_64': 0x01000007}
ELF_MACHINE = {'aarch64': 183, 'x86_64': 62}
PE_MACHINE = {'amd64': 0x8664}


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
        elif m := re.search(r'win_(\w+)$', tag):
            machine, characteristics, linked = pe_imports(data)
            print(f'the library imports {sorted(linked)}')
            self.assertEqual(machine, PE_MACHINE[m.group(1)], f'{library}: its CPU')
            self.assertTrue(characteristics & 0x2000, f'{library} is a DLL')
            foreign = sorted(n for n in linked if n.lower() not in WINDOWS_LIBRARIES
                             and not n.lower().startswith(WINDOWS_API_SETS))
            self.assertEqual(foreign, [], f'{library} imports what a Windows machine may lack')
        else:
            self.fail(f'no platform this test reads: {tag}')

    def test_the_licences_ship_inside_it(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            info = {n.rsplit('/', 1)[1]: n for n in wheel.namelist() if '.dist-info/' in n}
            notice = wheel.read(info['NOTICE']).decode()
            licence = wheel.read(info['LICENSE']).decode()
        self.assertIn('Apache License', licence)
        for credited in ('clean-sheet implementation', 'legend-pure', 'legend-engine', 'legend-studio',
                         'Copyright 2020 Goldman Sachs', 'Apache Arrow', 'CC BY 4.0'):
            self.assertIn(credited, notice)
        self.assertIn('THIRD_PARTY_NOTICES.md', info, 'the third-party software it redistributes is listed')
        for graalvm in ('GRAALVM-LICENSE.txt', 'GRAALVM-LICENSE-NATIVEIMAGE.txt', 'GRAALVM-THIRD-PARTY-LICENSE.txt'):
            self.assertIn(graalvm, info, 'the runtime compiled into the library brings its licences')

    def test_the_metadata_says_what_it_needs(self):
        with zipfile.ZipFile(WHEEL) as wheel:
            info = next(n for n in wheel.namelist() if n.endswith('.dist-info/METADATA'))
            metadata = wheel.read(info).decode()
        self.assertIn('Requires-Python: >=3.12', metadata)
        self.assertIn('License: Apache-2.0', metadata)
        # the compiler alone needs nothing; DataCube's packages are its extra's (and the notebook's, with anywidget)
        self.assertEqual([line for line in metadata.splitlines() if line.startswith('Requires-Dist:') and 'extra ==' not in line],
                         [], 'the wheel alone requires nothing')
        for extra in ('datacube', 'notebook'):
            for requirement in ('duckdb>=1.5.5', 'mini-racer>=0.14.1', 'pyarrow>=23.0.1'):
                self.assertIn(f"Requires-Dist: {requirement}; extra == '{extra}'", metadata)
        self.assertIn("Requires-Dist: anywidget>=0.11.0; extra == 'notebook'", metadata)


class Installed(unittest.TestCase):
    def test_installed_it_shows_a_frame_and_answers_a_query_on_its_own(self):
        place = tempfile.TemporaryDirectory(dir=os.environ.get('TEST_TMPDIR'))
        self.addCleanup(place.cleanup)
        root = Path(place.name)
        # nothing of the repository's or the machine's: no PYTHONPATH, no LEGEND_LITE_*, no PATH, no pip.conf
        env = {'HOME': str(root), 'TMPDIR': str(root), 'PYTHONDONTWRITEBYTECODE': '1', 'PIP_CONFIG_FILE': os.devnull}
        if sys.platform == 'win32':
            # what a Windows process cannot start without (its system folder: sockets, randomness), and its own
            # profile and temporary folders in place of the machine's
            env |= {'SYSTEMROOT': os.environ['SYSTEMROOT'], 'TEMP': str(root), 'TMP': str(root),
                    'USERPROFILE': str(root), 'APPDATA': str(root), 'LOCALAPPDATA': str(root)}
        venv = root / 'venv'
        subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True, env=env, timeout=120)
        python = str(venv / 'Scripts' / 'python.exe') if sys.platform == 'win32' else str(venv / 'bin' / 'python')
        pip = [python, '-m', 'pip', 'install', '--no-index', '--no-deps', '--disable-pip-version-check', '-q']
        # the wheel alone: the compiler, and DataCube refused in one sentence naming its extra
        subprocess.run([*pip, str(WHEEL)], check=True, env=env, timeout=240)
        core = subprocess.run([python, '-I', '-c', CORE], cwd=root, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(core.returncode, 0, core.stdout + core.stderr)
        alone = json.loads(core.stdout.strip().splitlines()[-1])
        self.assertIn('body', alone['parsed'], 'the compiler alone parses')
        self.assertEqual(alone['homed'], [200, '[]'], 'a model home answers without the datacube extra (_pool.py)')
        for name, said in alone['refused'].items():
            self.assertIsNotNone(said, f'{name} came without the datacube extra')
            # a notebook's cube names its own extra, which brings DataCube's packages and anywidget
            extra = 'notebook' if name == 'DataCube' else 'datacube'
            self.assertIn(f"pip install 'legend-lite[{extra}]'", said, name)
        # then its extras' packages (the datacube extra's, the notebook's, pandas)
        subprocess.run([*pip, *map(str, DEPENDENCIES)], check=True, env=env, timeout=240)
        # what each wheel says it needs (legend-lite's Requires-Dist, pandas' own) is what is installed
        checked = subprocess.run([python, '-m', 'pip', 'check', '--disable-pip-version-check'], env=env,
                                 capture_output=True, text=True, timeout=60)
        self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
        # an Iceberg table the installed engine reads, served from here
        import functools
        import http.server
        import threading
        import iceberg_table

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass
        (root / 'served').mkdir()
        files = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=str(root / 'served')))
        threading.Thread(target=files.serve_forever, daemon=True).start()
        self.addCleanup(files.shutdown)
        table = iceberg_table.write(root / 'served', 'trades', f'http://127.0.0.1:{files.server_address[1]}',
                                    [[('FX', 1)], [('EQ', 2)]])
        ran = subprocess.run([python, '-I', '-c', SCRIPT, f'{table}#iceberg'], cwd=root, env=env, capture_output=True,
                             text=True, timeout=120)
        self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
        out = json.loads(ran.stdout.strip().splitlines()[-1])
        # as paths, not text: Windows writes its separators both ways
        self.assertTrue(Path(out['module']).is_relative_to(venv), out['module'])
        self.assertTrue(Path(out['library']).is_relative_to(venv), out['library'])
        self.assertEqual((out['page'], out['cube']), (200, ['model', 'runtime', 'source', 'title', 'version']))
        self.assertEqual(out['rows'], [{'desk': 'EQ', 'q': 2.5}, {'desk': 'FX', 'q': 5.5}])
        # the notebook extra installed: the cube's loader, and DataCube's module (about 1.3 MB) and styles, from the wheel
        self.assertTrue(out['notebook']['loader'], out['notebook'])
        self.assertEqual([status for status, _ in out['notebook']['answers']], [200, 200, 200], out['notebook'])
        self.assertGreater(out['notebook']['answers'][0][1], 500_000, out['notebook'])
        # the datacube extra installed: a grid viewed by DataCube's own code, its bundle the wheel's
        self.assertEqual(out['headless']['groups'], [['EQ', 2.5], ['FX', 5.5]], out['headless'])
        # a remote file read through the httpfs extension inside the wheel, none downloaded
        self.assertEqual((out['remote']['status'], out['remote']['rows']), (200, 2), out['remote'])
        self.assertTrue(Path(out['remote']['httpfs']).is_relative_to(venv), out['remote'])
        self.assertEqual((out['iceberg']['status'], out['iceberg']['rows']), (200, 2), out['iceberg'])
        self.assertEqual(len(out['iceberg']['extensions']), 2, out['iceberg'])
        self.assertTrue(all(Path(p).is_relative_to(venv) for p in out['iceberg']['extensions']), out['iceberg'])
        self.assertTrue(Path(out['headless']['bundle']).is_relative_to(venv), out['headless'])
        # Query and Studio inside it (E): each served, its link traded for the cookie its calls then carry; Studio's demo
        # projects shipped, DuckDB-WASM's builds not (Python's DuckDB runs the queries)
        for app, keys in (('query', ['depot', 'execution', 'projects', 'vendor']),
                          ('studio', ['depot', 'engine', 'sdlc', 'vendor', 'worker'])):
            self.assertEqual(out['apps'][app], {'traded': 303, 'page': 200, 'config': keys, 'user': [200, '"this user"'],
                                                'no_cookie': 401}, out['apps'])
        self.assertEqual((out['apps']['demo_projects'], out['apps']['duckdb_wasm']), (200, []), out['apps'])
