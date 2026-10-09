"""JupyterLab for a browser test (//datacube:jupyterlab_test, beside it), installed as a developer installs it: a fresh
virtual environment of the repository's Python, legend-lite's wheel (with its `notebook` and `pandas` extras) and
JupyterLab installed into it by pip, offline, from Bazel's own downloads of the pinned wheels -- nothing of the
repository's on its path, nothing of the machine's. Then this program becomes that environment's JupyterLab (offline, in
folders of its own, the test's notebook in its root): it prints one JSON line, the server's address and token, and
serves until its standard input closes; then it stops, and its kernels with it.

    jupyterlab_fixture --notebook <the notebook>

The wheels come from the test, by runfiles path: LEGEND_LITE_WHEEL (legend-lite's) and LEGEND_LITE_DEPENDENCY_WHEELS (a
folder of every pinned wheel), resolved by the runfiles library. Linux and macOS (the BUILD file).
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from python.runfiles import runfiles

# run by the installed environment's own Python: JupyterLab's server on a free port of 127.0.0.1, offline
LAUNCH = r'''
import json, os, sys, threading
from jupyterlab.labapp import LabApp
root, token = sys.argv[1], sys.argv[2]
server = LabApp.initialize_server(argv=[
    "--no-browser", "--ServerApp.ip=127.0.0.1", "--ServerApp.port=0", f"--IdentityProvider.token={token}",
    f"--ServerApp.root_dir={root}", "--ServerApp.terminals_enabled=False",
    # a container that runs its tests as root (Docker's default) runs this one too
    "--ServerApp.allow_root=True",
    # offline: no news, no update check, no extension index (PyPI)
    "--LabApp.news_url=None", "--LabApp.check_for_updates_class=jupyterlab.NeverCheckForUpdate",
    "--LabApp.extension_manager=readonly",
    # the test drives the page through JupyterLab's own commands
    "--LabApp.expose_app_in_browser=True",
])
print(json.dumps({"url": server.connection_url, "token": token}), flush=True)
# the test closes this program's input to end it: the server stops, on its own loop, and its kernels with it
threading.Thread(target=lambda: (sys.stdin.read(), server.io_loop.add_callback(server.stop)), daemon=True).start()
server.start()
'''


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--notebook', required=True)
    given = arguments.parse_args()
    files = runfiles.Create()
    wheel = Path(files.Rlocation(os.environ['LEGEND_LITE_WHEEL']))
    # every pinned wheel in one folder: pip's index for this install
    index = Path(files.Rlocation(os.environ['LEGEND_LITE_DEPENDENCY_WHEELS']))
    home = Path(tempfile.mkdtemp(dir=os.environ.get('TEST_TMPDIR')))
    venv = home / 'venv'
    # nothing of the repository's or the machine's: no PYTHONPATH, no LEGEND_LITE_*, no pip.conf, and no program of the
    # machine's -- PATH is the environment's own bin, where pip and jupyter find their scripts (no terminal is opened:
    # terminals are off); every folder Jupyter reads or writes is the test's
    env = {'HOME': str(home / 'home'), 'TMPDIR': str(home), 'PATH': str(venv / 'bin'), 'PIP_CONFIG_FILE': os.devnull,
           'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUTF8': '1', 'JUPYTER_PLATFORM_DIRS': '1',
           'JUPYTER_CONFIG_DIR': str(home / 'config'), 'JUPYTER_DATA_DIR': str(home / 'data'),
           'JUPYTER_RUNTIME_DIR': str(home / 'runtime'), 'IPYTHONDIR': str(home / 'ipython'),
           # the machine-wide folders too (Jupyter finds them through platformdirs: /usr/share/jupyter, /etc/xdg/jupyter,
           # /Library/Application Support/jupyter), where a machine's server settings, kernels and page extensions live
           'XDG_DATA_DIRS': str(home / 'system-data'), 'XDG_CONFIG_DIRS': str(home / 'system-config')}
    for folder in ('home', 'config', 'data', 'runtime', 'ipython', 'system-data', 'system-config'):
        (home / folder).mkdir()
    subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True, env=env, timeout=120)
    python = venv / 'bin' / 'python'
    subprocess.run([str(python), '-m', 'pip', 'install', '--no-index', '--find-links', str(index),
                    '--disable-pip-version-check', '-q', f'{wheel}[notebook,pandas]', 'jupyterlab'],
                   check=True, env=env, timeout=600, stdout=sys.stderr)
    # no question about Jupyter's news in the page
    settings = home / 'config' / 'lab' / 'user-settings' / '@jupyterlab' / 'apputils-extension'
    settings.mkdir(parents=True)
    (settings / 'notification.jupyterlab-settings').write_text('{"fetchNews": "false"}')
    root = home / 'notebooks'
    root.mkdir()
    # the developer's own notebook: writable (Bazel's copy of it is read-only)
    notebook = root / 'datacube.ipynb'
    shutil.copyfile(given.notebook, notebook)
    notebook.chmod(0o644)
    # this process becomes the installed JupyterLab: the test's input and output are its own. Isolated (-I): the
    # server reads no PYTHON* setting and no user site (the kernel it starts, from the environment's own kernelspec, has
    # the settings above)
    os.execve(str(python), [str(python), '-I', '-c', LAUNCH, str(root), os.urandom(16).hex()], env)


if __name__ == '__main__':
    main()
