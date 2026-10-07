"""The bindings' suites under Bazel (//python:bindings_test): BUILD.bazel gives each file as a runfiles path
($(rlocationpath)), resolved here by the runfiles library -- never the working directory -- before legend_lite or a
suite reads one."""

import os
import sys
import unittest
from pathlib import Path

from python.runfiles import runfiles

files = runfiles.Create()
for name in ('LEGEND_LITE_LIBRARY', 'LEGEND_LITE_CORPUS_MODEL', 'LEGEND_LITE_CORPUS_QUERIES', 'LEGEND_LITE_JVM_ANSWERS'):
    path = files.Rlocation(os.environ[name])
    if not path or not Path(path).is_file():
        sys.exit(f'{name}: {os.environ[name]} is not in the runfiles')
    os.environ[name] = path

here = Path(__file__).resolve().parent
result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(here), top_level_dir=str(here)))
sys.exit(0 if result.wasSuccessful() else 1)
