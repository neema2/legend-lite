"""A suite of the bindings under Bazel: the test modules named on the command line (BUILD.bazel). Each file comes
as a runfiles path ($(rlocationpath)), resolved by the runfiles library -- never the working directory -- before
legend_lite or a suite reads one."""

import os
import sys
import unittest
from pathlib import Path

from python.runfiles import runfiles

files = runfiles.Create()
for name in ('LEGEND_LITE_LIBRARY', 'LEGEND_LITE_CORPUS_MODEL', 'LEGEND_LITE_CORPUS_QUERIES', 'LEGEND_LITE_JVM_ANSWERS'):
    if name not in os.environ:
        continue
    path = files.Rlocation(os.environ[name])
    if not path or not Path(path).is_file():
        sys.exit(f'{name}: {os.environ[name]} is not in the runfiles')
    os.environ[name] = path

sys.path.insert(0, str(Path(__file__).resolve().parent))
suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:])
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() and suite.countTestCases() > 0 else 1)
