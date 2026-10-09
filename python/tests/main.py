"""A suite of the bindings under Bazel: the test modules named on the command line (BUILD.bazel). Each file comes
as a runfiles path ($(rlocationpath)), resolved by the runfiles library -- never the working directory -- before
legend_lite or a suite reads one."""

import os
import sys
import unittest
from pathlib import Path

from python.runfiles import runfiles

files = runfiles.Create()
for name in ('LEGEND_LITE_LIBRARY', 'LEGEND_LITE_CORPUS_MODEL', 'LEGEND_LITE_CORPUS_QUERIES', 'LEGEND_LITE_JVM_ANSWERS',
             'LEGEND_LITE_WHEEL', 'LEGEND_LITE_DEPENDENCY_WHEELS'):
    if name not in os.environ:
        continue
    # one path, or several separated by spaces ($(rlocationpaths ...))
    paths = [files.Rlocation(p) for p in os.environ[name].split()]
    for given, path in zip(os.environ[name].split(), paths):
        if not path or not Path(path).is_file():
            sys.exit(f'{name}: {given} is not in the runfiles')
    os.environ[name] = ' '.join(paths)

sys.path.insert(0, str(Path(__file__).resolve().parent))
suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:])
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() and suite.countTestCases() > 0 else 1)
