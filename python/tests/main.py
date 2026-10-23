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
             'LEGEND_LITE_WHEEL', 'LEGEND_LITE_DEPENDENCY_WHEELS', 'LEGEND_LITE_SITE', 'LEGEND_LITE_DATACUBE',
             'LEGEND_LITE_DUCKDB_EXTENSIONS', 'LEGEND_LITE_MODEL_CASES', 'LEGEND_LITE_SQLAPI_CORPUS', 'LEGEND_LITE_CHART_RULES',
             'LEGEND_LITE_PARITY', 'LEGEND_LITE_CORPUS_PAGE'):
    if name not in os.environ:
        continue
    # one path, or several separated by spaces ($(rlocationpaths ...)), handed on as os.pathsep separates them (a
    # resolved path may hold a space); the site and DuckDB's extension folder are directories, every other a file
    is_there = Path.is_dir if name in ('LEGEND_LITE_SITE', 'LEGEND_LITE_DUCKDB_EXTENSIONS') else Path.is_file
    paths = [files.Rlocation(p) for p in os.environ[name].split()]
    for given, path in zip(os.environ[name].split(), paths):
        if not path or not is_there(Path(path)):
            sys.exit(f'{name}: {given} is not in the runfiles')
    os.environ[name] = os.pathsep.join(paths)

# this user's legend-lite folder (the pages DataCube saves) a folder of the test's own, never a person's: a test that
# does not name one still asks where it is (services.json), and a Windows test is given no home to find one in
os.environ.setdefault('LEGEND_LITE_HOME', str(Path(os.environ.get('TEST_TMPDIR') or '.', 'legend-lite-home').resolve()))

sys.path.insert(0, str(Path(__file__).resolve().parent))
suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:])
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() and suite.countTestCases() > 0 else 1)
