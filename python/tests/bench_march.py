"""THROWAWAY (probe/march): the compiler library built for the architecture's oldest CPUs (-march=compatibility)
against GraalVM's default target, planning the corpus, rounds alternated so the machine's drift falls on both."""
import os
import statistics
import subprocess
import sys
import unittest
from pathlib import Path

from python.runfiles import runfiles

ONE = r'''
import os, sys, time
from pathlib import Path
from legend_lite._library import library
model = Path(sys.argv[1]).read_text()
queries = [l.split("\t", 1)[1] for l in Path(sys.argv[2]).read_text().split("\n") if l]
lib = library()
for q in queries:
    lib.call("lite_plan_text", model, q, "trades::RT")
passes = []
for _ in range(20):
    t0 = time.perf_counter()
    for q in queries:
        lib.call("lite_plan_text", model, q, "trades::RT")
    passes.append(time.perf_counter() - t0)
passes.sort()
print(passes[len(passes) // 2])
'''


class March(unittest.TestCase):
    def test_time_both(self):
        files = runfiles.Create()
        libs = {name: files.Rlocation(os.environ[name], source_repo='') for name in ('LIB_COMPATIBLE', 'LIB_DEFAULT')}
        model = files.Rlocation(os.environ['CORPUS_MODEL'], source_repo='')
        queries = files.Rlocation(os.environ['CORPUS_QUERIES'], source_repo='')
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        times = {name: [] for name in libs}
        for _ in range(5):
            for name, lib in libs.items():
                out = subprocess.run([sys.executable, '-c', ONE, model, queries], env=dict(env, LEGEND_LITE_LIBRARY=lib),
                                     capture_output=True, text=True, check=True)
                times[name].append(float(out.stdout.strip()))
        a, b = statistics.median(times['LIB_COMPATIBLE']), statistics.median(times['LIB_DEFAULT'])
        print(f'MARCH compatible {a * 1000:.1f} ms/pass, default {b * 1000:.1f} ms/pass, default/compatible {b / a:.3f}; '
              f'rounds: {times}')
