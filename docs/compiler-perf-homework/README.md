# Compiler performance homework (2026-11-13)

This is the evidence for `docs/COMPILER_PERFORMANCE_2026_11_13.md`. It holds measurements only: no product code changed.

- [PLAN.md](PLAN.md): the workloads (W1–W11), metrics, tools and noise rules. Approved by the orchestrator, with its
  four additions in §8.
- [BASELINE.md](BASELINE.md): the numbers per workload, noise included.
- `scripts/*.txt`: every script and probe used, kept as text, never built by the repository. To rerun them:
  - Copy them to a scratch directory.
  - Build `//core:stress_tool` and capture its classpath. `run.sh` expects `cp.txt`: the absolute classpath of
    `bazel-bin/core/stress_tool`'s runfiles, in that runfiles' order.
  - Compile the probes against that classpath with the product JDK (`javac --release 21`).
  - `gen_models.sh` writes the generated models; `w1.sh`, `w3.sh`, `winc.sh`, `w5.sh`, `w9.sh` and `w10.sh` run the
    workloads; `jfr_stack.py`, `jfr_share.py` and `jfr_below.py` read JFR recordings.
- `raw/`: the run logs. Each run records its command, load average and steal. In the logs, `$STRESS_TOOL_CLASSPATH`
  and `$SP` replace the long classpath and the scratch path. The folder also holds:
  - W1's per-case TSV for one warm pass;
  - W5's and W10's per-query TSVs;
  - W3's summary;
  - `raw/jfr/*.md`: each profile summarized (stages, hottest frames, allocation). The `.jfr` recordings themselves
    are not committed (17–250 MB each); the scripts regenerate them.
