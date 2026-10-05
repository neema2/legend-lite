"""The relational corpus lanes (Maven gates 4, 5 and 11): each lane's HOST-judge pass is a cached build action, and
its test runs the DATABASE-judge pass and joins the two per assert (Bazel workplan P3-01)."""

load("//tools/generators:defs.bzl", "UPSTREAM_ROOTS", "UPSTREAM_TREES", "program_jvm_flags")
load("//tools/java_run:defs.bzl", "java_run")
load("//tools/junit:defs.bzl", "junit_test")

_SELECT = ["--select-class=com.legend.rcorpus.MinimalCorpusTest"]

# the engine's scan order, which the corpus goldens were written under: every pass of every lane, set by the build
# and read once by DuckDb's pass list (TestLaneOrderGuardrailTest)
_SCAN_ORDER = "-Dlegend.exec.engineScanOrder=true"

def corpus_lane(
        name,
        memory_mb,
        host_memory_mb,
        data,
        runtime_deps,
        jvm_flags = [],
        test_jvm_flags = [],
        host_jvm_flags = [],
        test_data = [],
        host_srcs = [],
        tags = []):
    """`judge_host_<lane>`, the host pass, and `name`, the lane's test.

    The host pass depends on the lane's backend and flags (its registers are per lane and per judge mode), so each
    lane has its own. It carries every gate-4 assertion; its verdict and log are outputs, and the test fails, quoting
    the log, before the database pass runs when the host pass failed.

    Args:
      name: the test, `corpus_<lane>`.
      memory_mb: the test's heap.
      host_memory_mb: the host pass's heap: one of java_run's sizes.
      data: what both passes read.
      runtime_deps: the corpus and what the lane links.
      jvm_flags: flags both passes take (the backend).
      test_jvm_flags: flags only the test takes ($(rootpath) forms).
      host_jvm_flags: flags only the host pass takes ($(execpath) forms).
      test_data: files only the test reads.
      host_srcs: files only the host pass reads.
      tags: both targets' tags.
    """
    host = "judge_host_" + name.removeprefix("corpus_")
    ledger = host + "/judge-host.tsv"
    verdict = host + "/verdict.txt"
    log = host + "/host.log"
    java_run(
        name = host,
        testonly = True,
        srcs = data + UPSTREAM_TREES + host_srcs,
        roots = UPSTREAM_ROOTS,
        outs = [ledger, verdict, log],
        arguments = ["{OUT_DIR}/verdict.txt", "{OUT_DIR}/host.log", "{OUT_DIR}/judge-host.tsv", "--"] +
                    _SELECT + ["--fail-if-no-tests"],
        jvm_flags = program_jvm_flags("spec") + [
            # DuckDB's JDBC driver loads its library: allowed, so the JDK prints no warning on the build's console
            "--enable-native-access=ALL-UNNAMED",
            _SCAN_ORDER,
            "-Dlegend.judge.mode=host",
            "-Dlegend.judge.ledger={OUT_DIR}/judge-host.tsv",
        ] + jvm_flags + host_jvm_flags,
        main_class = "com.legend.tools.junit.JUnitAction",
        memory_mb = host_memory_mb,
        mnemonic = "CorpusHostPass",
        deps = runtime_deps + ["//tools/junit"],
        tags = tags,
    )
    junit_test(
        name = name,
        memory_mb = memory_mb,
        data = data + test_data + [":" + ledger, ":" + verdict, ":" + log],
        jvm_flags = [
            _SCAN_ORDER,
            "-Dlegend.judge.mode=database",
            "-Dlegend.judge.ledger.host=$(rlocationpath :%s)" % ledger,
            "-Dlegend.judge.host.verdict=$(rlocationpath :%s)" % verdict,
            "-Dlegend.judge.host.log=$(rlocationpath :%s)" % log,
        ] + jvm_flags + test_jvm_flags,
        runtime_deps = runtime_deps,
        select = _SELECT,
        tags = tags,
        upstream = True,
    )
