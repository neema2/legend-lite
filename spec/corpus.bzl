"""The relational corpus lanes (Maven gates 4, 5 and 11): both passes are cached build actions, and the lane is a
cheap test of their verdicts plus the diff tests of what they measure (Bazel workplan P3-01, P2-15)."""

load("@bazel_lib//lib:write_source_files.bzl", "write_source_files")
load("//tools/generators:defs.bzl", "UPSTREAM_ROOTS", "UPSTREAM_TREES", "program_jvm_flags")
load("//tools/java_run:defs.bzl", "java_run")
load("//tools/junit:defs.bzl", "junit_test")

_SELECT = ["--select-class=com.legend.rcorpus.MinimalCorpusTest"]

# the engine's scan order, which the corpus goldens were written under: every pass of every lane, set by the build
# and read once by DuckDb's pass list (TestLaneOrderGuardrailTest)
_SCAN_ORDER = "-Dlegend.exec.engineScanOrder=true"

# what each pass MEASURES (MinimalCorpusTest.MEASURED): generated into its outputs, held to the committed
# spec/src/test/resources/rcorpus/ copies by the lane's diff tests; never edited by hand
_HOST_MEASURED = ["fail-roster", "skipped-roster", "unordered-register", "engine-order-register"]

_DATABASE_MEASURED = ["database-engine-order-register"]

def _pass_flags(golden):
    return program_jvm_flags("spec") + [
        # DuckDB's JDBC driver loads its library: allowed, so the JDK prints no warning on the build's console
        "--enable-native-access=ALL-UNNAMED",
        _SCAN_ORDER,
    ] + (["-Drcorpus.measured.out={OUT_DIR}"] if golden else [])

def corpus_lane(
        name,
        rosters,
        memory_mb,
        data,
        runtime_deps,
        jvm_flags = [],
        host_jvm_flags = [],
        host_srcs = [],
        golden = True,
        tags = []):
    """`judge_host_<lane>` and `judge_database_<lane>`, the two passes, and `name`, the lane.

    The host pass depends on the lane's backend and flags (its registers are per lane and per judge mode), so each
    lane has its own. Each pass's verdict and log are outputs, and a pass succeeds as an action whatever its tests
    say; `name` is the red or green: CorpusVerdictTest over both verdicts, and (golden = True) the diff tests that
    hold the committed rosters to the measured ones.

    Args:
      name: the lane, `corpus_<lane>`: a test_suite.
      rosters: the rosters' prefix, `duckdb` or `h2` (the warehouse lane measures against DuckDB's).
      memory_mb: each pass's heap: one of java_run's sizes.
      data: what both passes read.
      runtime_deps: the corpus and what the lane links.
      jvm_flags: flags both passes take (the backend).
      host_jvm_flags: flags the passes take that name a file ($(execpath) forms).
      host_srcs: files only those flags name.
      golden: whether the lane's measured rosters are committed (generated and diff-tested). A lane without its
        own (the warehouse's) keeps the passes' exact checks, against the committed copies of `rosters`.
      tags: every target's tags.
    """
    lane = name.removeprefix("corpus_")
    host = "judge_host_" + lane
    database = "judge_database_" + lane
    host_measured = ["%s/%s-%s.txt" % (host, rosters, m) for m in _HOST_MEASURED] if golden else []
    committed = [] if golden else [
        "src/test/resources/rcorpus/%s-%s.txt" % (rosters, m)
        for m in _HOST_MEASURED + _DATABASE_MEASURED
    ]
    database_roots = dict(UPSTREAM_ROOTS)
    database_roots[":%s/verdict.txt" % host] = "{HOST_DIR}"
    database_measured = ["%s/%s-%s.txt" % (database, rosters, m) for m in _DATABASE_MEASURED] if golden else []
    # a lane without its own rosters reads the committed ones, named by one of them (MinimalCorpusTest.readRoster)
    committed_flag = [] if golden else ["-Drcorpus.committed=$(execpath %s)" % committed[0]]
    java_run(
        name = host,
        testonly = True,
        srcs = data + UPSTREAM_TREES + host_srcs + committed,
        roots = UPSTREAM_ROOTS,
        outs = [host + "/judge-host.tsv", host + "/verdict.txt", host + "/host.log"] + host_measured,
        arguments = ["{OUT_DIR}/verdict.txt", "{OUT_DIR}/host.log", "{OUT_DIR}/judge-host.tsv"] +
                    ["{OUT_DIR}/" + f.split("/")[-1] for f in host_measured] + ["--"] +
                    _SELECT + ["--fail-if-no-tests"],
        jvm_flags = _pass_flags(golden) + [
            "-Dlegend.judge.mode=host",
            "-Dlegend.judge.ledger={OUT_DIR}/judge-host.tsv",
        ] + committed_flag + jvm_flags + host_jvm_flags,
        main_class = "com.legend.tools.junit.JUnitAction",
        memory_mb = memory_mb,
        mnemonic = "CorpusHostPass",
        deps = runtime_deps + ["//tools/junit"],
        tags = tags,
    )
    java_run(
        name = database,
        testonly = True,
        srcs = data + UPSTREAM_TREES + host_srcs + committed + [":" + host],
        roots = database_roots,
        outs = [database + "/judge-database.tsv", database + "/verdict.txt", database + "/database.log"] +
               database_measured,
        arguments = ["{OUT_DIR}/verdict.txt", "{OUT_DIR}/database.log", "{OUT_DIR}/judge-database.tsv"] +
                    ["{OUT_DIR}/" + f.split("/")[-1] for f in database_measured] + ["--"] +
                    _SELECT + ["--fail-if-no-tests"],
        jvm_flags = _pass_flags(golden) + [
            "-Dlegend.judge.mode=database",
            "-Dlegend.judge.ledger={OUT_DIR}/judge-database.tsv",
            # the host pass's outputs: its verdict (the database pass means nothing without it), its ledger (the
            # per-assert join) and its measured rosters (the database pass's LOST/GAINED are against them)
            "-Dlegend.judge.host.verdict={HOST_DIR}/verdict.txt",
            "-Dlegend.judge.host.log={HOST_DIR}/host.log",
            "-Dlegend.judge.ledger.host={HOST_DIR}/judge-host.tsv",
        ] + (["-Drcorpus.host.measured={HOST_DIR}"] if golden else []) + committed_flag + jvm_flags + host_jvm_flags,
        main_class = "com.legend.tools.junit.JUnitAction",
        memory_mb = memory_mb,
        mnemonic = "CorpusDatabasePass",
        deps = runtime_deps + ["//tools/junit"],
        tags = tags,
    )
    verdicts = [
        ":%s/verdict.txt" % host,
        ":%s/host.log" % host,
        ":%s/verdict.txt" % database,
        ":%s/database.log" % database,
    ]
    junit_test(
        name = name + "_verdict",
        size = "small",
        memory_mb = 256,
        data = verdicts,
        jvm_flags = [
            "-Dlegend.judge.host.verdict=$(rlocationpath :%s/verdict.txt)" % host,
            "-Dlegend.judge.host.log=$(rlocationpath :%s/host.log)" % host,
            "-Dlegend.judge.database.verdict=$(rlocationpath :%s/verdict.txt)" % database,
            "-Dlegend.judge.database.log=$(rlocationpath :%s/database.log)" % database,
        ],
        runtime_deps = runtime_deps,
        select = ["--select-class=com.legend.rcorpus.CorpusVerdictTest"],
        tags = tags,
    )
    suite = [":" + name + "_verdict"]
    if golden:
        write_source_files(
            name = "update_rcorpus_" + lane,
            testonly = True,
            diff_test_failure_message = "{{DEFAULT_MESSAGE}}\nA measured corpus roster moved -- bazel run //spec:update_rcorpus_" + lane + ", and give the reason in the commit (a test that newly fails is a regression to fix or a policy row to add, never a roster line to accept silently)",
            files = {
                "src/test/resources/rcorpus/" + f.split("/")[-1]: ":" + f
                for f in host_measured + database_measured
            },
            tags = tags,
            # //:update_generated runs it with every other generated file
            visibility = ["//:__pkg__"],
        )
        suite.append(":update_rcorpus_%s_tests" % lane)
    native.test_suite(
        name = name,
        tests = suite,
        tags = tags,
    )
