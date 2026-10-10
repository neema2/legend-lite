"""THE JAR POOLS and WHO MAY USE EACH (Bazel workplan P1-25): one list, read by every place that names the pools.

What may never SHIP: @maven_upstream (legend-engine, legend-pure), @maven_runner and @maven_test are testonly as a
whole (TESTONLY_POOLS; MODULE.bazel amends every listed jar testonly, and
third_party/rules_jvm_external_testonly_closure.patch marks whatever only they reach), and .bazelrc's
--incompatible_check_testonly_for_output_files extends Bazel's testonly check to generated files, so Bazel refuses
any non-test target that reaches one of their jar targets. //tools/deps:pools_list_test keeps each of those
pools testonly as a whole.

What this file adds is which TEST code may use which pool: POOL_USERS names, for each pool, the packages (or single
targets, "package:name") that may depend on its jars, so, for example, core's own tests never use legend-engine
(the reference-checkout tenet). check_pool_use enforces it when a BUILD file loads, in the macros that make
first-party Java targets: legend_java_library, legend_java_binary, junit_test, java_run, teavm_wasm. Raw rules
outside them are not checked here; a graph-wide check over every package comes with P6-00's inventory. Core
itself reaches no outside jar at all: //tools/deps:core_closure_test.

Adding a user is a reviewed edit here, with the reason. The product's jars (JDBC drivers, the warehouse's DuckDB) are not
pools: http_jar fetches them (jars.bzl), jars_table.bzl names each one's users, and check_pool_use holds them to it
too.
"""

load(":jars_table.bzl", "JARS", "jar_of_label")

POOL_USERS = {
    # legend-engine's execution stack, for the engine runner tool only
    "maven_runner": ["tools/engine-runner"],
    # GraalVM's native-image API (@CEntryPoint and its C types), compile-only: the compiler's C entry points, which
    # native-image builds into a shared library (//native:compiler, for the Python bindings in //python)
    "maven_native": ["native"],
    # TeaVM: the WebAssembly compiler and the class library the planner compiles against; and sdlc-server, whose
    # rules (with depot-server's) compile to the page's SDLC module the same way (2026-10-04, the Studio line,
    # docs/STUDIO_DESIGN_2026_10_02.md S21: its :teavm_api and :page targets, as //wasm's)
    # warehouse: the SQL API compiled to WebAssembly, //warehouse:sqlapi_wasm (Bazel workplan P3-24)
    # third_party/teavm_classlib: TeaVM's class library corrected where lite needs it exact, compiled against TeaVM's
    "maven_teavm": ["sdlc-server", "third_party/teavm_classlib", "tools/teavm", "warehouse", "wasm"],
    # test tooling: JUnit, ArchUnit; for the packages with tests. sdlc-server also takes JGit from here, the judge
    # of the git repository it writes by hand (2026-10-04, the Studio line: no host git in tests)
    "maven_test": ["core", "json", "parser-equivalence", "pct", "sdlc-server", "spec", "third_party/teavm_classlib", "tools/bump", "tools/deps", "tools/engine-runner", "tools/guards", "tools/junit", "tools/legend", "warehouse"],
    # compiler plugins (NullAway), never on a classpath
    "maven_tools": ["tools/nullaway"],
    # legend-engine and legend-pure: TEST INPUTS ONLY, for the packages that referee lite against them (AGENTS.md,
    # reference-checkout tenet); in tools/junit only runner_test, which takes JUnit 4 from here for its JUnit 3
    # fixtures (tools/junit:junit is on every test's classpath, so the package as a whole is not a user)
    "maven_upstream": ["parser-equivalence", "pct", "tools/junit:runner_test", "tools/par", "tools/reference"],
}

POOLS = sorted(POOL_USERS.keys())

# The pools that are testonly as a whole (MODULE.bazel: every listed jar amended testonly, and the closure patch).
# //tools/deps:pools_list_test holds MODULE.bazel to it.
TESTONLY_POOLS = ["maven_runner", "maven_test", "maven_upstream"]

def _pool_of_label(label):
    # canonical repository names: rules_jvm_external++maven+maven_upstream
    repo = label.repo_name
    if "++maven+" not in repo:
        return None
    return repo.split("++maven+")[-1]

def check_pool_use(name, *label_lists):
    """In a macro: fails if this target may not use a pool one of the labels names.

    Args:
        name: the target.
        *label_lists: lists of labels (deps, runtime_deps, exports, ...); a select() is refused.
    """
    package = native.package_name()
    what = "//%s:%s" % (package, name)
    for labels in label_lists:
        if type(labels) != "list":
            fail("%s: a dependency list must be a plain list here, not a %s (tools/deps/pools.bzl checks it)" % (what, type(labels)))
        for label in labels:
            # every spelling (@maven_x//:y, @@rules_jvm_external++maven+maven_x//:y, a per-jar repository) resolves
            # to one canonical repository; an unknown ++maven+ repository fails closed below
            resolved = native.package_relative_label(label)
            jar = jar_of_label(resolved)
            if jar:
                if jar not in JARS:
                    fail("%s uses %s, a product jar tools/deps/jars_table.bzl does not list" % (what, label))
                users = JARS[jar].users
                if package not in users and "%s:%s" % (package, name) not in users:
                    fail(("%s depends on @%s, which only %s may use (tools/deps/jars_table.bzl): add it there, with the " +
                          "reason, if it really needs that jar") % (what, jar, ", ".join(["//" + u for u in users])))
                continue
            pool = _pool_of_label(resolved)
            if not pool:
                continue
            users = POOL_USERS.get(pool)
            if users == None:
                fail("%s uses %s, from a Maven repository tools/deps/pools.bzl does not know as a pool" % (what, label))
            if package not in users and "%s:%s" % (package, name) not in users:
                fail(("%s depends on @%s, which only %s may use (tools/deps/pools.bzl, Bazel workplan P1-25): add " +
                      "it there, with the reason, if it really needs those jars") % (what, pool, ", ".join(["//" + u for u in users])))
