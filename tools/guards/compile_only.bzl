"""action_kinds_report: every kind of action the build targets perform (docs/BUILD_REBUILD_DESIGN_2026_10_05.md, step 1).

For each build target (//:java, //:web, //:wasm, //:native), it walks the target and everything it depends on, through
an aspect over every attribute. That includes the rule that generates a file named by its output label
(apply_to_generating_rules). It lists each action those targets register as
`<tier>\\t<mnemonic>\\t<rule kind>\\t<target>`, and each validation action Bazel would run as
`<tier>\\tValidation\\t<rule kind>\\t<target>`. //tools/guards:compile_only_test holds every (rule kind, mnemonic) pair to
a per-tier allowlist. A generator, a test or a measurement can then never become part of the build again, whatever
mnemonic it gives itself.

What is not walked:
  * the exec configuration. It builds the TOOLS the actions run (javac's toolchain, rules_jvm_external's jar tool,
    esbuild's launcher), and a tool's own build is not the product's. The report counts the exec targets it skipped
    (`<tier>\\t#exec-skipped\\t<count>`), so a change in how Bazel names exec configurations fails loudly.
  * a java_binary's data and launcher. //:java takes only each binary's runtime classpath (java_runtime_jars), so only
    the edges that feed it are walked (srcs, resources, classpath_resources, deps, runtime_deps). Bazel runs a
    validation action anywhere in a build's graph, data included, so validations are collected over every edge.
  * resolved toolchains (no toolchains_aspects), and actions that other aspects register: out of scope.

The list is written at analysis time (ctx.actions.write): there is nothing to execute, and it stays cached until the
graph changes.
"""

_ActionKindsInfo = provider(
    doc = "What a target and its dependencies would run.",
    fields = {
        "kinds": "depset of '<mnemonic>\\t<rule kind>\\t<target>': actions on the walked edges",
        "validations": "depset of '<rule kind>\\t<target>': targets with validation actions, on every edge",
        "exec_skipped": "depset of exec-configuration target labels the walk stopped at",
    },
)

# What //:java builds of a java_binary: what goes into its runtime classpath.
_JAVA_BINARY_EDGES = ["srcs", "resources", "classpath_resources", "deps", "runtime_deps"]

def _is_exec(ctx):
    # An exec-configuration output directory's name carries "-exec" (bazel-out/darwin_arm64-opt-exec/bin, k8-opt-exec,
    # x64_windows-opt-exec; -ST-<hash> after it under a transition). There is no public API for this; rules_rust's
    # is_exec_configuration uses the same test. The report's #exec-skipped count catches a renaming.
    return "-exec" in ctx.bin_dir.path

def _deps_of(value):
    """The targets an attribute value holds: a Target, a list of them, or a dict with Target keys or values."""
    kind = type(value)
    if kind == "Target":
        return [value]
    if kind == "list":
        return [v for v in value if type(v) == "Target"]
    if kind == "dict":
        return [v for v in value.keys() + value.values() if type(v) == "Target"]
    return []

def _has_validation(target):
    if OutputGroupInfo not in target:
        return False
    group = getattr(target[OutputGroupInfo], "_validation", None)
    return group != None and len(group.to_list()) > 0

def _action_kinds_aspect_impl(target, ctx):
    if _is_exec(ctx):
        return [_ActionKindsInfo(kinds = depset(), validations = depset(), exec_skipped = depset([str(target.label)]))]
    kind = ctx.rule.kind
    own = ["%s\t%s\t%s" % (a.mnemonic, kind, target.label) for a in target.actions]
    walked = _JAVA_BINARY_EDGES if kind == "java_binary" else dir(ctx.rule.attr)
    kinds, validations, skipped = [], [], []
    for name in dir(ctx.rule.attr):
        for dep in _deps_of(getattr(ctx.rule.attr, name, None)):
            if _ActionKindsInfo not in dep:
                continue
            info = dep[_ActionKindsInfo]
            validations.append(info.validations)
            skipped.append(info.exec_skipped)
            if name in walked:
                kinds.append(info.kinds)
    mine = ["%s\t%s" % (kind, target.label)] if _has_validation(target) else []
    return [_ActionKindsInfo(
        kinds = depset(own, transitive = kinds),
        validations = depset(mine, transitive = validations),
        exec_skipped = depset(transitive = skipped),
    )]

_action_kinds_aspect = aspect(
    implementation = _action_kinds_aspect_impl,
    attr_aspects = ["*"],
    apply_to_generating_rules = True,
)

def _action_kinds_report_impl(ctx):
    lines = {}
    for target, tier in ctx.attr.tiers.items():
        info = target[_ActionKindsInfo]
        for k in info.kinds.to_list():
            lines["%s\t%s" % (tier, k)] = True
        for v in info.validations.to_list():
            lines["%s\tValidation\t%s" % (tier, v)] = True
        lines["%s\t#exec-skipped\t%d" % (tier, len(info.exec_skipped.to_list()))] = True
    out = ctx.actions.declare_file(ctx.label.name + ".tsv")
    ctx.actions.write(out, "".join([line + "\n" for line in sorted(lines.keys())]))
    return [DefaultInfo(files = depset([out]))]

action_kinds_report = rule(
    implementation = _action_kinds_report_impl,
    attrs = {
        "tiers": attr.label_keyed_string_dict(
            aspects = [_action_kinds_aspect],
            mandatory = True,
            doc = "Each build target, and the tier name its lines carry.",
        ),
    },
    doc = "Every action kind the build targets perform, as <tier>\\t<mnemonic>\\t<rule kind>\\t<target> lines.",
)
