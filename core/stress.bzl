"""The stress corpus's layout, as ONE list (Bazel workplan P2-02).

STRESS_GENERATED: the ten stress files the corpus generators write, and which generator writes each ("dense":
dense_build.py's 59, 60, 64; "stress": build.py's 92-98). LINKED_PROJECTS: the projects/ the executable corpus
links, DEPENDENCIES BEFORE DEPENDENTS (scripts/corpus/model.py says why each is there).

Three readers, one list: core/BUILD.bazel (:stress_sources, :update_stress_corpus) and scripts/corpus/BUILD.bazel
(the generators' outputs) load it; stress_layout writes it as stress-layout.json, committed in core's test
resources and diff-tested (//:generated), which scripts/corpus/model.py and StressCorpus.java read.
"""

STRESS_GENERATED = {
    "59-dense-mapping.pure": "dense",
    "60-dense-store.pure": "dense",
    "64-combinations.pure": "dense",
    "92-services.pure": "stress",
    "93-testdata.pure": "stress",
    "94-fanout-services.pure": "stress",
    "95-function-tests.pure": "stress",
    "96-external-data.pure": "stress",
    "97-hier-execution.pure": "stress",
    "98-combination-execution.pure": "stress",
}

LINKED_PROJECTS = [
    "core-types",
    "core-tenor",
    "core-fx",
    "core-ratings",
    "core-instrument",
    "core-calendar",
    "core-units",
    "core-account",
    "core-geo",
    "fee-core",
    "index-core",
]

def _stress_layout_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    layout = {"generated": STRESS_GENERATED, "linked_projects": LINKED_PROJECTS}
    ctx.actions.write(out, json.encode_indent(layout, indent = "  ") + "\n")
    return [DefaultInfo(files = depset([out]))]

stress_layout = rule(
    implementation = _stress_layout_impl,
    doc = "STRESS_GENERATED and LINKED_PROJECTS as <name>.json, for the readers that are not Starlark.",
)
