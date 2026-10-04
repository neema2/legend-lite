"""markdown_report: this repository's Markdown files among each test's runtime files (G17, Bazel workplan P6-17).

For each test, every source file in its runfiles (what it reads when it runs: data, and its dependencies' runfiles)
that is a .md file of the main repository, one line each. Built for the configuration in use, so it fetches nothing
for another platform (a genquery over every test's closure loaded every platform's downloads). The pinned upstream
trees' Markdown is spec input and out of scope. //tools/guards:markdown_inputs_test reads every package's report.
"""

def _markdown_report_impl(ctx):
    lines = []
    for target in ctx.attr.targets:
        info = target[DefaultInfo]
        for f in info.default_runfiles.files.to_list() + info.data_runfiles.files.to_list():
            if f.is_source and f.owner and f.owner.repo_name == "" and f.basename.endswith(".md"):
                lines.append("%s\t%s" % (target.label, f.short_path))
    out = ctx.actions.declare_file(ctx.label.name + ".tsv")
    ctx.actions.write(out, "\n".join(sorted({l: True for l in lines}.keys())) + "\n")
    return [DefaultInfo(files = depset([out]))]

markdown_report = rule(
    implementation = _markdown_report_impl,
    attrs = {"targets": attr.label_list(doc = "The package's tests.")},
    doc = "This repository's Markdown files among each test's runtime files.",
)
