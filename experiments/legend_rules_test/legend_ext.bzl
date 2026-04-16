# Module extension: scans .pure files and extracts element FQNs via regex.
# Generates an elements.bzl per project with ELEMENTS = ["fqn1", "fqn2", ...].
#
# This runs in a special pre-analysis phase where file contents CAN be read.

def _scan_pure_files(mctx, src_dir):
    """Scan .pure files and extract element FQNs (iterative, no recursion)."""
    elements = []
    dirs_to_scan = [src_dir]
    for _i in range(100):  # max depth safety
        if not dirs_to_scan:
            break
        current = dirs_to_scan.pop(0)
        for f in current.readdir():
            fname = str(f)
            if fname.endswith(".pure"):
                content = mctx.read(f)
                for line in content.splitlines():
                    line = line.strip()
                    for keyword in ["Class", "Enum", "Association", "Mapping",
                                    "Function", "Database", "Runtime",
                                    "Connection", "Service", "Profile"]:
                        if line.startswith(keyword + " "):
                            rest = line[len(keyword) + 1:]
                            fqn = ""
                            for ch in rest.elems():
                                if ch in (" ", "{", "(", "\t", "\n"):
                                    break
                                fqn += ch
                            if "::" in fqn and fqn and fqn not in elements:
                                elements.append(fqn)
            elif f.is_dir:
                dirs_to_scan.append(f)
    return elements

def _elements_repo_impl(rctx):
    """Repository rule that writes an elements.bzl with pre-scanned FQNs."""
    rctx.file("BUILD.bazel", "# Auto-generated repo for element list\n")
    rctx.file("elements.bzl", "ELEMENTS = " + rctx.attr.elements_repr + "\n")

_elements_repo = repository_rule(
    implementation = _elements_repo_impl,
    attrs = {"elements_repr": attr.string()},
)

def _legend_impl(mctx):
    for tag in mctx.modules[0].tags.project:
        workspace = mctx.path(Label("@@//:MODULE.bazel")).dirname
        src_dir = workspace.get_child(tag.path)
        elements = _scan_pure_files(mctx, src_dir)

        # Create a named repo with elements.bzl
        _elements_repo(name = tag.name, elements_repr = repr(elements))

_project_tag = tag_class(attrs = {
    "name": attr.string(mandatory = True),
    "path": attr.string(mandatory = True),
})

legend = module_extension(
    implementation = _legend_impl,
    tag_classes = {"project": _project_tag},
)
