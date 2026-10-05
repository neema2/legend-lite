"""repo_inventory: every file and every package of the main repository, as files a test can read (Bazel workplan P6-00).

No glob crosses a package boundary, so a guard that must see "every file" (G1, G2, G5-G7, G9, G13) reads this
repository's files.txt instead. It walks the main workspace from its root, skipping what .bazelignore names (and
.git and the bazel-* convenience links), and writes:

  files.txt     every file, one workspace-relative path per line, sorted
  packages.txt  every directory holding a BUILD.bazel (or BUILD), sorted; "" is the root package
  packages.bzl  PACKAGES, the same list for a BUILD file: //tools/guards:repository_files collects every package's
                `all_files` from it, so a package without guards_package() fails analysis (G0), with no hand-kept list

Each directory is read with watch = "yes": Bazel re-runs this rule when any directory's ENTRIES change (a file
added, removed or renamed anywhere outside the ignored trees) and never for a file's contents, which no list here
depends on. The guards that read contents take them from each package's `all_files` (guards_package, defs.bzl).
"""

def _ignored(rctx, root):
    ignored = {".git": True}
    bazelignore = root.get_child(".bazelignore")
    rctx.watch(bazelignore)
    if bazelignore.exists:
        for line in rctx.read(bazelignore).splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                ignored[line.rstrip("/")] = True
    return ignored

def _repo_inventory_impl(rctx):
    root = rctx.workspace_root
    ignored = _ignored(rctx, root)
    files = []
    packages = []
    pending = [(root, "")]

    # Starlark has no while loop: each directory is visited once, and a repository has fewer than this many
    for _ in range(1000000):
        if not pending:
            break
        directory, relative = pending.pop()
        entries = directory.readdir(watch = "yes")
        if [e for e in entries if e.basename in ("BUILD.bazel", "BUILD") and not e.is_dir]:
            packages.append(relative)
        for entry in entries:
            path = entry.basename if not relative else relative + "/" + entry.basename
            if path in ignored or (not relative and entry.basename.startswith("bazel-")):
                continue
            if entry.is_dir:
                pending.append((entry, path))
            else:
                files.append(path)
    rctx.file("files.txt", "\n".join(sorted(files)) + "\n")
    rctx.file("packages.txt", "\n".join(sorted(packages)) + "\n")
    rctx.file("packages.bzl", "PACKAGES = [\n" + "".join(['    "%s",\n' % p for p in sorted(packages)]) + "]\n")
    rctx.file("BUILD.bazel", 'exports_files(["files.txt", "packages.txt", "packages.bzl"])\n')

repo_inventory = repository_rule(
    implementation = _repo_inventory_impl,
    doc = "Every file and package of the main repository: files.txt and packages.txt.",
    local = True,
)
