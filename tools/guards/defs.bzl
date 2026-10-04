"""guards_package: every package's files as one target the content guards read (Bazel workplan P6-00).

Every BUILD file calls guards_package() once. It adds `all_files`, the package's own files (a glob stops at
subpackages), visible to //tools/guards, whose :repository_files collects one per package from the inventory's package
list: a package that forgets the call fails analysis there.
"""

def guards_package():
    native.filegroup(
        name = "all_files",
        # the root package's glob would follow Bazel's convenience links (bazel-bin, bazel-out, ...) into the output
        # tree; their names vary by checkout, so .bazelignore cannot list them
        srcs = native.glob(["**"], exclude = [".git", "bazel-*/**"] if not native.package_name() else [], allow_empty = True),
        visibility = ["//tools/guards:__pkg__"],
    )
