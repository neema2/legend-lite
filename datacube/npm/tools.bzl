"""The pinned Node toolchain's npm: build the release archive and install it in a consumer test."""

_NODE = "@rules_nodejs//nodejs:runtime_toolchain_type"

def _npm_cli(info):
    entries = [f for f in info.npm_sources.to_list() if f.path.endswith("/bin/npm-cli.js")]
    if len(entries) != 1:
        fail("the Node toolchain must contain exactly one npm-cli.js, found %s" % entries)
    return entries[0]

def _npm_archive_impl(ctx):
    info = ctx.toolchains[_NODE].nodeinfo
    cli = _npm_cli(info)
    ctx.actions.run(
        executable = info.node,
        arguments = [ctx.file._script.path, cli.path, ctx.file.package.path, ctx.outputs.out.path],
        inputs = depset([ctx.file._script, ctx.file.package], transitive = [info.npm_sources]),
        outputs = [ctx.outputs.out],
        mnemonic = "NpmPack",
        progress_message = "Packing %{label}",
        toolchain = _NODE,
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]), runfiles = ctx.runfiles(files = [ctx.outputs.out]))]

npm_archive = rule(
    implementation = _npm_archive_impl,
    attrs = {
        "package": attr.label(mandatory = True, allow_single_file = True),
        "out": attr.output(mandatory = True),
        "_script": attr.label(default = Label("//datacube:npm/archive.mjs"), allow_single_file = True),
    },
    toolchains = [_NODE],
)

def _npm_test_tools_impl(ctx):
    info = ctx.toolchains[_NODE].nodeinfo
    cli = _npm_cli(info)
    path = cli.short_path[3:] if cli.short_path.startswith("../") else ctx.workspace_name + "/" + cli.short_path
    manifest = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.write(manifest, json.encode({"npmCli": path}))
    return [DefaultInfo(
        files = depset([manifest]),
        runfiles = ctx.runfiles(files = [manifest], transitive_files = info.npm_sources),
    )]

npm_test_tools = rule(
    implementation = _npm_test_tools_impl,
    toolchains = [_NODE],
)
