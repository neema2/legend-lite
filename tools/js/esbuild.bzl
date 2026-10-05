"""esbuild_bundle: a browser bundle, made by esbuild's native binary directly, with no Node.

docs/BUILD_REBUILD_DESIGN_2026_10_05.md (the native JS build). esbuild is a Go program. The npm route ran it through
three layers: a bash launcher (js_binary), Node, then esbuild's JavaScript launcher (esbuild/bin/esbuild), which
finally started the binary. This rule runs the binary itself: one action per bundle.

  * The binary: @esbuild_<platform>, the npm registry's @esbuild/<platform> tarball pinned by the sha512 the pnpm
    lock records (MODULE.bazel), chosen for the exec platform by //tools/js:esbuild.
  * The working directory: esbuild runs in the package's output directory (bazel-out/<config>/bin/<package>), as
    rules_js's js_run_binary(chdir = package_name()) ran it, so outputs and args are package-relative. Bazel's
    actions have no working-directory option, so bazel_lib's hermetic coreutils (`env -C <dir>`) sets it: a pinned
    binary for every platform, no shell.
  * The outputs: `outs` are output labels, so a consumer can name one (//datacube:demo/bundle.js), and they are
    JavaScript to rules_js (JsInfo), as js_run_binary's were.
  * The inputs: the srcs, copied into the output tree beside the npm packages esbuild resolves against, plus what
    rules_js's JsInfo says they reach (sources and npm packages; types are erased by esbuild). These are
    js_run_binary's defaults. rules_js stays only to fetch and lay out the npm packages, which runs no Node.
"""

load("@aspect_rules_js//js:libs.bzl", "js_lib_helpers")
load("@aspect_rules_js//js:providers.bzl", "js_info")
load("@bazel_lib//lib:copy_to_bin.bzl", "copy_to_bin")

_COREUTILS = "@bazel_lib//lib:coreutils_toolchain_type"

def _esbuild_bundle_impl(ctx):
    coreutils = ctx.toolchains[_COREUTILS].coreutils_info.bin
    esbuild = ctx.executable._esbuild
    outs = ctx.outputs.outs
    out_dirs = [ctx.actions.declare_directory(d) for d in ctx.attr.out_dirs]

    # esbuild runs in the package's output directory; the binary's path is relative to the execroot, so step back up
    workdir = ctx.bin_dir.path + ("/" + ctx.label.package if ctx.label.package else "")
    up = "/".join([".."] * len(workdir.split("/")))
    inputs = depset(
        ctx.files.srcs,
        transitive = [js_lib_helpers.gather_files_from_js_infos(
            targets = ctx.attr.srcs,
            include_sources = True,
            include_types = False,
            include_transitive_sources = True,
            include_transitive_types = False,
            include_npm_sources = True,
        )],
    )
    args = ctx.actions.args()
    args.add_all(["env", "-C", workdir, up + "/" + esbuild.path])
    args.add_all(ctx.attr.args)
    ctx.actions.run(
        executable = coreutils,
        arguments = [args],
        inputs = inputs,
        outputs = outs + out_dirs,
        tools = [esbuild],
        mnemonic = "Esbuild",
        progress_message = "Bundling %{label}",
    )
    files = depset(outs + out_dirs)

    # the outputs as JavaScript to rules_js consumers (a js_test's data), as js_run_binary marked them
    return [DefaultInfo(files = files), js_info(target = ctx.label, sources = files, transitive_sources = files)]

_esbuild_bundle = rule(
    implementation = _esbuild_bundle_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, doc = "What the bundle reads: copied sources and JS libraries."),
        # output labels (as js_run_binary's): a consumer names one, e.g. //datacube:demo/bundle.js
        "outs": attr.output_list(doc = "Output files, relative to the package."),
        "out_dirs": attr.string_list(doc = "Output directories, relative to the package (code splitting's chunks)."),
        "args": attr.string_list(doc = "esbuild's arguments, package-relative (it runs in the package's output directory)."),
        "_esbuild": attr.label(default = "//tools/js:esbuild", executable = True, allow_single_file = True, cfg = "exec"),
    },
    toolchains = [_COREUTILS],
)

def esbuild_bundle(name, srcs, outs = [], out_dirs = [], args = [], **kwargs):
    """A browser bundle by esbuild's native binary.

    Args:
        name: the target.
        srcs: source files and JS libraries the bundle reads; source files are copied into the output tree.
        outs: output files, relative to the package.
        out_dirs: output directories, relative to the package.
        args: esbuild's arguments, relative to the package.
        **kwargs: common attributes (visibility, tags, ...).
    """
    files = [s for s in srcs if not s.startswith(":") and not s.startswith("//") and not s.startswith("@")]
    targets = [s for s in srcs if s not in files]
    copy_to_bin(name = name + "_srcs", srcs = files)
    _esbuild_bundle(
        name = name,
        srcs = [":" + name + "_srcs"] + targets,
        outs = outs,
        out_dirs = out_dirs,
        args = args,
        **kwargs
    )
