"""teavm_wasm: a Java program compiled ahead of time to a WebAssembly-GC module.

The program is `deps` (their transitive runtime jars ARE its class path — TeaVM
compiles what `main_class` reaches from them, and nothing else), compiled by
TeaVmCompile inside one action. Outputs `<name>/classes.wasm` and TeaVM's host
runtime `<name>/wasm-gc-module-runtime.js`, side by side, as a loader wants them.
"""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")
load("//tools/deps:pools.bzl", "check_pool_use")

# The compiler's heap, as the scheduler sees it: //tools/teavm:compile's -Xmx2g (BUILD.bazel, measured there).
def _teavm_memory(os, inputs):
    return {"cpu": 1, "memory": 2048}

def _teavm_wasm_impl(ctx):
    # TeaVM's class library CORRECTED where lite needs it exact (//third_party/teavm_classlib): its own jar FIRST, so the
    # compiler's class loader finds its classes before TeaVM's own of the same name (the class library is on no other
    # loader: //tools/teavm:compile carries only teavm-core and teavm-tooling)
    fixes = ctx.attr._classlib_fixes[JavaInfo].runtime_output_jars
    jars = depset(fixes, transitive = [d[JavaInfo].transitive_runtime_jars for d in ctx.attr.deps], order = "preorder")
    wasm = ctx.actions.declare_file(ctx.label.name + "/classes.wasm")
    runtime = ctx.actions.declare_file(ctx.label.name + "/wasm-gc-module-runtime.js")
    args = ctx.actions.args()
    args.add(wasm.dirname)
    args.add(ctx.attr.main_class)
    classpath = ctx.actions.args()
    classpath.add_all(jars)
    classpath.use_param_file("@%s", use_always = True)
    classpath.set_param_file_format("multiline")
    ctx.actions.run(
        executable = ctx.executable._compiler,
        arguments = [args, classpath],
        inputs = jars,
        outputs = [wasm, runtime],
        mnemonic = "TeaVM",
        resource_set = _teavm_memory,
        progress_message = "TeaVM: compiling %s to WebAssembly (%%{label})" % ctx.attr.main_class,
    )
    return [DefaultInfo(files = depset([wasm, runtime]))]

_teavm_wasm = rule(
    implementation = _teavm_wasm_impl,
    attrs = {
        "deps": attr.label_list(providers = [JavaInfo], mandatory = True),
        "main_class": attr.string(mandatory = True),
        "_classlib_fixes": attr.label(
            default = "//third_party/teavm_classlib:fixes",
            providers = [JavaInfo],
        ),
        "_compiler": attr.label(
            default = "//tools/teavm:compile",
            executable = True,
            cfg = "exec",
        ),
    },
    doc = "Compiles main_class, over deps' runtime jars, to <name>/classes.wasm with TeaVM.",
)

def teavm_wasm(name, deps, **kwargs):
    """The teavm_wasm rule, after checking the package may use every Maven pool `deps` names (tools/deps/pools.bzl)."""
    check_pool_use(name, deps)
    _teavm_wasm(name = name, deps = deps, **kwargs)
