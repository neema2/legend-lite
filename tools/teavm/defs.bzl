"""teavm_wasm: a Java program compiled ahead of time to a WebAssembly-GC module.

The program is `deps` (their transitive runtime jars ARE its class path — TeaVM
compiles what `main_class` reaches from them, and nothing else), compiled by
TeaVmCompile inside one action. Outputs `<name>/classes.wasm` and TeaVM's host
runtime `<name>/wasm-gc-module-runtime.js`, side by side, as a loader wants them.
"""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _teavm_wasm_impl(ctx):
    jars = depset(transitive = [d[JavaInfo].transitive_runtime_jars for d in ctx.attr.deps])
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
        progress_message = "TeaVM: compiling %s to WebAssembly (%%{label})" % ctx.attr.main_class,
    )
    return [DefaultInfo(files = depset([wasm, runtime]))]

teavm_wasm = rule(
    implementation = _teavm_wasm_impl,
    attrs = {
        "deps": attr.label_list(providers = [JavaInfo], mandatory = True),
        "main_class": attr.string(mandatory = True),
        "_compiler": attr.label(
            default = "//tools/teavm:compile",
            executable = True,
            cfg = "exec",
        ),
    },
    doc = "Compiles main_class, over deps' runtime jars, to <name>/classes.wasm with TeaVM.",
)
