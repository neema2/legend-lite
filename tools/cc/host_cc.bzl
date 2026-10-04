"""The host C toolchain a native target needs, declared and checked (Bazel workplan P1-10, P1-11; decisions D1
revised and D5).

Linux links the native image with Bazel's own LLVM toolchain (P1-09) and needs nothing here. macOS keeps
Apple's Command Line Tools, and Windows keeps Visual Studio's MSVC: each is a prerequisite of the machine, so
it is checked when a native target first needs it, and a missing or too-old one fails then, naming what to
install, instead of failing deep inside native-image.

What was found is written to `toolchain.txt` (its exact versions), the file `:present` provides. A native
target takes it as data, which rules_graalvm makes an INPUT of the native-image action: a different compiler
gives a different cache key, so no cache ever serves a binary another toolchain linked. The rule watches the
tools it found, so a toolchain update re-runs it. Versions are recorded, never pinned: a routine update is a
rebuild, not a failure. Only a toolchain older than the floor below fails.
"""

_MSVC_COMPONENT = "Microsoft.VisualStudio.Component.VC.Tools.x86.x64"
_MSVC_FLOOR = "17.6"

# Apple clang 15 shipped with Xcode 15 / the macOS 14 SDK; older ones are not tested here.
_APPLE_CLANG_FLOOR = 15

def _fail(what):
    fail("{} (README: Prerequisites).".format(what))

def _run(rctx, args):
    result = rctx.execute(args)
    return result.stdout.strip() if result.return_code == 0 else None

def _first_line(text):
    return text.split("\n")[0].strip() if text else ""

def _env(rctx, name, default = None):
    """An environment variable by name, case-insensitively (Windows keeps names case-insensitive)."""
    for key, value in rctx.os.environ.items():
        if key.upper() == name.upper():
            return value
    return default

def _vc_tools(rctx, vc_dir, what):
    tools = rctx.path(vc_dir + "\\Auxiliary\\Build\\Microsoft.VCToolsVersion.default.txt")
    if not tools.exists:
        return None
    rctx.watch(tools)
    return ["msvc: " + what, "vc tools: " + rctx.read(tools).strip()]

def _windows(rctx):
    """The MSVC rules_cc will use, recorded the same way whatever the shell: BAZEL_VC if set, else the newest
    Visual Studio with the VC tools (vswhere). A developer shell's cl.exe is only the fallback."""
    missing = ("Windows native targets need Visual Studio 2022 Build Tools " + _MSVC_FLOOR + " or later, with " +
               "\"Desktop development with C++\" (" + _MSVC_COMPONENT + ")")
    bazel_vc = _env(rctx, "BAZEL_VC")
    if bazel_vc:
        found = _vc_tools(rctx, bazel_vc, "BAZEL_VC")
        if not found:
            _fail(missing + ": BAZEL_VC=" + bazel_vc + " holds no VC tools")
        return found
    program_files = _env(rctx, "ProgramFiles(x86)", "C:\\Program Files (x86)")
    vswhere = rctx.path(program_files + "\\Microsoft Visual Studio\\Installer\\vswhere.exe")
    if vswhere.exists:
        query = [vswhere, "-latest", "-products", "*", "-requires", _MSVC_COMPONENT, "-version", "[" + _MSVC_FLOOR + ","]
        installation = _run(rctx, query + ["-property", "installationPath"])
        if installation:
            version = _run(rctx, query + ["-property", "installationVersion"]) or "?"
            found = _vc_tools(rctx, installation + "\\VC", "Visual Studio " + version)
            if found:
                return found
    cl = rctx.which("cl.exe")
    if cl:
        rctx.watch(cl)
        banner = _first_line(rctx.execute([cl]).stderr)  # cl prints its version banner on stderr
        version = banner.split(" Version ")[-1].split(" ")[0] if " Version " in banner else ""
        parts = version.split(".")
        # cl 19.36 is Visual Studio 17.6
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit() and (int(parts[0]), int(parts[1])) >= (19, 36):
            return ["msvc: cl.exe on PATH", "cl: " + banner]
        _fail(missing + ": the cl.exe on PATH is older (" + banner + ")")
    _fail(missing)

def _macos(rctx):
    """Apple's clang and ld, as xcrun finds them (the Command Line Tools, or an Xcode)."""
    missing = "macOS native targets need Apple's Command Line Tools (`xcode-select --install`)"
    xcrun = rctx.which("xcrun")
    if not xcrun:
        _fail(missing + ": no xcrun on PATH")
    found = {}
    for tool in ["clang", "ld"]:
        result = rctx.execute([xcrun, "--find", tool])
        path = result.stdout.strip()
        if result.return_code != 0 or not path or not rctx.path(path).exists:
            # an Xcode whose licence was never accepted, or a broken xcode-select path, says so here
            _fail(missing + ": xcrun found no " + tool + (": " + result.stderr.strip() if result.stderr.strip() else ""))
        rctx.watch(path)
        found[tool] = path
    clang = _first_line(_run(rctx, [found["clang"], "--version"]))
    if not clang.startswith("Apple clang version "):
        _fail(missing + ": " + found["clang"] + " is not Apple's clang (" + clang + ")")
    major = clang[len("Apple clang version "):].split(".")[0]
    if not major.isdigit() or int(major) < _APPLE_CLANG_FLOOR:
        _fail(missing + ", Apple clang " + str(_APPLE_CLANG_FLOOR) + " or later: found " + clang)
    ld = rctx.execute([found["ld"], "-v"])  # ld prints its version on stderr
    return [
        "clang: " + clang,
        "ld: " + _first_line(ld.stderr),
        "sdk: " + (_run(rctx, [xcrun, "--show-sdk-version"]) or "?"),
    ]

def _host_c_toolchain_impl(rctx):
    os = rctx.os.name.lower()
    if os.startswith("windows"):
        found = _windows(rctx)
    elif os.startswith("mac"):
        found = _macos(rctx)
    else:
        found = ["linux: the hermetic LLVM toolchain (MODULE.bazel), no host C toolchain"]
    rctx.file("toolchain.txt", "\n".join(found) + "\n")
    rctx.file("BUILD.bazel", """# The host C toolchain, checked and its versions recorded when this repository was made (//tools/cc:host_cc.bzl).
filegroup(
    name = "present",
    srcs = ["toolchain.txt"],
    visibility = ["//visibility:public"],
)
""")

host_c_toolchain = repository_rule(
    implementation = _host_c_toolchain_impl,
    doc = "Checks the host's C toolchain for native targets (macOS: the CLT; Windows: MSVC) and records its versions.",
    environ = ["PATH", "ProgramFiles(x86)", "BAZEL_VC", "DEVELOPER_DIR"],
    local = True,
)
