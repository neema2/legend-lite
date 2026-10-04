"""browser_test: a Playwright harness as a hermetic js_test against the pinned Chromium (Bazel workplan P1-14)."""

load("@aspect_rules_js//js:defs.bzl", "js_test")

def browser_test(name, entry_point, data = [], env = {}, tags = [], size = "medium", **kwargs):
    """A js_test that drives the Chromium Bazel fetched (tools/browser/extensions.bzl), nothing from $HOME.

    The harness imports //tools/browser:pinned-chromium.mjs before 'playwright', serves on
    127.0.0.1 port 0, writes temp files under TEST_TMPDIR and artifacts under
    TEST_UNDECLARED_OUTPUTS_DIR.
    """
    js_test(
        name = name,
        entry_point = entry_point,
        data = data + [
            "//tools/browser:chromium_headless_shell",
            "//tools/browser:chromium_headless_shell_executable",
            "//tools/browser:pinned_chromium",
        ],
        env = dict({
            "PINNED_CHROMIUM": "$(rlocationpath //tools/browser:chromium_headless_shell_executable)",
            # belt and braces: if the import were forgotten, Playwright looks HERE, not in $HOME
            "PLAYWRIGHT_BROWSERS_PATH": "/nonexistent-use-pinned-chromium",
        }, **env),
        # the browser is ~200 MB of external files: runfiles symlinks, never copies into bazel-bin
        no_copy_to_bin = [
            "//tools/browser:chromium_headless_shell",
            "//tools/browser:chromium_headless_shell_executable",
        ],
        size = size,
        tags = tags + ["browser"],
        **kwargs
    )

def _executable_of_impl(ctx):
    exe = ctx.attr.binary[DefaultInfo].files_to_run.executable
    return [DefaultInfo(files = depset([exe]))]

executable_of = rule(
    implementation = _executable_of_impl,
    doc = """Exactly one file: a binary's executable (its launcher: a script on Linux and macOS, an .exe on
    Windows), so a test names it with $(rlocationpath ...) and starts it by the runfiles library, never by
    guessing among the binary's files. Put the binary itself in the test's data too, for its runfiles.""",
    attrs = {"binary": attr.label(mandatory = True, executable = True, cfg = "target")},
)
