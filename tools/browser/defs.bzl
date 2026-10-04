"""browser_test: a Playwright harness as a hermetic js_test against the pinned Chromium (Bazel workplan P1-14)."""

load("@aspect_rules_js//js:defs.bzl", "js_test")
load("//tools/platforms:defs.bzl", "compatible_with")

# The platforms Chrome for Testing builds chromium-headless-shell for (tools/browser/extensions.bzl): the pinned
# browser, its aliases and every browser_test are skipped everywhere else (Bazel workplan P1-19).
CHROMIUM_PLATFORMS = {
    "linux_aarch64": "linux_arm64",
    "linux_x86_64": "linux64",
    "macos_arm64": "mac_arm64",
    "macos_x86_64": "mac_x64",
    "windows_x86_64": "win64",
}

CHROMIUM_COMPATIBLE = compatible_with(CHROMIUM_PLATFORMS.keys())

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
        target_compatible_with = CHROMIUM_COMPATIBLE,
        **kwargs
    )
