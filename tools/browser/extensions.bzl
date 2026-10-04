"""THE PINNED BROWSER (Bazel workplan P1-14, spike S4): one chromium-headless-shell archive per platform.

MODULE.bazel holds the pin in one line:

    chromium.pin(revision = "1243", version = "153.0.8010.12", integrity = {"mac-arm64": "sha256-...", ...})

`revision` is the locked playwright-core's browsers.json revision for chromium-headless-shell, and
`version` its Chrome for Testing version (//tools/browser:revision_test holds them equal). Each archive
becomes `@chromium_headless_shell_<platform>`, fetched lazily: a host fetches only the one its select
picks. The prefix is the directory name Playwright's registry looks for, so PLAYWRIGHT_BROWSERS_PATH can
point at the repository's root (pinned-chromium.mjs). Two URLs per archive: Playwright's CDN, and Google's
public Chrome for Testing bucket, which serves the same bytes.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_BUILD = """\
filegroup(
    name = "files",
    srcs = glob(["chromium_headless_shell-{revision}/**"]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "executable",
    srcs = ["chromium_headless_shell-{revision}/chrome-headless-shell-{platform}/chrome-headless-shell{exe}"],
    visibility = ["//visibility:public"],
)
"""

_PLATFORMS = ["mac-arm64", "mac-x64", "linux64", "linux-arm64", "win64"]

def _pin_repo_impl(rctx):
    rctx.file("pin.json", json.encode({"revision": rctx.attr.revision, "version": rctx.attr.version}) + "\n")
    rctx.file("BUILD.bazel", 'exports_files(["pin.json"], visibility = ["//visibility:public"])\n')

# @chromium_pin//:pin.json: the pin's revision and version, for //tools/browser:revision_test, which then
# needs no ~100 MB archive to read them
_pin_repo = repository_rule(
    implementation = _pin_repo_impl,
    attrs = {"revision": attr.string(mandatory = True), "version": attr.string(mandatory = True)},
)

def _chromium_impl(mctx):
    pins = [tag for mod in mctx.modules for tag in mod.tags.pin]
    if len(pins) != 1:
        fail("chromium.pin: exactly one pin, in the root module; found %d" % len(pins))
    pin = pins[0]
    if sorted(pin.integrity.keys()) != sorted(_PLATFORMS):
        fail("chromium.pin: integrity must name exactly %s; got %s" % (_PLATFORMS, sorted(pin.integrity.keys())))
    for platform, integrity in pin.integrity.items():
        http_archive(
            name = "chromium_headless_shell_" + platform.replace("-", "_"),
            add_prefix = "chromium_headless_shell-" + pin.revision,
            build_file_content = _BUILD.format(
                revision = pin.revision,
                platform = platform,
                exe = ".exe" if platform == "win64" else "",
            ),
            integrity = integrity,
            urls = [
                "https://cdn.playwright.dev/builds/cft/%s/%s/chrome-headless-shell-%s.zip" % (pin.version, platform, platform),
                "https://storage.googleapis.com/chrome-for-testing-public/%s/%s/chrome-headless-shell-%s.zip" % (pin.version, platform, platform),
            ],
        )
    _pin_repo(name = "chromium_pin", revision = pin.revision, version = pin.version)
    return mctx.extension_metadata(reproducible = True)

chromium = module_extension(
    implementation = _chromium_impl,
    tag_classes = {
        "pin": tag_class(attrs = {
            "revision": attr.string(mandatory = True, doc = "chromium-headless-shell's revision in the locked playwright-core's browsers.json"),
            "version": attr.string(mandatory = True, doc = "its Chrome for Testing version (browsers.json browserVersion)"),
            "integrity": attr.string_dict(mandatory = True, doc = "platform (mac-arm64, mac-x64, linux64, linux-arm64, win64) to the archive's integrity (sha256-<base64>)"),
        }),
    },
)
