"""THE PINNED BROWSER (Bazel workplan P1-14, spike S4): one chromium-headless-shell archive per platform.

MODULE.bazel holds the pin in one line:

    chromium.pin(revision = "1243", version = "153.0.8010.12", sha256 = {"mac-arm64": "...", ...})

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

def _chromium_impl(mctx):
    pins = [tag for mod in mctx.modules for tag in mod.tags.pin]
    if len(pins) != 1:
        fail("chromium.pin: exactly one pin, in the root module; found %d" % len(pins))
    pin = pins[0]
    if sorted(pin.sha256.keys()) != sorted(_PLATFORMS):
        fail("chromium.pin: sha256 must name exactly %s; got %s" % (_PLATFORMS, sorted(pin.sha256.keys())))
    for platform, sha256 in pin.sha256.items():
        http_archive(
            name = "chromium_headless_shell_" + platform.replace("-", "_"),
            add_prefix = "chromium_headless_shell-" + pin.revision,
            build_file_content = _BUILD.format(
                revision = pin.revision,
                platform = platform,
                exe = ".exe" if platform == "win64" else "",
            ),
            sha256 = sha256,
            urls = [
                "https://cdn.playwright.dev/builds/cft/%s/%s/chrome-headless-shell-%s.zip" % (pin.version, platform, platform),
                "https://storage.googleapis.com/chrome-for-testing-public/%s/%s/chrome-headless-shell-%s.zip" % (pin.version, platform, platform),
            ],
        )
    return mctx.extension_metadata(reproducible = True)

chromium = module_extension(
    implementation = _chromium_impl,
    tag_classes = {
        "pin": tag_class(attrs = {
            "revision": attr.string(mandatory = True, doc = "chromium-headless-shell's revision in the locked playwright-core's browsers.json"),
            "version": attr.string(mandatory = True, doc = "its Chrome for Testing version (browsers.json browserVersion)"),
            "sha256": attr.string_dict(mandatory = True, doc = "platform (mac-arm64, mac-x64, linux64, linux-arm64, win64) to the archive's sha256"),
        }),
    },
)
