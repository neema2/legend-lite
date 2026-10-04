"""executable_of: name exactly one file of a binary, its executable, so a test passes it by $(rlocationpath)."""

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
