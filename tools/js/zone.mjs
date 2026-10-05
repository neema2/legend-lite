// node_test's time zone (defs.bzl), set before the test's first Date. The zone arrives as LEGEND_TZ as well as TZ:
// on Windows, rules_js's launcher is an MSYS bash, which drops TZ from the environment of the native node.exe it
// starts (main's CI, 2026-10-05: the Tokyo lane ran in the runner's own zone, TZ unset), so every Windows JavaScript
// test ran in the machine's zone. Node applies a TZ assigned at run time.
if (process.env['LEGEND_TZ']) {
  process.env['TZ'] = process.env['LEGEND_TZ'];
}
