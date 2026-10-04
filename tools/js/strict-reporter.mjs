// A node:test reporter whose only job is the exit code.
//
// Node 22's runner reports a `describe` whose BODY throws as `not ok`,
// then counts `# fail 0` and exits 0 — measured 2026-09-23 on 22.16.0.
// A guardrail that could not read its inputs was green that way, under
// npm and under Bazel alike: the suite never built, so none of its
// tests ran, and nothing failed. This reporter sees every `test:fail`,
// suites included, and fails the process on the first one.
//
// It prints nothing but the failures; the human-readable reporter runs
// beside it (see package.json and BUILD.bazel).
export default async function* strict(source) {
  for await (const event of source) {
    if (event.type === 'test:fail') {
      process.exitCode = 1;
      const { name, file, details } = event.data;
      yield `FAILED (${details?.type ?? 'test'}): ${name}  [${file ?? '?'}]\n`;
    }
  }
}
