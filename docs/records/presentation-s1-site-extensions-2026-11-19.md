# S1 of the presentation program: an upstream mode's words on the raise site (2026-11-19)

The record behind S1 on `claude/presentation-b1`. It amends §3.3 and the K10 record of the design
(`docs/records/presentation-program-design-2026-11-18.md`, on `claude/presentation-design`), as the
orchestrator accepted on 2026-11-19 (00:40 and 00:47 UTC).

## What S1 does

- `ExecutionPlan.Site` gains `List<Extension> extensions`. It holds the edge's opaque records of the site:
  today one `edge.SiteWords(words)`, an upstream mode's words, planned with the site by the style's two-form
  `planner.SiteFacts`.
- A run's raise already carries its whole site (`exec.RaisedErrors.Raised#site`). So the edge answers in
  those words with no lookup and no state: `RaisedWords.run` for pure/v1 and PCT channel A, and
  `EdgeHostWords#caught` for a test body's `assertError`.
- Deleted: the stateful recorder (`SiteWordsRecorder`, `PureText.recording`), `PlanStyle.siteWords`, the
  text-keyed tables (`Appeals`' table, `EdgeHearing.raisedWords`), the test plan's per-step `SiteWords(at, n, words)`
  records, and the hosts' `siteWords` table beside the plan.
- Plan v59 and test plan v33.

## Why an extension slot on `Site` keeps K10

K10 (the edge audit, 2026-11-17/18) ruled that legend-pure's words are never a component of the product's
site. The slot keeps that ruling, for three reasons:

1. **The words are never a typed product field.** `Site`'s product fields stay exactly as K10 left them:
   `(n, kind, where, message)`, with `message` lite's own sentence. The slot is typed by the product's opaque
   `Extension` interface. Only the edge's records implement it, and only the edge's codec reads or writes
   them (`ExtensionCodec`).
2. **The product never reads the slot.** No product class reads `Site#extensions`; the edge alone reads it.
   That is why `EdgeOnlyDataTest` reports "component ExecutionPlan$Site#extensions: by the edge". The row that
   rules it is pending the user's word, with this record as its reason (design §3.3: "it never reads
   extensions").
3. **A LITE plan's JSON is unchanged apart from its version.** The product's form of `SiteFacts` plans no
   record. An empty slot is omitted from the JSON, so a plan made in lite's own style writes v58's bytes, with
   only `"version":59` in place of 58.

## The two explicit-codec entries, and the guards they touch

WebAssembly's `ServiceLoader` returns no providers (`wasm/README.md`). The tab's edge boundary runs
`HostAnswers.arrowPlan`/`jsonPlan`, whose answers now carry the sites' words. So the edge hands its own
codec to the two writers it calls there:

- `PlanJson.write(ExecutionPlan, ExtensionCodec)`;
- `PlanJson.sitesOf(ExecutionPlan, String, ExtensionCodec)`. This replaces `sitesOf(ExecutionPlan, String)`,
  whose one caller was the edge.

A host declares a statement's sites from the answer's `sites` view and again from the plan carried beside it,
and refuses two that differ. So the view writes each site exactly as the plan does.

`EdgeOnlyMembersTest` sees both entries as product members only the edge calls. One is the existing ruled
`sitesOf` row, now with a new signature; the other is the new `write` overload. Both rows are pending the
user's word, together with `EdgeOnlyDataTest`'s.
