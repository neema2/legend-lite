# project-store

Projects, through upstream legend-sdlc's REST API and lite's text routes (design S15), for Studio
and every app here that reads projects. It is what `query-store/` is for saved queries: one client,
and where the SDLC lives -- `sdlc-server/`, a real legend-sdlc, or this page -- is only which `fetch`
it gets (design S21). No Depot here: a page has no published versions; a typed `depot-client/`
arrives when Query (by GAV) or Studio (dependencies) needs one.

- `src/wire.ts`: upstream's records (`Entity`, `Project`, `Workspace`, `Revision`,
  `ProjectConfiguration`, …) and the text routes' `PureFile` / `PureChange`.
- `src/client.ts`: **the one client**, `SdlcClient(api, fetch)`. It is typed, and refusals come back
  as `SdlcError(message, status)`.
- `src/wasm-server.ts`: **the SDLC in the page** (level 0: no server). It is not written here: it is
  `sdlc-server/`'s Java rules (`com.legend.sdlc.Sdlc`) compiled to WebAssembly (`//sdlc-server:page`),
  so the page and the server answer from one implementation (decided by the spike of 2026-10-04,
  design S21). This file is only the adapter:
  - it turns each `fetch` into one `handle` call;
  - it writes what each call changed to IndexedDB before answering;
  - it loads those records back when the page opens.
  The rules follow legend-sdlc's GitLab backend (`studio/docs/SDLC_CONTRACT_SLICE1.md`):
  - text is stored, one `.pure` file per element (S5);
  - imports are refused at save, as upstream SDLC refuses them (S20, v0);
  - entities are derived on read by lite's `grammarToJson`, never stored;
  - revisions are real git commits (ids checked against `git hash-object`), so a page's project can
    be pushed into a repository as it is.
- `src/records.ts`: where the page keeps them: IndexedDB (`legend-projects`), or memory in a test.

**One suite, every SDLC** (`test/conformance.ts`). It sends raw HTTP and checks the JSON field
orders, the statuses and the refusals word for word, then repeats through the client.
- `//project-store:wasm_test` runs it on the page's SDLC (the Java rules in WebAssembly).
- `sdlc-server/` over HTTP joins it when it lands.

## What the page's SDLC does not have

Reviews, versions and patches answer upstream's 501: `{"capability", "backendType": "page",
"message": "The backend \"page\" does not support REVIEWS"}`. So does `entityChanges`
(`ENTITY_CHANGES`, a capability name of lite's own): a JSON save is printed to text first (S5), and
the model printer is not built yet (S20). Group workspaces are not in this slice.

## Departures from upstream (each also a design §4 entry)

| # | Upstream (contract quirk) | Here |
|---|---|---|
| 1 | Project id is GitLab's number with a prefix (`PROD-123`); FS uses the name | `groupId:artifactId`, the id upstream's own `project.json` uses for a dependency. One project per coordinates (409 on a second). |
| 2 | Entity and text reads at a literal revision id do not check it (quirk 6) | 404 `Revision <r> is unknown for <desc>` unless it is on the ref's history |
| 3 | The stale-revision 409 comes after the operations are checked against the stale state, so a stale save may answer 500 (quirk 8) | The 409 comes first |
| 4 | Two changes to one path in a request are not checked (quirk 10) | 400 `Duplicate entity path: <p>` among the change errors |
| 5 | Entity list order is hash order (quirk 14) | By path |
| 6 | Configuration of a missing project answers 200 with a default (quirk 5) | 404 `Unknown project: <p>` |
| 7 | Revision, Workspace, User and Entity key order follows the JVM's method order (quirk 15) | The interfaces' declaration order |
| 8 | `canCreateVersion` follows the server's configuration | `false` here (no versions in a page) |

Kept on purpose, though odd:
- CREATE of an existing path and MODIFY/DELETE of a missing one answer 500 (quirk 9). One commit
  implementation serves both save routes, so both answer in upstream's words.
- Workspace create is a no-op when the workspace is already at the line's head, and 500 elsewhere
  (quirk 1).
- Deleting a missing workspace succeeds (quirk 3).
- `pureChanges`' `revisionId` is compared verbatim, as upstream compares `entityChanges`' (quirk 7).

The text routes (`…/pure`, `…/pure/{path}`, `…/pureChanges`) are lite's own (S15). They reuse
upstream's save rules and words, with these lite-specific refusals among the change errors:
- `Missing Pure code` / `Unexpected Pure code`;
- the grammar's own message for text it cannot read;
- `Mismatch between entity path ("<p>") and the element's path ("<q>")`;
- `Unsupported element type: <_type>`.

Upstream's own `.pure` rules apply verbatim:
- `No element found`;
- `Expected one element, found <n>`;
- `Expected at most one SectionIndex, found <n>`;
- `Imports in Pure files are not currently supported`.
