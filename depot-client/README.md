# depot-client

The client side of `depot-server/`: published versions through upstream legend-depot's read API, for
Studio (dependencies), Query and DataCube (models by coordinates). One typed client, `DepotClient(api,
fetch)`. Where the Depot lives is only which `fetch` it gets:
- `depot-server/`'s rules in the model home's server (`/depot/api`, beside the SDLC);
- a real legend-depot;
- the same rules compiled into the page with the SDLC's: `(await wasmSdlcServer(...)).depotFetch`
  (`sdlc-client/src/wasm-server.ts`). Its versions are the page's own.

**One suite, every Depot** (`test/conformance.ts`): versions are published through the SDLC beside it,
then read back. That includes a diamond, resolved nearest-wins as upstream's Aether does.
- `//depot-client:wasm_test` runs it on the page.
- `//depot-client:server_test` runs it on the model home over HTTP.

The rules: `studio/docs/DEPOT_CONTRACT.md`.

## Departures from upstream (design S8)

| Upstream | Here |
|---|---|
| A missing version on the entity and dependency routes is 500 | 404, the same message |
| Aliases `latest` and `head` match lowercase only | Any case (`HEAD` too) |
| Versions are listed in Mongo order | Version order, the snapshot last |
| `analyzeDependencyTree…` reports no conflicts (always `[]`) | Not served yet |
| `pureModelContextData` (the engine's pointer) | Not served yet (design S9, Phase 3) |

lite's own route: `POST /projects/dependencies/pure` takes the same body as
`dependenciesFromArtifactDependencies` and answers each version's files (`[{groupId, artifactId,
versionId, files: [{path, pureCode}]}]`). This is what the in-tab compiler reads.
