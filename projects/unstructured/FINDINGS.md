# Open findings of the unstructured study

The study these belong to is `docs/UNSTRUCTURED_STUDY_2026_11_09.md`; its §7 lists them with their tests.

Found while modelling the study's corpora. Upstream's answer below was read from the pinned sources (legend-engine
4.145.0) and asked of the engine runner (`//tools/engine-runner`, the recipe in `docs/result-boundary-homework/README.md`
§7), on the same models the pins use, with an H2 connection.

| id | what | lite today | legend-engine 4.145.0 | pinned by |
|---|---|---|---|---|
| F-U1 | A mapping can name a column its table does not have. | `compileModel` accepts it; only a query that reaches the property fails, at planning. So a model that compiles says little about its mappings. | Refuses at compile: `Can't find column 'NO_SUCH_COLUMN'`. | `OpenMappingGapsPinTest.fU1_aMappingToAColumnThatDoesNotExistCompiles` |
| F-U2 | An `AssociationMapping` can name a set id that does not exist. | Accepted by `compileModel` and by planning. | Compiles, then refuses at planning: `Expected 1 class mapping for 'B' in 'M', found 0`. | `OpenMappingGapsPinTest.fU2_anAssociationMappingToASetIdThatDoesNotExistPlans` |
| F-U5 | A stored value outside an `EnumerationMapping` reads back as NULL, even on a `[1]` property. | No refusal at load or at read; see below. | The same answer and the same SQL: matches upstream; the loader refuses out-of-enum values before the database sees them. | `OpenMappingGapsPinTest.fU5_aValueOutsideTheEnumMappingReadsBackAsNullOnARequiredProperty` |
| F-U3 | No vector column. | A Relational column has no VECTOR type (`DatabaseProtocolParser.parseDbType`) and Pure has no embedding type, so `corpus::Passage` carries no embedding. | not read: an absent feature here, not a divergence found | nothing |
| F-U4 | No long-text column. | There is no TEXT or CLOB, so long text is `VARCHAR(1000000)`: above the corpus's longest section (391,046 bytes) and within Postgres's limit for a sized VARCHAR (10,485,760). | not read: an absent feature here, not a divergence found | nothing |

When a pin turns red the behaviour has moved: read its failure message, then change the pin and its row deliberately.

## Needs a ruling: two defect reports

Lite is laxer than upstream in both, with no register row, so each is a defect under the upstream-fidelity tenet.
**Recommendation: close both to match upstream.** Nothing on this branch changes core's behaviour; if the owner wants the
fix, it is a separate PR. Both were run on the pin's own model with an H2 connection. The same model with the real column
or the real set id compiles and plans. Paths are at tag `legend-engine-4.145.0`.

### Defect F-U1: a mapping to a column that does not exist compiles

- **Upstream refuses at compile.** The engine's exact error from the run:
  `org.finos.legend.engine.shared.core.operational.errorManagement.EngineException: Can't find column 'NO_SUCH_COLUMN'`
- **Pinned source:** `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph/HelperRelationalBuilder.java:163`
- **Lite:** `compileModel` accepts it. Only a query that reaches the property fails, at planning.
- **Turns red when closed:** `OpenMappingGapsPinTest.fU1_aMappingToAColumnThatDoesNotExistCompiles`

### Defect F-U2: an association mapping to a set id that does not exist plans

- **Upstream compiles it, then refuses at planning.** The engine's exact error from the run:
  `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Expected 1 class mapping for 'B' in 'M', found 0"`
- **Pinned source:** `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:1934`
- **Lite:** `compileModel` and planning both accept it.
- **Turns red when closed:** `OpenMappingGapsPinTest.fU2_anAssociationMappingToASetIdThatDoesNotExistPlans`

## F-U5: matches upstream

Matches upstream; the loader refuses out-of-enum values before the database sees them. No register row: the register
records differences, and this is none (the owner, 2026-11-08).

Lite plans the mapping as a `CASE` whose fall-through is NULL (`lending::Collateral.kind`, from `model.pure`):

```sql
CASE WHEN t0.KIND = 'AllAssets' THEN 'AllAssets' WHEN t0.KIND = 'Receivables' THEN 'Receivables'
     WHEN t0.KIND = 'Inventory' THEN 'Inventory' WHEN t0.KIND = 'RealEstate' THEN 'RealEstate'
     WHEN t0.KIND = 'Equipment' THEN 'Equipment' WHEN t0.KIND = 'EquityPledge' THEN 'EquityPledge' ELSE NULL END AS c1
```

So a row whose column holds a misspelled value is accepted by the database and answered with NULL, on a property the
model declares `[1]`.

**legend-engine does the same, asked on the runner.** One row holds `'Blue'`, outside the mapping, the other `'Red'`:

```
select "root".ID as "id", case when "root".COLOR = 'Red' then 'Red' else null end as "color" from ITEM as "root"
"rows" : [{"values": ["a",null]},{"values": ["b","Red"]}]
```

The upstream-fidelity tenet decides it (the owner, 2026-11-08): lite answers null, and is not made stricter than upstream.
This corrects this file's earlier reading, which took the mapping to happen in Java after the rows return
(`ExecutionNodeResultHelper.buildReverseEnumFunc`). That code exists, but on this relation path the SQL does the mapping.

## F-U6: a repeated passage takes its newest date for recency

Found by the judge (`projects/unstructured/evals/DECISIONS.md`, entry 3). Hybrid dedupes by sha256 before it blends in
recency, so a hash held at several dates takes its most recent location's date. Boilerplate repeated across filings
therefore inherits the newest filing's date, which inflates its recency. Measured 2026-11-09: 171 of the 1,769 distinct
hashes are held at more than one date.

| Source | Hashes | At more than one date | Largest gap, days |
|---|---|---|---|
| `EDGAR` | 1,361 | 50 | 263 |
| `LENDING` | 243 | 80 | 1,015 |
| `SALES` | 165 | 41 | 216 |

Approved for the baseline. Scoring per location and keeping the best is a candidate tuning entry, not a baseline change.

## Design inputs for the judge

**Repeated passage text is expected, and is not a bug.** EDGAR's 1,499 passage rows hold 1,361 distinct sha256 values: 125
values sit on more than one row (138 extra rows), nearly all boilerplate repeated across filings. The synthetic corpora add
their own repeats: signature blocks, the disclaimer, standard clauses. An agreement's clauses say "the Borrower", so 14 of
the 65 covenant hashes and 27 of the 32 event-of-default hashes also appear in other agreements; `spans.csv` says which
clause each row means. No allocation request's hash repeats. Across all three corpora there are 2,595 rows and 1,769
distinct values.

The owner's ruling, 2026-11-08, which the judge inherits:

- The judge dedupes by sha256 before any cut.
- A result row is identified by `(path, charStart)`. `sha256` is its fingerprint, not its identity.

`load.py` refuses two passages with the same `(path, charStart)`, across all three corpora, so that identity always holds.

**The passage rule differs by corpus, by design (for the study's §4; the owner, 2026-11-08).** EDGAR passages merge to about
800 characters because filings have no natural unit. SALES and LENDING passages are the paragraph or the clause because the
ground truth hashes exactly that unit. `corpus/README.md` states it for the corpus.

## For the experiment

**A table-sourced fact gets its own span class, not a generalised `Passage`** (the author's decision, 2026-11-13, answering
the owner's review of PR #93, §1 and §2). It is a recommendation to bring to the owner in the experiment's first model
commit, and nothing on #93 changes the model:

- `corpus::SourceSpan` is the abstraction, with two kinds:
  - `corpus::Passage`, a span of text: `path`, `charStart`, `charEnd`, and the `sha256` of exactly those characters.
  - `corpus::TableRowSpan`, a span of a table's rows: `path`, `rowStart`, `rowEnd`, and the `sha256` of the rows' bytes.
- Every extracted fact (`sales::AllocationRequest`, `lending::Covenant`, `lending::EventOfDefault`, `edgar::Holding`)
  associates to a `SourceSpan`, and carries `extractorVersion` and `confidence`.
- The synthetic corpora's facts point at a `Passage`, their generator the extractor at confidence 1.0 and a named
  version. `edgar::Holding` points at a `TableRowSpan` of its `holdings.csv`, which `tools/edgar-corpus/extract.py` parses
  from the filing's information table.
- Real extraction stays out of scope: this is where its output would land.
- Structured-route receipts are judged through this association under a new key version, never by widening the frozen one.
