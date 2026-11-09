# Open findings of the unstructured study

Found while modelling the study's corpora. Whether legend-pure and legend-engine refuse F-U1 and F-U2 has not been read
from the pinned sources yet.

| id | what | today | pinned by |
|---|---|---|---|
| F-U1 | A mapping can name a column its table does not have. | `compileModel` accepts it; only a query that reaches the property fails, at planning. So a model that compiles says little about its mappings. | `OpenMappingGapsPinTest.fU1_aMappingToAColumnThatDoesNotExistCompiles` |
| F-U2 | An `AssociationMapping` can name a set id that does not exist. | Accepted by `compileModel` and by planning. | `OpenMappingGapsPinTest.fU2_anAssociationMappingToASetIdThatDoesNotExistPlans` |
| F-U5 | A stored value outside an `EnumerationMapping` reads back as NULL, even on a `[1]` property. | No refusal at load or at read; see below. | `OpenMappingGapsPinTest.fU5_aValueOutsideTheEnumMappingReadsBackAsNullOnARequiredProperty` |
| F-U3 | No vector column. | A Relational column has no VECTOR type (`DatabaseProtocolParser.parseDbType`) and Pure has no embedding type, so `corpus::Passage` carries no embedding. | nothing: an absent feature |
| F-U4 | No long-text column. | There is no TEXT or CLOB, so long text is `VARCHAR(1000000)`: above the corpus's longest section (391,046 bytes) and within Postgres's limit for a sized VARCHAR (10,485,760). | nothing: an absent feature |

When a pin turns red the gap has closed: delete the pin and its row.

## F-U5: a value outside the enum reads back as NULL

Lite plans the mapping as a `CASE` whose fall-through is NULL (`lending::Collateral.kind`, from `model.pure`):

```sql
CASE WHEN t0.KIND = 'AllAssets' THEN 'AllAssets' WHEN t0.KIND = 'Receivables' THEN 'Receivables'
     WHEN t0.KIND = 'Inventory' THEN 'Inventory' WHEN t0.KIND = 'RealEstate' THEN 'RealEstate'
     WHEN t0.KIND = 'Equipment' THEN 'Equipment' WHEN t0.KIND = 'EquityPledge' THEN 'EquityPledge' ELSE NULL END AS c1
```

So a row whose column holds a misspelled value is accepted by the database and answered with NULL, on a property the
model declares `[1]`. `tools/unstructured-corpus/load.py` refuses such a value before it loads, but that protects this
corpus only: any other way of writing the table still reads back NULL.

**What legend-engine does, read from the pinned source (tag `legend-engine-4.145.0`; read, not run):**
`RelationalResult.java` (its TDS transformers, lines 259-281) applies `ExecutionNodeTDSResultHelper.getTDSEnumTransformer`
to an enum column, which is `ExecutionNodeResultHelper.buildReverseEnumFunc` (lines 44-48):

```java
return s -> reverseEnumMap.get(s == null ? "" : s.toString());
```

`reverseEnumMap` is a `UnifiedMap` of stored value to enum name, with no check around the lookup, so a stored value
outside the mapping becomes null there too. The outcome reads the same; the place differs (upstream maps in Java after
the rows return, lite in the SQL). A class-result column goes through the same function
(`ExecutionNodeClassResultHelper`, line 76).

**Needs a ruling (the owner):** the source says legend-engine also answers null. Should this be confirmed against the
engine runner and then posted as the register row below, or should lite refuse even though upstream does not? If the
runner shows upstream refusing after all, this is a defect in lite instead.

Proposed row for `docs/SEMANTICS_REGISTER.md`, number left to the owner:

| id | kind | legend-pure / legend-engine | lite | why, decided by | evidence | owner |
|---|---|---|---|---|---|---|
| (owner) | mechanism | a stored value outside an `EnumerationMapping` maps to null in Java after the rows return (`ExecutionNodeResultHelper.buildReverseEnumFunc`, `reverseEnumMap.get`), with no refusal | the same value maps to NULL in the SQL (`CASE ... ELSE NULL END`), with no refusal | Java does not compute a value the database can; not yet ruled | `OpenMappingGapsPinTest.fU5_...`; legend-engine 4.145.0 source, read only | (owner) |
