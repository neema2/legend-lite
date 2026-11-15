# DuckDB, the first lean target: census and plan (2026-11-15)

The user's order (2026-11-15): DuckDB, Snowflake, Postgres, Databricks, ClickHouse, BigQuery. This is DuckDB's
census and the forms planned for it, written while this container's disk held no room for a build (measured from
the planned SQL of channel A, `//core:product_sql_measure measure`, and `//core:repeat_census`). Each step lands
only measured: characters, subselects, repeats, refusals and time, before and after, and no answer moved.

## Where DuckDB stands

| measure | value |
|---|---|
| statements | 2,442 planned (9 refused) |
| characters | 544,720 |
| repeated characters | 58,866 (10.8%) |
| subselects | 338 over 2,301 statements |

### Subselects by origin

| count | chars | origin | needed? |
|---|---|---|---|
| 134 | 50,505 | `t<N>` derived tables (below) | about half are not |
| 53 | 35,891 | `__lets`, P1a's bindings | yes: each removes a repeat |
| 67 | 7,775 | `> ANY (SELECT ...)`, `> ALL (SELECT ...)` | yes: the user's own quantified comparison |
| 16 | 2,848 | a value list's `value` frames | to check |
| 13 | 1,843 | `IN (SELECT ...)` | yes |
| 12 | 11,911 | `_rows<N>`: a sorted extend's order key hidden | no (step 2) |
| 10 | 4,965 | `_pct_keys`: a dynamic pivot's keys | yes: its design |
| 10 | 549 | `_uz`: two lists zipped for covariance | yes: DuckDB has no list covariance |
| 10 | 988 | a scalar subselect in a CASE's ELSE | to check |

The `t<N>` derived tables, by shape (inner clauses / outer clauses):

| count | shape | plan |
|---|---|---|
| 47 | none / none | a `COUNT(*)` over a pass-through select (a relation's `size`), or a tagged list's binding (needed) |
| 16 | none / ORDER BY | to check: an order over a projection |
| 12 | none / WHERE | a filter over a projection: fold the WHERE into the inner select |
| 8 | WHERE / none | a projection over a filter: to check |
| 8 | DISTINCT / WHERE | needed (the filter is over the distinct rows) |
| 4 | GROUP BY / WHERE | the filter over the groups: HAVING |
| 4 + | LIMIT, OFFSET stacked | needed: stacked offsets (ruled 2026-11-15: folding adds the literals, which is computing; revisit with the database's own expression form if product queries show the shape) |

### Repeats by origin (`//core:repeat_census`, DuckDB)

- In the tree: 3,106 excess nodes. 54% are closed repeats too small for P1a's saving rule, 19% raise-guarded
  operands, 14% tagged or checked, 9% lambda reads, 3% row reads.
- Rendered, the largest:
  - TDS literal rows that are equal as data (`(200, TIMESTAMP '...', 10)` written 104 times): data, not repetition.
  - an instance printed as `json_object('_type', ...)` per read of a `let` (4.5K, 11 copies);
  - the `__l0 IS NULL THEN error(...)` guards over a bound instance (2.8K + 1.8K);
  - a variant flatten's `json_type(CAST((CASE WHEN ...` checks (8.1K over 6 statements);
  - `struct_extract({'intId': ...})` of an instance literal (1.9K).

## The plan, in order

1. **A relation's size is `COUNT(*)` over its rows, with no wrapper.** Today:
   `SELECT (SELECT COUNT(*) FROM (SELECT _tds0.val, ... FROM (VALUES ...) AS _tds0(...)) AS t0) AS value`.
   - Planned: `SELECT COUNT(*) AS value FROM (VALUES ...) AS _tds0(...)`.
   - It applies wherever the counted select keeps its source's rows: no DISTINCT, GROUP BY, LIMIT, OFFSET or
     HAVING. A window column added by an extend does not change the count, so it is dropped with the projections.
   - The root's scalar subquery goes too: a root that is one aggregate is that aggregate's select.
   - Expected: about 30 subselects and their projections. A lowering rule, so every dialect gains.
2. **A sorted extend's order key is not hidden in a derived table.** Today a select whose sort key is not an output
   is wrapped:
   `SELECT _rows1.a, ... FROM (SELECT ..., <key> AS _ord0 FROM ... ORDER BY <key>) AS _rows1 ORDER BY _rows1._ord0`.
   That is one subselect, the key written twice and the sort done twice.
   - Planned: `SELECT ... FROM ... ORDER BY <key>`. Standard SQL orders a non-DISTINCT select by an expression it
     does not project, and DuckDB, Postgres, Snowflake, Trino, Databricks, ClickHouse and BigQuery all do.
   - Where the key is a projected expression (`finalFullName`'s concat in the example), it orders by that output.
   - The wrapper stays only where its select is DISTINCT, or is a set operation's branch.
   - Expected: 12 subselects and 11.9K characters on DuckDB; more on the dialects that write the same wrapper.
3. **A filter over a projection folds into it; a filter over groups is HAVING.** 12 + 4 `t<N>` wrappers.
   - It applies only where the filter reads no window column and the projection is not DISTINCT. A window
     column is computed after WHERE, so filtering on one needs the wrapper (Pure's `extend(over)->filter`).
4. **An instance read twice is built once.** A `let` of an instance prints `json_object('_type', ...)` at each
   read, and its guard (`CASE WHEN __l0 IS NULL THEN error(...)`) writes it again.
   - Planned: the instance literal is never NULL (`SqlTyping.mayBeNull` of a struct literal), so its guard is not
     written, the null-guard rule P4 applied to the dialects.
   - The printed form is bound with the instance, P1a's binding widened to struct values: a struct literal cannot
     raise.
5. **A variant flatten reads its value once.** 8.1K over 6 statements. `json_type(CAST((CASE WHEN ...` is written
   for each check of the same value. Planned: bound in DuckDB's own form, a `LATERAL (SELECT <value> AS v)`
   measured against `list_transform([<value>], v -> ...)[1]`, the faster one kept.

Each step: `//core:lean_sql_baseline_test` pins moved with the step's date; channel A and channel B on DuckDB and
core's integration lane green; the step's statements timed (`//core:product_sql_measure time`, best of 7) and none
slower.

## Open question for the orchestrator

- **Stacked LIMIT and OFFSET.** `drop(2)->drop(1)` is `OFFSET 2` inside `OFFSET 1`. One select needs the bounds'
  sum, `OFFSET 3`, a value computed at planning. The world-map rule ("compare, never compute") reads as forbidding
  that.
- DuckDB takes an expression there (`OFFSET 2 + 1`), which the database computes. That would be a DuckDB-only
  form; Postgres, Snowflake, Trino and BigQuery do not all take one.
- Options:
  - (a) keep the wrapper;
  - (b) DuckDB's expression form, measured;
  - (c) fold at planning, as a ruled exception.
- Recommendation: (a) for now. The 4 statements are tests of the stacking itself.
