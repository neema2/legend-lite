# AggregationAware — Rosetta Stone

> Covers: **E4** (AggregationAware mapping)

---

## What It Does

AggregationAware is a mapping feature that automatically routes queries to pre-aggregated summary tables when possible, falling back to the detail table otherwise. It wraps:

- A **main mapping** (detail table — full granularity)
- N **aggregate Views**, each with a contract declaring:
  - `~groupByFunctions` — which dimensions the view can serve
  - `~aggregateValues` — which pre-computed aggregations are available (mapFn + aggregateFn pairs)
  - `~canAggregate` — whether the view supports further aggregation

The router intercepts `groupBy()` queries, pattern-matches keys/aggs against each view's contract, and rewrites to use the best matching aggregate table.

---

## Upstream Mapping Syntax (legend-engine)

From `core_relational/relational/aggregationAware/test/`:

### Domain model
```pure
Class meta::relational::tests::aggregationAware::domain::Wholesales extends Sales {
    discount: Float[0..1];
    amount: Integer[1];
}

Class meta::relational::tests::aggregationAware::domain::Sales {
    id: Integer[1];
    salesDate: FiscalCalendar[1];
    revenue: Revenue[0..1];
    product: Product[0..1];
    isCancelled: Boolean[1];
}
```

### Store (detail + aggregate tables)
```pure
###Relational
Database db
(
    Schema base_view
    (
        // Detail table — full granularity
        Table SalesTable (
            id INT PK, sales_date DATE PK, is_cancelled_flag VARCHAR(1) PK,
            product_id BIGINT PK, revenue BIGINT PK, discount BIGINT, emp_id BIGINT PK
        )
    )

    Schema user_view_agg
    (
        // Daily aggregate — pre-summed by day
        Table SalesTable_Day (
            sales_date DATE PK, is_cancelled_flag VARCHAR(1) PK,
            product_id BIGINT PK, revenue BIGINT PK, discount BIGINT, emp_id BIGINT PK
        )

        // Monthly aggregate — pre-summed by month
        Table SalesTable_Month (
            sales_date DATE PK, is_cancelled_flag VARCHAR(1) PK,
            product_id BIGINT PK, revenue BIGINT PK, discount BIGINT
        )

        // Quarterly aggregate
        Table SalesTable_Qtr (
            sales_date DATE PK, is_cancelled_flag VARCHAR(1) PK,
            revenue BIGINT PK
        )
    )
)
```

### Mapping
```pure
###Mapping
Mapping test::M
(
    Wholesales[SCT]: AggregationAware
    {
        Views: [
            // View 1: Daily aggregate
            (
                ~modelOperation: {
                    ~canAggregate true,
                    ~groupByFunctions (
                        $this.isCancelled,
                        $this.salesDate.fiscalDay,
                        $this.product.productId,
                        $this.revenue.price
                    ),
                    ~aggregateValues (
                        ( ~mapFn: $this.revenue.price, ~aggregateFn: $mapped->sum() ),
                        ( ~mapFn: $this.discount, ~aggregateFn: $mapped->sum() )
                    )
                },
                ~aggregateMapping: Relational {
                    scope([db]user_view_agg.SalesTable_Day) (
                        isCancelled: is_cancelled_flag == 'Y',
                        salesDate(@Sales_Day_To_Calendar) { ... },
                        product(@Sales_Day_To_Product) { ... },
                        revenue( price: revenue )
                    )
                }
            ),
            // View 2: Monthly aggregate (fewer dimensions)
            (
                ~modelOperation: {
                    ~canAggregate true,
                    ~groupByFunctions (
                        $this.isCancelled,
                        $this.salesDate.fiscalMonth,
                        $this.salesDate.fiscalYear,
                        $this.product.productId
                    ),
                    ~aggregateValues (
                        ( ~mapFn: $this.revenue.price, ~aggregateFn: $mapped->sum() )
                    )
                },
                ~aggregateMapping: Relational { ... }
            )
        ],
        // Main mapping — detail table (fallback)
        ~mainMapping: Relational {
            scope([db]base_view.SalesTable) (
                isCancelled: is_cancelled_flag == 'Y',
                salesDate(@Sales_To_Calendar) { ... },
                product(@Sales_To_Product) { ... },
                revenue( price: revenue )
            )
        }
    }
)
```

### Query routing examples (from upstream tests)
```pure
// groupBy(isCancelled) + sum(price) → Day aggregate (has isCancelled + price→sum)
Wholesales.all()->groupBy([x|$x.isCancelled], [agg(x|$x.revenue.price, y|$y->sum())], ...)
// SQL: SELECT ... FROM user_view_agg.SalesTable_Day GROUP BY ...

// groupBy(fiscalMonth) + sum(price) → Month aggregate (has month + price→sum)
Wholesales.all()->groupBy([x|$x.salesDate.fiscalMonth], [agg(x|$x.revenue.price, y|$y->sum())], ...)
// SQL: SELECT ... FROM user_view_agg.SalesTable_Month GROUP BY ...

// groupBy(fiscalMonth) + sum(discount) → Detail (Month view doesn't have discount→sum)
// SQL: SELECT ... FROM base_view.SalesTable GROUP BY ...
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented — Proposed Design: Option D (measures-only)

`->aggregate(summaryRelation, ~[measures])` — chainable, measures-only declaration. Keys are **inferred from matching column names** between detail and aggregate schemas.

```pure
function model::wholesales(): Relation<(desk:String, product:Integer,
    month:Integer, year:Integer, price:Float, discount:Float, trader:String)>[1] {

    // Detail table — full granularity
    #>{db.SalesTable}#->extend(~[
        desk: r|if($r.IS_CANCELLED == 'Y', |'true', |'false'),
        product: r|$r.PRODUCT_ID,
        month: r|$r->traverse(@Sales_To_Calendar).FISCAL_MONTH,
        year: r|$r->traverse(@Sales_To_Calendar).FISCAL_YEAR,
        price: r|$r.REVENUE,
        discount: r|$r.DISCOUNT,
        trader: r|$r->traverse(@Sales_To_Employee).LAST_NAME
    ])

    // Monthly aggregate — no `trader` column → can't serve trader-level queries
    ->aggregate(
        #>{db.SalesTable_Month}#->extend(~[
            desk: r|if($r.IS_CANCELLED == 'Y', |'true', |'false'),
            product: r|$r.PRODUCT_ID,
            month: r|$r->traverse(@Sales_Month_To_Calendar).FISCAL_MONTH,
            year: r|$r->traverse(@Sales_Month_To_Calendar).FISCAL_YEAR,
            price: r|$r.REVENUE,
            discount: r|$r.DISCOUNT
        ]),
        ~[price: x|$x->sum()]   // price in summary = sum(price) from detail
    )

    // Yearly aggregate — only desk + year
    ->aggregate(
        #>{db.SalesTable_Year}#->extend(~[
            desk: r|$r.DESK,
            year: r|$r.FISCAL_YEAR,
            price: r|$r.REVENUE
        ]),
        ~[price: x|$x->sum()]
    )
}
```

### How Option D works

1. Both detail and aggregate are Relation expressions with named columns
2. Columns present in **BOTH** = key dimensions (implicit matching by name)
3. Columns in detail but **NOT** in aggregate = fallback triggers
4. `~[measures]` declares which columns are pre-computed aggregations and what function was used
5. The optimizer picks the most specific view that covers the query's needs

**Signature**: `Relation<T>.aggregate(summary: Relation<S>, measures: ColSpec<...>[*]): Relation<T>`

**Return type** = detail's type (aggregate is transparent to downstream).

### Routing examples (optimizer behavior)
```
groupBy(desk) + sum(price)             → Year aggregate (desk + price→sum ✓)
groupBy(desk, month) + sum(price)      → Month aggregate (desk, month + price→sum ✓)
groupBy(desk, trader) + sum(price)     → Detail (no aggregate has trader)
project(desk, price) — no groupBy      → Detail (aggregates only for groupBy queries)
```

### Named function form
```pure
function model::wholesales(): Relation<...>[1] {
    model::wholesalesDetail()
      ->aggregate(model::wholesalesMonth(), ~[price: x|$x->sum()])
      ->aggregate(model::wholesalesYear(),  ~[price: x|$x->sum()])
}
```

### Multiple measures
```pure
->aggregate(model::summary(), ~[price: x|$x->sum(), discount: x|$x->avg()])
```

### canAggregate
Inferred from the measure function:
- `sum`, `count`, `max`, `min` — re-aggregatable (`sum(sum(x)) == sum(x)`)
- `avg`, `stddev` — NOT re-aggregatable (need sum+count column pairs)

### Design alternatives considered
- **Option B** (structured keys + measures): `~keys: ~[desk], ~measures: ~[price: ~TOTAL:x|$x->sum()]` — rejected (introduces `~col: ~col` syntax that doesn't exist elsewhere)
- **Option C** (predicate-based): `{s,t| $s.desk == $t.desk && $s.price->sum() == $t.price}` — rejected (verbose when names match; kept as possible escape hatch)

### Tests in legend-lite

**Not implemented (❌) — no existing tests.**

**Test strategy for E4 (AggregationAware):**
1. **Grammar test**: Parse `AggregationAware` class mapping with `~mainMapping` and `~aggregateViews`; verify AST extraction of detail mapping + aggregate views (keys, measures)
2. **Simple routing test**: Define detail table and monthly summary table. Run `groupBy(~month, ~price:x|$x->sum())` and verify SQL hits the summary table, not the detail table
3. **Fallback test**: Query with a column NOT in any aggregate view → verify it falls through to the detail (main) mapping
4. **canAggregate test**: Verify `avg()` on a pre-aggregated `sum` column is rejected (not re-aggregatable), while `sum()` on pre-aggregated `sum` column is accepted
5. **Multiple aggregate views**: Register two views (monthly, yearly). Verify the optimizer picks the most specific one matching the query's `groupBy` keys
6. **Integration test**: Full E2E with DuckDB — verify correct results from summary table vs. detail table

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| E4 AggregationAware | ❌🔧 | 1. Grammar addition for `AggregationAware` keyword (or Relation-native `->aggregate()`). 2. AST node to store aggregate view registrations. 3. Optimizer pass intercepting `groupBy()` calls. 4. Pattern matching: query keys/aggs vs view contract. 5. Query rewrite to substitute aggregate relation. |

**Priority**: Hard🔧. This is a query optimizer feature, not just parsing/compilation.

**Key files to modify**:
- Grammar: add `aggregate()` function or `AggregationAware` keyword
- Compiler/Checker: validate aggregate schema compatibility
- PlanGenerator or MappingResolver: query rewrite logic
