# Frequently Asked Questions

## SQL Generation & Optimization

### Q: Should `WHERE EXISTS` come before or after the `JOIN` in generated SQL?

**Short Answer:** It depends on the query semantics, but for Legend Lite's current implementation, **after the JOIN is correct** and often equivalent due to modern query optimizer capabilities.

---

### Detailed Explanation

Consider this Pure query that both filters AND projects through an association:

```pure
Person.all()
    ->filter({p | $p.addresses.city == 'New York'})
    ->project({p | $p.firstName}, {p | $p.addresses.street})
```

Legend Lite generates:

```sql
SELECT t0.FIRST_NAME AS firstName, j2.STREET AS street 
FROM T_PERSON AS t0 
LEFT OUTER JOIN T_ADDRESS AS j2 ON t0.ID = j2.PERSON_ID
WHERE EXISTS (
    SELECT 1 FROM T_ADDRESS AS sub1 
    WHERE sub1.PERSON_ID = t0.ID AND sub1.CITY = 'New York'
)
```

#### Why This Structure?

**1. The JOIN is required for projection**

The `LEFT OUTER JOIN` is necessary because we need to actually retrieve address data for the `SELECT` clause. Without it, we couldn't project `$p.addresses.street`.

**2. EXISTS is required to prevent filter row explosion**

Using `EXISTS` for the filter (instead of a regular `WHERE j2.CITY = 'New York'`) ensures we're filtering *people* who have a New York address, not filtering *person-address combinations*.

**3. They serve different purposes**

| Construct | Purpose | Rows Affected |
|-----------|---------|---------------|
| `LEFT JOIN` | Retrieve associated data for projection | Multiplies rows (one per address) |
| `EXISTS` | Check if *any* matching associated row exists | Filters base entity rows |

#### Alternative: EXISTS Before JOIN (Subquery Approach)

An alternative would be to filter first, then join:

```sql
SELECT t0.FIRST_NAME AS firstName, j2.STREET AS street 
FROM (
    SELECT * FROM T_PERSON AS t0
    WHERE EXISTS (
        SELECT 1 FROM T_ADDRESS AS sub1 
        WHERE sub1.PERSON_ID = t0.ID AND sub1.CITY = 'New York'
    )
) AS t0
LEFT OUTER JOIN T_ADDRESS AS j2 ON t0.ID = j2.PERSON_ID
```

**Pros:**
- Conceptually cleaner: "filter first, then enrich"
- Could be faster if EXISTS is highly selective (eliminates rows before join)

**Cons:**
- More complex SQL generation (nested subqueries)
- Harder to read and debug
- Modern optimizers typically rewrite to the same plan anyway

#### Why Modern Optimizers Make This Equivalent

Both DuckDB and SQLite (and PostgreSQL, MySQL, etc.) have sophisticated query optimizers that:

1. **Recognize filter pushdown opportunities** - They'll apply selective filters before expensive joins
2. **Flatten unnecessary subqueries** - Nested subqueries are often unnested
3. **Choose optimal join order** - Based on statistics, not SQL text order

So while the SQL *text* shows `JOIN` before `WHERE EXISTS`, the *execution plan* may process the EXISTS first if that's more efficient.

#### When Explicit Ordering Matters

Explicit subquery ordering might matter for:

1. **Very complex queries** where optimizer hints are needed
2. **Databases with less sophisticated optimizers**
3. **CTEs (WITH clauses)** that force materialization
4. **Debugging/explaining** query plans

#### Legend Lite's Design Choice

We chose the "flat" approach (`JOIN ... WHERE EXISTS`) because:

1. **Simpler code generation** - One pass through the AST
2. **Readable output** - Easier to understand and debug
3. **Optimizer trust** - Modern databases handle this well
4. **Flexibility** - Can add optimizer hints later if needed

---

### Q: Why use `EXISTS` instead of `INNER JOIN` for to-many filters?

Consider finding all people who have *any* address in New York:

```pure
Person.all()->filter({p | $p.addresses.city == 'New York'})
```

**Wrong approach (INNER JOIN):**
```sql
SELECT DISTINCT t0.* 
FROM T_PERSON t0 
INNER JOIN T_ADDRESS a ON t0.ID = a.PERSON_ID 
WHERE a.CITY = 'New York'
```

Problems:
- Requires `DISTINCT` to de-duplicate (expensive)
- Join processes ALL matching rows before deduplication
- Semantic confusion: are we querying people or person-address pairs?

**Correct approach (EXISTS):**
```sql
SELECT t0.* 
FROM T_PERSON t0 
WHERE EXISTS (
    SELECT 1 FROM T_ADDRESS a 
    WHERE a.PERSON_ID = t0.ID AND a.CITY = 'New York'
)
```

Benefits:
- No row explosion
- Database can "short-circuit" on first match
- Clear semantics: we're querying people
- No `DISTINCT` needed

---

### Q: Why use `LEFT OUTER JOIN` for to-many projections?

When projecting through a to-many association, we intentionally want row multiplication:

```pure
Person.all()->project({p | $p.firstName}, {p | $p.addresses.street})
```

```sql
SELECT t0.FIRST_NAME, j2.STREET 
FROM T_PERSON t0 
LEFT OUTER JOIN T_ADDRESS j2 ON t0.ID = j2.PERSON_ID
```

**Why LEFT (not INNER) JOIN?**
- Preserves people with no addresses (they'll have NULL for street)
- Matches Pure semantics: `[*]` multiplicity means "zero or more"

**Why JOIN (not EXISTS)?**
- We need the actual data, not just existence check
- Row multiplication is desired here (one row per address)

---

## Architecture & Design

### Q: Why not use ANTLR for parsing Pure?

We chose a hand-written recursive descent parser because:

1. **Simplicity** - No additional build-time dependencies or generated code
2. **GraalVM compatibility** - ANTLR runtime uses reflection
3. **Debugging** - Easier to step through and understand
4. **Subset focus** - We only need a small subset of Pure syntax

For a full Pure implementation, ANTLR would be preferred for maintainability.

### Q: Why records instead of classes?

Java records provide:
- Immutability by default
- Automatic `equals()`, `hashCode()`, `toString()`
- Pattern matching in switch expressions
- Concise syntax (less boilerplate)

This aligns with our functional programming approach.

### Q: Why sealed interfaces?

Sealed interfaces (`Type`, `RelationNode`, `Expression`) enable:
- Exhaustive pattern matching in switch expressions
- Compiler verification that all cases are handled
- Clear type hierarchies without runtime instanceof checks

---

## Results & Serialization

### Q: When should I use streaming vs buffered results?

Legend Lite supports two result modes for query execution:

| Mode | Use When | Memory | Connection |
|------|----------|--------|------------|
| **Buffered** (default) | Small results, need random access, re-iteration | O(rows × cols) | Closed after fetch |
| **Streaming** | Large results, single pass, memory-constrained | O(batch size) | Held during iteration |

**Buffered (default):**
```java
BufferedResult result = queryService.execute(pureSource, query, runtime);
// Random access, re-iteration OK
Object value = result.getValue(5, "name");
result.stream().forEach(...);  // Can iterate multiple times
```

**Streaming (large results):**
```java
try (Result result = queryService.execute(pureSource, query, runtime, ResultMode.STREAMING)) {
    result.stream()
        .limit(1000)  // Can stop early
        .forEach(row -> process(row));
}  // Resources released automatically
```

**Direct serialization (best for HTTP responses):**
```java
queryService.executeAndSerialize(pureSource, query, runtime, outputStream, "json");
```

---

### Q: How do I add a custom serializer (e.g., Arrow, Protobuf, Parquet)?

**Step 1: Implement `ResultSerializer`**

```java
public class ArrowSerializer implements ResultSerializer {

    public static final ArrowSerializer INSTANCE = new ArrowSerializer();

    @Override
    public String formatId() {
        return "arrow";
    }

    @Override
    public String contentType() {
        return "application/vnd.apache.arrow.stream";
    }

    @Override
    public boolean supportsStreaming() {
        return true;  // Can write incrementally
    }

    @Override
    public void serialize(BufferedResult result, OutputStream out) throws IOException {
        // Write Arrow IPC format from buffered data
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = buildSchema(result.columns(), allocator)) {
            // ... populate vectors and write
        }
    }

    @Override
    public void serializeStreaming(StreamingResult result, OutputStream out) throws IOException {
        // Write Arrow record batches as rows arrive
        // ... stream in batches of 1024 rows
    }
}
```

**Step 2: Register the serializer**

```java
// At application startup
SerializerRegistry.register(ArrowSerializer.INSTANCE);
```

Or via **ServiceLoader** for modular dependencies:
```
# META-INF/services/org.finos.legend.engine.serialization.ResultSerializer
com.mycompany.ArrowSerializer
```

**Step 3: Use it**

```java
queryService.executeAndSerialize(pureSource, query, runtime, out, "arrow");
```

---

### Q: What serializers are built-in?

| Format | Class | Streaming | Dependencies |
|--------|-------|-----------|--------------|
| **JSON** | `JsonSerializer` | ✅ Yes | None (hand-rolled) |
| **CSV** | `CsvSerializer` | ✅ Yes | None (hand-rolled) |

Built-in serializers have zero external dependencies and are fully GraalVM native-image compatible.

For Arrow, Protobuf, or Parquet, implement custom serializers in optional modules with explicit native-image configuration.

---

## Contributing

Have a question not answered here? Open an issue on GitHub!
