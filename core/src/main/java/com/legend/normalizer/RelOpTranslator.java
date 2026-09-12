package com.legend.normalizer;

import com.legend.builtin.DynaFn;
import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.TypeExpression;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LogicalOp;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Legacy {@link RelationalOperation} trees &rarr; Pure value expressions —
 * extracted from the MappingNormalizer god class (AUDIT_2026_07 §16 seam b).
 *
 * <p>Context: {@code tableScope} binds table names to row expressions;
 * {@code targetVarOrNull} binds {@code {target}.col} reads (join
 * conditions); {@code rowBindOrNull} is the pipeline row variable; the
 * {@link PipelineView} exposes what the surrounding join pipeline knows
 * (ambiguous tables; hoisted join slots for nested JoinNavigation).
 */
final class RelOpTranslator {

    private RelOpTranslator() {
    }

    /**
     * Relational dynafunctions whose PURE spelling differs from their DSL
     * name. Every other dynafunction passes through NAME-PRESERVING; the
     * entries here rewrite — the engine DSL exposes per-digest names
     * (md5/sha1/sha256, its SQL-ish surface) while real pure spells the
     * capability as ONE function + an enum: hash(text, HashType.X)
     * (core_functions_unclassified/hash/hash.pure). Future divergent
     * dynafunctions belong HERE, not in ad-hoc predicates.
     */
    /** The engine's hashing dynafunctions → upstream's {@code hash(String, HashType)}. */
    private static final Map<DynaFn, String> DYNA_HASH_TYPES =
            Map.of(DynaFn.MD5, "MD5", DynaFn.SHA1, "SHA1", DynaFn.SHA256, "SHA256");

    /** The registry entry of a mapping-expression call, or null when the engine
     *  registers no dynafunction of that name (a plain pure function call). */
    private static @com.legend.Nullable DynaFn dyna(RelationalOperation.FunctionCall call) {
        return DynaFn.of(call.name()).orElse(null);
    }

    /** What the surrounding pipeline exposes to expression translation. */
    interface PipelineView {
        Set<String> ambiguousTables();

        boolean hasSlots();

        /** Alias of the hoisted join step for {@code chain}; loud if absent. */
        @com.legend.Nullable String slotFor(List<JoinChainElement> chain);

        @com.legend.Nullable String targetTable(@com.legend.Nullable String alias);

        /** Whether the hoisted slot's target relation declares {@code column}
         * (unquoted-identifier case rules). */
        boolean targetHasColumn(@com.legend.Nullable String alias, String column);

        /** Outside any pipeline: nothing is ambiguous, no slots exist. */
        PipelineView NONE = new PipelineView() {
            @Override public boolean targetHasColumn(
                    @com.legend.Nullable String alias, String column) {
                return false;
            }
            @Override public Set<String> ambiguousTables() {
                return Set.of();
            }
            @Override public boolean hasSlots() {
                return false;
            }
            @Override public String slotFor(List<JoinChainElement> chain) {
                throw new IllegalStateException("no pipeline slots in this context");
            }
            @Override public @com.legend.Nullable String targetTable(
                    @com.legend.Nullable String alias) {
                return null;
            }
        };
    }

    static void collectTablesIn(RelationalOperation op, Set<String> sink) {
        switch (op) {
            case RelationalOperation.ColumnRef cr            -> sink.add(cr.table());
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored         -> { }
            case RelationalOperation.FunctionCall fc         -> fc.args().forEach(a -> collectTablesIn(a, sink));
            case RelationalOperation.Comparison c            -> { collectTablesIn(c.left(), sink); collectTablesIn(c.right(), sink); }
            case RelationalOperation.BooleanOp b             -> { collectTablesIn(b.left(), sink); collectTablesIn(b.right(), sink); }
            case RelationalOperation.IsNull n                -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.IsNotNull n             -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.Group g                 -> collectTablesIn(g.inner(), sink);
            case RelationalOperation.ArrayLiteral a          -> a.elements().forEach(e -> collectTablesIn(e, sink));
            case RelationalOperation.Lambda lam              -> collectTablesIn(lam.body(), sink);
            case RelationalOperation.LambdaParam ignored     -> { }
            case RelationalOperation.JoinNavigation ignored  -> throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "JoinNavigation inside expression");
        }
    }

    static ValueSpecification columnRead(String table, String column,
                                                Map<String, ValueSpecification> tableScope,
                                                String defaultTable, PipelineView pipeline) {
        ValueSpecification base = tableScope.get(table);
        if (base == null && pipeline.ambiguousTables().contains(table)) {
            throw ambiguousTableRef(table, column);
        }
        if (base == null) base = tableScope.get(defaultTable);
        if (base == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "No row variable in scope for table '" + table
                  + "'; available=" + tableScope.keySet());
        }
        return new AppliedProperty(base, column);
    }

    /**
     * DYNA-WIDE MULTIPLICITY CONVENTION: inside a dynafunction call's
     * ARGUMENTS, a column reference is an SQL scalar — nullable and
     * null-propagating — so an optional column read conforms by EMISSION
     * (toOne; erasure at lowering). Without this, one [0..1] column
     * infects the whole arithmetic chain with [*] and scalar signatures
     * (substring, minus, ...) reject the synthesized mapping body.
     * Comparisons/logical ops do NOT route here — their optionality
     * semantics are their own.
     */
    private static List<ValueSpecification> translateArgs(
            RelationalOperation.FunctionCall call,
            Map<String, ValueSpecification> tableScope,
            @com.legend.Nullable ValueSpecification targetVarOrNull, @com.legend.Nullable Variable rowBindOrNull,
            PipelineView pipeline) {
        return call.args().stream()
                .map(a -> {
                    ValueSpecification t = translate(a, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    // JOIN CONDITIONS (a target var is bound) stay VERBATIM
                    // — the engine preserves them and join-key extraction
                    // reads bare column shapes; only PROPERTY expressions
                    // get the scalar conform.
                    return targetVarOrNull == null
                            && a instanceof RelationalOperation.ColumnRef
                            ? new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(t)) : t;
                })
                .toList();
    }

    /** {@code cast(v, @String)} — the SQL VARCHAR coercion emission. */
    /** The dyna lane's to-one trust wrap (SQL null-propagates; the
     * lowering's erasure makes toOne free) — ONE spelling for every
     * dyna emission whose pure counterpart is strict [1]. */
    private static ValueSpecification toOne(ValueSpecification v) {
        return new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(v));
    }

    private static List<ValueSpecification> toOneAll(List<ValueSpecification> vs) {
        // COLLECTION operands stay bare (an in-list is genuinely many —
        // toOne over it would be the very stamp lie the invariant
        // catches; the corpus 'in'-prefix family proved it)
        return vs.stream().map(v -> v instanceof
                        com.legend.protocol.spec.PureCollection ? v : toOne(v))
                .toList();
    }

    private static ValueSpecification strCast(ValueSpecification v) {
        return new AppliedFunction("cast", List.of(v,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef("String"))));
    }

    /** The SQL type name in {@code extractFromSemiStructured}'s third
     *  argument, as the pure primitive it extracts to (the engine's own
     *  tests spell 'VARCHAR'/'INTEGER'/...). */
    private static String pureTypeFor(String sqlType) {
        return switch (sqlType.toUpperCase(java.util.Locale.ROOT)) {
            case "VARCHAR", "CHAR" -> "String";
            case "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT" -> "Integer";
            case "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC" -> "Float";
            case "BOOLEAN", "BIT" -> "Boolean";
            case "DATE" -> "StrictDate";
            case "TIMESTAMP" -> "DateTime";
            default -> throw new ModelException(
                    LegendCompileException.Phase.NORMALIZE,
                    "Unsupported SQL type '" + sqlType
                            + "' in extractFromSemiStructured");
        };
    }

    private static ModelException ambiguousTableRef(String table, String column) {
        return new ModelException(LegendCompileException.Phase.NORMALIZE, 
                "Ambiguous column reference '" + table + "." + column + "': the join "
              + "chain reaches table '" + table + "' through more than one path, so a "
              + "bare column reference cannot identify which sub-row is meant. Pin the "
              + "intended sub-row with a join-terminal column (e.g. @SomeJoin | "
              + table + "." + column + ").");
    }

    static ValueSpecification translate(RelationalOperation op,
                                                    Map<String, ValueSpecification> tableScope,
                                                    @com.legend.Nullable ValueSpecification targetVarOrNull,
                                                    @com.legend.Nullable Variable rowBindOrNull,
                                                    PipelineView pipeline) {
        return switch (op) {
            case RelationalOperation.ColumnRef ref -> {
                // scope registers canonical names (MappingNormalizer owns
                // the default-schema spelling rule)
                String refTable = MappingNormalizer.canonicalTable(ref.table());
                ValueSpecification path = tableScope.get(refTable);
                if (path == null && pipeline.ambiguousTables().contains(refTable)) {
                    throw ambiguousTableRef(refTable, ref.column());
                }
                if (path == null) {
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                            "ColumnRef references table '" + refTable
                          + "' not in scope; available=" + tableScope.keySet());
                }
                yield new AppliedProperty(path, ref.column());
            }
            case RelationalOperation.TargetColumnRef tref -> {
                if (targetVarOrNull == null) {
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                            "TargetColumnRef {target}." + tref.column()
                          + " outside a join condition context");
                }
                yield new AppliedProperty(targetVarOrNull, tref.column());
            }
            case RelationalOperation.Literal lit -> literalToValueSpec(lit.value());
            case RelationalOperation.FunctionCall call
                    when dyna(call) instanceof DynaFn hashed && DYNA_HASH_TYPES.containsKey(hashed) ->
                    // SQL-lane operand: optional column reads null-
                    // propagate; hash(String[1], ...) is strict — the
                    // 'position' toOne idiom
                    new AppliedFunction("hash", List.of(
                            toOne(translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)),
                            new EnumValue("meta::pure::functions::hash::HashType",
                                    java.util.Objects.requireNonNull(DYNA_HASH_TYPES.get(hashed)))));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.DAY_OF_WEEK && call.args().size() == 1 ->
                    // the DYNA returns the day NAME (a string); pure's
                    // dayOfWeek returns the enum — toString is the name
                    new AppliedFunction("toString", List.of(new AppliedFunction(
                            "dayOfWeek", translateArgs(call, tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline))));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.DAY_OF_WEEK_NUMBER
                    && (call.args().size() == 1 || call.args().size() == 2) -> {
                    // Engine H2: 1-arg = DAY_OF_WEEK (SUNDAY=1); the 2-arg
                    // form fixes the week start — 'Monday' emits
                    // ISO_DAY_OF_WEEK, 'Sunday' emits DAY_OF_WEEK, anything
                    // else asserts (dayOfWeekNumberH2). The pure native
                    // lowers to isodow (Monday=1), so Sunday-based forms
                    // conform by emission: mod(isodow, 7) + 1.
                    String weekStart = "Sunday";
                    if (call.args().size() == 2) {
                        if (!(call.args().get(1) instanceof RelationalOperation.Literal lit)
                                || !(lit.value() instanceof String ws)
                                || !(ws.equalsIgnoreCase("Monday")
                                        || ws.equalsIgnoreCase("Sunday"))) {
                            throw new NotImplementedException(
                                    "dayOfWeekNumber requires 'Sunday' or"
                                  + " 'Monday' as the week start (engine assert)");
                        }
                        weekStart = (String) lit.value();
                    }
                    // SQL-lane operand (the 'position' toOne idiom):
                    // dayOfWeekNumber's real signature is strict Date[1]
                    ValueSpecification iso = new AppliedFunction("dayOfWeekNumber",
                            List.of(toOne(translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline))));
                    yield weekStart.equalsIgnoreCase("Monday")
                            ? iso
                            : AppliedFunction.infixRun("plus", List.of(
                                    new AppliedFunction("mod", List.of(iso,
                                            new CInteger(7L))),
                                    new CInteger(1L)));
            }
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.ADJUST && call.args().size() == 3
                    && call.args().get(2) instanceof RelationalOperation.Literal ul
                    && ul.value() instanceof String unit -> {
                    // the dyna spells the DurationUnit as a string literal;
                    // operands are SQL-lane (the 'position' toOne idiom)
                    yield new AppliedFunction("adjust", List.of(
                            toOne(translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)),
                            toOne(translate(call.args().get(1), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)),
                            new EnumValue("meta::pure::functions::date::DurationUnit",
                                    unit.toUpperCase())));
            }
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.CONVERT_TIME_ZONE && call.args().size() == 3 ->
                    new AppliedFunction(Pure.Lite.CONVERT_TIME_ZONE_FORMAT, translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline));
            // FORMAT dynafunctions: parseDate/convertDate/convertDateTime/
            // toTimestamp with a format string route to the lite natives
            // (strptime with translated tokens at lowering); convertDate
            // without a format is the ISO spelling; convertVarchar128 is
            // the VARCHAR coercion.
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.PARSE_DATE && call.args().size() == 2 ->
                    new AppliedFunction(Pure.Lite.PARSE_DATE_FORMAT, translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.CONVERT_DATE && call.args().size() <= 2 -> {
                    List<ValueSpecification> as = translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    yield new AppliedFunction(Pure.Lite.CONVERT_DATE_FORMAT,
                            as.size() == 2 ? as
                                    : List.of(as.get(0), new CString("yyyy-MM-dd")));
            }
            case RelationalOperation.FunctionCall call
                    when (dyna(call) == DynaFn.CONVERT_DATE_TIME
                            || dyna(call) == DynaFn.TO_TIMESTAMP)
                    && call.args().size() == 2 ->
                    new AppliedFunction(Pure.Lite.CONVERT_DATE_TIME_FORMAT, translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.CONVERT_VARCHAR128 && call.args().size() == 1 ->
                    strCast(translate(call.args().get(0), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.SPLIT_PART && call.args().size() == 3 -> {
                    // the DYNAFUNCTION accepts a string-typed part index (the
                    // corpus maps VARCHAR columns); pure splitPart requires
                    // Integer — conform by EMISSION: cast(@Integer) is a
                    // no-op on integer columns, CAST AS BIGINT on text
                    ValueSpecification a0 = translate(call.args().get(0), tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    ValueSpecification a1 = translate(call.args().get(1), tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    ValueSpecification a2 = translate(call.args().get(2), tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    // token/part are strict [1] in pure's signature; a
                    // column operand is SQL-lane null-propagating — the
                    // 'position' toOne idiom (a0 stays bare: pure's own
                    // first param is [0..1])
                    yield new AppliedFunction("splitPart", List.of(a0,
                            toOne(a1),
                            new AppliedFunction("cast", List.of(toOne(a2),
                                    new TypeAnnotation.Named(
                                            new TypeExpression.NameRef("Integer"))))));
            }
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.CASE && call.args().size() >= 3
                            && call.args().size() % 2 == 1 -> {
                    // the relational 'case' dynafunction:
                    // case(c1, v1 [, c2, v2 ...], default) — pure spells it
                    // as nested if(cond, {|then}, {|else})
                    ValueSpecification tail = translate(
                            call.args().get(call.args().size() - 1),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline);
                    for (int i = call.args().size() - 3; i >= 0; i -= 2) {
                        ValueSpecification cond = translate(call.args().get(i),
                                tableScope, targetVarOrNull, rowBindOrNull, pipeline);
                        ValueSpecification then = translate(call.args().get(i + 1),
                                tableScope, targetVarOrNull, rowBindOrNull, pipeline);
                        tail = new AppliedFunction("if", List.of(cond,
                                new LambdaFunction(List.of(), List.of(then)),
                                new LambdaFunction(List.of(), List.of(tail))));
                    }
                    yield tail;
            }
            default -> translateTail(op, tableScope, targetVarOrNull,
                    rowBindOrNull, pipeline);
        };
    }

    /** Arm group 2 of the relational-op dispatch (sequential order
     * preserved — the split is at an arm boundary). */
    private static ValueSpecification translateTail(RelationalOperation op,
            Map<String, ValueSpecification> tableScope,
            @com.legend.Nullable ValueSpecification targetVarOrNull,
            @com.legend.Nullable Variable rowBindOrNull,
            PipelineView pipeline) {
        return switch (op) {
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.CONCAT && call.args().size() >= 2 -> {
                    // The variadic 'concat' dynafunction has NO pure-function
                    // counterpart (engine renders it per-dialect straight to
                    // SQL); real pure spells string concatenation with plus.
                    // SQL concat COERCES its arguments — each wraps in
                    // cast(@String), whose lowering is the SQL VARCHAR cast
                    // (the DATABASE's own formatting: '2014-01-01 06:30:00',
                    // not pure toString's ISO form — audit), a no-op for
                    // strings.
                    // ONE run: string::plus(String[*]) — the engine's n-ary
                    // carrier, exactly what the parser spells for a + b + c
                    List<ValueSpecification> parts = new ArrayList<>(call.args().size());
                    for (RelationalOperation part : call.args()) {
                        parts.add(toOne(strCast(translate(part, tableScope,
                                targetVarOrNull, rowBindOrNull, pipeline))));
                    }
                    yield parts.size() == 1 ? parts.get(0)
                            : AppliedFunction.infixRun("plus", parts);
            }
            // extractFromSemiStructured(col, 'path', 'SQLTYPE'): the ENGINE's
            // semistructured scalar extraction (core_relational grammar —
            // probe json-get-spelling) — real pure's to(get(col, 'path'),
            // @Type) (the same emission the JSON-source synthesizer uses:
            // text-extraction then cast).
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.EXTRACT_FROM_SEMI_STRUCTURED
                    && call.args().size() == 3
                    && call.args().get(2) instanceof RelationalOperation.Literal lit
                    && lit.value() instanceof String sqlType ->
                    new AppliedFunction("to", List.of(
                            new AppliedFunction("get", List.of(
                                    translate(call.args().get(0), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline),
                                    translate(call.args().get(1), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline))),
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef(pureTypeFor(sqlType)))));
            // Dynafunction spellings with no same-named pure native:
            // isNull/isNotNull ARE isEmpty/isNotEmpty on [0..1] values;
            // group(x) is parenthesization; if's branches must be THUNKS
            // (real pure's if takes zero-param lambdas — the dynafunction
            // spelling passes plain expressions).
            // The 'indexOf' DYNAFUNCTION has SQL locate() semantics —
            // 1-BASED position, and since C1.5c the pure-relational
            // indexOf rule IS 1-based locate (engine parity), so the
            // dynafunction forwards VERBATIM (the old +1 emission paired
            // with the 0-based rule; both sides dropped together).
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.INDEX_OF && call.args().size() == 2 ->
                    new AppliedFunction("indexOf", List.of(
                            translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline),
                            translate(call.args().get(1), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)));
            // dyna 'substring' is SQL SUBSTRING (1-based start, LENGTH
            // third arg) and the RELATIONAL substring rule is now a
            // VERBATIM passthrough with the H2 sub-1-start clamp — args
            // forward unchanged (the old pure-semantics pre-shift paired
            // with the lowering's re-shift; both sides dropped together).
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.SUBSTRING
                    && (call.args().size() == 2 || call.args().size() == 3) ->
                new AppliedFunction("substring", translateArgs(call,
                        tableScope, targetVarOrNull, rowBindOrNull, pipeline));
            // dyna 'add'/'sub' are SQL ARITHMETIC — pure spells them
            // plus/minus; the bare names would hit pure's COLLECTION
            // add(T[*],T[1]) and type [*]
            // The dyna lane is SQL: optional column reads flow into the
            // arithmetic and null-propagate. Pure's plus/minus are [1]
            // (the strict kernel now enforces real MultiplicityMatch), so
            // the translated operands wrap in toOne — the 'position'
            // idiom below, now uniform across the dyna emissions.
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.ADD && call.args().size() == 2 ->
                    AppliedFunction.infixRun("plus", toOneAll(translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull,
                            pipeline)));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.SUB && call.args().size() == 2 ->
                    AppliedFunction.infixRun("minus", toOneAll(translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull,
                            pipeline)));
            // SQL POSITION(needle, haystack) — 1-based, arguments REVERSED
            // vs pure's indexOf(haystack, needle); forwards verbatim like
            // the indexOf dynafunction above (C1.5c made the rule 1-based)
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.POSITION && call.args().size() == 2 ->
                    // toOne on the haystack: an OPTIONAL column read would
                    // otherwise infect the whole arithmetic chain with [*]
                    // (SQL null-propagates; erasure makes toOne free)
                    new AppliedFunction("indexOf", List.of(
                            new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                                    translate(call.args().get(1), tableScope,
                                            targetVarOrNull, rowBindOrNull,
                                            pipeline))),
                            translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.IS_NULL && call.args().size() == 1 ->
                    new AppliedFunction("isEmpty", List.of(translate(call.args().get(0),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline)));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.IS_NOT_NULL && call.args().size() == 1 ->
                    new AppliedFunction("isNotEmpty", List.of(translate(call.args().get(0),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline)));
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.GROUP && call.args().size() == 1 ->
                    translate(call.args().get(0), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline);
            case RelationalOperation.FunctionCall call
                    when dyna(call) == DynaFn.IF && call.args().size() == 3 ->
                    new AppliedFunction("if", List.of(
                            translate(call.args().get(0), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            new LambdaFunction(List.of(), List.of(
                                    translate(call.args().get(1), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline))),
                            new LambdaFunction(List.of(), List.of(
                                    translate(call.args().get(2), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline)))));
            case RelationalOperation.FunctionCall call
                    when (dyna(call) == DynaFn.OR || dyna(call) == DynaFn.AND)
                    && call.args().size() > 2 -> {
                // The or/and DYNAs are VARIADIC (corpus testMerge.pure:121
                // or(4 disjuncts)); real pure boolean::or/and are binary —
                // conform by emission: fold left into nested binary calls.
                ValueSpecification acc = translate(call.args().get(0),
                        tableScope, targetVarOrNull, rowBindOrNull, pipeline);
                for (int i = 1; i < call.args().size(); i++) {
                    acc = new AppliedFunction(call.name(), List.of(acc,
                            translate(call.args().get(i), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)));
                }
                yield acc;
            }
            // Wire-vocabulary passthrough — the DATA BOUNDARY: a name in
            // the engine's dynaFn vocabulary that types against a
            // lite-internal shim is respelled to its exact identity HERE
            // (wireEmissionName); real pure names pass through and
            // resolve in the user namespace. Operands are SQL-LANE:
            // optional column reads null-propagate, while most pure
            // counterparts are strict [1] (the kernel enforces real
            // MultiplicityMatch since audit slice 2) — every arg wraps
            // in the 'position' toOne idiom (typing-level trust; the
            // lowering's erasure keeps SQL identical, and [1] args are
            // unaffected).
            case RelationalOperation.FunctionCall call -> operatorCall(
                    dynaFnName(call),
                    toOneAll(translateArgs(call, tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline)));
            case RelationalOperation.Comparison cmp -> {
                // a RELATIONAL comparison over a column has SQL null
                // semantics BY DEFINITION — conform by EMISSION (toOne;
                // erasure at lowering) so the [1] overload applies and no
                // [0..1] guard conjunct spells (engine mapping-~filter /
                // view-filter SQL is bare; the guards belong to USER pure
                // filters only). Join conditions stay verbatim.
                java.util.function.Function<RelationalOperation,
                        ValueSpecification> side = o -> {
                    ValueSpecification t = translate(o, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    return targetVarOrNull == null
                            && o instanceof RelationalOperation.ColumnRef
                            ? new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(t)) : t;
                };
                AppliedFunction c = new AppliedFunction(
                        comparisonFn(cmp.op()),
                        List.of(side.apply(cmp.left()),
                                side.apply(cmp.right())));
                // NEQ emits not(equal(...)) — real pure has no notEqual.
                yield cmp.op() == ComparisonOp.NEQ
                        ? new AppliedFunction("not", List.of(c)) : c;
            }
            case RelationalOperation.BooleanOp bo -> new AppliedFunction(
                    bo.op() == LogicalOp.AND ? "and" : "or",
                    List.of(translate(bo.left(),  tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            translate(bo.right(), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline)));
            // Relational 'is (not) null' translates to REAL pure's
            // isEmpty/isNotEmpty — identical semantics on [0..1] values
            // (the lite isNull/isNotNull natives are gone).
            case RelationalOperation.IsNull n -> new AppliedFunction("isEmpty",
                    List.of(translate(n.operand(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline)));
            case RelationalOperation.IsNotNull n -> new AppliedFunction("isNotEmpty",
                    List.of(translate(n.operand(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline)));
            case RelationalOperation.Group g ->
                    translate(g.inner(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline);
            case RelationalOperation.ArrayLiteral arr -> new PureCollection(
                    arr.elements().stream()
                            .map(e -> translate(e, tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline))
                            .toList());
            case RelationalOperation.JoinNavigation jn -> joinNavigation(jn,
                    tableScope, targetVarOrNull, rowBindOrNull, pipeline);
            // group-1 types never reach here (their arms matched above);
            // javac still needs coverage over the sealed hierarchy
            default -> throw new IllegalStateException(
                    "relational-op dispatch: unexpected " + op.getClass());
        };
    }

    /** A hoisted join chain: the pipeline carries it as a join(~alias, ...)
     * step whose sub-row is $row.&lt;alias&gt;; the terminal (if any) reads
     * from that sub-row's table scope. */
    private static ValueSpecification joinNavigation(
            RelationalOperation.JoinNavigation jn,
            Map<String, ValueSpecification> tableScope,
            @com.legend.Nullable ValueSpecification targetVarOrNull,
            @com.legend.Nullable Variable rowBindOrNull,
            PipelineView pipeline) {
        if (rowBindOrNull == null || !pipeline.hasSlots()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Nested JoinNavigation in scope without pipeline; "
                  + "JoinNav inside association predicates or join "
                  + "conditions is not supported.");
        }
        String alias = java.util.Objects.requireNonNull(pipeline.slotFor(jn.chain()),
                "no pipeline slot for join chain");
        ValueSpecification subRow = new AppliedProperty(rowBindOrNull, alias);
        if (jn.terminal() == null) {
            return subRow;
        }
        String terminalTable = pipeline.targetTable(alias);
        Map<String, ValueSpecification> innerScope = new LinkedHashMap<>(tableScope);
        if (terminalTable != null) {
            innerScope.put(terminalTable, subRow);
        }
        // ENGINE (pureToSQLQuery.pure resolveJoinElement): the terminal's
        // columns are re-resolved in the JOINED cursor — reprocessAliases
        // (OldAliasToNewAlias(tac.alias -> op.alias)) for a plain column,
        // the extracted columns for a DynaFunction. The spelled table is
        // grammar: `| firmTable.ADDRESSID` after a chain ending on
        // personTable reads personTable's ADDRESSID (golden:
        // "persontable_0".ADDRESSID exported from the isolated subselect).
        // Resolving it by its spelled name read the ROOT row and dropped
        // the chain's fan-out (testIsolatioWhereNoConstaintsAndInnerJoin).
        // A column the chain end does NOT declare stays where it is
        // spelled (TestMappingWithViewJoins: `| firmTable.LEGALNAME` after
        // a hop onto a view without LEGALNAME reads the root — the
        // engine's extracted-column projection finds it there). Batch 62.
        RelationalOperation terminal = terminalTable == null
                ? jn.terminal()
                : rebaseToTable(jn.terminal(), terminalTable,
                        c -> !c.table().equals(terminalTable)
                                && pipeline.targetHasColumn(alias, c.column()));
        return translate(terminal, innerScope, targetVarOrNull, rowBindOrNull,
                pipeline);
    }

    /** Every column reference inside a join chain's terminal re-aliased to
     * the chain-end table (the engine's reprocessAliases); nested join
     * navigations keep their own chains. */
    private static RelationalOperation rebaseToTable(RelationalOperation t,
            String table,
            java.util.function.Predicate<RelationalOperation.ColumnRef> moves) {
        return switch (t) {
            case RelationalOperation.ColumnRef cr -> moves.test(cr)
                    ? new RelationalOperation.ColumnRef(cr.databaseName(), table,
                            cr.column())
                    : cr;
            case RelationalOperation.JoinNavigation nested -> nested;
            default -> t.mapChildren(x -> rebaseToTable(x, table, moves));
        };
    }

    static ValueSpecification literalToValueSpec(Object value) {
        if (value instanceof String s)  return new CString(s);
        if (value instanceof Long l)    return new CInteger(l);
        if (value instanceof Integer i) return new CInteger((long) i);
        if (value instanceof Double d)  return new CFloat(d);
        if (value instanceof Boolean b) return new CBoolean(b);
        throw new ModelException(LegendCompileException.Phase.NORMALIZE, "Unsupported literal type: "
                + (value == null ? "null" : value.getClass().getName()));
    }

    static String comparisonFn(ComparisonOp op) {
        // ORDERING ops route the Any-typed Lite shims: the engine never
        // type-checks DynaFunc condition operands (untyped Literal), so
        // a Date column vs a quoted string literal must not die in
        // pure's same-family overload table (ledger cluster 18;
        // EQ/NEQ already route the Any-typed equal).
        return switch (op) {
            case EQ  -> "equal";
            case NEQ -> "equal";   // wrapped in not(...) at the call site
            case LT  -> com.legend.builtin.Pure.Lite.LESS_THAN_ANY;
            case LTE -> com.legend.builtin.Pure.Lite.LESS_THAN_EQUAL_ANY;
            case GT  -> com.legend.builtin.Pure.Lite.GREATER_THAN_ANY;
            case GTE -> com.legend.builtin.Pure.Lite.GREATER_THAN_EQUAL_ANY;
        };
    }

    /** The pure spelling of an engine dynafunction the arms above did not
     *  rewrite — THE ONE lookup (DynaFn): a PURE operator passes through under
     *  its own name (the catalog's overloads decide), a SHIM takes its Lite
     *  identity, an UNSUPPORTED one fails loud naming the operator; a name the
     *  engine registers as no dynafunction is a plain pure function the
     *  expression calls, under its own name (no respelling: the 2026-09-11
     *  audit deleted the bare-name respelling table). */
    static String dynaFnName(RelationalOperation.FunctionCall call) {
        DynaFn d = dyna(call);
        if (d == null) {
            return call.name();
        }
        return switch (d.resolution()) {
            case PURE -> call.name();
            case SHIM -> d.liteFqn();
            case TRANSLATED -> throw new IllegalStateException("dynafunction '" + call.name()
                    + "' is declared TRANSLATED but no translator arm rewrote it");
            case UNSUPPORTED -> throw new com.legend.error.NotImplementedException(
                    "engine dynafunction '" + call.name() + "' is not supported by this platform yet"
                    + " (DynaFn." + d.name() + ", registered by " + d.dialects() + ")");
        };
    }

    /** The engine's dynaFn call {@code plus(a, b)} names a VARIADIC pure
     *  native ({@code Pure.isVariadicRun}): the arguments ARE its one
     *  collection — the parser's n-ary carrier. */
    static List<ValueSpecification> variadicRun(String fn, List<ValueSpecification> args) {
        return args.size() >= 2 && Pure.isVariadicRun(fn)
                ? List.of(new PureCollection(args)) : args;
    }

    /** The engine's arithmetic dynaFn IS the infix operator: its call is
     *  spelled exactly as the parser spells {@code a + b}. */
    private static AppliedFunction operatorCall(String fn, List<ValueSpecification> args) {
        List<ValueSpecification> shaped = variadicRun(fn, args);
        return shaped == args ? new AppliedFunction(fn, args) : AppliedFunction.infixRun(fn, args);
    }
}
