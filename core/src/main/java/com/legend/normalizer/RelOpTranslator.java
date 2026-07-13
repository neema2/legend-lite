package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.element.ComparisonOp;
import com.legend.parser.element.LogicalOp;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.LinkedHashMap;
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
    private static final Map<String, String> DYNA_HASH_TYPES =
            Map.of("md5", "MD5", "sha1", "SHA1", "sha256", "SHA256");

    /** What the surrounding pipeline exposes to expression translation. */
    interface PipelineView {
        Set<String> ambiguousTables();

        boolean hasSlots();

        /** Alias of the hoisted join step for {@code chain}; loud if absent. */
        String slotFor(List<JoinChainElement> chain);

        String targetTable(String alias);

        /** Outside any pipeline: nothing is ambiguous, no slots exist. */
        PipelineView NONE = new PipelineView() {
            @Override public Set<String> ambiguousTables() {
                return Set.of();
            }
            @Override public boolean hasSlots() {
                return false;
            }
            @Override public String slotFor(List<JoinChainElement> chain) {
                throw new IllegalStateException("no pipeline slots in this context");
            }
            @Override public String targetTable(String alias) {
                return null;
            }
        };
    }

    static void collectTablesIn(RelationalOperation op, Set<String> sink) {
        switch (op) {
            case RelationalOperation.ColumnRef cr            -> sink.add(cr.table());
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored         -> { }
            case RelationalOperation.TypeRef ignored          -> { }
            case RelationalOperation.FunctionCall fc         -> fc.args().forEach(a -> collectTablesIn(a, sink));
            case RelationalOperation.Comparison c            -> { collectTablesIn(c.left(), sink); collectTablesIn(c.right(), sink); }
            case RelationalOperation.BooleanOp b             -> { collectTablesIn(b.left(), sink); collectTablesIn(b.right(), sink); }
            case RelationalOperation.IsNull n                -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.IsNotNull n             -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.Group g                 -> collectTablesIn(g.inner(), sink);
            case RelationalOperation.ArrayLiteral a          -> a.elements().forEach(e -> collectTablesIn(e, sink));
            case RelationalOperation.JoinNavigation ignored  -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
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
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "No row variable in scope for table '" + table
                  + "'; available=" + tableScope.keySet());
        }
        return new AppliedProperty(base, column);
    }

    private static List<ValueSpecification> translateArgs(
            RelationalOperation.FunctionCall call,
            Map<String, ValueSpecification> tableScope,
            ValueSpecification targetVarOrNull, Variable rowBindOrNull,
            PipelineView pipeline) {
        return call.args().stream()
                .map(a -> translate(a, tableScope, targetVarOrNull, rowBindOrNull, pipeline))
                .toList();
    }

    /** {@code cast(v, @String)} — the SQL VARCHAR coercion emission. */
    private static ValueSpecification strCast(ValueSpecification v) {
        return new AppliedFunction("cast", List.of(v,
                new com.legend.parser.spec.TypeAnnotation.Named(
                        new com.legend.parser.TypeExpression.NameRef("String"))));
    }

    private static com.legend.error.ModelException ambiguousTableRef(String table, String column) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "Ambiguous column reference '" + table + "." + column + "': the join "
              + "chain reaches table '" + table + "' through more than one path, so a "
              + "bare column reference cannot identify which sub-row is meant. Pin the "
              + "intended sub-row with a join-terminal column (e.g. @SomeJoin | "
              + table + "." + column + ").");
    }

    static ValueSpecification translate(RelationalOperation op,
                                                    Map<String, ValueSpecification> tableScope,
                                                    ValueSpecification targetVarOrNull,
                                                    Variable rowBindOrNull,
                                                    PipelineView pipeline) {
        return switch (op) {
            case RelationalOperation.ColumnRef ref -> {
                // 'default.T' and 'T' are the same table — the default-schema
                // prefix is spelling, not identity (scope registers canonical)
                String refTable = ref.table().startsWith("default.")
                        ? ref.table().substring("default.".length()) : ref.table();
                ValueSpecification path = tableScope.get(refTable);
                if (path == null && pipeline.ambiguousTables().contains(refTable)) {
                    throw ambiguousTableRef(refTable, ref.column());
                }
                if (path == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "ColumnRef references table '" + refTable
                          + "' not in scope; available=" + tableScope.keySet());
                }
                yield new AppliedProperty(path, ref.column());
            }
            case RelationalOperation.TargetColumnRef tref -> {
                if (targetVarOrNull == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "TargetColumnRef {target}." + tref.column()
                          + " outside a join condition context");
                }
                yield new AppliedProperty(targetVarOrNull, tref.column());
            }
            case RelationalOperation.Literal lit -> literalToValueSpec(lit.value());
            case RelationalOperation.FunctionCall call
                    when DYNA_HASH_TYPES.containsKey(call.name()) ->
                    new AppliedFunction("hash", List.of(
                            translate(call.args().get(0), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            new EnumValue("meta::pure::functions::hash::HashType",
                                    DYNA_HASH_TYPES.get(call.name()))));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("dayOfWeek") && call.args().size() == 1 ->
                    // the DYNA returns the day NAME (a string); pure's
                    // dayOfWeek returns the enum — toString is the name
                    new AppliedFunction("toString", List.of(new AppliedFunction(
                            "dayOfWeek", translateArgs(call, tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline))));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("isNumeric") && call.args().size() == 1 ->
                    new AppliedFunction("isNumeric", translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("dayOfWeekNumber")
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
                            throw new com.legend.error.NotImplementedException(
                                    "dayOfWeekNumber requires 'Sunday' or"
                                  + " 'Monday' as the week start (engine assert)");
                        }
                        weekStart = (String) lit.value();
                    }
                    ValueSpecification iso = new AppliedFunction("dayOfWeekNumber",
                            List.of(translate(call.args().get(0), tableScope,
                                    targetVarOrNull, rowBindOrNull, pipeline)));
                    yield weekStart.equalsIgnoreCase("Monday")
                            ? iso
                            : new AppliedFunction("plus", List.of(
                                    new AppliedFunction("mod", List.of(iso,
                                            new CInteger(7L))),
                                    new CInteger(1L)));
            }
            case RelationalOperation.FunctionCall call
                    when call.name().equals("adjust") && call.args().size() == 3
                    && call.args().get(2) instanceof RelationalOperation.Literal ul
                    && ul.value() instanceof String unit -> {
                    // the dyna spells the DurationUnit as a string literal
                    yield new AppliedFunction("adjust", List.of(
                            translate(call.args().get(0), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            translate(call.args().get(1), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            new EnumValue("meta::pure::functions::date::DurationUnit",
                                    unit.toUpperCase())));
            }
            case RelationalOperation.FunctionCall call
                    when call.name().equals("convertTimeZone") && call.args().size() == 3 ->
                    new AppliedFunction("convertTimeZoneFormat", translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline));
            // FORMAT dynafunctions: parseDate/convertDate/convertDateTime/
            // toTimestamp with a format string route to the lite natives
            // (strptime with translated tokens at lowering); convertDate
            // without a format is the ISO spelling; convertVarchar128 is
            // the VARCHAR coercion.
            case RelationalOperation.FunctionCall call
                    when call.name().equals("parseDate") && call.args().size() == 2 ->
                    new AppliedFunction("parseDateFormat", translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("convertDate") && call.args().size() <= 2 -> {
                    List<ValueSpecification> as = translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline);
                    yield new AppliedFunction("convertDateFormat",
                            as.size() == 2 ? as
                                    : List.of(as.get(0), new CString("yyyy-MM-dd")));
            }
            case RelationalOperation.FunctionCall call
                    when (call.name().equals("convertDateTime")
                            || call.name().equals("toTimestamp"))
                    && call.args().size() == 2 ->
                    new AppliedFunction("convertDateTimeFormat", translateArgs(call,
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("convertVarchar128") && call.args().size() == 1 ->
                    strCast(translate(call.args().get(0), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("splitPart") && call.args().size() == 3 -> {
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
                    yield new AppliedFunction("splitPart", List.of(a0, a1,
                            new AppliedFunction("cast", List.of(a2,
                                    new com.legend.parser.spec.TypeAnnotation.Named(
                                            new com.legend.parser.TypeExpression.NameRef("Integer"))))));
            }
            case RelationalOperation.FunctionCall call
                    when call.name().equals("case") && call.args().size() >= 3
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
            case RelationalOperation.FunctionCall call
                    when call.name().equals("concat") && call.args().size() >= 2 -> {
                    // The variadic 'concat' dynafunction has NO pure-function
                    // counterpart (engine renders it per-dialect straight to
                    // SQL); real pure spells string concatenation with plus.
                    // SQL concat COERCES its arguments — each wraps in
                    // cast(@String), whose lowering is the SQL VARCHAR cast
                    // (the DATABASE's own formatting: '2014-01-01 06:30:00',
                    // not pure toString's ISO form — audit), a no-op for
                    // strings.
                    ValueSpecification chain = strCast(translate(call.args().get(0),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline));
                    for (int i = 1; i < call.args().size(); i++) {
                        chain = new AppliedFunction("plus", List.of(chain,
                                strCast(translate(call.args().get(i), tableScope,
                                        targetVarOrNull, rowBindOrNull, pipeline))));
                    }
                    yield chain;
            }
            // get(col, 'key', @Type): TYPED variant extraction — real pure's
            // to(get(col, 'key'), @Type) (the same emission the JSON-source
            // synthesizer uses: text-extraction then cast).
            case RelationalOperation.FunctionCall call
                    when call.name().equals("get") && call.args().size() == 3
                    && call.args().get(2) instanceof RelationalOperation.TypeRef tr ->
                    new AppliedFunction("to", List.of(
                            new AppliedFunction("get", List.of(
                                    translate(call.args().get(0), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline),
                                    translate(call.args().get(1), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline))),
                            new com.legend.parser.spec.TypeAnnotation.Named(
                                    new com.legend.parser.TypeExpression.NameRef(tr.typeName()))));
            // Dynafunction spellings with no same-named pure native:
            // isNull/isNotNull ARE isEmpty/isNotEmpty on [0..1] values;
            // group(x) is parenthesization; if's branches must be THUNKS
            // (real pure's if takes zero-param lambdas — the dynafunction
            // spelling passes plain expressions).
            // The 'indexOf' DYNAFUNCTION has SQL locate() semantics —
            // 1-BASED position (engine renders locate(sub, str)); pure's
            // indexOf is 0-based. Conform by EMISSION: +1.
            case RelationalOperation.FunctionCall call
                    when call.name().equals("indexOf") && call.args().size() == 2 ->
                    new AppliedFunction("plus", List.of(
                            new AppliedFunction("indexOf", List.of(
                                    translate(call.args().get(0), tableScope,
                                            targetVarOrNull, rowBindOrNull, pipeline),
                                    translate(call.args().get(1), tableScope,
                                            targetVarOrNull, rowBindOrNull, pipeline))),
                            new com.legend.parser.spec.CInteger(1)));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("isNull") && call.args().size() == 1 ->
                    new AppliedFunction("isEmpty", List.of(translate(call.args().get(0),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline)));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("isNotNull") && call.args().size() == 1 ->
                    new AppliedFunction("isNotEmpty", List.of(translate(call.args().get(0),
                            tableScope, targetVarOrNull, rowBindOrNull, pipeline)));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("group") && call.args().size() == 1 ->
                    translate(call.args().get(0), tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline);
            case RelationalOperation.FunctionCall call
                    when call.name().equals("if") && call.args().size() == 3 ->
                    new AppliedFunction("if", List.of(
                            translate(call.args().get(0), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline),
                            new LambdaFunction(List.of(), List.of(
                                    translate(call.args().get(1), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline))),
                            new LambdaFunction(List.of(), List.of(
                                    translate(call.args().get(2), tableScope, targetVarOrNull,
                                            rowBindOrNull, pipeline)))));
            case RelationalOperation.FunctionCall call -> new AppliedFunction(
                    call.name(),
                    call.args().stream()
                            .map(a -> translate(a, tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline))
                            .toList());
            case RelationalOperation.Comparison cmp -> {
                AppliedFunction c = new AppliedFunction(
                        comparisonFn(cmp.op()),
                        List.of(translate(cmp.left(),  tableScope, targetVarOrNull,
                                        rowBindOrNull, pipeline),
                                translate(cmp.right(), tableScope, targetVarOrNull,
                                        rowBindOrNull, pipeline)));
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
            case RelationalOperation.TypeRef tr -> throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.NORMALIZE,
                    "'@" + tr.typeName() + "' type argument is only supported as"
                            + " the third argument of get(col, key, @Type)");
            case RelationalOperation.ArrayLiteral arr -> new PureCollection(
                    arr.elements().stream()
                            .map(e -> translate(e, tableScope, targetVarOrNull,
                                    rowBindOrNull, pipeline))
                            .toList());
            case RelationalOperation.JoinNavigation jn -> {
                // The chain has been hoisted into the pipeline as a
                // join(~alias, ...) step. Its sub-row is $row.<alias>;
                // the terminal (if any) reads from that sub-row's
                // table scope.
                if (rowBindOrNull == null || !pipeline.hasSlots()) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "Nested JoinNavigation in scope without pipeline; "
                          + "JoinNav inside association predicates or join "
                          + "conditions is not supported.");
                }
                String alias = pipeline.slotFor(jn.chain());
                ValueSpecification subRow = new AppliedProperty(rowBindOrNull, alias);
                if (jn.terminal() == null) yield subRow;
                String terminalTable = pipeline.targetTable(alias);
                Map<String, ValueSpecification> innerScope = new LinkedHashMap<>(tableScope);
                if (terminalTable != null) innerScope.put(terminalTable, subRow);
                yield translate(jn.terminal(), innerScope, targetVarOrNull,
                        rowBindOrNull, pipeline);
            }
        };
    }

    static ValueSpecification literalToValueSpec(Object value) {
        if (value instanceof String s)  return new CString(s);
        if (value instanceof Long l)    return new CInteger(l);
        if (value instanceof Integer i) return new CInteger((long) i);
        if (value instanceof Double d)  return new CFloat(d);
        if (value instanceof Boolean b) return new CBoolean(b);
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, "Unsupported literal type: "
                + (value == null ? "null" : value.getClass().getName()));
    }

    static String comparisonFn(ComparisonOp op) {
        return switch (op) {
            case EQ  -> "equal";
            case NEQ -> "equal";   // wrapped in not(...) at the call site
            case LT  -> "lessThan";
            case LTE -> "lessThanEqual";
            case GT  -> "greaterThan";
            case GTE -> "greaterThanEqual";
        };
    }
}
