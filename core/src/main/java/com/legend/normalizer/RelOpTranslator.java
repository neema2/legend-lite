package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.TypeExpression;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LogicalOp;
import com.legend.model.RelationalOperation;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
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
            ValueSpecification targetVarOrNull, Variable rowBindOrNull,
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
                            ? new AppliedFunction("toOne", List.of(t)) : t;
                })
                .toList();
    }

    /** {@code cast(v, @String)} — the SQL VARCHAR coercion emission. */
    private static ValueSpecification strCast(ValueSpecification v) {
        return new AppliedFunction("cast", List.of(v,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef("String"))));
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
                                                    ValueSpecification targetVarOrNull,
                                                    Variable rowBindOrNull,
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
                            throw new NotImplementedException(
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
                                    new TypeAnnotation.Named(
                                            new TypeExpression.NameRef("Integer"))))));
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
            default -> translateTail(op, tableScope, targetVarOrNull,
                    rowBindOrNull, pipeline);
        };
    }

    /** Arm group 2 of the relational-op dispatch (sequential order
     * preserved — the split is at an arm boundary). */
    private static ValueSpecification translateTail(RelationalOperation op,
            Map<String, ValueSpecification> tableScope,
            ValueSpecification targetVarOrNull,
            Variable rowBindOrNull,
            PipelineView pipeline) {
        return switch (op) {
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
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef(tr.typeName()))));
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
                            new CInteger(1)));
            // dyna 'substring' is SQL SUBSTRING — 1-BASED start (H2 clamps
            // start < 1 to 1: the corpus passes literal 0); pure's
            // substring is 0-based. Conform by emission for BOTH arities:
            // start' = max(start - 1, 0).
            case RelationalOperation.FunctionCall call
                    when call.name().equals("substring")
                    && (call.args().size() == 2 || call.args().size() == 3) -> {
                List<ValueSpecification> args = translateArgs(call, tableScope,
                        targetVarOrNull, rowBindOrNull, pipeline);
                List<ValueSpecification> out = new java.util.ArrayList<>(args.size());
                out.add(args.get(0));
                out.add(new AppliedFunction("max", List.of(
                        new AppliedFunction("minus",
                                List.of(args.get(1), new CInteger(1))),
                        new CInteger(0))));
                if (args.size() == 3) {
                    out.add(args.get(2));
                }
                yield new AppliedFunction("substring", out);
            }
            // dyna 'add'/'sub' are SQL ARITHMETIC — pure spells them
            // plus/minus; the bare names would hit pure's COLLECTION
            // add(T[*],T[1]) and type [*]
            case RelationalOperation.FunctionCall call
                    when call.name().equals("add") && call.args().size() == 2 ->
                    new AppliedFunction("plus", translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline));
            case RelationalOperation.FunctionCall call
                    when call.name().equals("sub") && call.args().size() == 2 ->
                    new AppliedFunction("minus", translateArgs(call, tableScope,
                            targetVarOrNull, rowBindOrNull, pipeline));
            // SQL POSITION(needle, haystack) — 1-based, arguments REVERSED
            // vs pure's indexOf(haystack, needle); same +1 emission as the
            // indexOf dynafunction above
            case RelationalOperation.FunctionCall call
                    when call.name().equals("position") && call.args().size() == 2 ->
                    // toOne on the haystack: an OPTIONAL column read would
                    // otherwise infect the whole arithmetic chain with [*]
                    // (SQL null-propagates; erasure makes toOne free)
                    new AppliedFunction("plus", List.of(
                            new AppliedFunction("indexOf", List.of(
                                    new AppliedFunction("toOne", List.of(
                                            translate(call.args().get(1), tableScope,
                                                    targetVarOrNull, rowBindOrNull,
                                                    pipeline))),
                                    translate(call.args().get(0), tableScope,
                                            targetVarOrNull, rowBindOrNull, pipeline))),
                            new CInteger(1)));
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
                    translateArgs(call, tableScope, targetVarOrNull,
                            rowBindOrNull, pipeline));
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
            case RelationalOperation.TypeRef tr -> throw new ModelException(
                    LegendCompileException.Phase.NORMALIZE,
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
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
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
            // group-1 types never reach here (their arms matched above);
            // javac still needs coverage over the sealed hierarchy
            default -> throw new IllegalStateException(
                    "relational-op dispatch: unexpected " + op.getClass());
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
