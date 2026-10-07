package com.legend.compiler.spec;

import com.legend.platform.CoreFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCLatestDate;
import com.legend.compiler.spec.typed.TypedCTime;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CTime;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.PathLiteral;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.values.PureDateLiteral;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The typer's pre-dispatch DESUGARS of the legacy TDS surface (engine tds.pure spellings): the getter surface
 * ({@code isNull}/{@code isNotNull}, {@code get()->toString()}, the untyped {@code $r.get('COL')}) and the
 * schema-computing spellings ({@code renameColumn(s)}, {@code olapGroupBy}, {@code projectWithColumnSubset},
 * window-column projects, cell-index reads), each rewritten to modern natives and typed through {@link Typer#synth};
 * null when none applies. Moved out of {@code Typer} whole by execution plan W1.6 (2026-09-29): a pure move, so that
 * W2 and W3 have room in the typer, and so the desugar pass the plan names (A05; W2.7) has one home.
 */
final class TdsDesugars {

    private final Typer t;

    TdsDesugars(Typer t) {
        this.t = t;
    }

    private TypedSpec synth(ValueSpecification vs, Env env) {
        return t.synth(vs, env);
    }

    /** The TDS GETTER surface (engine tds.pure spellings):
     * isNull/isNotNull cell tests, get()->toString() TDSNull print,
     * and the untyped $r.get('COL') getter (row frame: toOne cell;
     * relation frame: TDSNull-total auto-map). Null = not one of these. */
    /** An ERASED row (a TDSRow or execute::Row in a type position; the raw-SQL grid
     *  takes the same path): its cells are read only through the owner's accessors. */
    private static boolean erasedRow(TypedSpec recv) {
        return Type.schemaView(recv.info().type()) instanceof Type.RelationType rt
                && rt.isLateBound();
    }

    /** The cell of an erased row by name, for the untyped accessors ({@code get},
     *  {@code isNull}, {@code isNotNull}): the trusted {@code Any[0..1]} read the typed
     *  getters start from too (Typer.rowCellReadOnRow). A bare {@code $r.col} on an
     *  erased row is refused (Typer.relationColumn); an accessor's read is the row's
     *  one honest read -- the engine's tds.pure:76-120 declares the accessors as
     *  TDSRow's qualified properties and no bare column. Built on the TYPED receiver:
     *  no second synthesis of the receiver's syntax. */
    private static TypedSpec erasedCell(TypedSpec recv, String col) {
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(recv, col,
                new ExprType(new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY),
                        Multiplicity.Bounded.ZERO_ONE));
    }

    /** A native call over TYPED arguments: the overload the kernel picks for their
     *  types, as for any call; no syntax is rebuilt and re-typed. */
    private TypedSpec nativeCall(String fqn, List<TypedSpec> args) {
        List<com.legend.compiler.element.TypedFunction> fns = t.model().findFunction(fqn);
        if (fns.isEmpty()) {
            throw new TypeInferenceException("the module carries no declaration of " + fqn);
        }
        InferenceKernel.Resolution r = t.kernel().resolveOverload(fns,
                args.stream().map(TypedSpec::info).toList());
        return new com.legend.compiler.spec.typed.TypedNativeCall(r.chosen(), args, r.output());
    }

    @com.legend.base.Nullable TypedSpec tdsGetterDesugars(AppliedFunction af, Env env) {
        // $r.isNotNull('COL') / isNull — TDSRow null tests on the named
        // cell (tds.pure); the cell read is optional-typed, so the tests
        // ARE emptiness (same conform-by-emission as the dynafunction
        // spellings in RelOpTranslator)
        if ((rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NOT_NULL) || rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NULL))
                && af.parameters().size() == 2
                && literalColName(af.parameters().get(1)) != null) {
            TypedSpec nrecv = synth(af.parameters().get(0), env);
            if (Typer.tdsReceiver(nrecv.info().type())) {
                boolean notNull = rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NOT_NULL);
                String ncol = java.util.Objects.requireNonNull(
                        literalColName(af.parameters().get(1)),
                        "TDS null test requires a literal column name");
                if (erasedRow(nrecv)) {
                    return nativeCall(notNull ? com.legend.compiler.element.type.PlatformTypes.IS_NOT_EMPTY
                            : com.legend.compiler.element.type.PlatformTypes.IS_EMPTY,
                            List.of(erasedCell(nrecv, ncol)));
                }
                return synth(new AppliedFunction(notNull ? "isNotEmpty" : "isEmpty",
                        List.of(new AppliedProperty(af.parameters().get(0), ncol))), env);
            }
        }
        // engine TDSRow.get()->toString(): a NULL cell prints 'TDSNull'
        // (tds.pure:131-133 — the engine materializes ^TDSNull() instances;
        // our erasure emits the equivalent conditional string)
        if (com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.TO_STRING) && af.parameters().size() == 1
                && af.parameters().get(0) instanceof AppliedFunction g
                && rowGetter(g, com.legend.builtin.NativeFn.RowGetter.GET) && g.parameters().size() == 2
                && g.parameters().get(1) instanceof CString gc) {
            TypedSpec grecv0 = synth(g.parameters().get(0), env);
            if (Typer.tdsReceiver(grecv0.info().type())) {
                if (erasedRow(grecv0)) {
                    TypedSpec cell = erasedCell(grecv0, gc.value());
                    var str1 = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
                    return new com.legend.compiler.spec.typed.TypedIf(
                            nativeCall(com.legend.compiler.element.type.PlatformTypes.IS_EMPTY, List.of(cell)),
                            new com.legend.compiler.spec.typed.TypedCString(
                                    com.legend.compiler.element.type.PlatformTypes.TDS_NULL_CELL, str1),
                            java.util.Optional.of(nativeCall(com.legend.compiler.element.type.PlatformTypes.TO_STRING,
                                    List.of(nativeCall(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(cell))))),
                            str1);
                }
                var read = new AppliedProperty(g.parameters().get(0), gc.value());
                return synth(new AppliedFunction("if", List.of(
                        new AppliedFunction("isEmpty", List.of(read)),
                        new com.legend.protocol.spec.LambdaFunction(List.of(),
                                List.of(new CString(com.legend.compiler.element.type
                                        .PlatformTypes.TDS_NULL_CELL))),
                        new com.legend.protocol.spec.LambdaFunction(List.of(),
                                List.of(new AppliedFunction("toString", List.of(
                                        new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                                                List.of(read)))))))), env);
            }
        }
        // the UNTYPED TDSRow getter $r.get('COL') — same desugar, but the
        // name collides with variant/map get: divert ONLY when the receiver
        // is relation-shaped (type-aware, unlike the typed getters above).
        // The CELL is one value like the typed getters (engine tds.pure
        // get: Any[1]) — same toOne emission over non-[1] columns.
        if (rowGetter(af, com.legend.builtin.NativeFn.RowGetter.GET) && af.parameters().size() == 2
                && af.parameters().get(1) instanceof CString gcol) {
            TypedSpec grecv = synth(af.parameters().get(0), env);
            if (Typer.tdsReceiver(grecv.info().type())) {
                if (erasedRow(grecv)) {
                    // the engine's get is Any[1] (tds.pure); the cell under the
                    // SQL-lane conformance wrap, the shape the trusted-column
                    // read took before the erased row refused bare names
                    return nativeCall(com.legend.builtin.Pure.Lite.TRUST_ONE,
                            List.of(erasedCell(grecv, gcol.value())));
                }
                TypedSpec gcell = synth(new AppliedProperty(
                        af.parameters().get(0), gcol.value()), env);
                // Exactly-[1] cell: the read IS the getter. MANY-stamped
                // (a STANDALONE relation receiver — frame-honest column
                // stamp, C2c): rows.get('COL') is the engine TDS getter
                // auto-mapped over the rows, and the engine getter is
                // TOTAL — an empty cell IS ^TDSNull() (tds.pure:131-133),
                // COUNT-PRESERVING (sqlQueryMerging pins ['8',^TDSNull(),
                // '8',^TDSNull()]). Desugar to the explicit map whose
                // per-row body is the TDSNull-if-empty read — every frame
                // stamped honestly (the toOne sits INSIDE the isEmpty
                // guard; a [1..1]-per-row column skips the guard). The
                // old toOne wrap stamped [1..1] on a whole-column LIST
                // collect — the census's union-family events.
                if (gcell.info().multiplicity() instanceof Multiplicity.Bounded gb) {
                    if (Integer.valueOf(1).equals(gb.upper()) && gb.lower() == 1) {
                        return gcell;
                    }
                    if (gb.isMany()) {
                        // ALWAYS guarded — a declared-[1] property still
                        // yields NULL cells through union threads and
                        // left-join misses (sqlQueryMerging: p1:String[1],
                        // data ['8',^TDSNull(),...]); the engine getter is
                        // TDSNull-total regardless of the declaration. The
                        // value branch upcasts to Any: TDS cells carry the
                        // STORE's kind, not the model's (same witness:
                        // p3:String[1] over an INT column asserts 2222) —
                        // the Any LUB puts the whole if on the variant
                        // carrier so sentinel and cell always co-type.
                        var rv = new Variable("_tg0");
                        var cell = new AppliedProperty(rv, gcol.value());
                        ValueSpecification body =
                                new AppliedFunction("if", List.of(
                                        new AppliedFunction("isEmpty", List.of(cell)),
                                        new LambdaFunction(List.of(),   // the ^TDSNull() INSTANCE (69a)
                                                List.of(new com.legend.protocol.spec.NewInstance(
                                                        com.legend.compiler.element.type.PlatformTypes.TDS_NULL_FQN,
                                                        List.of(), List.of(), List.of()))),
                                        new LambdaFunction(List.of(),
                                                List.of(new AppliedFunction("cast", List.of(
                                                        new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                                                                List.of(cell)),
                                                        new com.legend.protocol.spec
                                                                .TypeAnnotation.Named(
                                                                new com.legend.protocol
                                                                        .TypeExpression.NameRef(
                                                                        com.legend.compiler.element.type.PlatformTypes.ANY))))))));
                        return synth(new AppliedFunction("map", List.of(
                                af.parameters().get(0),
                                new LambdaFunction(List.of(rv), List.of(body)))), env);
                    }
                }
                return synth(new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                        new AppliedProperty(af.parameters().get(0), gcol.value()))), env);
            }
        }
        return null;
    }

    static String stripQuotes(String name) {
        return name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    /**
     * The SCHEMA-computing legacy TDS spellings (engine tds.pure host-graph
     * bodies), desugared to modern natives or folded to literals; null when
     * none applies — the caller continues down the ordinary dispatch.
     */
    @com.legend.base.Nullable TypedSpec tdsSchemaDesugars(AppliedFunction af, Env env) {
        // renameColumn(tds,'a','b') / renameColumns(tds, pair('a','b')...)
        // — desugar to the modern rename native (STATIC pair literals only)
        if (com.legend.builtin.TdsLegacy.RENAME_COLUMN.matches(af) && af.parameters().size() == 3
                && af.parameters().get(1) instanceof CString ro
                && af.parameters().get(2) instanceof CString rn) {
            return synth(new AppliedFunction("rename", List.of(
                    af.parameters().get(0),
                    new ColSpec(stripQuotes(ro.value()), null, null),
                    new ColSpec(stripQuotes(rn.value()), null, null))), env);
        }
        if (com.legend.builtin.TdsLegacy.RENAME_COLUMNS.matches(af) && af.parameters().size() == 2) {
            return renameColumnsDesugar(af, env);
        }
        // TDSColumn-metadata computations over `.columns` fold to literals
        // (engine TabularDataSet reflection: `$tds.columns->map(c|$c.name +
        // ':' + $c.type->elementToPath())` — column names and types are
        // STATIC FACTS of the typed relation). Only a FULLY static result
        // rewrites; anything else keeps the ordinary path and its walls.
        if (com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.MAP) && af.parameters().size() == 2
                && af.parameters().get(0) instanceof AppliedProperty colsRead
                && colsRead.property().equals("columns")) {
            ValueSpecification lit = new StaticFold(t, env).foldToLiteral(af);
            if (lit != null) {
                return synth(lit, env);
            }
        }
        // projectWithColumnSubset — the engine's demand-pruned project: the
        // emitted SQL computes ONLY the subset-named columns, so the desugar
        // IS project over the filtered column list. Two spellings:
        // (src, [col(fn,'name')...], [subsetNames]) and
        // (src, [lambdas], [allNames], [subsetNames]).
        if (com.legend.builtin.TdsLegacy.PROJECT_WITH_COLUMN_SUBSET.matches(af)) {
            AppliedFunction pcs = projectWithColumnSubsetDesugar(af);
            if (pcs != null) {
                return synth(pcs, env);
            }
        }
        // window cols in PROJECT position — the OLAP col overloads
        if (com.legend.builtin.TdsLegacy.PROJECT.matches(af)) {
            AppliedFunction wcd = windowColsProjectDesugar(af);
            if (wcd != null) {
                return synth(wcd, env);
            }
        }
        // paginated(set, page, size) — real pure collectionExtension.pure:236
        // body verbatim: slice((page-1)*size, page*size)
        if (com.legend.builtin.NativeFn.TyperForm.PAGINATED.matches(af.function())
                && af.parameters().size() == 3) {
            ValueSpecification pg = af.parameters().get(1);
            ValueSpecification sz = af.parameters().get(2);
            // arithmetic in the parser's (= the engine's) COLLECTION form:
            // plus/minus/times are declared over T[*] only (batch 5)
            return synth(new AppliedFunction("slice", List.of(
                    af.parameters().get(0),
                    AppliedFunction.infixRun("times", List.of(AppliedFunction.infixRun("minus", List.of(pg, new CInteger(1L))), sz)),
                    AppliedFunction.infixRun("times", List.of(pg, sz)))), env);
        }
        // olapGroupBy — the legacy TDS OLAP spellings; the modern construct
        // IS the windowed extend (see olapGroupByDesugar)
        if (com.legend.builtin.TdsLegacy.OLAP_GROUP_BY.matches(af)) {
            AppliedFunction olap = olapGroupByDesugar(af);
            if (olap != null) {
                return synth(olap, env);
            }
        }
        // union(a, b) — SQL UNION: distinct over the concatenation (the
        // same shape is pure's collection set-union, so both spellings
        // mean exactly this)
        if (com.legend.builtin.NativeFn.TyperForm.UNION.matches(af.function()) && af.parameters().size() == 2) {
            // union(a, [b,c,d]) — the collection overload chains the
            // concatenation member by member
            ValueSpecification acc = af.parameters().get(0);
            List<ValueSpecification> members =
                    af.parameters().get(1) instanceof PureCollection pc
                            ? pc.values() : List.of(af.parameters().get(1));
            for (ValueSpecification m : members) {
                acc = new AppliedFunction("concatenate", List.of(acc, m));
            }
            return synth(new AppliedFunction("distinct", List.of(acc)), env);
        }
        // columnValues(tds,'c') — the rows-mapped cell read
        if (com.legend.builtin.TdsLegacy.COLUMN_VALUES.matches(af) && af.parameters().size() == 2
                && af.parameters().get(1) instanceof CString cvCol) {
            return synth(new AppliedFunction("map", List.of(
                    new AppliedProperty(af.parameters().get(0), "rows"),
                    new LambdaFunction(List.of(new Variable("_cvr")),
                            List.of(new AppliedFunction("get", List.of(
                                    new Variable("_cvr"),
                                    new CString(cvCol.value()))))))), env);
        }
        return null;
    }

    /**
     * The legacy TDS OLAP spellings as the modern windowed extend:
     * {@code olapGroupBy([parts]?, [sortKeys]?, func('col',agg) | rankLambda,
     * 'name')} &rarr; {@code extend(over(~parts, [sortKeys]), ~name:…)}.
     * The agg form's column becomes the {p,w,r|$r.col} map lambda with the user's
     * reducer; a bare rank lambda becomes the modern window-function call. Null on
     * any other shape — the unknown-function wall stays loud. */
    private static @com.legend.base.Nullable AppliedFunction olapGroupByDesugar(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() < 3 || !(ps.get(ps.size() - 1) instanceof CString outName)) {
            return null;
        }
        int i = 1;
        List<ValueSpecification> partSpecs = new ArrayList<>();
        if (i < ps.size() - 2 && ps.get(i) instanceof CString p1) {
            partSpecs.add(new ColSpec(p1.value()));
            i++;
        } else if (i < ps.size() - 2 && ps.get(i) instanceof PureCollection pc
                && pc.values().stream().allMatch(v -> v instanceof CString)) {
            pc.values().forEach(v -> partSpecs.add(new ColSpec(((CString) v).value())));
            i++;
        }
        List<ValueSpecification> sortKeys = new ArrayList<>();
        if (i < ps.size() - 2 && isLegacySortKey(ps.get(i))) {
            sortKeys.add(ps.get(i));
            i++;
        } else if (i < ps.size() - 2 && ps.get(i) instanceof PureCollection sc
                && !sc.values().isEmpty()
                && sc.values().stream().allMatch(TdsDesugars::isLegacySortKey)) {
            sortKeys.addAll(sc.values());
            i++;
        }
        if (i != ps.size() - 2) {
            return null;
        }
        ValueSpecification op = ps.get(i);
        List<ValueSpecification> overArgs = new ArrayList<>();
        if (partSpecs.size() == 1) {
            overArgs.add(new PureCollection(partSpecs));
        } else if (!partSpecs.isEmpty()) {   // several partition columns = ONE ColSpecArray (`~[a, b]`)
            overArgs.add(new ColSpecArray(partSpecs.stream().map(ColSpec.class::cast).toList()));
        }
        if (!sortKeys.isEmpty()) {
            overArgs.add(new PureCollection(sortKeys));
        }
        if (overArgs.isEmpty()) {
            return null;
        }
        Variable p = new Variable("_olp");
        Variable w = new Variable("_olw");
        Variable r = new Variable("_olr");
        ColSpec col;
        if (op instanceof AppliedFunction fc && com.legend.builtin.TdsLegacy.FUNC.matches(fc)
                && fc.parameters().size() == 2
                && fc.parameters().get(0) instanceof CString aggCol
                && fc.parameters().get(1) instanceof LambdaFunction aggFn) {
            col = new ColSpec(outName.value(),
                    new LambdaFunction(List.of(p, w, r), List.of(
                            new AppliedProperty(r, aggCol.value()))),
                    aggFn);
        } else {
            LambdaFunction rankLam = op instanceof AppliedFunction fr
                    && com.legend.builtin.TdsLegacy.FUNC.matches(fr)
                    && fr.parameters().size() == 1
                    && fr.parameters().get(0) instanceof LambdaFunction inner
                    ? inner
                    : op instanceof LambdaFunction direct ? direct : null;
            String rankFn = rankLam == null ? null : legacyRankName(rankLam);
            if (rankFn == null) {
                return null;
            }
            List<ValueSpecification> rankArgs = rankFn.equals("rowNumber")
                    ? List.of(p, r) : List.of(p, w, r);
            col = new ColSpec(outName.value(),
                    new LambdaFunction(List.of(p, w, r), List.of(
                            new AppliedFunction(rankFn, rankArgs))), null);
        }
        return new AppliedFunction("extend", List.of(ps.get(0),
                new AppliedFunction("over", overArgs), col));
    }

    private static boolean isLegacySortKey(ValueSpecification v) {
        return v instanceof AppliedFunction sf
                && (CoreFn.of(sf.function()).orElse(null) == CoreFn.ASC
                        || CoreFn.of(sf.function()).orElse(null) == CoreFn.DESC)
                && sf.parameters().size() == 1
                && sf.parameters().get(0) instanceof CString;
    }

    /** The parse spelling of a modern relation function (its bare name). */
    private static String modernName(String fqn) {
        return fqn.substring(fqn.lastIndexOf("::") + 2);
    }

    /** The modern window-function name behind a legacy rank lambda
     * ({@code x|$x->rank()}); null for anything unrecognized. */
    private static @com.legend.base.Nullable String legacyRankName(LambdaFunction lam) {
        if (lam.parameters().size() != 1 || lam.body().size() != 1
                || !(lam.body().get(0) instanceof AppliedFunction call)
                || call.parameters().size() != 1
                || !(call.parameters().get(0) instanceof Variable v)
                || !v.name().equals(lam.parameters().get(0).name())) {
            return null;
        }
        // the legacy olap rank (upstream math::olap, a TDS-era spelling the
        // resolver has no catalog row for) maps to the modern window function
        // of the same name; averageRank has none (null — loud downstream)
        if (com.legend.builtin.TdsLegacy.OLAP_RANK.matches(call)) {
            return modernName(com.legend.compiler.element.type.PlatformTypes.RANK);
        }
        if (com.legend.builtin.TdsLegacy.OLAP_DENSE_RANK.matches(call)) {
            return modernName(com.legend.compiler.element.type.PlatformTypes.DENSE_RANK);
        }
        if (com.legend.builtin.TdsLegacy.OLAP_ROW_NUMBER.matches(call)) {
            return modernName(com.legend.compiler.element.type.PlatformTypes.ROW_NUMBER);
        }
        return null;
    }

    /** A TDS-row getter call spelled as a function ({@code get($r, 'c')},
     *  {@code isNull($r, 'c')}): the applied name IS the family member's
     *  registered property name (qualified properties have no FQN spelling). */
    static boolean rowGetter(AppliedFunction af, com.legend.builtin.NativeFn.RowGetter g) {
        return g.property().equals(af.function());
    }

    /** A window-col carrier: declared name, hidden partition/map input
     * column names, and the user's reducer lambda. */
    private record WinCol(String name, List<String> partCols, String mapCol,
            LambdaFunction agg) {
    }

    /** The WINDOW-COL project overloads (REAL tds.pure:233 —
     * {@code col(window(parts...), func(map, agg), name)}, name LAST,
     * OlapAggregation form) as the MODERN windowed extend: the project
     * carries hidden partition/map INPUT columns ({@code <name>__wpN} /
     * {@code <name>__wm}), each window col extends with
     * {@code over(~parts)} + the user's reducer, and a closing restrict
     * returns exactly the declared names in declaration order (hidden
     * inputs drop there). Null when no window col is present; the
     * sortInfo/rank overload variants keep the loud project wall. */
    private static @com.legend.base.Nullable AppliedFunction windowColsProjectDesugar(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 2 || !(ps.get(1) instanceof PureCollection cols)) {
            return null;
        }
        if (cols.values().stream().noneMatch(v -> v instanceof AppliedFunction cf
                && com.legend.builtin.TdsLegacy.COL.matches(cf)
                && !cf.parameters().isEmpty()
                && cf.parameters().get(0) instanceof AppliedFunction w0
                && com.legend.builtin.TdsLegacy.WINDOW.matches(w0))) {
            return null;
        }
        List<ValueSpecification> projCols = new ArrayList<>();
        List<ValueSpecification> declared = new ArrayList<>();
        List<WinCol> wins = new ArrayList<>();
        for (ValueSpecification v : cols.values()) {
            if (!(v instanceof AppliedFunction cf)
                    || !com.legend.builtin.TdsLegacy.COL.matches(cf)) {
                return null;
            }
            List<ValueSpecification> cps = cf.parameters();
            if (!(cps.get(0) instanceof AppliedFunction w
                    && com.legend.builtin.TdsLegacy.WINDOW.matches(w))) {
                // plain col (2-arg or 3-arg with doc): name at index 1
                if (cps.size() < 2 || !(cps.get(1) instanceof CString pn)) {
                    return null;
                }
                projCols.add(cf);
                declared.add(new CString(pn.value()));
                continue;
            }
            if (cps.size() != 3
                    || !(cps.get(1) instanceof AppliedFunction fc)
                    || !com.legend.builtin.TdsLegacy.FUNC.matches(fc)
                    || fc.parameters().size() != 2
                    || !(fc.parameters().get(0) instanceof LambdaFunction mapLam)
                    || !(fc.parameters().get(1) instanceof LambdaFunction aggLam)
                    || !(cps.get(2) instanceof CString wn)) {
                return null;
            }
            List<ValueSpecification> parts = w.parameters().size() == 1
                    && w.parameters().get(0) instanceof PureCollection wp
                    ? wp.values() : w.parameters();
            List<String> partCols = new ArrayList<>();
            for (int j = 0; j < parts.size(); j++) {
                if (!(parts.get(j) instanceof LambdaFunction pl)) {
                    return null;
                }
                String pn = wn.value() + "__wp" + j;
                projCols.add(new AppliedFunction("col",
                        List.of(pl, new CString(pn))));
                partCols.add(pn);
            }
            String mn = wn.value() + "__wm";
            projCols.add(new AppliedFunction("col",
                    List.of(mapLam, new CString(mn))));
            wins.add(new WinCol(wn.value(), partCols, mn, aggLam));
            declared.add(new CString(wn.value()));
        }
        ValueSpecification chain = new AppliedFunction("project",
                List.of(ps.get(0), new PureCollection(projCols)));
        for (WinCol wc : wins) {
            List<ValueSpecification> partSpecs = new ArrayList<>();
            for (String pc : wc.partCols()) {
                partSpecs.add(new ColSpec(pc));
            }
            Variable p = new Variable("_wcp");
            Variable ww = new Variable("_wcw");
            Variable r = new Variable("_wcr");
            chain = new AppliedFunction("extend", List.of(chain,
                    new AppliedFunction("over",
                            List.of(new PureCollection(partSpecs))),
                    new ColSpec(wc.name(),
                            new LambdaFunction(List.of(p, ww, r), List.of(
                                    new AppliedProperty(r, wc.mapCol()))),
                            wc.agg())));
        }
        return new AppliedFunction("restrict",
                List.of(chain, new PureCollection(declared)));
    }

    /** {@code projectWithColumnSubset} as plain {@code project} over the
     * subset-named columns (subset-list order, engine parity); null when the
     * shape is not the static legacy spelling — the generic path stays loud. */
    private static @com.legend.base.Nullable AppliedFunction projectWithColumnSubsetDesugar(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        java.util.LinkedHashMap<String, LambdaFunction> byName = new java.util.LinkedHashMap<>();
        List<String> subset;
        if (ps.size() == 3 && ps.get(1) instanceof PureCollection cols
                && ps.get(2) instanceof PureCollection subs) {
            subset = literalStrings(subs);
            if (subset == null) {
                return null;
            }
            for (ValueSpecification v : cols.values()) {
                if (v instanceof AppliedFunction cf && com.legend.builtin.TdsLegacy.COL.matches(cf)
                        && cf.parameters().size() == 2
                        && cf.parameters().get(0) instanceof LambdaFunction fn
                        && cf.parameters().get(1) instanceof CString nm) {
                    byName.put(nm.value(), fn);
                } else if (v instanceof ColSpec cs && cs.function1() != null) {
                    byName.put(cs.name(), cs.function1());
                } else {
                    return null;
                }
            }
        } else if (ps.size() == 4 && ps.get(1) instanceof PureCollection lams
                && ps.get(2) instanceof PureCollection allNames
                && ps.get(3) instanceof PureCollection subs) {
            subset = literalStrings(subs);
            List<String> names = literalStrings(allNames);
            if (subset == null || names == null
                    || names.size() != lams.values().size()
                    || !lams.values().stream().allMatch(v -> v instanceof LambdaFunction)) {
                return null;
            }
            for (int i = 0; i < names.size(); i++) {
                byName.put(names.get(i), (LambdaFunction) lams.values().get(i));
            }
        } else {
            return null;
        }
        List<ValueSpecification> outLams = new ArrayList<>(subset.size());
        List<ValueSpecification> outNames = new ArrayList<>(subset.size());
        for (String s : subset) {
            LambdaFunction fn = byName.get(s);
            if (fn == null) {
                return null;
            }
            outLams.add(fn);
            outNames.add(new CString(s));
        }
        return new AppliedFunction("project", List.of(ps.get(0),
                new PureCollection(outLams), new PureCollection(outNames)));
    }

    private static @com.legend.base.Nullable List<String> literalStrings(PureCollection c) {
        List<String> out = new ArrayList<>(c.values().size());
        for (ValueSpecification v : c.values()) {
            if (!(v instanceof CString s)) {
                return null;
            }
            out.add(s.value());
        }
        return out;
    }

    /** The LITERAL column name of a TDSRow accessor argument: a plain
     * 'COL' string, or the TDSColumn-object spelling
     * {@code $tds.columnByName('COL')[->toOne()]} (tds.pure:21/111-112 —
     * the qualified property filters columns by name, so a literal
     * argument IS the name; non-literal column expressions stay null and
     * the caller's arm passes). */
    static @com.legend.base.Nullable String literalColName(ValueSpecification v) {
        if (v instanceof CString cs) {
            return cs.value();
        }
        if (v instanceof AppliedFunction tf
                && com.legend.compiler.ResolvedNames.referents(tf).stream().anyMatch(com.legend.builtin.Pure::isToOneCall)
                && tf.parameters().size() == 1) {
            return literalColName(tf.parameters().get(0));
        }
        if (v instanceof AppliedFunction cf
                && com.legend.builtin.TdsLegacy.COLUMN_BY_NAME.matches(cf)
                && cf.parameters().size() == 2
                && cf.parameters().get(1) instanceof CString name) {
            return name.value();
        }
        return null;
    }

    /** The single-row PICKS whose result a `.values` read treats as ONE
     * TDSRow (cells in column order), not a relation to flatten. */
    private static final java.util.Set<String> ROW_PICK_FQNS = java.util.Set.of(
            "meta::pure::functions::collection::at",
            "meta::pure::functions::collection::first",
            "meta::pure::functions::collection::last",
            "meta::pure::functions::multiplicity::toOne",
            com.legend.builtin.Pure.Lite.TRUST_ONE);

    private static final java.util.Set<String> ROW_CELL_AT_FNS = java.util.Set.of(
            "at", "meta::pure::functions::collection::at");
    private static final java.util.Set<String> ROW_CELL_SIZE_FNS = java.util.Set.of(
            "size", "meta::pure::functions::collection::size");

    /** TDSRow cells by INDEX: {@code rows->at(i).values->at(k)} is CELL k
     * of the picked row — the k-th COLUMN read (engine tds.pure TDSRow
     * .values: Any[*] in column order), NOT a row slice; {@code ->size()}
     * over the same read is the column count. Only a single-row PICK
     * receiver diverts here — the bare {@code .values} flatten (whole-row
     * list compares) keeps its identity in the property arm. */
    @com.legend.base.Nullable TypedSpec tdsRowCellIndexRead(
            AppliedFunction af, Env env) {
        boolean isAt = ROW_CELL_AT_FNS.contains(af.function());
        if ((!isAt && !ROW_CELL_SIZE_FNS.contains(af.function()))
                || af.parameters().size() != (isAt ? 2 : 1)
                || !(af.parameters().get(0) instanceof AppliedProperty vp)
                || !vp.property().equals("values")) {
            return null;
        }
        TypedSpec pick = synth(vp.receiver(), env);
        // WHOLE-RELATION receiver ($tds.rows.values->at(k)): row-major
        // cell k = row k/C, column k%C (ledger cluster 33 — the .rows
        // marker keeps identity typing, so at(k) was a ROW slice). The
        // bare flatten (no ->at) stays identity; size() stays null here
        // (rows*cols is not statically known — never lie).
        if (pick instanceof com.legend.compiler.spec.typed
                        .TypedPropertyAccess rm
                && rm.property().equals(com.legend.compiler.element.type
                        .PlatformTypes.ROWS_MARKER)
                && Type.schemaView(pick.info().type()) instanceof Type.RelationType wrt
                && isAt
                && af.parameters().size() == 2
                && af.parameters().get(1) instanceof CInteger wk) {
            int cc = wrt.columns().size();
            long k = wk.value().longValue();
            if (cc > 0 && k >= 0) {
                return synth(new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                        new AppliedProperty(
                                new AppliedFunction("at", List.of(
                                        vp.receiver(),
                                        new CInteger(k / cc))),
                                wrt.columns().get((int) (k % cc)).name()))),
                        env);
            }
        }
        if (!(Type.schemaView(pick.info().type()) instanceof Type.RelationType prt)
                || !(pick instanceof TypedNativeCall pc)
                || !ROW_PICK_FQNS.contains(pc.callee().qualifiedName())) {
            return null;
        }
        if (!isAt) {
            return new TypedCInteger((long) prt.columns().size(),
                    ExprType.one(Type.Primitive.INTEGER));
        }
        if (!(af.parameters().get(1) instanceof CInteger ki)) {
            return null;
        }
        int k = ki.value().intValue();
        if (prt.columns().isEmpty()) {
            return null;    // schema unknown (executeInDb rows): the ordinary typing serves it
        }
        if (k < 0 || k >= prt.columns().size()) {
            throw new IllegalStateException(
                    "The system is trying to get an element at offset " + k
                    + " where the collection is of size "
                    + prt.columns().size());
        }
        return synth(new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                new AppliedProperty(vp.receiver(),
                        prt.columns().get(k).name()))), env);
    }

    /** ONE row's cells as TYPED per-column reads, in column order —
     * shared by the row-var {@code $r.values} synthesis and the
     * relation-cells flatten ({@code rows.values} = map over these). */
    static com.legend.compiler.spec.typed.TypedCollection rowCells(
            TypedSpec rowSource, Type.RelationType rt) {
        List<TypedSpec> cells = new java.util.ArrayList<>(rt.columns().size());
        Type elem = null;
        boolean mixed = false;
        for (Type.RelationType.Column c : rt.columns()) {
            cells.add(new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    rowSource, c.name(),
                    new ExprType(c.type(), c.multiplicity())));
            if (elem == null) {
                elem = c.type();
            } else if (!elem.equals(c.type())) {
                mixed = true;
            }
        }
        Type collElem = mixed || elem == null
                ? new Type.ClassType(
                        com.legend.compiler.element.type.PlatformTypes.ANY)
                : elem;
        // rowCells=true: the construction-declared TDS row-cells fact —
        // consumers (makeString sentinel, variant cell-slot law) read the
        // declaration, never the shape
        return new com.legend.compiler.spec.typed.TypedCollection(cells,
                new ExprType(collElem,
                        new com.legend.compiler.element.type.Multiplicity.Bounded(
                                cells.size(), cells.size())), true);
    }

    /** Segment-aware legacy tds.pure vocabulary match: the BARE simple
     * name or the exact {@code meta::pure::tds::} FQN — never a SUFFIX of
     * a longer user name (exact-FQN rule, audit 23 A1;
     * {@code my::customRenameColumn} calls the user function). */

    /** renameColumns(tds, pairs) desugar — literal pair(,)/^Pair(first=,second=) chains into rename natives. */
    private TypedSpec renameColumnsDesugar(AppliedFunction af, Env env) {
            List<ValueSpecification> pairs =
                    af.parameters().get(1) instanceof PureCollection pc
                            ? pc.values() : List.of(af.parameters().get(1));
            ValueSpecification acc = af.parameters().get(0);
            for (ValueSpecification pv : pairs) {
                String po = null;
                String pn = null;
                if (pv instanceof AppliedFunction pf
                        && com.legend.compiler.ResolvedNames.names(pf, com.legend.compiler.element.type.PlatformTypes.PAIR_FN)
                        && pf.parameters().size() == 2
                        && pf.parameters().get(0) instanceof CString pos
                        && pf.parameters().get(1) instanceof CString pns) {
                    po = pos.value();
                    pn = pns.value();
                }
                // the corpus's other literal spelling:
                // ^Pair<String,String>(first='old', second='new') — the
                // parser wraps the ctor as AppliedFunction("new",
                // [receiver, NewInstance])
                ValueSpecification pu = pv instanceof AppliedFunction nf
                        && AppliedFunction.isNew(nf)
                        && nf.parameters().size() == 2
                        ? nf.parameters().get(1) : pv;
                if (pu instanceof NewInstance ni
                        && (ni.className().equals("Pair") || ni.className()
                                .equals("meta::pure::functions::collection::Pair"))
                        && ni.first("first") != null
                        && ni.first("first").value()
                                instanceof CString pof
                        && ni.first("second") != null
                        && ni.first("second").value()
                                instanceof CString pnf) {
                    po = pof.value();
                    pn = pnf.value();
                }
                if (po == null) {
                    throw new SchemaInvariantException("renameColumns expects"
                            + " literal pair('old','new') /"
                            + " ^Pair(first=,second=) mappings");
                }
                acc = new AppliedFunction("rename", List.of(acc,
                        new ColSpec(stripQuotes(po), null, null),
                        new ColSpec(stripQuotes(java.util.Objects
                                .requireNonNull(pn, "pn")), null, null)));
            }
            return synth(acc, env);
    }

    /** extractEnumValue(Enumeration, 'NAME') — SPECIAL FORM against the
     * registered signature (real pure extractEnumValue.pure:25): a LITERAL
     * name constant-folds to the enum VALUE so downstream enum-literal
     * consumers (adjust's DurationUnit arm) see it; a non-literal name is
     * loud, never a silent string. Null = arg0 not Enumeration-shaped. */
    @com.legend.base.Nullable TypedSpec extractEnumValueFold(AppliedFunction af, Env env) {
        TypedSpec e0 = synth(af.parameters().get(0), env);
        if (!(e0.info().type() instanceof Type.GenericType gt)
                || !gt.rawFqn().equals(com.legend.compiler.element.type.PlatformTypes.ENUMERATION)
                || gt.arguments().size() != 1
                || !(gt.arguments().get(0) instanceof Type.EnumType et)) {
            return null;
        }
        if (!(af.parameters().get(1) instanceof CString nm)) {
            return null;    // no fold: the registered signature types it; the lowering walls
        }
        var en = t.model().findEnum(et.fqn()).orElseThrow(() ->
                new TypeInferenceException("unknown enumeration '"
                        + et.fqn() + "'"));
        if (!en.values().contains(nm.value())) {
            throw new TypeInferenceException("enumeration '" + et.fqn()
                    + "' has no value '" + nm.value() + "'");
        }
        return new TypedEnumValue(et.fqn(), nm.value(), ExprType.one(et));
    }
}
