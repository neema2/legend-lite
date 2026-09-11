package com.legend.compiler.spec;

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
 * The bidirectional expression type-checker (engine {@code TypeChecker}'s
 * expression half, PHASE_G_SPEC_COMPILER.md §2/§6): {@link #typeBody} walks a
 * parsed {@link ValueSpecification} into a {@link TypedSpec}, either
 * <em>synthesizing</em> its type or <em>checking</em> it against an
 * {@link Expected} one.
 *
 * <p><strong>Layering (Driver &rarr; Typer &rarr; Checkers &rarr; Kernel).</strong>
 * The {@link SpecCompiler} driver owns whole-function compilation; this class owns
 * exactly two things &mdash; the <em>forms</em> (literals, variables, collections,
 * property access, colspec/enum values) and the one <em>generic application
 * rule</em> ({@link #checkGeneric}: overload resolution, deferred lambda/colspec
 * arguments, signature-driven outputs). Each core construct's shape decisions,
 * desugars, and HIR emission live in its own {@code *Checker} class (engine's
 * checker layout, minus the god-base), reached through the exhaustive
 * {@link CoreFn} switch in {@link #applyCore}; the pure type machinery
 * (unify / constraints / resolve / lattice) is the {@link InferenceKernel}.
 */
final class Typer {

    private final ModelContext ctx;
    private final InferenceKernel kernel;

    /** A class name's FQN through the model (identity when unknown) —
     * TypedFrom's unchecked-body walk canonicalizes refs with it. */
    String classFqnOf(String name) {
        return ctx.findClass(name)
                .map(c -> c.qualifiedName()).orElse(name);
    }

    /** Type-annotation resolution (@T, @Pair<String,Integer>, @Relation<(…)>),
     * with the enclosing function's type-parameter frame. */
    private final TypeAnnotations annotations;

    Typer(ModelContext ctx, InferenceKernel kernel) {
        this.ctx = ctx;
        this.kernel = kernel;
        this.annotations = new TypeAnnotations(ctx);
    }

    /** Type under {@code fn}'s type-parameter frame: its parameters resolve
     * as type VARIABLES in annotations and are RIGID in the kernel. */
    <R> R inFunctionScope(com.legend.compiler.element.TypedFunction fn,
            java.util.function.Supplier<R> body) {
        java.util.List<String> rigid = new ArrayList<>(fn.typeParameters());
        rigid.addAll(fn.multiplicityParameters());
        return annotations.inFrame(fn.typeParameters(), () -> kernel.withRigid(rigid, body));
    }

    /** The model snapshot &mdash; the checkers' lookup surface. */
    ModelContext model() {
        return ctx;
    }

    /** The type machinery &mdash; unification, constraints, resolution, the lattice. */
    InferenceKernel kernel() {
        return kernel;
    }

    /**
     * Type-check {@code vs} in {@code env}, under the bidirectional {@code expected}
     * mode &mdash; the expression-level entry point used by the {@link SpecCompiler}
     * driver (and, via its delegate, by in-package tests).
     */
    TypedSpec typeBody(ValueSpecification vs, Env env, Expected expected) {
        TypedSpec node = synth(vs, env);
        if (expected instanceof Expected.Check check) {
            requireConforms(node.info(), check.expected());
        }
        return node;
    }

    /** Synthesis (inference) mode: produce the node and its intrinsic type. */
    TypedSpec synth(ValueSpecification vs, Env env) {
        return switch (vs) {
            // Path literals normally dissolve at resolution; an unresolved one types as
            // its desugared lambda.
            case PathLiteral pl -> synth(pl.desugared(), env);
            // service-test parameter literal — never reaches query typing
            case com.legend.protocol.spec.CByteArray b ->
                    throw new com.legend.error.NotImplementedException(
                            "byte-array literals type only in service-test"
                                    + " parameters");
            case com.legend.protocol.spec.GraphFetchLiteral gf -> synth(gf.desugared(), env);
            // the quote/eval carrier TYPES as the call it wraps (the
            // engine's native is Any[1]; the ->cast supplies the type) —
            // the TREE face is consumed by GraphFetchChecker only
            case com.legend.protocol.spec.QuotedTreeCall q ->
                    synth(q.original(), env);
            // the grammar carrier: compileLegendGrammar(...) DENOTES its
            // element collection; a function element's value is its
            // lambda — the collection of lambdas types (->at(i)->cast
            // then selects, the chain assembly peels structurally)
            case com.legend.protocol.spec.QuotedGrammarCall q ->
                    synth(new PureCollection(
                            List.<ValueSpecification>copyOf(q.functions())), env);
            case com.legend.protocol.spec.TdsLiteral tl -> synth(tl.desugared(), env);
            case com.legend.protocol.spec.SqlIsland si ->
                    throw new com.legend.error.NotImplementedException(
                            "#SQL{...}# expression islands are not compilable —"
                                    + " an inline SQL string bypasses the typed"
                                    + " lowering pipeline");
            case com.legend.protocol.spec.GqlIsland gi ->
                    throw new com.legend.error.NotImplementedException(
                            "#GQL{...}# expression islands are not compilable"
                                    + " — the graphQL extension is parse-only"
                                    + " surface (like #SQL{...}#)");
            case CInteger lit -> new TypedCInteger(lit.value(), ExprType.one(Type.Primitive.INTEGER));
            case CString lit -> new TypedCString(lit.value(), ExprType.one(Type.Primitive.STRING));
            case CBoolean lit -> new TypedCBoolean(lit.value(), ExprType.one(Type.Primitive.BOOLEAN));
            case CFloat lit -> new TypedCFloat(lit.value(), lit.exact(), ExprType.one(Type.Primitive.FLOAT));
            case CDecimal lit -> {
                BigDecimal dv = lit.value();
                // a PROMOTED literal (no D suffix — the parser's
                // precision-promotion of a bare float literal) beyond the
                // DECIMAL(38) carrier ROUNDS to the carrier's edge: the
                // exact tail is physically unrepresentable, and 38 digits
                // is still ~10^21x tighter than the double it was promoted
                // from (essential testComplexPow's 44-digit literal). An
                // EXPLICIT D-suffixed decimal keeps the loud reject —
                // silent truncation of a declared decimal lies.
                if (dv.scale() > Type.PrecisionDecimal.MAX_PRECISION
                        && (lit.written() == null
                                || !lit.written().toUpperCase(java.util.Locale.ROOT)
                                        .endsWith("D"))) {
                    dv = dv.setScale(Type.PrecisionDecimal.MAX_PRECISION,
                            java.math.RoundingMode.HALF_EVEN);
                }
                yield new TypedCDecimal(dv, ExprType.one(decimalType(dv)));
            }
            // Date literals type by PRECISION (engine's CStrictDate/CDateTime split):
            // year/year-month -> Date, full day -> StrictDate, any time part -> DateTime.
            case CDate lit -> new TypedCDate(lit.value(), ExprType.one(dateType(lit.value())));
            case CTime lit -> new TypedCTime(lit.requireValue(),
                    ExprType.one(Type.Primitive.STRICT_TIME));
            case CLatestDate ignored -> new TypedCLatestDate(ExprType.one(Type.Primitive.LATEST_DATE));
            case TypeAnnotation ta -> typeRef(ta);
            case Variable v -> new TypedVariable(v.name(), env.lookup(v.name()).orElseThrow(
                    () -> new TypeInferenceException(env.exprAlias(v.name()).isPresent()
                            // a PARKED deferred binding (bind-once): usable
                            // only where a consuming checker types it
                            ? "deferred let binding '$" + v.name() + "' has no"
                                    + " type outside a consuming call position"
                                    + " (tree/colspec bindings resolve at"
                                    + " their call sites)"
                            : "unbound variable '$" + v.name() + "'")));
            case AppliedFunction af -> applyFunction(af, env);
            case AppliedProperty ap -> accessProperty(ap, env);
            case PureCollection coll -> collection(coll, env);
            // a BARE TDSNull reference ($v != TDSNull, tds.pure
            // firstNotNull) is the same null-cell value as ^TDSNull() —
            // one funnel: both resolve to sqlNull()
            case PackageableElementPtr ref
                    when ref.fullPath().equals("TDSNull")
                    || ref.fullPath().equals("meta::pure::tds::TDSNull") ->
                    synth(new AppliedFunction("sqlNull", List.of()), env);
            case PackageableElementPtr ref -> classReference(ref);
            case NewInstance ni -> {
                // ^TDSNull() — the TDS null-cell INSTANCE (engine
                // tds.pure:127): a VALUE stamped [1], never an empty —
                // NewChecker types it like any construction (the class is
                // registered, Pure.TDS_NULL). Only the BARE reference
                // ($v != TDSNull) stays the sqlNull() funnel above: THAT
                // position is the presence test (NullSemantics null-literal
                // arms), and its Nil[0] is the registered signature.
                // Representation is unchanged either way — the instance
                // lowers to the SQL NULL literal in scalar position and to
                // the dialect's JSON null on the variant lane (a value
                // survives the lane; tds grid convention, Lowerer).
                yield NewChecker.check(this, ni, env);
            }
            case ColSpec cs -> typedColSpec(cs);
            case ColSpecArray arr -> typedColSpecArray(arr);
            case EnumValue ev -> enumValue(ev);
            // EXHAUSTIVE over sealed ValueSpecification — no default arm
            // (root package-info invariant): a new AST variant is a COMPILE
            // error here, not a runtime surprise. The two arms below are the
            // deliberate not-yet-implemented forms.
            case LambdaFunction lf -> {
                // A lambda LITERAL types WITHOUT a call when its parameter
                // types are knowable: zero-arg, or FULLY-ANNOTATED params
                // ({id:Integer[1], name:String[*]|...} — real pure types
                // these directly; audit 19d B4 + the executionPlan family).
                // Leading lets bind statement-style (multi-statement zero-arg
                // thunks); a partially-annotated lambda stays loud.
                boolean annotated = !lf.parameters().isEmpty()
                        && lf.parameters().stream().allMatch(pv -> pv.type() != null);
                if (lf.parameters().isEmpty() || annotated) {
                    Env scope = env;
                    List<String> names = new ArrayList<>();
                    List<Type.Param> params = new ArrayList<>();
                    for (Variable pv : lf.parameters()) {
                        Type pt = namedType(java.util.Objects.requireNonNull(pv.type(),
                                "lambda parameter without a declared type"));
                        Multiplicity pm = pv.multiplicity() == null
                                ? Multiplicity.Bounded.ONE
                                : Multiplicity.from(pv.multiplicity());
                        names.add(pv.name());
                        params.add(new Type.Param(pt, pm));
                        scope = scope.with(pv.name(), new ExprType(pt, pm));
                    }
                    List<TypedSpec> stmts = new ArrayList<>();
                    for (int si = 0; si < lf.body().size() - 1; si++) {
                        if (lf.body().get(si) instanceof AppliedFunction lset
                                && CoreFn.of(lset.function()).orElse(null) == CoreFn.LET
                                && lset.parameters().size() == 2
                                && lset.parameters().get(0) instanceof CString ln) {
                            // bind-once (family A): deferred-kind rhs
                            // parks (same rule as the statement folds)
                            if (deferredLetRhs(lset.parameters().get(1))) {
                                scope = scope.withDeferred(ln.value(),
                                        lset.parameters().get(1));
                                continue;
                            }
                            TypedSpec val = synth(lset.parameters().get(1), scope);
                            scope = scope.withLet(ln.value(), val.info(),
                                    lset.parameters().get(1));
                            stmts.add(new com.legend.compiler.spec.typed.TypedLet(
                                    ln.value(), val, val.info()));
                            continue;
                        }
                        // bind-once family B tail: a non-let intermediate
                        // is an EXPRESSION STATEMENT (value discarded —
                        // real pure body semantics); it types in the same
                        // scope and rides the body like a let's value.
                        stmts.add(synth(lf.body().get(si), scope));
                    }
                    TypedSpec body = synth(lf.body().get(lf.body().size() - 1), scope);
                    stmts.add(body);
                    var fnType = new Type.FunctionType(params,
                            new Type.Param(body.info().type(),
                                    body.info().multiplicity()));
                    yield new TypedLambda(names, List.copyOf(stmts),
                            new ExprType(fnType, Multiplicity.Bounded.ONE));
                }
                if (System.getenv("LL_TMP_DEBUG") != null) {
                    System.err.println("[bare-lambda] " + lf);
                    Thread.dumpStack();
                }
                throw new TypeInferenceException(
                        "a bare lambda has no type outside a call position"
                                + " (lambdas type against their call's signature)");
            }
            // ^Class($src): the MAPPING CAST — an upstream class value fed
            // through Class's mapping (M2M). Typed nominally here; the
            // RESOLVER composes it during class-source extraction (H5).
            case NewInstanceCast nc -> {
                if (!nc.typeArguments().isEmpty()) {
                    throw new TypeInferenceException("generic mapping cast ^"
                            + nc.className() + "<...>($src) is not supported yet");
                }
                TypedSpec src = synth(nc.src(), env);
                String fqn = nc.className();   // NameResolver already qualified it
                if (ctx.findClass(fqn).isEmpty()) {
                    throw new TypeInferenceException("Unknown type: '" + fqn
                            + "' is not a known class (in ^" + fqn + "(...) cast)");
                }
                yield new com.legend.compiler.spec.typed.TypedNewInstanceCast(fqn, src,
                        new ExprType(new Type.ClassType(fqn),
                                src.info().multiplicity()),
                        nc.targetSetId());
            }
        };
    }

    // =====================================================================
    // Application &mdash; CoreFn dispatch + the generic signature-driven path
    // =====================================================================

    /**
     * A function application. The name resolves to a {@link CoreFn} exactly once;
     * a core construct dispatches through the exhaustive {@code switch} in
     * {@link #applyCore}, anything else is a library call on the generic path.
     */
    /** The TDS GETTER surface (engine tds.pure spellings):
     * isNull/isNotNull cell tests, get()->toString() TDSNull print,
     * and the untyped $r.get('COL') getter (row frame: toOne cell;
     * relation frame: TDSNull-total auto-map). Null = not one of these. */
    private @com.legend.Nullable TypedSpec tdsGetterDesugars(AppliedFunction af, Env env) {
        // $r.isNotNull('COL') / isNull — TDSRow null tests on the named
        // cell (tds.pure); the cell read is optional-typed, so the tests
        // ARE emptiness (same conform-by-emission as the dynafunction
        // spellings in RelOpTranslator)
        if ((rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NOT_NULL) || rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NULL))
                && af.parameters().size() == 2
                && literalColName(af.parameters().get(1)) != null
                && tdsReceiver(synth(af.parameters().get(0), env)
                        .info().type())) {
            return synth(new AppliedFunction(
                    rowGetter(af, com.legend.builtin.NativeFn.RowGetter.IS_NOT_NULL) ? "isNotEmpty" : "isEmpty",
                    List.of(new AppliedProperty(af.parameters().get(0),
                            java.util.Objects.requireNonNull(
                                    literalColName(af.parameters().get(1)),
                                    "TDS null test requires a literal column"
                                    + " name")))), env);
        }
        // engine TDSRow.get()->toString(): a NULL cell prints 'TDSNull'
        // (tds.pure:131-133 — the engine materializes ^TDSNull() instances;
        // our erasure emits the equivalent conditional string)
        if (com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.TO_STRING) && af.parameters().size() == 1
                && af.parameters().get(0) instanceof AppliedFunction g
                && rowGetter(g, com.legend.builtin.NativeFn.RowGetter.GET) && g.parameters().size() == 2
                && g.parameters().get(1) instanceof CString gc) {
            TypedSpec grecv0 = synth(g.parameters().get(0), env);
            if (tdsReceiver(grecv0.info().type())) {
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
            if (tdsReceiver(grecv.info().type())) {
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

    /** The parser's INFIX marker rides the typed run ({@code TypedCollection
     *  .operatorRun}) — on BOTH typing paths (eager and deferred), the one
     *  place an application's typed arguments are final. */
    private static void markOperatorRun(boolean infix, List<TypedSpec> args) {
        if (infix && args.size() == 1
                && args.get(0) instanceof com.legend.compiler.spec.typed.TypedCollection run) {
            args.set(0, run.asOperatorRun());
        }
    }

    private TypedSpec applyFunction(AppliedFunction af, Env env) {
        // (infix arithmetic types as the parser spells it — the engine's
        // n-ary carrier plus([a,b,c]) against upstream's plus(Number[*]) &
        // co.; the pairwise desugar and Pure.java's binary inventions are
        // gone, batch 5 leg 5 — the lowering folds a literal run to a chain)
        // Legacy TDS surface desugars (engine's TDS-era spellings):
        // col(fn, 'name') is the function-column spec — the modern ~name:fn
        if (com.legend.builtin.TdsLegacy.COL.matches(af) && af.parameters().size() == 2
                && af.parameters().get(0) instanceof LambdaFunction fn
                && af.parameters().get(1) instanceof CString name) {
            return synth(new ColSpec(name.value(), fn, null), env);
        }
        TypedSpec tdsSchema = tdsSchemaDesugars(af, env);
        if (tdsSchema != null) {
            return tdsSchema;
        }
        // tdsRows(tds) = $tds.rows (real tds.pure:301) — the rows-marker
        // read; emptiness et al. compose over it like any rows access
        if (com.legend.builtin.TdsLegacy.TDS_ROWS.matches(af)
                && af.parameters().size() == 1) {
            return synth(new AppliedProperty(af.parameters().get(0),
                    com.legend.compiler.element.type.PlatformTypes.ROWS_MARKER),
                    env);
        }
        // $r.getString('COL') / Row.value('COL') — the typed row-cell
        // accessors (TDSRow + the ResultSet Row twin, one owner below)
        var getter = com.legend.builtin.NativeFn.RowGetter.of(af.function());
        if (getter.isPresent() && getter.get().typedCell() && af.parameters().size() == 2) {
            TypedSpec grecv = synth(af.parameters().get(0), env);
            if (tdsReceiver(grecv.info().type())) {
                if (literalColName(af.parameters().get(1)) != null) {
                    return rowCellRead(af, env, getter.get());   // the FOLD: the row's column
                }
                // a NON-literal column name: the call to the lifted qualified
                // property stands (typed by its declaration; RowGetters lowers it
                // by name once unroll/inlining has made the name literal)
                return liftedAccessorCall(getter.get(), grecv, synth(af.parameters().get(1), env));
            }
        }
        TypedSpec tdsGetter = tdsGetterDesugars(af, env);
        if (tdsGetter != null) {
            return tdsGetter;
        }
        TypedSpec rowCell = tdsRowCellIndexRead(af, env);
        if (rowCell != null) {
            return rowCell;
        }
        // restrict(['c1','c2']) — the legacy TDS column-subset select
        if ((com.legend.builtin.TdsLegacy.RESTRICT.matches(af) || com.legend.builtin.TdsLegacy.RESTRICT_DISTINCT.matches(af))
                && af.parameters().size() == 2) {
            List<ValueSpecification> cols = af.parameters().get(1) instanceof PureCollection c
                    ? c.values() : List.of(af.parameters().get(1));
            if (!cols.isEmpty() && cols.stream().allMatch(v -> v instanceof CString)) {
                List<ColSpec> specs = cols.stream()
                        .map(v -> new ColSpec(stripQuotes(((CString) v).value()), null, null))
                        .toList();
                AppliedFunction select = new AppliedFunction("select",
                        List.of(af.parameters().get(0),
                                new com.legend.protocol.spec.ColSpecArray(specs)));
                return synth(com.legend.builtin.TdsLegacy.RESTRICT_DISTINCT.matches(af)
                        ? new AppliedFunction("distinct", List.of(select)) : select, env);
            }
        }
        if (com.legend.builtin.NativeFn.TyperForm.EXTRACT_ENUM_VALUE.matches(af.function())
                && af.parameters().size() == 2) {
            TypedSpec folded = extractEnumValueFold(af, env);
            if (folded != null) {
                return folded;
            }
            // not Enumeration-shaped: the generic path types it against
            // the registered signature (loud on mismatch)
        }
        // REAL PURE ROUTES THE RECEIVER'S OWN QUALIFIED PROPERTY FIRST — before
        // a special form of the same name ($schema.join($other) is
        // SchemaState.join, never tds::join; FunctionExpressionProcessor's
        // ordering). Decided from the receiver's KNOWN type — a bound
        // variable, so the common path types nothing twice (Phase 5 batch
        // 147: the engine's schema-resolution hook).
        ExprType rt = !af.parameters().isEmpty() && af.parameters().get(0) instanceof Variable rv
                ? env.lookup(rv.name()).orElse(null) : null;
        if (rt != null) {
            String rcls = rt.type() instanceof Type.ClassType ct ? ct.fqn()
                    : rt.type() instanceof Type.GenericType g ? g.rawFqn() : null;
            // the call may already carry an import-resolved FQN (meta::pure::tds::join):
            // the property is looked up by its simple name
            int sep = af.function().lastIndexOf("::");
            String simple = sep < 0 ? af.function() : af.function().substring(sep + 2);
            if (rcls != null
                    && ctx.findProperty(rcls, simple).orElse(null)
                            instanceof Property.Derived d
                    && d.parameters().size() == af.parameters().size() - 1
                    && rt.multiplicity() instanceof Multiplicity.Bounded rb && !rb.isMany()) {
                java.util.List<ValueSpecification> qargs = new ArrayList<>(af.parameters());
                if (rb.lower() != 1) {
                    qargs.set(0, new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                            List.of(qargs.get(0))));
                }
                return applyGeneric(new AppliedFunction(d.bodyFunctionFqn(), qargs), env);
            }
        }
        Optional<CoreFn> core = CoreFn.of(af.function());
        if (core.isPresent()) {
            // real pure resolves by TYPE: a model function of this name whose
            // first parameter is the receiver's CLASS (Database.join(name),
            // relational.pure) out-ranks the bare special form, which only
            // ever meant relations, tables and class extents
            TypedFunction owned = ReceiverOwnedFunctions.of(this, af, env);
            if (owned != null) {
                return applyGeneric(new AppliedFunction(owned.qualifiedName(),
                        af.parameters()), env);
            }
            return applyCore(core.get(),
                    aliasNormalized(core.get(), af), env);
        }
        // instanceOf(cell, TDSNull): the null-cell type test IS the SQL
        // null test (the engine materializes ^TDSNull() for null cells;
        // tds.pure) — typed as isEmpty so every consumer shares the scalar
        // IS NULL lowering. Exact names only; instanceOf against any other
        // type stays the loud unknown (audit 19d B7 — this transplant
        // lived in the harness's pre-typing substitute).
        if (com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.INSTANCE_OF)
                && af.parameters().size() == 2
                && af.parameters().get(1)
                        instanceof com.legend.protocol.spec.PackageableElementPtr pep
                && (pep.fullPath().equals("meta::pure::tds::TDSNull")
                        || pep.fullPath().equals("TDSNull"))) {
            return synth(new AppliedFunction("isEmpty",
                    List.of(af.parameters().get(0))), env);
        }
        // PARAMETERIZED qualified property: $p.synonymByType(X) routes to the
        // externalized body function <owner>$prop$<name>(this, args...) and
        // β-inlines with every other user call — never shadows a real function
        // OF THIS ARITY (a same-name function family elsewhere — tds::join
        // beside SchemaState.join(other) — does not hide the receiver's
        // property: real pure routes the property first; Phase 5 batch 147)
        if (!af.parameters().isEmpty() && functionCandidates(af).stream()
                .noneMatch(f -> f.parameters().size() == af.parameters().size())) {
            TypedSpec recv = synth(af.parameters().get(0), env);
            String classFqn = recv.info().type() instanceof Type.ClassType ct ? ct.fqn()
                    : recv.info().type() instanceof Type.GenericType g ? g.rawFqn() : null;
            // by the SIMPLE name, as the two sibling routes do: the name
            // resolver qualifies a bare `x.toSQLString(…)` to the same-named
            // FUNCTION's FQN when one is in scope, and the class declares the
            // qualified property under its bare name (batch 5 leg 5c)
            int qcut = af.function().lastIndexOf("::");
            String qname = qcut < 0 ? af.function() : af.function().substring(qcut + 2);
            if (classFqn != null
                    && ctx.findProperty(classFqn, qname).orElse(null)
                            instanceof Property.Derived d
                    && (d.parameters().size() == af.parameters().size() - 1
                            // an OVERLOAD by arity (res() / res(z)) shares the lifted FQN;
                            // the call picks among its signatures like any function
                            || derivedOverloadArity(classFqn, qname,
                                    af.parameters().size() - 1))) {
                // AUTO-MAP: a qualifier call on a MANY receiver applies per
                // element (engine qualified-property auto-map:
                // $o.product($bd).qualifier() over a [*] milestoned read)
                // — rewrite as receiver->map(v|$v.qualifier(args))
                if (recv.info().multiplicity()
                        instanceof Multiplicity.Bounded rb && rb.isMany()) {
                    Variable mv = new Variable("v_qam");
                    java.util.List<ValueSpecification> inner =
                            new ArrayList<>(af.parameters());
                    inner.set(0, mv);
                    return synth(new AppliedFunction("map", List.of(
                            af.parameters().get(0),
                            new LambdaFunction(List.of(mv), List.of(
                                    af.withParameters(inner))))),
                            env);
                }
                // [0..1] receivers run like [1] (the engine's no-guard
                // qualifier doctrine, ledger cluster 48) — spell the
                // conformance with toOne at this synth site.
                java.util.List<ValueSpecification> qargs =
                        new ArrayList<>(af.parameters());
                if (!(recv.info().multiplicity()
                        instanceof Multiplicity.Bounded rb1
                        && rb1.lower() == 1)) {
                    qargs.set(0, new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                            List.of(qargs.get(0))));
                }
                return applyGeneric(new AppliedFunction(d.bodyFunctionFqn(),
                        qargs), env);
            }
            // MILESTONED property functions (real pure GENERATES these on
            // ends targeting a temporal class): prop(date) — point access;
            // propAllVersions() — version sweep; propAllVersionsInRange(s, e).
            if (classFqn != null) {
                String name = af.function();
                String base = name;
                boolean sweep = false;
                int wantDates = 1;
                if (name.endsWith("AllVersionsInRange")) {
                    base = name.substring(0, name.length() - "AllVersionsInRange".length());
                    sweep = true;
                    wantDates = 2;
                } else if (name.endsWith("AllVersions")) {
                    base = name.substring(0, name.length() - "AllVersions".length());
                    sweep = true;
                    wantDates = 0;
                }
                var prop = ctx.findProperty(classFqn, base).orElse(null);
                String targetFqn = prop != null
                        && prop.type() instanceof Type.ClassType pct ? pct.fqn() : null;
                com.legend.compiler.element.MilestoningStrategy targetStrat
                        = targetFqn == null ? null
                        : com.legend.compiler.element.Temporal.strategyOf(ctx, targetFqn);
                boolean arityOk = af.parameters().size() - 1 == wantDates;
                if (targetStrat == com.legend.compiler.element
                        .MilestoningStrategy.BITEMPORAL && !sweep) {
                    // product(processingDate, businessDate) — or the 1-date
                    // generated form (the owner's dimension fills the other)
                    int n2 = af.parameters().size() - 1;
                    arityOk = n2 == 2 || n2 == 1;
                }
                if (targetFqn != null && arityOk && targetStrat != null) {
                    List<TypedSpec> dates = new ArrayList<>();
                    for (int i = 1; i < af.parameters().size(); i++) {
                        dates.add(synth(af.parameters().get(i), env));
                    }
                    var mprop = java.util.Objects.requireNonNull(prop, "prop");
                    return new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            recv, base, dates, sweep,
                            new ExprType(mprop.type(), mprop.multiplicity()));
                }
            }
        }
        return applyGeneric(af, env);
    }

    private static String stripQuotes(String name) {
        return name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    /**
     * The SCHEMA-computing legacy TDS spellings (engine tds.pure host-graph
     * bodies), desugared to modern natives or folded to literals; null when
     * none applies — the caller continues down the ordinary dispatch.
     */
    private @com.legend.Nullable TypedSpec tdsSchemaDesugars(AppliedFunction af, Env env) {
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
            ValueSpecification lit = new StaticFold(this, env).foldToLiteral(af);
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
     * The agg form's column becomes the {p,w,r|$r.col} map lambda with the
     * user's reducer; a bare rank lambda ({@code x|$x->rank()}) becomes the
     * modern window-function call ({@code {p,w,r|$p->rank($w,$r)}}). Null on
     * any other shape — the unknown-function wall stays loud.
     */
    private static @com.legend.Nullable AppliedFunction olapGroupByDesugar(AppliedFunction af) {
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
                && sc.values().stream().allMatch(Typer::isLegacySortKey)) {
            sortKeys.addAll(sc.values());
            i++;
        }
        if (i != ps.size() - 2) {
            return null;
        }
        ValueSpecification op = ps.get(i);
        List<ValueSpecification> overArgs = new ArrayList<>();
        if (!partSpecs.isEmpty()) {
            overArgs.add(new PureCollection(partSpecs));
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
    private static @com.legend.Nullable String legacyRankName(LambdaFunction lam) {
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
    private static boolean rowGetter(AppliedFunction af, com.legend.builtin.NativeFn.RowGetter g) {
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
    private static @com.legend.Nullable AppliedFunction windowColsProjectDesugar(AppliedFunction af) {
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
    private static @com.legend.Nullable AppliedFunction projectWithColumnSubsetDesugar(AppliedFunction af) {
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

    private static @com.legend.Nullable List<String> literalStrings(PureCollection c) {
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
    private static @com.legend.Nullable String literalColName(ValueSpecification v) {
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
    private @com.legend.Nullable TypedSpec tdsRowCellIndexRead(
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
    private static com.legend.compiler.spec.typed.TypedCollection rowCells(
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
    private @com.legend.Nullable TypedSpec extractEnumValueFold(AppliedFunction af, Env env) {
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
        var en = ctx.findEnum(et.fqn()).orElseThrow(() ->
                new TypeInferenceException("unknown enumeration '"
                        + et.fqn() + "'"));
        if (!en.values().contains(nm.value())) {
            throw new TypeInferenceException("enumeration '" + et.fqn()
                    + "' has no value '" + nm.value() + "'");
        }
        return new TypedEnumValue(et.fqn(), nm.value(), ExprType.one(et));
    }

    /** CURATED-alias spellings (tds::distinct, relation::eval) have no
     * FQN-registered native — dispatch under the bare parse name so the
     * checker's checkGeneric resolves candidates. */
    private static AppliedFunction aliasNormalized(CoreFn core, AppliedFunction af) {
        return !af.function().equals(core.parseName())
                && af.function().contains("::")
                && com.legend.builtin.Pure.nativeFunctionsAt(af.function()).isEmpty()
                ? new AppliedFunction(core.parseName(), af.parameters()) : af;
    }

    /**
     * The core-construct dispatch &mdash; exhaustive over {@link CoreFn} (a new
     * construct cannot be added without a rule here), one line per construct: the
     * construct's shape decisions, desugars, and emission live in its
     * {@code *Checker}. An arm owns <em>all</em> overloads of its name; where only
     * some shapes are structural (relation vs collection {@code sort}), its checker
     * delegates the rest back to {@link #applyGeneric}.
     */
    private TypedSpec applyCore(CoreFn fn, AppliedFunction af, Env env) {
        af = CallShapes.expandLetBoundLambdaArgs(af, env);
        return switch (fn) {
            case LET -> LetChecker.check(this, af, env);
            // leg 3b: deactivate(e) is COMPILE-TIME reflection — the node
            // carries the argument's DECLARED type (a match reports its
            // all-branch LUB, never the emission-narrowed selection) and
            // folds away entirely at .genericType.rawType (accessProperty)
            case DEACTIVATE -> {
                if (af.parameters().size() != 1) {
                    throw new TypeInferenceException(
                            "deactivate takes exactly one argument");
                }
                TypedSpec inner = synth(af.parameters().get(0), env);
                ExprType declared = inner instanceof
                        com.legend.compiler.spec.typed.TypedMatch tm
                        ? tm.declared() : inner.info();
                yield new com.legend.compiler.spec.typed.TypedDeactivate(
                        inner, declared, false, ExprType.one(new Type.ClassType(
                                "meta::pure::metamodel::valuespecification::ValueSpecification")));
            }
            case IF -> IfChecker.check(this, af, env);
            // TDG lane S1: compile-time reflection (C1.6, the DEACTIVATE
            // sibling) — the census FOLDS to instance literals here
            case GET_RELATIONAL_CSV_DATA -> CsvCensusChecker.check(this, af, env);
            // TDG lane S2: runtime data extraction — carrier here, the
            // orchestrator executes the fetches and splices literals
            case GENERATE_TEST_DATA -> GenerateTestDataChecker.check(this, af, env);
            case MAY_EXECUTE_ALLOY_TEST, MAY_EXECUTE_LEGEND_TEST ->
                    MayExecuteChecker.check(this, af, env);
            case GENERATE_SEED_DATA_STRING ->
                    GenerateTestDataChecker.checkSeed(this, af, env);
            case PLAN_TEST_DATA_GENERATION ->
                    GenerateTestDataChecker.checkPlan(this, af, env);
            // ^Class(...) desugars to new(PackageableElementPtr, NewInstance); the inner node
            // carries the payload. ^$var(...) (a Variable receiver) is COPY-
            // with-update — the class is the variable's static type. Other
            // arities/shapes of `new` ride the generic path.
            case NEW -> {
                if (af.parameters().size() == 2
                        && af.parameters().get(1) instanceof NewInstance ni) {
                    // the non-copy branch routes through synth's NewInstance
                    // case so BOTH spellings share its arms — the direct
                    // NewChecker call bypassed the ^TDSNull() short-circuit
                    // (41 corpus tests: user-written ^TDSNull() desugars to
                    // new(...) and died at 'unknown class').
                    // COPY dispatch keys on the EMPTY className (the parser
                    // contract for ^$var(...)), NOT on the receiver being a
                    // Variable: the harness let-substitution legitimately
                    // replaces the receiver with the let's RHS expression
                    // (^$runtime(...) after substitute() carries the
                    // testRuntime() call), and checkCopy types any receiver.
                    yield ni.className().isEmpty()
                            ? NewChecker.checkCopy(this, af.parameters().get(0), ni, env)
                            : synth(ni, env);
                }
                yield applyGeneric(af, env);
            }
            case CAST, TO, TO_MANY -> CastChecker.check(this, af, env);
            // typeAsDeclared: the MAPPING-side type assertion (engine
            // parity — the engine types binding reads by the DECLARED
            // property and never casts the SQL). Shape (value, @Type);
            // types as the annotation, KEEPS the value's multiplicity,
            // lowers to the value unchanged (Scalars passthrough).
            case TYPE_AS_DECLARED, CAST_AS_DECLARED -> {
                Application ta = checkGeneric(af, env);
                if (ta.args().size() != 2
                        || !(ta.args().get(1) instanceof
                                com.legend.compiler.spec.typed.TypedTypeRef tr)) {
                    throw new TypeInferenceException(
                            af.function() + " expects (value, @Type)");
                }
                ExprType out = new ExprType(tr.target(),
                        ta.args().get(0).info().multiplicity());
                if (com.legend.builtin.Pure.Lite.CAST_AS_DECLARED.equals(af.function())) {
                    // a WIRE-flagged cast: every TypedCast consumer rides
                    // it unchanged; only the lowering treats it specially
                    yield new com.legend.compiler.spec.typed.TypedCast(
                            ta.args().get(0), tr.target(), out, true);
                }
                var callees = model().findFunction(
                        "meta::legend::lite::typeAsDeclared");
                yield new com.legend.compiler.spec.typed.TypedNativeCall(
                        callees.get(0),
                        List.of(ta.args().get(0)), out);
            }
            case MATCH -> MatchChecker.check(this, af, env);
            case EVAL -> EvalChecker.check(this, af, env);
            case TDS -> TdsChecker.check(this, af, env);
            case SORT_BY -> SortChecker.sortBy(this, af, env, true);
            case SORT_BY_REVERSED -> SortChecker.sortBy(this, af, env, false);
            case GET_ALL -> GetAllChecker.check(this, af, env);
            case GET_ALL_FOR_EACH_DATE ->
                    GetAllChecker.checkForEachDate(this, af, env);
            case GET_ALL_VERSIONS, GET_ALL_VERSIONS_IN_RANGE ->
                    GetAllChecker.checkVersions(this, af, env);
            case FROM -> FromChecker.check(this, af, env);
            case WRITE -> WriteChecker.check(this, af, env);
            case FOLD -> FoldChecker.check(this, af, env);
            case NAVIGATE -> NavigateChecker.check(this, af, env);
            // legacyNavigate: the pre-map rule under the legacy bridge's
            // name, with the target's table rows spelled into the call.
            case LEGACY_NAVIGATE -> NavigateChecker.legacy(this, af, env);
            case GRAPH_FETCH -> GraphFetchChecker.graphFetch(this, af, env);
            case GRAPH_FETCH_CHECKED ->
                    GraphFetchChecker.graphFetchChecked(this, af, env);
            case SERIALIZE -> GraphFetchChecker.serialize(this, af, env);
            case OVER -> OverChecker.check(this, af, env);
            case SOURCE_URL -> SourceUrlChecker.check(this, af, env);
            case FLATTEN -> FlattenChecker.check(this, af, env);
            case PIVOT -> PivotChecker.check(this, af, env);
            case COLUMNS -> ColumnsChecker.check(this, af, env);
            case TO_JSON -> TdsJsonChecker.check(this, af, env);
            case TDS_TO_JSON_KV -> TdsJsonChecker.checkKeyValue(this, af, env);
            case TABLE_REFERENCE -> TableReferenceChecker.check(this, af);
            case TABLE_TO_TDS -> TableReferenceChecker.checkTableToTds(this, af, env);
            case PROJECT -> ProjectChecker.check(this, af, env);
            case EXTEND -> ExtendChecker.check(this, af, env);
            case GROUP_BY -> GroupByChecker.check(this, af, env);
            case GROUP_BY_WITH_WINDOW_SUBSET -> GroupByChecker.checkWindowSubset(this, af, env);
            case AGGREGATE -> AggregateChecker.check(this, af, env);
            case JOIN -> JoinChecker.check(this, af, env);
            case AS_OF_JOIN -> AsOfJoinChecker.check(this, af, env);
            case SORT -> SortChecker.check(this, af, env);
            case ASC -> SortChecker.sortInfo(this, af, env, true);
            case DESC -> SortChecker.sortInfo(this, af, env, false);
            case RENAME -> RenameChecker.check(this, af, env);
            case SELECT -> SelectChecker.check(this, af, env);
            case DISTINCT -> DistinctChecker.check(this, af, env);
            case CONCATENATE -> ConcatenateChecker.check(this, af, env);
            case LIMIT, TAKE -> SlicingChecker.limit(this, af, env);
            case DROP -> SlicingChecker.drop(this, af, env);
            case SLICE -> SlicingChecker.slice(this, af, env);
            case FILTER -> {
                // the meta::json member idiom (keyValuePairs->filter by key)
                // is a JSON access, never a collection filter
                TypedSpec jm = JsonChecker.filter(this, af, env);
                yield jm != null ? jm : FilterChecker.check(this, af, env);
            }
            case MAP -> MapChecker.check(this, af, env);
        };
    }

    /**
     * The one generic application rule (engine {@code ScalarChecker}): type the
     * arguments, resolve the overload against the registered signatures, and read
     * the output from the resolved return (§5) &mdash; emitting the plain call
     * node. Checkers whose non-structural overloads ride this path (collection
     * {@code sort}, non-{@code ^} {@code new}) call it directly.
     */
    TypedSpec applyGeneric(AppliedFunction af, Env env) {
        com.legend.protocol.spec.ValueSpecification coerced = CallShapes.toMultiplicityDesugar(af);
        if (coerced != null) {
            return synth(coerced, env);
        }
        TypedSpec autoMapped = CallShapes.autoMapReceiver(this, af, env);
        if (autoMapped != null) {
            return autoMapped;
        }
        Application a = checkGeneric(af, env);
        // format's %s slots print a CLASS-typed argument by its own
        // toString() (real pure's format calls toString per value): rewrite
        // those slots as `$arg->toString()` and type the call again — the
        // receiver's own body runs (derivedShadow); the rewritten slots are
        // Strings, so the second pass finds nothing to rewrite
        AppliedFunction printed = CallShapes.formatSlotsByToString(af, a);
        if (printed != null) {
            return applyGeneric(printed, env);
        }
        // MONOMORPHIZE AT THE APPLICATION: an executed call has its
        // arguments here; inside a stored lambda literal there are none
        // yet, so the call keeps its signature typing (the engine types
        // a closure body by signatures too; it pastes nothing)
        if (requiresNormalization(a.chosen()) && storedLambdas.isEmpty()) {
            return inlineNormalized(af, a.chosen(), env);
        }
        TypedSpec shadow = derivedShadow(af, a, env);
        if (shadow != null) {
            return shadow;
        }
        return rawGridOrSelf(emitCall(a.chosen(), a.args(), a.out(), af.pos()));
    }

    /** Leg 6a — the receiver's OWN qualified property SHADOWS an
     * Any-first native: real pure routes property access FIRST and
     * consults the function library only when no property matched
     * (FunctionExpressionProcessor's ordering) — ClassWithComplexToString
     * declares {@code toString()} and {@code ->toString()} runs IT, not
     * {@code string::toString(Any)}. Decided AFTER one ordinary typing
     * pass (no double typing on the common path): the chosen native's
     * first parameter must be the TOP type (a catch-all — an
     * exact-typed native is more specific than the property and keeps
     * winning), the typed receiver's class must declare the same-name
     * {@code Property.Derived} at matching arity, and the rewrite
     * re-enters through the ordinary externalized-body route (the
     * shadow target is a user function, so no re-shadowing recursion). */
    private @com.legend.Nullable TypedSpec derivedShadow(AppliedFunction af,
            Application a, Env env) {
        if (af.parameters().isEmpty() || !a.chosen().isNative()
                || a.chosen().parameters().isEmpty()
                || !com.legend.compiler.element.type.PlatformTypes.isAny(
                        a.chosen().parameters().get(0).type())) {
            return null;
        }
        TypedSpec recv = a.args().get(0);
        String classFqn = recv.info().type() instanceof Type.ClassType ct ? ct.fqn()
                : recv.info().type() instanceof Type.GenericType g ? g.rawFqn() : null;
        if (classFqn == null
                || recv.info().multiplicity() instanceof Multiplicity.Bounded rb
                        && rb.isMany()) {
            return null;
        }
        // by the function's SIMPLE name: a fully qualified spelling of the
        // native (`->meta::pure::functions::string::toString()`, the PCT
        // printer's form) names the same call and real pure routes the
        // receiver's own toString() all the same (testPairToString, batch 152)
        int cut = af.function().lastIndexOf("::");
        String simple = cut < 0 ? af.function() : af.function().substring(cut + 2);
        if (!(ctx.findProperty(classFqn, simple).orElse(null)
                        instanceof Property.Derived d)
                || d.parameters().size() != af.parameters().size() - 1) {
            return null;
        }
        java.util.List<ValueSpecification> qargs = new ArrayList<>(af.parameters());
        if (!(recv.info().multiplicity() instanceof Multiplicity.Bounded rb1
                && rb1.lower() == 1)) {
            // the same [0..1]-runs-like-[1] conformance spelling as the
            // main derived route (ledger cluster 48)
            qargs.set(0, new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                    List.of(qargs.get(0))));
        }
        return applyGeneric(new AppliedFunction(d.bodyFunctionFqn(), qargs), env);
    }

    /** Phase 1c (One-Platform Plan): {@code executeInDb} with a LITERAL
     * single READ — and a {@code fetchDb*} catalog call with literal
     * patterns — types as the RELATION it produces, columns late-bound
     * (the dynamic-pivot rule; the execution boundary stamps the real
     * schema). The spec surface stays byte-identical
     * ({@code executeInDb: ResultSet[1]}); the TYPE is the platform's,
     * exactly as TDS types as its relation. Statement shapes (DDL, DML,
     * multi-statement blobs) keep the declared opaque handle — they are
     * EFFECTS and ride the execute-once path. */
    private TypedSpec rawGridOrSelf(TypedSpec call) {
        if (!(call instanceof TypedNativeCall nc)) {
            return call;
        }
        String fqn = nc.callee().qualifiedName();
        String sql = null;
        if ((com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB.equals(fqn)
                    || com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB_TO_TDS.equals(fqn))
                && !nc.args().isEmpty()
                && nc.args().get(0) instanceof
                        com.legend.compiler.spec.typed.TypedCString lit
                && com.legend.sql.RawSql.isSingleQuery(lit.value())) {
            sql = com.legend.sql.RawSql.splitStatements(lit.value()).get(0);
        } else if (com.legend.builtin.NativeFn.Carrier.fetchDbGrid(fqn) != null) {
            sql = CatalogGrids.sql(nc);
            if (sql != null) {
                // §4bZ-U leg 4: the JDBC spec fixes the metadata result
                // shape and the catalog projections are OURS — a
                // DECLARED table-function schema, never late-bound (no
                // LIMIT-0 probe, reads stamp statically)
                return new com.legend.compiler.spec.typed
                        .TypedRawSqlRelation(sql, ExprType.one(
                                Type.relation(CatalogGrids.gridSchema(java.util.Objects.requireNonNull(com.legend.builtin.NativeFn.Carrier.fetchDbGrid(fqn))))));
            }
        }
        if (sql == null) {
            return call;
        }
        return new com.legend.compiler.spec.typed.TypedRawSqlRelation(
                sql, ExprType.one(Type.relation(Type.RelationType.lateBound())));
    }

    /**
     * Engine {@code <<functionType.NormalizeRequiredFunction>>} doctrine: a
     * TDS-erased module function's body COMPUTES the plan (its schema
     * expressions read {@code .columns} facts and build colspecs), so it is
     * normalized away at its CALL SITE — β-substitute the raw arguments into
     * the parsed body, statically fold the schema vocabulary
     * ({@link StaticFold}), and type the result in the caller's env. The
     * gate is the stereotype, plus the TDS-erased helper shape those
     * functions call privately ({@code TDSColumn[*]} params —
     * {@code extendMatchColumns}): a signature over the schema-erasing
     * nominals cannot type standalone, only monomorphized.
     */
    private boolean requiresNormalization(TypedFunction f) {
        if (f.isNative() || f.body().isEmpty() || f.definition() == null) {
            return false;
        }
        boolean stereotyped = f.definition().stereotypes().stream()
                .anyMatch(s -> s.stereotypeName().equals("NormalizeRequiredFunction"));
        return stereotyped || f.parameters().stream()
                .anyMatch(p -> isSchemaErased(p.type()))
                || isSchemaErased(f.returnType());
    }

    private static boolean isSchemaErased(com.legend.compiler.element.type.Type t) {
        // a FUNCTION over the erased nominals is itself erased — a helper
        // returning Function<{TDSRow[1]->Boolean[1]}> exists only inlined
        // (bare or Function<{...}>-wrapped alike)
        // the ERASED ROW itself (TDSRow in a type position — PlatformTypes.eraseTdsRow)
        if (t instanceof com.legend.compiler.element.type.Type.RelationType r && r.isLateBound()) {
            return true;
        }
        com.legend.compiler.element.type.Type.FunctionType ft = asFunctionType(t);
        if (ft != null) {
            return ft.params().stream().anyMatch(p -> isSchemaErased(p.type()))
                    || isSchemaErased(ft.result().type());
        }
        String raw = switch (t) {
            case com.legend.compiler.element.type.Type.ClassType c -> c.fqn();
            case com.legend.compiler.element.type.Type.GenericType g -> g.rawFqn();
            default -> null;
        };
        return com.legend.compiler.element.type.PlatformTypes.TABULAR_DATA_SET.equals(raw)
                || com.legend.compiler.element.type.PlatformTypes.TDS_ROW.equals(raw)
                || "meta::pure::tds::TDSColumn".equals(raw)
                // column specs are PLAN vocabulary — a spec-building helper
                // (getCols():ColumnSpecification<T>[*]) exists only inlined.
                // The bare spellings appear because module signatures keep
                // the IMPORT-scoped name unresolved (corpus testSimple.pure
                // declares `ColumnSpecification<Person>` under an import) —
                // retire them when signature types resolve through imports.
                || "meta::pure::tds::ColumnSpecification".equals(raw)
                || "meta::pure::tds::BasicColumnSpecification".equals(raw)
                || "ColumnSpecification".equals(raw)
                || "BasicColumnSpecification".equals(raw)
                // aggregate specs are the same plan vocabulary (legacy
                // groupBy's agg(mapFn, aggFn) literals)
                || "meta::pure::functions::collection::AggregateValue".equals(raw)
                || "AggregateValue".equals(raw);
    }

    private final java.util.ArrayDeque<String> normalizing = new java.util.ArrayDeque<>();
    /** A record FIELD's value: a lambda literal there is STORED — it has no
     * application at this site (a let-bound lambda the same body applies
     * does, and keeps the eager paste: the TDS-extension programs rely on
     * it — measured, batch 147). */
    TypedSpec synthRecordField(ValueSpecification v, Env env) {
        if (!(v instanceof LambdaFunction lf)) {
            return synth(v, env);
        }
        storedLambdas.push(lf);
        try {
            return synth(v, env);
        } finally {
            storedLambdas.pop();
        }
    }

    /** The RECORD-FIELD lambda literals enclosing the current position (an
     * explicit frame stack, like {@code normalizing}). Monomorphization
     * needs an application: inside such a literal a schema-dependent helper
     * call is typed by its signature and pasted only when the closure is
     * applied (Phase 5 batch 147, row 15). */
    private final java.util.ArrayDeque<LambdaFunction> storedLambdas = new java.util.ArrayDeque<>();

    private TypedSpec inlineNormalized(AppliedFunction af, TypedFunction chosen, Env env) {
        String key = chosen.signatureKey();
        if (normalizing.contains(key)) {
            throw new TypeInferenceException("recursive NormalizeRequired function '"
                    + chosen.qualifiedName() + "' cannot be inlined ("
                    + String.join(" -> ", normalizing) + " -> " + key + ")");
        }
        LambdaFunction folded = SourceSubst.inlineLets(
                new LambdaFunction(List.of(),
                        chosen.body().orElseThrow()));
        if (folded == null) {
            throw new TypeInferenceException("NormalizeRequired function '"
                    + chosen.qualifiedName()
                    + "' has non-let intermediate statements — cannot inline");
        }
        java.util.Map<String, ValueSpecification> subst = new java.util.LinkedHashMap<>();
        for (int i = 0; i < chosen.parameters().size(); i++) {
            subst.put(chosen.parameters().get(i).name(), af.parameters().get(i));
        }
        // α-hygiene (the UserCallInliner rule at source level): the body's
        // lambda binders rename to fresh _nr<N> — a caller variable spelled
        // like a body binder (corpus: `let r = execute(...)` vs the body's
        // `filter(r:TDSRow[1]|…)`) must never be captured by the splice.
        ValueSpecification body = SourceSubst.substitute(
                alpha.apply(folded.body().get(0)), subst);
        normalizing.push(key);
        try {
            return synth(new StaticFold(this, env).fold(body), env);
        } finally {
            normalizing.pop();
        }
    }

    /** A helper CALL returning a function value over the schema-erasing TDS
     * nominals expands to its lambda literal IN ARGUMENT POSITION — the
     * literal types against the surrounding signature like any inline lambda
     * (a TDSRow annotation refines nothing; a function VALUE over TDSRow can
     * never unify with a row-bound type variable). */
    private AppliedFunction expandFunctionValuedHelperArgs(AppliedFunction af) {
        List<ValueSpecification> np = null;
        for (int i = 0; i < af.parameters().size(); i++) {
            if (!(af.parameters().get(i) instanceof AppliedFunction call)
                    || !functionValuedHelperCall(call)) {
                continue;
            }
            ValueSpecification ex = rawSchemaErasedExpansion(call);
            if (ex == null) {
                continue;
            }
            if (np == null) {
                np = new ArrayList<>(af.parameters());
            }
            np.set(i, ex);
        }
        return np == null ? af : af.withParameters(np);
    }

    /** A helper CALL returning a function value over the schema-erasing TDS
     * nominals — what {@link #rawSchemaErasedExpansion} expands. */
    boolean functionValuedHelperCall(AppliedFunction call) {
        return functionCandidates(call).stream()
                .filter(c -> c.parameters().size() == call.parameters().size())
                .anyMatch(c -> {
                    Type.FunctionType ft = asFunctionType(c.returnType());
                    return ft != null && isSchemaErased(ft);
                });
    }

    /**
     * RAW β-expansion of a schema-erased helper call in a SPEC position
     * ({@code project(getCols())} — the col() literals must reach the
     * checker's SHAPE normalization, so typed inlining is too late).
     * Exactly one arity-matching NormalizeRequired candidate with a body
     * expands; anything else returns null and the checker's wall stands.
     */
    @com.legend.Nullable ValueSpecification rawSchemaErasedExpansion(ValueSpecification v) {
        if (!(v instanceof AppliedFunction af)) {
            return null;
        }
        List<TypedFunction> arityCands = functionCandidates(af).stream()
                .filter(c -> c.parameters().size() == af.parameters().size())
                .toList();
        // a NATIVE overload owns the call (concatenateTemporalTdsQueries:
        // the corpus re-definition is M3-reflective plan surgery; the
        // registered native is the platform's semantics) — never expand
        if (arityCands.stream().anyMatch(TypedFunction::isNative)) {
            return null;
        }
        // NormalizeRequired bodies, and any body RETURNING a function value
        // over the TDS nominals (a private accessor helper's literal must
        // reach its consumer's shape rules)
        List<TypedFunction> cands = arityCands.stream()
                .filter(c -> requiresNormalization(c)
                        || (c.body().isPresent()
                                && asFunctionType(c.returnType()) instanceof Type.FunctionType rft
                                && isSchemaErased(rft)))
                .toList();
        if (System.getenv("LEGEND_LITE_RAW_EXPAND_TRACE") != null) {
            System.err.println("[raw-expand] " + af.function() + " cands="
                    + cands.size() + " all=" + functionCandidates(af).stream()
                            .map(c -> c.qualifiedName() + " ret="
                                    + c.returnType().typeName()).toList());
        }
        if (cands.size() != 1) {
            return null;
        }
        TypedFunction chosen = cands.get(0);
        LambdaFunction folded = SourceSubst.inlineLets(
                new LambdaFunction(List.of(),
                        chosen.body().orElseThrow()));
        if (folded == null) {
            return null;
        }
        java.util.Map<String, ValueSpecification> subst = new java.util.LinkedHashMap<>();
        for (int i = 0; i < chosen.parameters().size(); i++) {
            subst.put(chosen.parameters().get(i).name(), af.parameters().get(i));
        }
        return SourceSubst.substitute(
                alpha.apply(folded.body().get(0)), subst);
    }

    /** Fresh binder names for inlined bodies (one counter per typer). */
    private final AlphaRename alpha = new AlphaRename();

    /** α-hygiene for a body about to be spliced under caller scope (the
     * UserCallInliner rule at source level) — shared with StaticFold's
     * user-call inlining. */
    ValueSpecification alphaRename(ValueSpecification v) {
        return alpha.apply(v);
    }

    /** The generic application rule without emitting a node — the CHECK
     * half of the check/emit split ({@link Application}); calls with
     * deferred arguments take {@link #checkWithDeferred}. */
    Application checkGeneric(AppliedFunction af, Env env) {
        return checkGeneric(af, env, null);
    }

    /** {@link #checkGeneric(AppliedFunction, Env)} with the caller's EXPECTED
     *  type of the call's value (bidirectional inference — the deferred slot
     *  of an enclosing call knows the parameter type it is filling). */
    Application checkGeneric(AppliedFunction af, Env env, @com.legend.Nullable Type expected) {
        af = expandFunctionValuedHelperArgs(af);
        if (af.parameters().stream().anyMatch(DeferredArgs::deferredArg)) {
            return checkWithDeferred(af, env);
        }
        List<TypedSpec> args = new ArrayList<>(af.parameters().size());
        for (ValueSpecification p : af.parameters()) {
            args.add(synth(p, env));
        }
        markOperatorRun(af.infix(), args);
        return checkGenericTyped(af, args, expected);
    }

    /** The generic check over ALREADY-TYPED arguments (ConcatenateChecker
     * reads a type before choosing its rule; each argument synths ONCE —
     * a second synth re-registers typer state: TDS literals, plan params). */
    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args) {
        return checkGenericTyped(af, args, null);
    }

    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args,
            @com.legend.Nullable Type expected) {
        List<ExprType> argTypes = args.stream().map(TypedSpec::info).toList();
        List<TypedFunction> candidates = functionCandidates(af);
        if (candidates.isEmpty()) {
            // C0.5a: zero candidates = the name is NOT IN THE CATALOG (an
            // unported platform function, usually) — say so plainly
            throw new TypeInferenceException("unknown function '"
                    + af.function() + "' — no function of this name in the"
                    + " native or user catalog (unported platform function,"
                    + " or a misspelling)");
        }
        InferenceKernel.Resolution r = kernel.resolveOverload(candidates, argTypes, expected);
        return new Application(r.chosen(), args,
                refineParseDate(r.chosen(), args, refineDecimalCarrier(r.chosen(), r.output())));
    }

    /** parseDate over a LITERAL refines its abstract Date output to the
     * kind the string's shape determines ('...T...' = DateTime, bare =
     * StrictDate) — real pure returns the written kind; the abstract root
     * cannot tell a midnight DateTime from a StrictDate at the wire. */
    private static ExprType refineParseDate(TypedFunction chosen, List<TypedSpec> args, ExprType out) {
        if (out.type() == com.legend.compiler.element.type.Type.Primitive.DATE
                && "meta::pure::functions::string::parseDate".equals(chosen.qualifiedName())
                && args.size() == 1
                && args.get(0) instanceof com.legend.compiler.spec.typed.TypedCString s) {
            String v = s.value().trim();
            if (v.matches("-?\\d{4,}-\\d{2}-\\d{2}[T ]\\d.*")) {
                return new ExprType(
                        com.legend.compiler.element.type.Type.Primitive.DATE_TIME,
                        out.multiplicity());
            }
            if (v.matches("-?\\d{4,}-\\d{2}-\\d{2}")) {
                return new ExprType(
                        com.legend.compiler.element.type.Type.Primitive.STRICT_DATE,
                        out.multiplicity());
            }
            return out;   // partial or exotic shapes keep the abstract Date
        }
        return out;
    }

    /** Build the call node for the chosen overload &mdash; the resolved callee rides the node, never a name. */
    static TypedSpec emitCall(TypedFunction chosen, List<TypedSpec> args, ExprType out) {
        return emitCall(chosen, args, out, null);
    }

    /** The parsed-call form: the source span (the call-NAME token, the parser's
     * named-call convention) rides the native node — the raise-emission
     * provenance channel (leg 2). */
    static TypedSpec emitCall(TypedFunction chosen, List<TypedSpec> args, ExprType out,
            com.legend.protocol.@com.legend.Nullable SourceInfo pos) {
        return chosen.isNative()
                ? NormalizeFolds.foldReflection(new TypedNativeCall(chosen, args, out, pos))
                : new TypedUserCall(chosen, args, out);
    }

    /**
     * A call carrying <em>deferred</em> arguments &mdash; lambdas and mapped column
     * specifications ({@code ~alias:x|…}), whose types are not known until the
     * surrounding call is partly resolved (the bidirectional step, §3.4). So: type
     * the value args, pick the overload from them (plus the deferred args'
     * <em>syntactic shape</em>), solve its type variables, then type each deferred
     * slot against its now-concrete parameter &mdash; a lambda against its function
     * type (binding any unbound return variable, e.g. {@code map}'s {@code V}), a
     * mapped colspec against its {@code FuncColSpec<F,Z>} (binding {@code Z} from
     * the checked lambda bodies).
     */
    private Application checkWithDeferred(AppliedFunction af, Env env) {
        List<ValueSpecification> raw = af.parameters();
        List<TypedFunction> candidates = functionCandidates(af);
        List<TypedFunction> arity = candidates.stream()
                .filter(c -> c.parameters().size() == raw.size())
                .filter(c -> DeferredArgs.shapesMatch(this, c, raw))
                .toList();
        if (arity.isEmpty()) {
            throw new TypeInferenceException("no overload of '" + af.function()
                    + "' matches " + raw.size() + " argument(s) of these shapes"
                    + (candidates.isEmpty() ? " (no candidates at all)"
                            : " — candidates: " + candidates.stream()
                                    .map(c -> c.qualifiedName() + "/"
                                            + c.parameters().size())
                                    .distinct().toList()));
        }

        TypedSpec[] typed = new TypedSpec[raw.size()];
        for (int i = 0; i < raw.size(); i++) {
            if (!DeferredArgs.deferredArg(raw.get(i))) {
                typed[i] = synth(raw.get(i), env);   // value args first
            }
        }

        // REAL Pure searches with rollback (FunctionExpressionProcessor):
        // when the best-scored candidate dies typing a DEFERRED slot
        // (lambda/colspec), the next candidate gets its turn — selection
        // previously COMMITTED on non-lambda args and a deferred-slot
        // mismatch was a hard failure even with a fitting overload next
        // in line (study #14; program meaning depended on source order).
        List<TypedFunction> ranked =
                selectRankedByPresentArgs(af.function(), arity, typed, raw);
        TypeInferenceException firstFailure = null;
        for (TypedFunction cand : ranked) {
            try {
                return bindDeferredAndBuild(cand, raw, typed.clone(), env, af.infix());
            } catch (SchemaInvariantException invariant) {
                throw invariant;   // the program's defect, never a
                                   // candidate mismatch — no retry
            } catch (TypeInferenceException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        throw java.util.Objects.requireNonNull(firstFailure);
    }

    /** The post-selection phase: unify present args, type deferred slots
     *  against the candidate, resolve the output. Throws
     *  {@link TypeInferenceException} when the candidate cannot host the
     *  deferred arguments — the caller's retry loop moves on. */
    private Application bindDeferredAndBuild(TypedFunction chosen,
            List<ValueSpecification> raw, TypedSpec[] typed, Env env, boolean infix) {
        Bindings b = new Bindings();
        for (int i = 0; i < raw.size(); i++) {
            if (typed[i] != null) {
                kernel.unify(chosen.parameters().get(i).type(), typed[i].info().type(), b);
                kernel.unifyMult(chosen.parameters().get(i).multiplicity(),
                        typed[i].info().multiplicity(), typed[i].info().type(), b);
            }
        }

        for (int i = 0; i < raw.size(); i++) {
            if (typed[i] == null) {
                if (DeferredArgs.isOverCall(raw.get(i))) {
                    // the expected window type, its T resolved from the bindings the
                    // relation argument made — real pure's context inference
                    Type expected = kernel.resolve(chosen.parameters().get(i).type(), b);
                    typed[i] = OverChecker.check(this, (AppliedFunction) raw.get(i), env, expected);
                    kernel.unify(chosen.parameters().get(i).type(), typed[i].info().type(), b);
                    kernel.unifyMult(chosen.parameters().get(i).multiplicity(),
                            typed[i].info().multiplicity(), typed[i].info().type(), b);
                    continue;
                }
                if (raw.get(i) instanceof LambdaFunction
                        || (DeferredArgs.isLambdaCollection(raw.get(i))
                                && (chosen.parameters().get(i).type()
                                        instanceof Type.TypeVar
                                    || com.legend.compiler.element.type.PlatformTypes
                                            .isAny(chosen.parameters().get(i).type())))) {
                    // A NOMINAL function carrier (FunctionDefinition<Any> —
                    // no structural signature to solve the lambda's params
                    // from) types like a TypeVar slot: the lambda is
                    // self-typable, then the carrier lattice judges
                    // (LambdaFunction ≤ FunctionDefinition).
                    // — and a lambda COLLECTION against Any (upstream's size(Any[*])
                    // over lambdas): the values type themselves, Any takes them
                    if (chosen.parameters().get(i).type()
                            instanceof Type.TypeVar
                            || nominalFunctionCarrier(
                                    chosen.parameters().get(i).type())
                            || (DeferredArgs.isLambdaCollection(raw.get(i))
                                    && com.legend.compiler.element.type.PlatformTypes
                                            .isAny(chosen.parameters().get(i).type()))) {
                        // self-typable lambda against T: synthesize
                        // standalone, bind the variable to its type
                        typed[i] = synth(raw.get(i), env);
                        kernel.unify(chosen.parameters().get(i).type(),
                                typed[i].info().type(), b);
                        kernel.unifyMult(chosen.parameters().get(i).multiplicity(),
                                typed[i].info().multiplicity(),
                                typed[i].info().type(), b);
                        continue;
                    }
                    if (!(raw.get(i) instanceof LambdaFunction lam)) {
                        throw new IllegalStateException("typer bug: lambda"
                                + " collection against a non-variable"
                                + " non-function param slipped the shape gate");
                    }
                    // the TOP type takes a self-typed lambda as a VALUE
                    // (cast(lambda, @FunctionDefinition<Any>)): no signature
                    // to type against — the literal types itself
                    if (chosen.parameters().get(i).type() instanceof Type.ClassType ac0
                            && ac0.fqn().equals(com.legend.compiler.element.type.PlatformTypes.ANY)) {
                        typed[i] = synth(lam, env);
                        kernel.unifyMult(chosen.parameters().get(i).multiplicity(),
                                typed[i].info().multiplicity(),
                                typed[i].info().type(), b);
                        continue;
                    }
                    typed[i] = typeLambda(lam, chosen.parameters().get(i).type(), b, env);
                } else if (DeferredArgs.isLambdaCollection(raw.get(i))) {
                    // pure [f] ≡ f in call position — each element types
                    // against the chosen signature's function parameter
                    // (the corpus's filter([t|...]) / project([λ..], names));
                    // a SINGLETON collapses to the bare lambda (downstream
                    // consumers dispatch on TypedLambda)
                    PureCollection pc = (PureCollection) raw.get(i);
                    List<TypedSpec> els = new ArrayList<>(pc.values().size());
                    for (ValueSpecification v : pc.values()) {
                        els.add(typeLambda((LambdaFunction) v,
                                chosen.parameters().get(i).type(), b, env));
                    }
                    typed[i] = els.size() == 1 ? els.get(0)
                            : new TypedCollection(els, new ExprType(
                                    els.get(0).info().type(),
                                    new Multiplicity.Bounded(els.size(), els.size())));
                } else if (genericRawIs(chosen.parameters().get(i).type(),
                        com.legend.compiler.element.type.PlatformTypes.COL_SPEC_ARRAY)) {
                    // an empty colspec array chosen against a PLAIN
                    // ColSpecArray param types as an ordinary value (it
                    // deferred only because its flavor was parameter-
                    // determined) — unify like the value loop would have
                    typed[i] = synth(raw.get(i), env);
                    kernel.unify(chosen.parameters().get(i).type(),
                            typed[i].info().type(), b);
                    kernel.unifyMult(chosen.parameters().get(i).multiplicity(),
                            typed[i].info().multiplicity(), typed[i].info().type(), b);
                } else {
                    typed[i] = typeFuncColSpec(raw.get(i),
                            chosen.parameters().get(i).type(), b, env);
                }
            }
        }

        ExprType out = kernel.resolveOutput(chosen.returnType(), chosen.returnMultiplicity(), b,
                java.util.Arrays.stream(typed).map(TypedSpec::info).toList());
        List<TypedSpec> typedArgs = new ArrayList<>(List.of(typed));
        markOperatorRun(infix, typedArgs);
        return new Application(chosen, typedArgs,
                refineImportDataFlow(chosen, raw, typed, env, refineDecimalCarrier(chosen, out)));
    }

    /** The execute exeCtx overload under {@code importDataFlow}: the
     * Result's relation gains the union's key threads the executed
     * projection carries ({@link ImportDataFlow}) — a refinement of the
     * signature's output from the call's own facts, like the Decimal
     * carrier. */
    private ExprType refineImportDataFlow(TypedFunction chosen, List<ValueSpecification> raw,
            TypedSpec[] typed, Env env, ExprType out) {
        if (raw.size() != 5
                || !Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY.signatureKey()
                        .equals(chosen.signatureKey())
                || !ImportDataFlow.requested(env.resolveAlias(raw.get(3)), typed[3])) {
            return out;
        }
        if (!(typed[1] instanceof com.legend.compiler.spec.typed.TypedPackageableRef mref)) {
            throw new com.legend.error.NotImplementedException("importDataFlow: the execute"
                    + " call's mapping must be a mapping reference");
        }
        return ImportDataFlow.widen(out, ImportDataFlow.columns(mref.fullPath(), typed[0], ctx));
    }

    /**
     * Decimal-PRODUCING conversions refine their declared bare Decimal to
     * the carrier precision — the engine's Decimal(38,18); a refinement of
     * the registered signature's output, never a bypass of its checks.
     */
    private static ExprType refineDecimalCarrier(TypedFunction chosen, ExprType out) {
        if (out.type() == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                && DECIMAL_CARRIER_PRODUCERS.contains(chosen.qualifiedName())) {
            return new ExprType(
                    new com.legend.compiler.element.type.Type.PrecisionDecimal(38, 18),
                    out.multiplicity());
        }
        return out;
    }

    private static final java.util.Set<String> DECIMAL_CARRIER_PRODUCERS = java.util.Set.of(
            "meta::pure::functions::string::parseDecimal",
            "meta::pure::functions::math::toDecimal");

    boolean isFunctionTyped(Type t) {
        return t instanceof Type.FunctionType
                || (t instanceof Type.GenericType g && g.arguments().size() == 1
                        && g.arguments().get(0) instanceof Type.FunctionType)
                // an m3 Function SUBCLASS value (Property<U,V|m>) is a
                // function value: the kernel reads its instantiated supertype
                || kernel.functionTypeOf(t).isPresent()
                // FunctionDefinition<Any> etc: the whole carrier family is
                // function-typed even when the argument is not spelled as
                // a function type (E2E §4.4 cluster 2 — the kernel already
                // names FUNCTION_CARRIER_FQNS)
                || (t instanceof Type.GenericType g2
                        && com.legend.compiler.spec.InferenceKernel
                                .FUNCTION_CARRIER_FQNS.contains(g2.rawFqn()));
    }

    private static boolean genericRawIs(Type t, com.legend.model.ClassDefinition def) {
        return genericRawIs(t, def.qualifiedName());
    }

    static boolean genericRawIs(Type t, String rawFqn) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(rawFqn);
    }

    /** A function-carrier formal whose argument is NOMINAL, not a structural
     * signature ({@code FunctionDefinition<Any>}): function-typed, but with
     * no parameter shapes to drive a deferred lambda — the lambda
     * self-types and the carrier lattice judges conformance. */
    private static boolean nominalFunctionCarrier(Type t) {
        return t instanceof Type.GenericType g
                && com.legend.compiler.spec.InferenceKernel
                        .FUNCTION_CARRIER_FQNS.contains(g.rawFqn())
                && com.legend.compiler.element.type.PlatformTypes
                        .functionTypeOf(t) == null;
    }

    /** Candidates best-score-first (stable — declaration order breaks
     *  ties, preserving first-max semantics for the winner); arity
     *  misfits filtered; empty = the same loud no-overload error. */
    private List<TypedFunction> selectRankedByPresentArgs(String name,
            List<TypedFunction> arity, TypedSpec[] typed,
            @com.legend.Nullable List<ValueSpecification> raw) {
        List<ExprType> argTypes = new ArrayList<>(typed.length);
        for (TypedSpec t : typed) {
            argTypes.add(t == null ? null : t.info());
        }
        record Scored(TypedFunction fn, long score, int declIdx) {
        }
        List<Scored> scored = new ArrayList<>();
        String arityRejection = null;
        for (int i = 0; i < arity.size(); i++) {
            TypedFunction c = arity.get(i);
            if (raw != null && !lambdaAritiesFit(c, raw, typed)) {
                if (arityRejection == null) {
                    arityRejection = lambdaArityMismatch(c, raw, typed);
                }
                continue;
            }
            scored.add(new Scored(c, kernel.scoreNonLambda(c, argTypes), i));
        }
        if (scored.isEmpty()) {
            throw new TypeInferenceException(
                    "no overload of '" + name + "' matches the argument types"
                            + (arityRejection != null
                                    ? " (" + arityRejection + ")" : ""));
        }
        scored.sort(java.util.Comparator
                .comparingLong(Scored::score).reversed()
                .thenComparingInt(Scored::declIdx));
        return scored.stream().map(Scored::fn).toList();
    }

    /** Whether every DEFERRED lambda argument's parameter count fits the
     * candidate's function-typed parameter at that slot. TypeVar params
     * accept any lambda (self-typable); a non-function param facing a
     * lambda rejects the candidate. */
    private static boolean lambdaAritiesFit(TypedFunction c,
            List<ValueSpecification> raw, TypedSpec[] typed) {
        for (int i = 0; i < raw.size() && i < c.parameters().size(); i++) {
            if (typed[i] != null) {
                continue;
            }
            Type pt = c.parameters().get(i).type();
            if (pt instanceof Type.TypeVar || nominalFunctionCarrier(pt)) {
                // no structural signature to fit against — self-typable slot
                continue;
            }
            Integer want;
            try {
                want = extractFunctionType(pt).params().size();
            } catch (TypeInferenceException e) {
                // a deferred LAMBDA against a non-function, non-variable
                // param can never type — except the TOP type: a lambda
                // IS an Any (cast(lambda, @FunctionDefinition<Any>))
                if (raw.get(i) instanceof LambdaFunction
                        && !(com.legend.compiler.element.type.PlatformTypes.isAny(pt))) {
                    return false;
                }
                continue;
            }
            if (raw.get(i) instanceof LambdaFunction lf
                    && lf.parameters().size() != want) {
                return false;
            }
            if (raw.get(i) instanceof PureCollection pc) {
                for (ValueSpecification v : pc.values()) {
                    if (v instanceof LambdaFunction lf2
                            && lf2.parameters().size() != want) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /** The first lambda-vs-function-type parameter-count mismatch that
     * made {@code lambdaAritiesFit} reject {@code c}, spelled for the
     * no-overload diagnostic ("lambda has 2 parameter(s) but the
     * function type expects 1"). */
    private static @com.legend.Nullable String lambdaArityMismatch(
            TypedFunction c, List<ValueSpecification> raw, TypedSpec[] typed) {
        for (int i = 0; i < raw.size() && i < c.parameters().size(); i++) {
            if (typed[i] != null
                    || c.parameters().get(i).type() instanceof Type.TypeVar) {
                continue;
            }
            int want;
            try {
                want = extractFunctionType(c.parameters().get(i).type())
                        .params().size();
            } catch (TypeInferenceException e) {
                continue;
            }
            LambdaFunction lf = raw.get(i) instanceof LambdaFunction l ? l
                    : raw.get(i) instanceof PureCollection pc
                            ? pc.values().stream()
                                    .filter(v -> v instanceof LambdaFunction l2
                                            && l2.parameters().size() != want)
                                    .map(v -> (LambdaFunction) v)
                                    .findFirst().orElse(null)
                            : null;
            if (lf != null && lf.parameters().size() != want) {
                return "lambda has " + lf.parameters().size()
                        + " parameter(s) but the function type expects " + want;
            }
        }
        return null;
    }

    TypedSpec synthBody(LambdaFunction lam, Env scope) {   // LambdaBodies: the one body rule
        return LambdaBodies.synthBody(this, lam, scope);
    }

    /** Type a lambda argument against its function-type parameter, with type vars partly solved in {@code b}. */
    TypedSpec typeLambda(LambdaFunction lam, Type functionParamType, Bindings b, Env env) {
        Type.FunctionType ftype = extractFunctionType(functionParamType);
        boolean multiStatement = false;
        if (ftype.params().size() != lam.parameters().size()) {
            throw new TypeInferenceException("lambda has " + lam.parameters().size()
                    + " parameter(s) but the function type expects " + ftype.params().size());
        }
        if (lam.body().size() != 1 && !lam.parameters().isEmpty()) {
            // A parameterized lambda's [let*, final] body FOLDS by
            // source-level let-inlining (pure lets are value bindings —
            // β-substitution is exact, and the single-expression result
            // drops nothing at lowering). Non-let intermediates stay loud.
            LambdaFunction folded = SourceSubst.inlineLets(lam);
            if (folded == null) {
                multiStatement = true;   // the shared body rule, once params are in scope
            } else {
                lam = folded;
            }
        }

        Env lambdaScope = env;
        List<String> names = new ArrayList<>();
        List<Type.Param> scopeParams = new ArrayList<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            Type paramType = kernel.resolve(ftype.params().get(i).type(), b);   // T -> the solved element type
            // C4 (STAMP_DISCIPLINE_PROGRAM): the TYPE resolves through
            // the binding but the MULTIPLICITY was taken RAW from the
            // signature — a Var("m") flowed into the lambda scope and
            // survived to lowering (37 census events: sort comparators'
            // y, fold accumulators). Resolve when bound; a var still
            // OPEN here (bound only by the body's result) stays, and the
            // census keeps counting it.
            Multiplicity paramMult = ftype.params().get(i).multiplicity();
            if (paramMult instanceof Multiplicity.Var mv) {
                paramMult = b.mult(mv.name()).orElse(paramMult);
            }
            Variable pv = lam.parameters().get(i);
            // a SOURCE annotation refines a signature-side Any (real pure:
            // the declared annotation is authoritative for the lambda's
            // own scope — executionPlan's Function<{Any[1]->Any[*]}>
            // param family relies on it for {var:String[1]|...})
            if (pv.type() != null && paramType instanceof Type.ClassType ct
                    && com.legend.compiler.element.type.PlatformTypes.ANY.equals(ct.fqn())) {
                paramType = namedType(pv.type());
                if (pv.multiplicity() != null) {
                    paramMult = Multiplicity.from(pv.multiplicity());
                }
            }
            names.add(pv.name());
            scopeParams.add(new Type.Param(paramType, paramMult));
            lambdaScope = lambdaScope.with(pv.name(),
                    new ExprType(paramType, paramMult));
        }

        // ZERO-ARG multi-statement bodies: leading lets bind into scope
        // (real pure statement semantics; the typed lets stay as STATEMENTS
        // for the consumer's sequencing), the final expression is the value.
        List<TypedSpec> typedStmts = new ArrayList<>();
        for (int si = 0; !multiStatement && si < lam.body().size() - 1; si++) {
            ValueSpecification st = lam.body().get(si);
            if (st instanceof AppliedFunction lf2
                    && CoreFn.of(lf2.function()).orElse(null) == CoreFn.LET
                    && lf2.parameters().size() == 2
                    && lf2.parameters().get(0) instanceof CString ln) {
                TypedSpec val = synth(lf2.parameters().get(1), lambdaScope);
                lambdaScope = lambdaScope.withLet(ln.value(), val.info(),
                        lf2.parameters().get(1));
                typedStmts.add(new com.legend.compiler.spec.typed.TypedLet(
                        ln.value(), val, val.info()));
                continue;
            }
            // a non-let statement: typed and KEPT as a statement (real pure
            // sequences it and discards the value — |[]->toOneMany(); 1;);
            // whether SQL can sequence it is the lowering's question
            typedStmts.add(synth(st, lambdaScope));
        }
        TypedSpec body = multiStatement ? synthBody(lam, lambdaScope)
                : synth(lam.body().get(lam.body().size() - 1), lambdaScope);

        // An unbound return variable (map's V) is inferred from the body. A
        // solved return is resolved then CHECKED (subtype-friendly, bindings
        // untouched — the long-standing semantics; SchemaAlgebra always takes
        // this path, since resolve() owns return-position algebra). ONLY a
        // structured return still carrying free vars ({->Relation<T>[1]}: the
        // slot-join thunk) — where the resolve path could only throw — unifies
        // UNRESOLVED into b, so the body's shape SOLVES the vars and later
        // parameters (the cond lambda's T[1] rows) see the solution.
        Type retType = ftype.result().type();
        if (retType instanceof Type.TypeVar rv && !b.hasType(rv.name())) {
            b.bindType(rv.name(), body.info().type());
        } else if (retType instanceof Type.TypeVar rv
                && kernel.resolve(retType, b) instanceof Type.ClassType nil
                && nil.fqn().equals(com.legend.compiler.element.type.PlatformTypes.NIL)) {
            // The return variable was solved to Nil by a []-born argument
            // (fold's init): BOTTOM carries no constraint — the body's type
            // IS the solution (covariant upgrade; Nil vanishes, the same rule
            // as in collection LUBs and type-var accumulation).
            b.bindType(rv.name(), body.info().type());
        } else if (retType instanceof Type.SchemaAlgebra || !kernel.hasFreeTypeVars(retType, b)) {
            kernel.unify(kernel.resolve(retType, b), body.info().type(), new Bindings());
        } else {
            kernel.unify(retType, body.info().type(), b);
        }
        // The body's MULTIPLICITY must satisfy the declared return too — a many-valued
        // body cannot serve a to-one slot (engine rejects sortBy on a to-many key:
        // {T[1]->U[1]} with a [*] body is a type error, not a silent acceptance).
        // LOWER bound stays LENIENT for lambda results — the reference's
        // observed covariance (its own corpus compiles sortBy over
        // optional association paths, a [0..1] body against U[1]);
        // kernel.unifyMultResult is the one owner of that rule.
        // EXCEPT a NIL-typed body (println side effects: Nil[0] is the
        // bottom VALUE and conforms to any return slot — real pure
        // compiles rows->map(r|println(...)); corpus testWithFilterGroupBy).
        boolean nilBody = body.info().type()
                instanceof Type.ClassType nbc
                && com.legend.compiler.element.type.PlatformTypes.NIL
                        .equals(nbc.fqn());
        if (!nilBody) {
            kernel.unifyMultResult(ftype.result().multiplicity(),
                    body.info().multiplicity(), body.info().type(), b);
        }

        ExprType info = new ExprType(
                new Type.FunctionType(scopeParams,
                        new Type.Param(body.info().type(), body.info().multiplicity())),
                Multiplicity.Bounded.ONE);
        typedStmts.add(body);
        return new TypedLambda(names, List.copyOf(typedStmts), info);
    }

    /** Unwrap a {@code Function<{…}>} (or a bare {@code FunctionType}) parameter to its function type. */
    /** The function type a declared type carries — bare or Function<{...}>
     * wrapped — or null when it is not function-typed at all. */
    private static Type.@com.legend.Nullable FunctionType asFunctionType(Type t) {
        if (t instanceof Type.FunctionType ft) {
            return ft;
        }
        if (t instanceof Type.GenericType g && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType ft) {
            return ft;
        }
        return null;
    }

    static Type.FunctionType extractFunctionType(Type t) {
        if (t instanceof Type.FunctionType ft) {
            return ft;
        }
        if (t instanceof Type.GenericType g && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType ft) {
            return ft;
        }
        throw new TypeInferenceException("expected a function-typed parameter, got " + t.typeName());
    }

    /**
     * Type a mapped column specification against its {@code FuncColSpec<F,Z>} /
     * {@code FuncColSpecArray<F,Z>} parameter: each {@code alias:x|body} lambda is
     * checked against {@code F} (whose parameter is the already-bound source row /
     * element), and {@code Z} binds to the row the aliases + body types form. This
     * is the one place unification <em>drives the lambda typer</em> &mdash; the
     * output schema stays signature-computed ({@code Relation<Z>}, {@code T+Z}).
     */
    private TypedSpec typeFuncColSpec(ValueSpecification vs, Type formal, Bindings b, Env env) {
        if (!(formal instanceof Type.GenericType g) || g.arguments().size() < 2) {
            throw new TypeInferenceException("expected a mapped column-spec parameter, got "
                    + formal.typeName());
        }
        if (genericRawIs(formal, com.legend.compiler.element.type.PlatformTypes.AGG_COL_SPEC) || genericRawIs(formal, com.legend.compiler.element.type.PlatformTypes.AGG_COL_SPEC_ARRAY)) {
            return typeAggColSpec(vs, g, b, env);
        }
        Type.FunctionType f = extractFunctionType(g.arguments().get(0));
        List<ColSpec> specs = vs instanceof ColSpecArray arr ? arr.colSpecs() : List.of((ColSpec) vs);

        List<TypedFuncCol> cols = new ArrayList<>(specs.size());
        List<Type.Column> schema = new ArrayList<>(specs.size());
        for (ColSpec cs : specs) {
            if (cs.function1() == null) {
                throw new TypeInferenceException(
                        "~" + cs.name() + " needs a mapping expression (alias:x|…) here");
            }
            if (schema.stream().anyMatch(c -> c.name().equals(cs.name()))) {
                throw new SchemaInvariantException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            TypedLambda lam = (TypedLambda) typeLambda(cs.function1(), f, b, env);
            Type.Param result = lam.functionType().result();
            cols.add(new TypedFuncCol(cs.name(), lam));
            schema.add(new Type.Column(cs.name(), result.type(), result.multiplicity()));
        }

        if (!(g.arguments().get(1) instanceof Type.TypeVar z)) {
            throw new TypeInferenceException("the column-spec schema slot must be a variable, got "
                    + g.arguments().get(1).typeName());
        }
        b.bindType(z.name(), new Type.RelationType(schema));
        Type solved = new Type.GenericType(g.rawFqn(), List.of(f, new Type.RelationType(schema)));
        return vs instanceof ColSpecArray
                ? new TypedFuncColSpecArray(cols, ExprType.one(solved))
                : new TypedFuncColSpec(cols.get(0), ExprType.one(solved));
    }

    /**
     * Type an aggregate column specification against {@code AggColSpec<F1,F2,R>} /
     * {@code AggColSpecArray<F1,F2,R>}: per column, the map lambda checks against
     * {@code F1 = {T[1]->K[0..1]}} (binding {@code K} from its body) and the reduce
     * lambda against {@code F2 = {K[*]->V[0..1]}}; the column's type is the reduce
     * body's. {@code K}/{@code V} solve in a <em>per-column copy</em> of the
     * bindings &mdash; the array signature shares them only syntactically, each
     * aggregate's value type is its own (engine compiles each colspec independently).
     * {@code R} binds in the parent for the enclosing {@code Z+R}/{@code T+R} output.
     */
    private TypedSpec typeAggColSpec(ValueSpecification vs, Type.GenericType g, Bindings b, Env env) {
        if (g.arguments().size() != 3) {
            throw new TypeInferenceException("an aggregate column-spec parameter needs <map, reduce, R>, got "
                    + g.typeName());
        }
        Type.FunctionType mapF = extractFunctionType(g.arguments().get(0));
        Type.FunctionType reduceF = extractFunctionType(g.arguments().get(1));
        List<ColSpec> specs = vs instanceof ColSpecArray arr ? arr.colSpecs() : List.of((ColSpec) vs);

        List<TypedAggCol> cols = new ArrayList<>(specs.size());
        List<Type.Column> schema = new ArrayList<>(specs.size());
        for (ColSpec cs : specs) {
            if (cs.function1() == null || cs.function2() == null) {
                throw new TypeInferenceException("~" + cs.name()
                        + " needs a map and a reduce expression (alias:x|…:y|…) here");
            }
            if (schema.stream().anyMatch(c -> c.name().equals(cs.name()))) {
                throw new SchemaInvariantException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            Bindings local = b.copy();   // K/V are per-column (see javadoc)
            TypedLambda map = (TypedLambda) typeLambda(cs.function1(), mapF, local, env);
            TypedLambda reduce = (TypedLambda) typeLambda(cs.function2(), reduceF, local, env);
            Type.Param result = reduce.functionType().result();
            cols.add(new TypedAggCol(cs.name(), map, reduce, null, true));
            schema.add(new Type.Column(cs.name(), result.type(), result.multiplicity()));
        }

        if (!(g.arguments().get(2) instanceof Type.TypeVar r)) {
            throw new TypeInferenceException("the aggregate schema slot must be a variable, got "
                    + g.arguments().get(2).typeName());
        }
        b.bindType(r.name(), new Type.RelationType(schema));
        Type solved = new Type.GenericType(g.rawFqn(),
                List.of(mapF, reduceF, new Type.RelationType(schema)));
        return vs instanceof ColSpecArray
                ? new TypedAggColSpecArray(cols, ExprType.one(solved))
                : new TypedAggColSpec(cols.get(0), ExprType.one(solved));
    }

    // =====================================================================
    // Forms &mdash; the non-application ValueSpecification shapes
    // =====================================================================

    /**
     * PCT function-POINTER spellings carry the engine's mangled signature
     * tail ({@code tanh_Number_1__Float_1_}) — strip it and resolve the
     * plain name; the call's ACTUAL arguments pick the overload. A plain
     * name that resolves directly never demangles.
     */
    List<TypedFunction> functionCandidates(String name) {
        List<TypedFunction> found = ctx.findFunction(name);
        if (!found.isEmpty()) {
            return found;
        }
        // a MANGLED engine id names ONE overload: the declarations under a
        // prefix of it are SPELLED and the exact match kept (a spelling this
        // platform cannot reproduce is a miss, never a redirect)
        return SignatureMangle.resolve(name, ctx::findFunction, TypedFunction::definition).exact();
    }

    /**
     * Call-aware overload set: when the resolver recorded IMPORT-AMBIGUITY
     * candidates on the node (several imported packages define the simple
     * name), the overload set is the UNION across all of them — real
     * pure's function matching collects across imports and signature
     * scoring picks. Single-referent calls keep the plain name path.
     */
    List<TypedFunction> functionCandidates(AppliedFunction af) {
        if (af.candidateFqns().isEmpty()) {
            return functionCandidates(af.function());
        }
        List<TypedFunction> union = new ArrayList<>();
        RuntimeException firstBroken = null;
        for (String fqn : af.candidateFqns()) {
            try {
                union.addAll(ctx.findFunction(fqn));
            } catch (RuntimeException e) {
                // an import candidate whose overloads are ALL signature-
                // broken (tolerant module): it cannot be meant — the
                // healthy candidates decide, exactly like a broken overload
                // inside one FQN. Surface it only if NOTHING is healthy.
                if (firstBroken == null) {
                    firstBroken = e;
                }
            }
        }
        if (union.isEmpty() && firstBroken != null) {
            throw firstBroken;
        }
        return union;
    }

    /**
     * A packageable-element reference used as a value &mdash; currently a class
     * reference (the {@code Person} in {@code Person.all()}), typed as
     * {@code Class<Person>[1]} so {@code getAll<T>(Class<T>[1]):T[*]} resolves to
     * {@code Person[*]} via the generic native path.
     */
    private TypedSpec classReference(PackageableElementPtr ref) {
        if (ref.fullPath().equals("::")) {
            // the ROOT package literal (^Database(package = ::)): a
            // Package value, real m3's Root
            return new TypedPackageableRef("::",
                    ExprType.one(new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.PACKAGE)));
        }
        var cls = ctx.findClass(ref.fullPath());
        if (cls.isPresent()) {
            // The node carries the RESOLVED FQN — a bare name accepted by
            // the simple-name fallback must not leak downstream (the H
            // resolver's mapping bindings are FQN-keyed).
            String fqn = cls.get().qualifiedName();
            Type classOf = new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS,
                    List.of(new Type.ClassType(fqn)));
            return new TypedPackageableRef(fqn, ExprType.one(classOf));
        }
        // A bare ENUMERATION reference (STR_GeographicEntityType->toString())
        // is a value of Enumeration<E>[1] (real m3's enumeration metaclass).
        var en = ctx.findEnum(ref.fullPath());
        if (en.isPresent()) {
            String fqn = en.get().qualifiedName();
            Type enumOf = new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.ENUMERATION,
                    List.of(new Type.EnumType(fqn)));
            return new TypedPackageableRef(fqn, ExprType.one(enumOf));
        }
        // A DATABASE reference is a value of the store metaclass (real m3:
        // meta::relational::metamodel::Database) — the corpus's
        // testRuntime(db:Database[1]) overload family dispatches on it.
        // Database <: Any, so from/write's Any[1] parameters still accept it.
        if (ctx.isDatabase(ref.fullPath())) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                    new Type.ClassType("meta::relational::metamodel::Database")));
        }
        // A MAPPING reference is a value of the mapping metaclass (real m3:
        // meta::pure::mapping::Mapping) — corpus helpers dispatch on it
        // (getModelChainRuntime(m:Mapping[1]); the Database precedent
        // above). Mapping <: Any keeps from/execute's Any[1] params fine.
        if (ctx.findMapping(ref.fullPath()).isPresent()) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                    new Type.ClassType("meta::pure::mapping::Mapping")));
        }
        // A PROFILE reference is a value of the Profile metaclass (real m3:
        // meta::pure::metamodel::extension::Profile — the spec's test
        // surveyor reads test.p_stereotypes)
        if (ctx.findProfile(ref.fullPath()).isPresent()) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.PROFILE)));
        }
        // A MEASURE reference is a value of Measure; M~unit is a value of
        // Unit (m3 Measure/Unit; the spec's unit tests: RomanLength~Pes)
        if (ctx.findMeasure(ref.fullPath()).isPresent()) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.MEASURE)));
        }
        int tilde = ref.fullPath().indexOf('~');
        if (tilde > 0) {
            String measure = ref.fullPath().substring(0, tilde);
            String unit = ref.fullPath().substring(tilde + 1);
            var md = ctx.findMeasure(measure);
            boolean known = md.isPresent() && (
                    (md.get().canonicalUnit() != null && md.get().canonicalUnit().name().equals(unit))
                    || md.get().nonCanonicalUnits().stream().anyMatch(u -> u.name().equals(unit)));
            if (known) {
                return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                        new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.UNIT)));
            }
        }
        // A PACKAGE reference is a value of Package (m3: Root and every
        // proper prefix of an element's name — elementToPath(meta::pure))
        if (ref.fullPath().equals("Root") || ctx.isPackage(ref.fullPath())) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.PACKAGE)));
        }
        // a RUNTIME element is upstream's PackageableRuntime — from(T[m],
        // PackageableRuntime[1]) is declared over it; a Runtime[1] slot
        // (execute) takes its runtimeValue, spelled by the caller
        if (ctx.findRuntime(ref.fullPath()).isPresent()) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(new Type.ClassType(
                    com.legend.compiler.element.type.PlatformTypes.PACKAGEABLE_RUNTIME)));
        }
        // a CONNECTION element stays Any[1] — the prelude carries no
        // PackageableConnection; its typing is owed with the connection natives
        if (ctx.isExecutionContextElement(ref.fullPath())) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(InferenceKernel.anyType()));
        }
        // A FUNCTION REFERENCE used as a value (removeDuplicates(eq_Any_1__...))
        // ETA-EXPANDS: the reference becomes the lambda calling it — one
        // uniform function-value story, no new node kind. Only an
        // UNAMBIGUOUS (single-overload) target expands.
        // m3's PACKAGEABLE MULTIPLICITY constants (m3.pure:1411 — PureOne,
        // PureZero, ZeroOne, ZeroMany, OneMany) are instance VALUES of
        // Multiplicity[1], spelled from the spec (Phase 5 batch 147: the
        // engine's plan-execution hooks compare against them)
        var constant = PlatformConstants.multiplicity(ref.fullPath());
        if (constant.isPresent()) {
            return constant.get();
        }
        List<TypedFunction> fns = functionCandidates(ref.fullPath());
        // a MANGLED id names ONE overload — the signature tail's segment
        // count disambiguates. The handling runs for ZERO candidates too
        // (ledger cluster 26: the size>1 gate made the zero-candidate
        // fallback dead — a mangled id naming a function this platform
        // spells differently, e.g. the TDS groupBy the checker desugars
        // at call sites, must still reference as an opaque Function).
        if (fns.size() != 1) {
            var res = SignatureMangle.resolve(ref.fullPath(), ctx::findFunction,
                    TypedFunction::definition);
            if (res.exact().size() == 1) {
                fns = res.exact();
            } else if (res.baseExists()) {
                // BASE-EXISTS is the safety property: a misspelled or
                // absent base still fails the lookup and throws below.
                return new TypedPackageableRef(ref.fullPath(),
                        ExprType.one(new Type.GenericType(
                                "meta::pure::metamodel::function::Function",
                                List.of(InferenceKernel.anyType()))));
            }
        }
        if (fns.size() == 1) {
            TypedFunction fn = fns.get(0);
            List<String> params = new ArrayList<>(fn.parameters().size());
            List<TypedSpec> argRefs = new ArrayList<>(fn.parameters().size());
            List<Type.FunctionType.Param> ftParams = new ArrayList<>(fn.parameters().size());
            for (int i = 0; i < fn.parameters().size(); i++) {
                var fp = fn.parameters().get(i);
                String name = "_fr" + i;
                params.add(name);
                argRefs.add(new TypedVariable(name,
                        new ExprType(fp.type(), fp.multiplicity())));
                ftParams.add(new Type.FunctionType.Param(fp.type(), fp.multiplicity()));
            }
            ExprType out = new ExprType(fn.returnType(), fn.returnMultiplicity());
            TypedSpec body = Typer.emitCall(fn, argRefs, out);
            var ft = new Type.FunctionType(ftParams,
                    new Type.FunctionType.Param(fn.returnType(), fn.returnMultiplicity()));
            // The eta-expanded VALUE is a lambda, but the reference's m3
            // classifier is the referenced function's:
            // ConcreteFunctionDefinition<ft> (⊆ FunctionDefinition — a
            // pkOfFunc-shaped formal accepts it; a lambda-literal stamp
            // here would be pure-false). Passed explicitly so the
            // TypedLambda constructor keeps it.
            return new TypedLambda(params, List.of(body), ExprType.one(
                    com.legend.compiler.element.type.PlatformTypes
                            .concreteFunctionDefinitionType(ft)));
        }
        // Semantically a RESOLUTION failure (an unresolvable name), even
        // though it surfaces during type-checking — typed for what it MEANS.
        throw new com.legend.error.ResolutionException("'" + ref.fullPath()
                + "' is not a known class, mapping, runtime, connection, or database"
                + (ref.fullPath().contains("::")
                        ? "" : " — user elements in a query need a fully qualified name"));
    }

    /** A collection literal {@code [a,b,c]}: element type = common supertype; multiplicity = exact count. */
    private TypedSpec collection(PureCollection coll, Env env) {
        List<TypedSpec> elements = new ArrayList<>(coll.values().size());
        for (ValueSpecification v : coll.values()) {
            TypedSpec e = synth(TdsNullForms.listElement(v), env);
            // pure has NO nested collections: [['a','b'],'c'] IS
            // ['a','b','c'] — a collection-valued element SPLICES into
            // the enclosing literal (real pure value semantics)
            if (e instanceof TypedCollection tc) {
                elements.addAll(tc.elements());
            } else {
                elements.add(e);
            }
        }
        Type elementType = elements.stream()
                .map(e -> e.info().type())
                .reduce(kernel::commonSupertype)
                // The empty collection [] types as Nil[0] — the BOTTOM type
                // (real pure), so it conforms to any expected element type
                // and vanishes in LUBs: if(c, {|Status}, {|[]}) is
                // Status[0..1], not Any.
                .orElseGet(() -> new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.NIL));
        // multiplicity = the SUM of element bounds, not the element
        // count (audit-of-R1: [[]->first(), 'a'] is [1..2] in pure —
        // the [2..2] stamp made the §5 egress wall fire on a correct
        // one-element result). An unbounded element makes the sum
        // unbounded; a non-Bounded element stamp cannot reach a literal
        // (checker invariant) and falls back to 1..1 for that slot.
        int lo = 0;
        Integer hi = 0;
        for (TypedSpec e : elements) {
            if (e.info().multiplicity() instanceof Multiplicity.Bounded b) {
                lo += b.lower();
                hi = hi == null || b.upper() == null ? null
                        : hi + b.upper();
            } else {
                lo += 1;
                hi = hi == null ? null : hi + 1;
            }
        }
        Multiplicity mult = new Multiplicity.Bounded(lo, hi);
        return new TypedCollection(elements, new ExprType(elementType, mult));
    }

    /**
     * Object-graph property access {@code $source.property}: type the receiver
     * (which must be a class), look up the property's signature via
     * {@link ModelContext#findProperty} (which walks inheritance and
     * association-injected properties), and <em>compose</em> the receiver's
     * multiplicity with the property's along the path.
     */
    /** The relation's column NAMES or pure TYPE NAMES as a static string collection. */
    /** The typed row-cell read ({@code $r.getString('COL')},
     * {@code Row.value('COL')} — TDSRow and its ResultSet twin): the
     * named COLUMN of the relation row (a plain property access
     * post-desugar; a type mismatch is loud downstream). The CELL is
     * one value whatever the column's declared multiplicity — a non-[1]
     * column read conforms BY EMISSION (toOne; lowering is erasure).
     * Over the ROWS COLLECTION itself ({@code $rs.rows.value('N')} —
     * Row[*] in pure terms) the read AUTO-MAPS per pure's own dot rule
     * (map.pure grammarDoc): the column's values, one per row — never a
     * single-row read over many rows. */
    /** The lifted qualified property a row accessor names — TDSRow$prop$getString —
     *  typed by ITS declaration (the prelude's), the platform's implementation
     *  being RowGetters. Loud when the module does not carry it. */
    private TypedSpec liftedAccessorCall(com.legend.builtin.NativeFn.RowGetter getter,
            TypedSpec receiver, TypedSpec name) {
        InferenceKernel.Resolution r = liftedAccessor(getter, receiver.info(), name.info());
        return new com.legend.compiler.spec.typed.TypedUserCall(r.chosen(), List.of(receiver, name), r.output());
    }

    /** The lifted overload the accessor call resolves to — upstream declares
     *  {@code getString(colName:String[1])} beside {@code getString(col:TDSColumn[1])};
     *  the kernel picks, exactly as for any qualified property. Loud when the
     *  module does not carry the declaration. */
    private InferenceKernel.Resolution liftedAccessor(com.legend.builtin.NativeFn.RowGetter getter,
            ExprType receiver, ExprType name) {
        List<TypedFunction> lifted = ctx.findFunction(getter.fqn());
        if (lifted.isEmpty()) {
            throw new TypeInferenceException("row accessor '" + getter.property()
                    + "': the module carries no declaration of " + getter.fqn());
        }
        return kernel.resolveOverload(lifted, List.of(receiver, name));
    }

    private TypedSpec rowCellRead(AppliedFunction af, Env env,
            com.legend.builtin.NativeFn.RowGetter getter) {
        // COLLECTION frame BY TYPE (Row-vs-Relation): a WRAPPED
        // Relation<T> receiver is the rows collection — the typed
        // getter AUTO-MAPS per row (map.pure's dot rule); a bare
        // struct receiver IS one row and takes the per-cell path
        // below. Late-bound schemas need no special case: a late-bound
        // ROW is a bare struct whose schema carries the wildcard.
        TypedSpec grecv = synth(af.parameters().get(0), env);
        if (Type.relationValued(grecv.info())) {
            return synth(new AppliedFunction("map", List.of(
                    af.parameters().get(0),
                    new LambdaFunction(List.of(new Variable("_amc")),
                            List.of(new AppliedFunction(af.function(),
                                    List.of(new Variable("_amc"),
                                            af.parameters().get(1))))))),
                    env);
        }
        return rowCellReadOnRow(af, env, getter, grecv);
    }

    private TypedSpec rowCellReadOnRow(AppliedFunction af, Env env,
            com.legend.builtin.NativeFn.RowGetter getter, TypedSpec grecv) {
        String colRef = java.util.Objects.requireNonNull(
                literalColName(af.parameters().get(1)),
                "TDS cell read requires a literal column name");
        TypedSpec cell = synth(new AppliedProperty(
                af.parameters().get(0), colRef), env);
        // an ERASED row (a TDSRow in a type position): the column's type is
        // the accessor's DECLARED type (getString: String[1]) — real pure knows
        // no more at type time either; a concrete row keeps its schema's type
        if (Type.schemaView(grecv.info().type()) instanceof Type.RelationType erased
                && erased.isLateBound()) {
            return cell.withInfo(liftedAccessor(getter, grecv.info(),
                    ExprType.one(Type.Primitive.STRING)).output());
        }
        // getNullableString returns String[0..1] (tds.pure:82/112) —
        // the optional cell read IS the semantics, no strictening
        if (rowGetter(af, com.legend.builtin.NativeFn.RowGetter.GET_NULLABLE_STRING)
                || (cell.info().multiplicity() instanceof Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper())
                        && b.lower() == 1)) {
            return cell;
        }
        return synth(new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                new AppliedProperty(af.parameters().get(0), colRef))), env);
    }

    /** LATE-BOUND grid reads that cannot resolve statically (Phase 1c):
     * {@code .values} (the cells — count unknown) and
     * {@code .columnNames} (the names — first exist at execution)
     * SURVIVE as identity-preserving MARKERS, the same node shapes the
     * ResultSet class declaration produced pre-retype; the execution
     * BOUNDARY RESOLVER (RawGridSchema) substitutes them against the
     * stamped schema, and reaching the Lowerer instead is a loud wall,
     * never a silent guess. Null = not such a read. */
    static @com.legend.Nullable TypedSpec lateBoundGridMarker(
            TypedSpec source, AppliedProperty ap, Type.RelationType rt2) {
        if (!rt2.isLateBound()) {
            return null;
        }
        if (ap.property().equals("values")) {
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    source, "values",
                    new ExprType(new Type.ClassType(
                            com.legend.compiler.element.type.PlatformTypes.ANY),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ZERO_MANY));
        }
        if (ap.property().equals("columnNames")) {
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    source, "columnNames",
                    new ExprType(Type.Primitive.STRING,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ZERO_MANY));
        }
        return null;
    }

    /** Surrounding double quotes are SPELLING, not identity, for the
     * quote-fallback column match (both sides normalize). */
    private static String stripColQuotes(String n) {
        return n.length() >= 2 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    /** The {@code .values} read over a schema-viewed source — split
     * from accessProperty (G1 method-length seam): the row-var CELLS
     * read, the RELATION-CELLS flatten (TypedMap synthesis), and the
     * identity arms for picks/class shapes. */
    TypedSpec tdsValuesRead(TypedSpec source, Type.RelationType rt2) {
            // On a ROW VARIABLE (bare struct, at-most-one stamp — a
            // lambda's in-scope row): TDSRow.values = the row's
            // CELLS in column order, statically enumerable per-cell
            // reads. On everything else — a wrapped table, the
            // .rows collection, or a PICK-rooted row
            // ($tds.rows->at(0).values): IDENTITY — the wire
            // flatten IS row-major cell order and keeps NULL cells
            // as TDSNull (the cells-collection channel would drop
            // them; the lower-bound honesty guard caught exactly
            // that). The variable test is a LEXICAL binding fact,
            // not type inference. (The Result-ENVELOPE .values
            // never reaches the Typer: the test driver peels it at
            // substitution.)
            if (source.info().type() instanceof Type.RelationType
                    && !Type.relationValued(source.info())
                    && source instanceof
                            com.legend.compiler.spec.typed.TypedVariable) {
                // Row-var cells are TYPED per-column reads (at(N) and
                // typed compares keep their kinds); print consumers
                // (makeString/joinStrings) stringify at LOWERING, which
                // also bypasses the Any-JSON carrier's quoting.
                return rowCells(source, rt2);
            }
            // RELATION-SOURCED rows.values: IDENTITY — the grid IS the
            // carrier (REVERSAL 2026-08-24, charter record in
            // F10_CARRIER_DESIGN.md). Semantically the read is an ordered
            // list (tds.pure:79 values:Any[*], row-major, TDSNull slots);
            // its SQL carrier stays the QUERY, whose rows natively hold
            // the list's contract — order (row order), per-element kinds
            // (column types), null slots (NULL cells, grid convention).
            // The 2026-08-24 TypedMap flatten re-carried the read as a
            // SQL LIST VALUE, which holds NONE of those properties, and
            // every consumer (order, print, instanceOf(TDSNull),
            // temporals) needed carrier compensation — gate-caught and
            // REVERTED same day. Consumers map onto the grid instead:
            // at(k)/size() by row-major arithmetic (tdsRowCellIndexRead,
            // ledger cluster 33), grid-vs-list asserts by the referee's
            // row-major cell walk (the engine's own text-compare
            // convention for this family). CARRIER RULE (the lesson):
            // pick the carrier by the contract's required properties —
            // order/kinds/null-slots demand the query carrier; the array
            // carrier is for scalar-position literal collections only.
            return source;
    }

    private TypedSpec accessProperty(AppliedProperty ap, Env env) {
        TypedSpec colsMeta = ColumnsMetaFold.read(this, ap, env);
        if (colsMeta != null) {
            return colsMeta;
        }
        TypedSpec source = synth(ap.receiver(), env);
        // the meta::json tree classes: reads are JSON navigation on the
        // variant lane (JsonChecker), never class-property access
        if (com.legend.compiler.element.type.PlatformTypes
                .isJsonElement(source.info().type())) {
            return JsonChecker.access(this, ap, source, env);
        }
        // leg 3b: the deactivate reflection chain folds HERE, at TYPE time
        // — .genericType hops the carrier, .rawType lands the DECLARED
        // type as a TypedTypeRef (the verdict layer's existing surface).
        // A LET-bound carrier resolves through the exprAlias channel
        // (referentially transparent, same as match branches). Any other
        // property over the carrier is a LOUD wall — the metamodel class
        // is deliberately unmodeled (absence, never fabrication).
        TypedSpec reflect = source;
        if (source instanceof com.legend.compiler.spec.typed.TypedVariable
                && ap.receiver() instanceof com.legend.protocol.spec.Variable pv
                // GATED on the metamodel carrier TYPE: an ordinary
                // let-bound variable's property access must never
                // re-synth its binding (cost + effect duplication)
                && source.info().type() instanceof Type.ClassType mc
                && (mc.fqn().equals(
                        "meta::pure::metamodel::valuespecification::ValueSpecification")
                    || mc.fqn().equals(
                        "meta::pure::metamodel::type::generics::GenericType"))) {
            var aliased = env.exprAlias(pv.name());
            if (aliased.isPresent()) {
                reflect = synth(aliased.get(), env);
            }
        }
        if (reflect instanceof com.legend.compiler.spec.typed.TypedDeactivate td) {
            if (!td.generic() && ap.property().equals("genericType")) {
                return new com.legend.compiler.spec.typed.TypedDeactivate(
                        td.inner(), td.declared(), true,
                        ExprType.one(new Type.ClassType(
                                "meta::pure::metamodel::type::generics::GenericType")));
            }
            if (td.generic() && ap.property().equals("rawType")) {
                Type declared = td.declared().type();
                return new com.legend.compiler.spec.typed.TypedTypeRef(declared,
                        ExprType.one(declared));
            }
            throw new TypeInferenceException("reflection property '"
                    + ap.property() + "' over deactivate is not modeled"
                    + " (only genericType.rawType is)");
        }
        Type.RelationType rt2 = Type.schemaView(source.info().type());
        if (rt2 != null) {
            // the TDS surface over tables and rows (TdsSurfaceReads)
            TypedSpec surface = TdsSurfaceReads.read(this, source, ap, rt2);
            if (surface != null) {
                return surface;
            }
        }
        // a zero-arg DERIVED read IS a call of its externalized body —
        // route and β-inline so downstream sees plain navigation
        if (source.info().type() instanceof Type.ClassType ct
                && ctx.findProperty(ct.fqn(), ap.property()).orElse(null)
                        instanceof Property.Derived d
                && d.parameters().isEmpty()) {
            // AUTO-MAP (real pure — map.pure grammarDoc: "map is auto
            // generated when the . operator is used to access a property
            // value on a element of multiplicity different from [1]" —
            // that INCLUDES [0..1], audit 22a H2: β-inlining a NON-STRICT
            // derived body over a possibly-empty receiver manufactures a
            // value where pure yields empty). Only an exactly-[1]
            // receiver β-inlines directly.
            boolean exactlyOne = source.info().multiplicity()
                    instanceof com.legend.compiler.element.type
                            .Multiplicity.Bounded b1
                    && b1.lower() == 1 && b1.upper() != null
                    && b1.upper() == 1;
            if (!exactlyOne && source.info().multiplicity().isMany()) {
                return synth(new AppliedFunction("map", List.of(ap.receiver(),
                        new LambdaFunction(
                                List.of(new Variable("_am0")),
                                List.of(new AppliedProperty(
                                        new Variable("_am0"), ap.property()))))),
                        env);
            }
            // [0..1] receivers β-inline like [1] (ledger cluster 48):
            // engine processQualifiedProperty runs the qualifier body
            // against the cursor with NO presence guard — an absent
            // LEFT-joined receiver evaluates the body over NULL columns,
            // and the corpus pins the MANUFACTURED value
            // (testQualifierWithInThroughJoin: cat='B' for a trade whose
            // account row is absent). The null-strict whitelist encoded
            // the opposite belief and is deleted with its helpers.
            // A [0..1] receiver β-inlines like [1] (ledger cluster 48:
            // engine processQualifiedProperty runs with NO presence
            // guard) — the strict kernel demands the conformance be
            // SPELLED: the receiver wraps in toOne at this synth site
            // (SQL null-propagates through the inlined body, which IS
            // the engine's no-guard behavior).
            return applyGeneric(new AppliedFunction(d.bodyFunctionFqn(),
                    List.of(exactlyOne
                            ? ap.receiver()
                            : new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE,
                                    List.of(ap.receiver())))), env);
        }
        // the AllVersions PROPERTY spelling (no parens): a version-sweep
        // navigation — normalized to the same TypedMilestonedAccess the
        // call spelling produces, so every downstream layer sees ONE shape
        if (source.info().type() instanceof Type.ClassType ctv
                && ap.property().endsWith("AllVersions")
                // a DECLARED property of that exact name wins — never
                // shadowed by the generated spelling (audit 10)
                && ctx.findProperty(ctv.fqn(), ap.property()).isEmpty()) {
            String base = ap.property().substring(0,
                    ap.property().length() - "AllVersions".length());
            var bp = ctx.findProperty(ctv.fqn(), base).orElse(null);
            String tFqn = bp != null && bp.type() instanceof Type.ClassType bct
                    ? bct.fqn() : null;
            if (tFqn != null && com.legend.compiler.element.Temporal
                    .strategyOf(ctx, tFqn) != null) {
                var mbp = java.util.Objects.requireNonNull(bp, "bp");
                return new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                        source, base, List.of(), true,
                        new ExprType(mbp.type(),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ZERO_MANY));
            }
        }
        // The member is either a class property ($obj.prop) or a relation column ($row.col).
        String relColName = null;
        ExprType member = switch (source.info().type()) {
            case Type.ClassType ct -> {
                Property prop = ctx.findProperty(ct.fqn(), ap.property()).orElse(null);
                if (prop == null) {
                    // real pure GENERATES the milestoning member surface
                    // (businessDate/processingDate, the milestoning struct
                    // and its members) — ONE registry, shared with graph
                    // trees (Temporal.generatedMember)
                    ExprType gen = com.legend.compiler.element.Temporal
                            .generatedMember(ctx, ct.fqn(), ap.property());
                    if (gen != null) {
                        yield gen;
                    }
                    // real M3: Any.elementOverride surfaces on every class
                    // (the corpus KeyInformation guard); folded to empty
                    // below. Since batch 163 Any DECLARES both properties in
                    // the prelude module (no layout slot); these arms remain
                    // for receivers whose class does not spell `extends Any`
                    if (ap.property().equals("elementOverride")) {
                        yield new ExprType(new Type.ClassType(
                                com.legend.compiler.element.type.PlatformTypes.ELEMENT_OVERRIDE),
                                Multiplicity.Bounded.ZERO_ONE);
                    }
                    // the same for Any.classifierGenericType (m3.pure: GenericType
                    // [0..1]; the engine's plan-execution hooks read it) — served
                    // here, never declared on Any (Phase 5 batch 147 ledger row 1)
                    if (ap.property().equals("classifierGenericType")) {
                        yield new ExprType(new Type.ClassType(
                                com.legend.compiler.element.type.PlatformTypes.GENERIC_TYPE),
                                Multiplicity.Bounded.ZERO_ONE);
                    }
                    throw new TypeInferenceException("class " + ct.fqn()
                            + " has no property '" + ap.property() + "'");
                }
                yield new ExprType(prop.type(), prop.multiplicity());
            }
            // A TABLE receiver (wrapped Relation<T> — Row-vs-Relation):
            // the column read is the auto-mapped cell COLLECTION,
            // always (map.pure's dot rule). A per-row read has a bare
            // struct receiver and takes the row arm below. No walk, no
            // inference, no blind spot.
            // — a relation CARRIER subclass (TDS<T>: upstream declares csv on it)
            // serves its own DECLARED property first; the columns otherwise
            case Type.GenericType g
                    when Type.relationSchema(g) instanceof Type.RelationType rel
                    && !g.rawFqn().equals(com.legend.compiler.element.type.PlatformTypes.RELATION)
                    && ctx.findProperty(g.rawFqn(), ap.property()).isPresent() ->
                    genericReceiverProperty(g, ap);
            case Type.GenericType g
                    when Type.relationSchema(g) instanceof Type.RelationType rel -> {
                Type.Column col = relationColumn(rel, ap.property());
                relColName = col.name();
                yield new ExprType(col.type(), Multiplicity.Bounded.ZERO_MANY);
            }
            // A PARAMETERIZED class receiver (Pair<Integer,String>.first,
            // Result<String|1>.values): instantiation extracted to its
            // own method (CodeShape seam — leg 2 grew this arm).
            case Type.GenericType g -> genericReceiverProperty(g, ap);
            // A ROW receiver (Row-vs-Relation): a bare struct IS one
            // row — one cell, the per-cell stamp BY TYPE. This arm is
            // the detective's replacement: no walk, no inference — the
            // type says row.
            case Type.RelationType rowT -> {
                // QUOTE-BEARING column identity (the pivot rule's sibling):
                Type.Column col = relationColumn(rowT, ap.property());
                relColName = col.name();
                yield new ExprType(col.type(), col.multiplicity());
            }
            // an ENUM VALUE's name (real m3 Enum.name) — the SQL value of an enum IS its name
            case Type.EnumType ignored when ap.property().equals("name") ->
                    new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
            // a lambda VALUE's m3 classifier is LambdaFunction (⊆ FunctionDefinition):
            // $f.expressionSequence reads the definition (spec reactivate tests)
            case Type.FunctionType ignored when lambdaClassifierProperty(ap.property()) != null ->
                    java.util.Objects.requireNonNull(lambdaClassifierProperty(ap.property()));
            default -> {
                throw new TypeInferenceException("cannot access '" + ap.property()
                    + "' on " + source.info().type().typeName());
            }
        };
        Multiplicity mult = compose(source.info().multiplicity(), member.multiplicity());
        if ((ap.property().equals("elementOverride")   // M3: never
                || ap.property().equals("classifierGenericType"))
                && source.info().type() instanceof Type.ClassType) {
            return new com.legend.compiler.spec.typed.TypedCollection(
                    List.of(), new ExprType(member.type(),
                            Multiplicity.Bounded.ZERO_ONE));
        }
        return new TypedPropertyAccess(source,
                relColName != null ? relColName : ap.property(),
                new ExprType(member.type(), mult));
    }

    /** A PARAMETERIZED class receiver's property: the declared type is
     * written in the class's type parameters — instantiate them at the
     * receiver's arguments (positional, real pure's generic
     * instantiation). Leg 2 (Result&lt;T|m&gt;, engine parity): the
     * class parameter list is LUMPED source-order type-then-mult names
     * (the M3-dialect parser convention); the receiver may spell its
     * multiplicity arguments or omit them (Result&lt;X&gt; vs
     * Result&lt;X|1&gt; — the leniency): bind what's supplied
     * POSITIONALLY; an omitted mult falls back to [*] (pre-leg
     * behavior), an unbound TYPE variable stays loud at kernel
     * resolve; over-supply is always an error. A property multiplicity
     * spelled with the class's multiplicity parameter
     * ({@code values: T[m]}) instantiates at the receiver's argument —
     * a serialize execute's {@code Result<String|1>.values} types
     * {@code String[1]}, never {@code [*]}. */
    /** A property of the lambda literal's m3 classifier (LambdaFunction ⊆
     * FunctionDefinition): $f.expressionSequence; null when none. */
    private @com.legend.Nullable ExprType lambdaClassifierProperty(String name) {
        return ctx.findProperty(com.legend.compiler.element.type.PlatformTypes.LAMBDA_FUNCTION, name)
                .map(pd -> new ExprType(pd.type(), pd.multiplicity())).orElse(null);
    }

    private ExprType genericReceiverProperty(Type.GenericType g,
            AppliedProperty ap) {
        var cls = ctx.findClass(g.rawFqn()).orElseThrow(() -> new TypeInferenceException(
                "unknown class '" + g.rawFqn() + "'"));
        // Any's served properties (the ClassType arm's rule) apply to every
        // receiver — a parameterized one included (Function<Any>.classifierGenericType)
        if (ap.property().equals("classifierGenericType")
                && ctx.findProperty(g.rawFqn(), ap.property()).isEmpty()) {
            return new ExprType(new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.GENERIC_TYPE),
                    Multiplicity.Bounded.ZERO_ONE);
        }
        if (ap.property().equals("elementOverride")
                && ctx.findProperty(g.rawFqn(), ap.property()).isEmpty()) {
            return new ExprType(new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ELEMENT_OVERRIDE),
                    Multiplicity.Bounded.ZERO_ONE);
        }
        Property prop = ctx.findProperty(g.rawFqn(), ap.property()).orElseThrow(() ->
                new TypeInferenceException("class " + g.rawFqn()
                        + " has no property '" + ap.property() + "'"));
        int supplied = g.arguments().size() + g.multArguments().size();
        if (supplied > cls.typeParameters().size()) {
            throw new TypeInferenceException("class " + g.rawFqn() + " declares "
                    + cls.typeParameters().size() + " type parameter(s) but the receiver "
                    + g.typeName() + " supplies " + supplied);
        }
        Bindings b = new Bindings();
        int pi = 0;
        for (Type targ : g.arguments()) {
            b.bindType(cls.typeParameters().get(pi++), targ);
        }
        for (Multiplicity marg : g.multArguments()) {
            b.bindMult(cls.typeParameters().get(pi++), marg);
        }
        Multiplicity pm = prop.multiplicity();
        if (pm instanceof Multiplicity.Var mv) {
            pm = b.mult(mv.name()).orElse(Multiplicity.Bounded.ZERO_MANY);
        }
        return new ExprType(kernel.resolve(prop.type(), b), pm);
    }

    /** A TDS-surface receiver: a whole relation (wrapped) OR one row of
     * one (a bare struct) — the getter surface serves both
     * (Row-vs-Relation split; the ROW case is the getters' primary
     * frame, stated by TYPE). */
    private static boolean tdsReceiver(Type t) {
        return t instanceof Type.RelationType || Type.isRelation(t);
    }

    /** Store-declared column lookup: a quoted "FIRST NAME" carries its
     * quotes as identity — exact match wins, then the quote-stripped
     * fallback; the access adopts the column's own spelling (task #78). */
    private static Type.Column relationColumn(Type.RelationType rel,
            String name) {
        return rel.columns().stream()
                .filter(c -> c.name().equals(name)).findFirst()
                .orElseGet(() -> rel.columns().stream()
                        .filter(c -> stripColQuotes(c.name())
                                .equals(stripColQuotes(name)))
                        .findFirst()
                        .orElseGet(() -> {
                            if (rel.isLateBound()) {
                                return Type.RelationType.trustedColumn(name);
                            }
                            throw new TypeInferenceException(
                                    "relation has no column '" + name + "'");
                        }));
    }

    /**
     * Multiplicity composition along a navigation path — ONE owner:
     * {@link Multiplicity#product} (audit sections 1d/1e: this copy's Var
     * arm silently dropped a variable source's cardinality across
     * inlining; the owner keeps the variable through the {@code [1]}
     * identity and is loud otherwise).
     */
    private static Multiplicity compose(Multiplicity outer, Multiplicity inner) {
        return Multiplicity.product(outer, inner);
    }

    /** An enum value reference {@code Kind.VALUE}: both the enumeration and the value must exist. */
    private TypedSpec enumValue(EnumValue ev) {
        if (System.getenv("LL_TDG_DEBUG") != null
                && ev.fullPath().contains("DatabaseType")) {
            System.err.println("[tdg-debug] enumValue fqn=" + ev.fullPath()
                    + " found=" + ctx.findEnum(ev.fullPath()).isPresent());
        }
        // Enum.VALUE and <element>.property parse identically — a
        // DATABASE or MAPPING element on the left is METAMODEL property
        // access over the element's own system-store row (db.schemas,
        // mapping.enumerationMappings — the typeInference walk surface)
        String elCls = ctx.findEnum(ev.fullPath()).isEmpty()
                ? CallShapes.metamodelElementClass(ctx, ev.fullPath()) : null;
        if (elCls != null) {
            String elFqn = elCls.equals(com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS)
                    ? ctx.findClass(ev.fullPath()).orElseThrow().qualifiedName()
                    : ev.fullPath();
            Type elType = elCls.equals(com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS)
                    ? new Type.GenericType(elCls, List.of(new Type.ClassType(elFqn)))
                    : new Type.ClassType(elCls);
            var elRef = new com.legend.compiler.spec.typed
                    .TypedPackageableRef(elFqn, ExprType.one(elType));
            // the property under the INSTANCE's arguments (Class<LA_Person>.properties
            // is Property<LA_Person,Any|*>[*]) — the generic receiver rule
            ExprType pt = elType instanceof Type.GenericType eg
                    ? genericReceiverProperty(eg, new AppliedProperty(ev, ev.value()))
                    : new ExprType(ctx.findProperty(elCls, ev.value()).orElseThrow(
                            () -> new TypeInferenceException("class " + elCls
                                    + " has no property '" + ev.value() + "'")).type(),
                            ctx.findProperty(elCls, ev.value()).orElseThrow().multiplicity());
            return new TypedPropertyAccess(elRef, ev.value(), pt);
        }
        var en = ctx.findEnum(ev.fullPath()).orElseThrow(() -> new TypeInferenceException(
                "unknown enumeration '" + ev.fullPath() + "'"));
        if (!en.values().contains(ev.value())) {
            throw new TypeInferenceException("enumeration " + ev.fullPath()
                    + " has no value '" + ev.value() + "'");
        }
        return new TypedEnumValue(ev.fullPath(), ev.value(),
                ExprType.one(new Type.EnumType(ev.fullPath())));
    }

    /**
     * A {@code @Type} annotation used as a value: resolved to its target type and
     * typed as a <em>prototype value of that type</em> ({@code target[1]}) &mdash;
     * real Pure's convention, so {@code cast<T|m>(Any[m], type:T[1]):T[m]} and the
     * {@code to}/{@code toMany} signatures bind their target variable from this
     * value on the plain generic path (see {@link TypedTypeRef}).
     */
    private TypedSpec typeRef(TypeAnnotation ta) {
        Type target = annotationType(ta);
        if (ta instanceof TypeAnnotation.MultiplicityRef mr) {
            // @[m]: the prototype value carries m — a |z signature binds z from it
            return new TypedTypeRef(target, new ExprType(target,
                    com.legend.compiler.element.type.Multiplicity.from(mr.multiplicity())));
        }
        return new TypedTypeRef(target, ExprType.one(target));
    }

    private Type annotationType(TypeAnnotation ta) {
        return annotations.annotationType(ta);
    }

    /** True when class {@code classFqn} declares a derived property {@code name}
     * taking exactly {@code arity} parameters (an overload of the one findProperty returned). */
    private boolean derivedOverloadArity(String classFqn, String name, int arity) {
        return ctx.findClassDefinition(classFqn)
                .map(cd -> cd.derivedProperties().stream()
                        .anyMatch(dp -> dp.name().equals(name) && dp.parameters().size() == arity))
                .orElse(false);
    }

    Type namedType(TypeExpression te) {
        return annotations.namedType(te);
    }

    /** Date-literal precision: year/year-month = Date; a full day = StrictDate; any time part = DateTime. */
    private static Type dateType(PureDateLiteral lit) {
        PureDateLiteral.Precision p = lit.precision();
        return p.atLeast(PureDateLiteral.Precision.HOUR) ? Type.Primitive.DATE_TIME
                : p == PureDateLiteral.Precision.DAY ? Type.Primitive.STRICT_DATE
                : Type.Primitive.DATE;
    }

    /** The CLOSED deferred-kind list (bind-once, family A): a let rhs
     * whose meaning is decided at the USE site, not the binding site —
     * it has no type in isolation, so the binding PARKS the raw syntax
     * ({@link Env#withDeferred}) instead of dying here. Exactly the
     * kinds whose standalone synth walls: a graph-fetch tree literal
     * and a mapped/aggregate column spec. (The engine needs no parking:
     * its trees are first-class {@code RootGraphFetchTree} values, and
     * it REJECTS the other shapes at the binding — parking mirrors its
     * use-site inScopeVars resolution at the checker layer.) */
    /** The legacy {@code col(lambda, 'name')} column-spec constructor
     * (BasicColumnSpecification), before its desugar to a ColSpec. */
    private static boolean legacyColCall(ValueSpecification v) {
        return ProjectChecker.isLegacyColumnCall(v)
                && ((AppliedFunction) v).parameters().get(0)
                        instanceof LambdaFunction;
    }

    private static boolean legacyAggCall(ValueSpecification v) {
        return v instanceof AppliedFunction c
                && GroupByChecker.isAggSpelling(c.function())
                && c.parameters().size() == 2
                && c.parameters().get(0) instanceof LambdaFunction
                && c.parameters().get(1) instanceof LambdaFunction;
    }

    static boolean deferredLetRhs(ValueSpecification v) {
        // a quote/eval tree carrier — bare, or under the corpus's
        // ->cast(@RootGraphFetchTree<T>) — is a tree LITERAL for binding
        // purposes: it types only at its graphFetch/serialize consumer
        // (GraphFetchChecker.unwrapCompiledTree strips the cast)
        if (v instanceof AppliedFunction c
                && CoreFn.of(c.function()).orElse(null) == CoreFn.CAST
                && c.parameters().size() == 2
                && (c.parameters().get(0) instanceof com.legend.protocol.spec.QuotedTreeCall
                        || c.parameters().get(0)
                                instanceof com.legend.protocol.spec.GraphFetchLiteral)) {
            return true;
        }
        // a let-bound column-spec COLLECTION (`let cols = [col(f|...,'a'),
        // ...]->cast(@BasicColumnSpecification<Firm>)`) parks the same
        // way: its specs type only against the project that consumes it
        if (v instanceof AppliedFunction c2
                && CoreFn.of(c2.function()).orElse(null) == CoreFn.CAST
                && c2.parameters().size() == 2
                && deferredLetRhs(c2.parameters().get(0))) {
            return true;
        }
        if (v instanceof PureCollection pc && !pc.values().isEmpty()
                && pc.values().stream().allMatch(e -> (e instanceof ColSpec cs2
                                && cs2.function1() != null)
                        || legacyColCall(e))) {
            return true;
        }
        if (legacyColCall(v)) {
            return true;
        }
        // a legacy AGGREGATE value (`agg(x|…, y|…)`, engine AggregateValue)
        // — bare or a collection of them — types only against the groupBy
        // that consumes it (GroupByChecker.legacyToModern chases the let)
        if (legacyAggCall(v) || v instanceof PureCollection apc && !apc.values().isEmpty()
                && apc.values().stream().allMatch(Typer::legacyAggCall)) {
            return true;
        }
        return v instanceof com.legend.protocol.spec.GraphFetchLiteral
                || v instanceof com.legend.protocol.spec.QuotedTreeCall
                || v instanceof ColSpec cs && cs.function1() != null
                || v instanceof ColSpecArray arr && arr.colSpecs().stream()
                        .anyMatch(c -> c.function1() != null);
    }

    /** A bare {@code ~col}: a first-class {@code ColSpec<(col:?)>[1]} value (see {@link TypedColSpec}). */
    private TypedSpec typedColSpec(ColSpec cs) {
        if (cs.function1() != null) {
            throw new TypeInferenceException("~" + cs.name()
                    + ": mapped/aggregate column specifications need an enclosing call to type against");
        }
        Type row = new Type.RelationType(List.of(unknownColumn(cs.name())));
        return new TypedColSpec(cs.name(),
                ExprType.one(new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.COL_SPEC, List.of(row))));
    }

    /** A bare {@code ~[a,b]}: a first-class {@code ColSpecArray<(a:?, b:?)>[1]} value. */
    private TypedSpec typedColSpecArray(ColSpecArray arr) {
        // ~[] is LEGAL where zero columns mean something: groupBy(~[], aggs)
        // is the whole-relation aggregate (the engine's empty-key grouping).
        if (arr.colSpecs().isEmpty()) {
            return new TypedColSpecArray(List.of(),
                    ExprType.one(new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.COL_SPEC_ARRAY,
                            List.of(new Type.RelationType(List.of())))));
        }
        List<Type.Column> cols = new ArrayList<>(arr.colSpecs().size());
        List<String> names = new ArrayList<>(arr.colSpecs().size());
        for (ColSpec cs : arr.colSpecs()) {
            if (cs.function1() != null) {
                throw new TypeInferenceException("~" + cs.name()
                        + ": mapped/aggregate column specifications need an enclosing call to type against");
            }
            if (names.contains(cs.name())) {
                throw new SchemaInvariantException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            names.add(cs.name());
            cols.add(unknownColumn(cs.name()));
        }
        Type row = new Type.RelationType(cols);
        return new TypedColSpecArray(names,
                ExprType.one(new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.COL_SPEC_ARRAY, List.of(row))));
    }

    /** A column of a colspec VALUE: named, with the unknown type {@code ?} until ⊆/= solves it. */
    private static Type.Column unknownColumn(String name) {
        return new Type.Column(name, InferenceKernel.UNKNOWN_COLUMN_TYPE, Multiplicity.Bounded.ONE);
    }

    // =====================================================================
    // Small shared helpers
    // =====================================================================

    /** Check mode: the synthesized type/multiplicity must conform to {@code expected}. */
    void requireConforms(ExprType actual, ExprType expected) {
        // Reuse the kernel: unify(expected, actual) checks actual <: expected for scalars
        // (throws on mismatch). Empty bindings — expected is concrete, nothing to solve.
        // NOTE: class-subtype conformance for user-call arguments is deferred with the user path.
        kernel.unify(expected.type(), actual.type(), new Bindings());
        kernel.unifyMult(expected.multiplicity(), actual.multiplicity(), actual.type(), new Bindings());
    }

    /**
     * Decimal literal type: precision 38, scale from the literal text (§8). A negative scale
     * (e.g. {@code 1E3d}) normalizes to 0; a scale that genuinely exceeds 38 cannot be represented
     * as a {@code DECIMAL(38, s)} &mdash; reject it loudly rather than silently truncate the value.
     */
    private static Type decimalType(BigDecimal value) {
        int scale = Math.max(0, value.scale());
        if (scale > Type.PrecisionDecimal.MAX_PRECISION) {
            throw new TypeInferenceException("decimal literal '" + value.toPlainString()
                    + "' needs scale " + scale + ", exceeding the maximum of " + Type.PrecisionDecimal.MAX_PRECISION);
        }
        return new Type.PrecisionDecimal(Type.PrecisionDecimal.MAX_PRECISION, scale);
    }
}
