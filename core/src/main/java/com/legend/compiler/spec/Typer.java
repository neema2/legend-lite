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

    /** The legacy TDS desugars, tried before the core dispatch (plan W1.6: moved out whole). */
    private final TdsDesugars desugars;

    /** The overload machinery: the generic application rule and candidate ranking (plan W1.6: moved out whole). */
    private final Overloads overloads;

    Typer(ModelContext ctx, InferenceKernel kernel) {
        this.ctx = ctx;
        this.kernel = kernel;
        this.annotations = new TypeAnnotations(ctx);
        this.desugars = new TdsDesugars(this);
        this.overloads = new Overloads(this, ctx, kernel);
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
            // a tree types where it is READ -- the tree argument of graphFetch,
            // graphFetchChecked, serialize, isDistinct (let-bound ones through the
            // alias channel); on its own it has no type here yet (the engine's is
            // RootGraphFetchTree<T>)
            case com.legend.protocol.spec.GraphFetchLiteral gf -> throw new TypeInferenceException(
                    "graph-fetch tree #{" + gf.className() + "{...}}# types only as the tree"
                            + " argument of graphFetch, graphFetchChecked, serialize or isDistinct");
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
                    synth(new AppliedFunction(com.legend.builtin.Pure.SQL_NULL.qualifiedName(), List.of()), env);
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
                        ExprType declared = declaredType(pv);
                        names.add(pv.name());
                        params.add(new Type.Param(declared.type(), declared.multiplicity()));
                        scope = scope.with(pv.name(), declared);
                    }
                    List<TypedSpec> stmts = new ArrayList<>();
                    for (int si = 0; si < lf.body().size() - 1; si++) {
                        if (lf.body().get(si) instanceof AppliedFunction lset
                                && com.legend.compiler.ResolvedNames.form(lset).orElse(null) == CoreFn.LET
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


    /** The parser's INFIX marker rides the typed run ({@code TypedCollection
     *  .operatorRun}) — on BOTH typing paths (eager and deferred), the one
     *  place an application's typed arguments are final. */
    static void markOperatorRun(boolean infix, List<TypedSpec> args) {
        if (infix && args.size() == 1
                && args.get(0) instanceof com.legend.compiler.spec.typed.TypedCollection run) {
            args.set(0, run.asOperatorRun());
        }
    }

    /**
     * A function application. The name resolves to a {@link CoreFn} exactly once;
     * a core construct dispatches through the exhaustive {@code switch} in
     * {@link #applyCore}, anything else is a library call on the generic path.
     */
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
        TypedSpec tdsSchema = desugars.tdsSchemaDesugars(af, env);
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
                if (TdsDesugars.literalColName(af.parameters().get(1)) != null) {
                    return rowCellRead(af, env, getter.get());   // the FOLD: the row's column
                }
                // a NON-literal column name: the call to the lifted qualified
                // property stands (typed by its declaration; RowGetters lowers it
                // by name once unroll/inlining has made the name literal)
                return liftedAccessorCall(getter.get(), grecv, synth(af.parameters().get(1), env));
            }
        }
        TypedSpec tdsGetter = desugars.tdsGetterDesugars(af, env);
        if (tdsGetter != null) {
            return tdsGetter;
        }
        TypedSpec rowCell = desugars.tdsRowCellIndexRead(af, env);
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
                        .map(v -> new ColSpec(TdsDesugars.stripQuotes(((CString) v).value()), null, null))
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
            TypedSpec folded = desugars.extractEnumValueFold(af, env);
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
                com.legend.builtin.DecisionProbe.lifted(af.propertyCall() ? "qp-var:dot" : "qp-var:call", simple, d.bodyFunctionFqn(), ctx.findFunction(d.bodyFunctionFqn()).size());
                return applyGeneric(new AppliedFunction(d.bodyFunctionFqn(), qargs), env);
            }
        }
        Optional<CoreFn> core = com.legend.compiler.ResolvedNames.form(af);
        if (core.isPresent()) {
            com.legend.builtin.DecisionProbe.form(af.function(), core.get().name());
            // real pure resolves by TYPE: a model function of this name whose
            // first parameter is the receiver's CLASS (Database.join(name),
            // relational.pure) out-ranks the bare special form, which only
            // ever meant relations, tables and class extents
            TypedFunction owned = ReceiverOwnedFunctions.of(this, af, env);
            if (owned != null) {
                return applyGeneric(new AppliedFunction(owned.qualifiedName(),
                        af.parameters()), env);
            }
            return applyCore(core.get(), af, env);
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
        // β-inlines with every other user call. A DOT call ($r.toSQLString(t))
        // is a qualified-property expression in real pure: the receiver's
        // property comes first and the function library only when it has none
        // (FunctionExpressionProcessor.matchFunction), whatever functions share
        // the name (build rebuild Phase 3: with every upstream version a
        // candidate, toSQL(...).toSQLString(t) met toSQLString functions of its
        // arity). A call spelled with -> tries the property only when no
        // function of its arity exists (tds::join beside SchemaState.join(other);
        // Phase 5 batch 147). The receiver typed here is typed again by the
        // route taken (applyGeneric), as the generic path's own auto-map probe
        // types a dot call's receiver before its arguments: a time cost, recorded as
        // PARKED_WORK_LEDGER PARK-6 (type each receiver once; Phase 3 audit S5, 2026-10-07).
        if (!af.parameters().isEmpty() && (af.propertyCall() || functionCandidates(af).stream()
                .noneMatch(f -> f.parameters().size() == af.parameters().size()))) {
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
                com.legend.builtin.DecisionProbe.lifted(af.propertyCall() ? "qp-arity:dot" : "qp-arity:call", qname, d.bodyFunctionFqn(), ctx.findFunction(d.bodyFunctionFqn()).size());
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
            case VALIDATE -> throw new TypeInferenceException("validate is desugared before typing"
                    + " (ValidateDesugar): a validate call reached the typer");
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
            // legacyNavigate: the pre-map rule, target table rows spelled in
            case LEGACY_NAVIGATE -> NavigateChecker.legacy(this, af, env);
            case ROUTE -> throw NavigateChecker.routeAlone();
            case IS_DISTINCT -> IsDistinctChecker.check(this, af, env);
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
            case TABLE_REFERENCE -> TableReferenceChecker.check(this, af, env);
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
            case EMPTY_FIRST -> SortChecker.nullOrder(this, af, env, TypedSortInfo.NullOrder.FIRST);
            case EMPTY_LAST -> SortChecker.nullOrder(this, af, env, TypedSortInfo.NullOrder.LAST);
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

    // ---- the overload machinery lives in Overloads (plan W1.6); delegates keep the callers unchanged ----

    TypedSpec applyGeneric(AppliedFunction af, Env env) {
        return overloads.applyGeneric(af, env);
    }

    TypedSpec synthRecordField(ValueSpecification v, Env env) {
        return overloads.synthRecordField(v, env);
    }

    boolean functionValuedHelperCall(AppliedFunction call) {
        return overloads.functionValuedHelperCall(call);
    }

    @com.legend.base.Nullable ValueSpecification rawSchemaErasedExpansion(ValueSpecification v) {
        return overloads.rawSchemaErasedExpansion(v);
    }

    ValueSpecification alphaRename(ValueSpecification v) {
        return overloads.alphaRename(v);
    }

    Application checkGeneric(AppliedFunction af, Env env) {
        return overloads.checkGeneric(af, env);
    }

    Application checkGeneric(AppliedFunction af, Env env, @com.legend.base.Nullable Type expected) {
        return overloads.checkGeneric(af, env, expected);
    }

    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args) {
        return overloads.checkGenericTyped(af, args);
    }

    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args,
            @com.legend.base.Nullable Type expected) {
        return overloads.checkGenericTyped(af, args, expected);
    }

    ModelContext ctx() {
        return overloads.ctx();
    }

    boolean isFunctionTyped(Type t) {
        return overloads.isFunctionTyped(t);
    }

    static boolean genericRawIs(Type t, com.legend.model.ClassDefinition def) {
        return Overloads.genericRawIs(t, def);
    }

    static boolean genericRawIs(Type t, String rawFqn) {
        return Overloads.genericRawIs(t, rawFqn);
    }

    TypedSpec synthBody(LambdaFunction lam, Env scope) {
        return overloads.synthBody(lam, scope);
    }

    TypedSpec typeLambda(LambdaFunction lam, Type functionParamType, Bindings b, Env env) {
        return overloads.typeLambda(lam, functionParamType, b, env);
    }

    List<TypedFunction> functionCandidates(String name) {
        return overloads.functionCandidates(name);
    }

    List<TypedFunction> functionCandidates(AppliedFunction af) {
        return overloads.functionCandidates(af);
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
            var res = com.legend.model.SignatureMangle.resolve(ref.fullPath(), ctx::findFunction,
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
            TypedSpec body = CallNodes.mint(ctx.implementations(), fn, argRefs, out);
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

    private static boolean isExactlyOne(Multiplicity m) {
        return m instanceof Multiplicity.Bounded b && b.lower() == 1 && b.upper() != null && b.upper() == 1;
    }

    /** A collection literal {@code [a,b,c]}: element type = common supertype; multiplicity = exact count. */
    private TypedSpec collection(PureCollection coll, Env env) {
        List<TypedSpec> elements = new ArrayList<>(coll.values().size());
        for (ValueSpecification v : coll.values()) {
            TypedSpec e = synth(TdsNullForms.listElement(v), env);
            // A literal of MORE than one value takes each element exactly [1] -- legend-engine
            // (ValueSpecificationBuilder.visit(Collection)) and legend-pure (InstanceValueValidator)
            // both refuse otherwise, before any overload is matched. So `$x.n * 1.1` over a nullable
            // column (the parser's run times([$x.n, 1.1])) is refused, as there: `->toOne()` says
            // what is meant. The 2026-09-11 loosening (MULTIPLICITY_AUDIT §4a) summed the bounds
            // instead and typed a possibly-NULL cell [1].
            if (coll.values().size() > 1 && !isExactlyOne(e.info().multiplicity())) {
                throw new TypeInferenceException("Collection element must have a multiplicity [1], found "
                        + e.info().multiplicity().text());
            }
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
        com.legend.builtin.DecisionProbe.literalMult(elements.size(), lo, hi);
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
                TdsDesugars.literalColName(af.parameters().get(1)),
                "TDS cell read requires a literal column name");
        // an ERASED row (a TDSRow in a type position): the column's type is
        // the accessor's DECLARED type (getString: String[1]) — real pure knows
        // no more at type time either; a concrete row keeps its schema's type.
        // The accessor IS the erased row's read (a bare $r.col on it is
        // refused — relationColumn), so the cell is built here, not synthesized.
        if (Type.schemaView(grecv.info().type()) instanceof Type.RelationType erased
                && erased.isLateBound()) {
            return new TypedPropertyAccess(grecv, colRef, liftedAccessor(getter,
                    grecv.info(), ExprType.one(Type.Primitive.STRING)).output());
        }
        TypedSpec cell = synth(new AppliedProperty(
                af.parameters().get(0), colRef), env);
        // getNullableString returns String[0..1] (tds.pure:82/112) —
        // the optional cell read IS the semantics, no strictening
        if (TdsDesugars.rowGetter(af, com.legend.builtin.NativeFn.RowGetter.GET_NULLABLE_STRING)
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
    static @com.legend.base.Nullable TypedSpec lateBoundGridMarker(
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
                return TdsDesugars.rowCells(source, rt2);
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
            com.legend.builtin.DecisionProbe.lifted("qp-zero-arg:dot", ap.property(), d.bodyFunctionFqn(), ctx.findFunction(d.bodyFunctionFqn()).size());
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
    private @com.legend.base.Nullable ExprType lambdaClassifierProperty(String name) {
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
    static boolean tdsReceiver(Type t) {
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
                            if (rel.isErasedRow()) {
                                // a bare $r.col on an ERASED row: refused -- the owner's
                                // accessors (get, isNull, getString, ...) read its cells
                                // (rowCellReadOnRow, TdsDesugars.erasedCell); real pure
                                // has no bare column on a TDSRow either
                                String owner = rel.dynamicColumns().get(0).type()
                                        instanceof Type.ClassType oc ? oc.fqn() : "row";
                                throw new TypeInferenceException("a " + owner
                                        + " has no property '" + name
                                        + "' (read a cell with its accessor, e.g. getString('"
                                        + name + "'))");
                            }
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

    /** A lambda parameter's declared type and multiplicity ({@code minQty: Integer[1]}; [1] when unstated). */
    ExprType declaredType(Variable parameter) {
        Type type = namedType(java.util.Objects.requireNonNull(parameter.type(),
                "lambda parameter without a declared type"));
        return new ExprType(type, parameter.multiplicity() == null
                ? Multiplicity.Bounded.ONE : Multiplicity.from(parameter.multiplicity()));
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
                && GroupByChecker.isAgg(c)
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
                && com.legend.compiler.ResolvedNames.form(c).orElse(null) == CoreFn.CAST
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
                && com.legend.compiler.ResolvedNames.form(c2).orElse(null) == CoreFn.CAST
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
