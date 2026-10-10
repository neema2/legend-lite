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
 * The typer's OVERLOAD MACHINERY: the generic application rule ({@link #applyGeneric}), the candidate sets
 * ({@link #functionCandidates}), the ranking and the retry over deferred lambda and column-spec arguments
 * ({@link #checkGeneric}, {@code checkWithDeferred}), lambda typing against a formal, and the NormalizeRequired and
 * schema-erased inlining the generic path performs. Moved out of {@code Typer} whole by execution plan W1.6
 * (2026-09-29), a pure move: W3.2 and W3.3 replace this machinery with the reference's matcher and candidate loop, and
 * it now has one home to replace. {@code Typer} keeps one-line delegates, so callers are unchanged.
 */
final class Overloads {

    private final Typer t;
    private final ModelContext ctx;
    private final InferenceKernel kernel;

    Overloads(Typer t, ModelContext ctx, InferenceKernel kernel) {
        this.t = t;
        this.ctx = ctx;
        this.kernel = kernel;
    }

    private TypedSpec synth(ValueSpecification vs, Env env) {
        return t.synth(vs, env);
    }

    private Type namedType(TypeExpression te) {
        return t.namedType(te);
    }

    private static void markOperatorRun(boolean infix, List<TypedSpec> args) {
        Typer.markOperatorRun(infix, args);
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
        TypedSpec autoMapped = CallShapes.autoMapReceiver(t, af, env);
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
        return rawGridOrSelf(CallNodes.mint(ctx.implementations(), a.chosen(), a.args(), a.out(), af.pos()));
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
    private @com.legend.base.Nullable TypedSpec derivedShadow(AppliedFunction af,
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
        com.legend.builtin.DecisionProbe.lifted(af.propertyCall() ? "qp-owned:dot" : "qp-owned:call", simple, d.bodyFunctionFqn(), ctx.findFunction(d.bodyFunctionFqn()).size());
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
        } else if (com.legend.builtin.NativeFn.Carrier.fetchDbGrid(nc.callee().id()) != null) {
            sql = CatalogGrids.sql(nc);
            if (sql != null) {
                // §4bZ-U leg 4: the JDBC spec fixes the metadata result
                // shape and the catalog projections are OURS — a
                // DECLARED table-function schema, never late-bound (no
                // LIMIT-0 probe, reads stamp statically)
                return new com.legend.compiler.spec.typed
                        .TypedRawSqlRelation(sql, ExprType.one(
                                Type.relation(CatalogGrids.gridSchema(java.util.Objects.requireNonNull(com.legend.builtin.NativeFn.Carrier.fetchDbGrid(nc.callee().id()))))));
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

    private final java.util.ArrayDeque<com.legend.model.FunctionId> normalizing = new java.util.ArrayDeque<>();
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
        com.legend.model.FunctionId key = chosen.id();
        if (normalizing.contains(key)) {
            throw new TypeInferenceException("recursive NormalizeRequired function '"
                    + chosen.qualifiedName() + "' cannot be inlined ("
                    + normalizing.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(" -> ")) + " -> " + key + ")");
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
            return synth(new StaticFold(t, env).fold(body), env);
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
    @com.legend.base.Nullable ValueSpecification rawSchemaErasedExpansion(ValueSpecification v) {
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
    Application checkGeneric(AppliedFunction af, Env env, @com.legend.base.Nullable Type expected) {
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
     * reads a type before choosing its rule; each argument synths ONCE — a
     * second synth repeats the typing, and nested, multiplies it; the typer
     * records no state per synth, so the cost is time: checked 2026-10-07,
     * Phase 3 audit S5). */
    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args) {
        return checkGenericTyped(af, args, null);
    }

    Application checkGenericTyped(AppliedFunction af, List<TypedSpec> args,
            @com.legend.base.Nullable Type expected) {
        List<ExprType> argTypes = args.stream().map(TypedSpec::info).toList();
        List<TypedFunction> candidates = functionCandidates(af);
        if (candidates.isEmpty()) {
            com.legend.builtin.DecisionProbe.unknownFunction(af.function(), af.propertyCall(), "generic");
            // C0.5a: zero candidates = the name is NOT IN THE CATALOG (an
            // unported platform function, usually) — say so plainly
            throw new TypeInferenceException("unknown function '"
                    + af.function() + "' — no function of this name in the"
                    + " native or user catalog (unported platform function,"
                    + " or a misspelling)");
        }
        InferenceKernel.Resolution r = kernel.resolveOverload(candidates, argTypes, expected);
        return new Application(r.chosen(), args, NumberKinds.refine(r.chosen(), args,
                refineParseDate(r.chosen(), args, refineDecimalCarrier(r.chosen(), r.output()))));
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

    /** The context's tables — the checkers mint call nodes by them. */
    ModelContext ctx() {
        return ctx;
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
                .filter(c -> DeferredArgs.shapesMatch(t, c, raw))
                .toList();
        if (arity.isEmpty()) {
            com.legend.builtin.DecisionProbe.unknownFunction(af.function(), af.propertyCall(), candidates.isEmpty() ? "deferred-none" : "deferred-arity");
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
                Application built = bindDeferredAndBuild(cand, raw, typed.clone(), env, af.infix());
                com.legend.builtin.DecisionProbe.retryAccept(af.function(), ranked.get(0).id().qualified(), cand.id().qualified(), ranked.indexOf(cand));
                return built;
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
                    typed[i] = OverChecker.check(t, (AppliedFunction) raw.get(i), env, expected);
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
        return new Application(chosen, typedArgs, NumberKinds.refine(chosen, typedArgs,
                refineImportDataFlow(chosen, raw, typed, env, refineDecimalCarrier(chosen, out))));
    }

    /** The execute exeCtx overload under {@code importDataFlow}: the
     * Result's relation gains the union's key threads the executed
     * projection carries ({@link ImportDataFlow}) — a refinement of the
     * signature's output from the call's own facts, like the Decimal
     * carrier. */
    private ExprType refineImportDataFlow(TypedFunction chosen, List<ValueSpecification> raw,
            TypedSpec[] typed, Env env, ExprType out) {
        if (raw.size() != 5
                || !com.legend.model.FunctionId.of(Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY)
                        .equals(chosen.id())
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

    static boolean genericRawIs(Type t, com.legend.model.ClassDefinition def) {
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

    /** Candidates best-first by legend-pure's ranking over the typed arguments ({@link
     *  InferenceKernel#rankNonLambda}; stable — declaration order breaks ties, preserving
     *  first-max semantics for the winner; a candidate a typed argument does not fit comes
     *  last); arity misfits filtered; empty = the same loud no-overload error. */
    private List<TypedFunction> selectRankedByPresentArgs(String name,
            List<TypedFunction> arity, TypedSpec[] typed,
            @com.legend.base.Nullable List<ValueSpecification> raw) {
        List<ExprType> argTypes = new ArrayList<>(typed.length);
        for (TypedSpec t : typed) {
            argTypes.add(t == null ? null : t.info());
        }
        record Ranked(TypedFunction fn, @com.legend.base.Nullable FunctionMatch rank, int declIdx) {
        }
        List<Ranked> ranked = new ArrayList<>();
        String arityRejection = null;
        for (int i = 0; i < arity.size(); i++) {
            TypedFunction c = arity.get(i);
            if (raw != null && !lambdaAritiesFit(c, raw, typed)) {
                if (arityRejection == null) {
                    arityRejection = lambdaArityMismatch(c, raw, typed);
                }
                continue;
            }
            ranked.add(new Ranked(c, kernel.rankNonLambda(c, argTypes), i));
        }
        if (ranked.isEmpty()) {
            throw new TypeInferenceException(
                    "no overload of '" + name + "' matches the argument types"
                            + (arityRejection != null
                                    ? " (" + arityRejection + ")" : ""));
        }
        ranked.sort((a, b) -> {
            FunctionMatch ra = a.rank();
            FunctionMatch rb = b.rank();
            if (ra == null || rb == null) {
                int c = Boolean.compare(ra == null, rb == null);
                return c != 0 ? c : Integer.compare(a.declIdx(), b.declIdx());
            }
            int c = ra.compareTo(rb);
            return c != 0 ? c : Integer.compare(a.declIdx(), b.declIdx());
        });
        return ranked.stream().map(Ranked::fn).toList();
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
    private static @com.legend.base.Nullable String lambdaArityMismatch(
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
        return LambdaBodies.synthBody(t, lam, scope);
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
                    && com.legend.compiler.ResolvedNames.form(lf2).orElse(null) == CoreFn.LET
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
    private static Type.@com.legend.base.Nullable FunctionType asFunctionType(Type t) {
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
            cols.add(new TypedAggCol(cs.name(), map, reduce, List.of()));
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
        // a function referenced by its engine SIGNATURE ID names ONE overload,
        // registered under that exact id (a spelling this platform cannot
        // reproduce is a miss, never a redirect)
        return ctx.findFunctionById(name);
    }

    /**
     * Call-aware overload set: when the resolver recorded IMPORT-AMBIGUITY
     * candidates on the node (several imported packages define the simple
     * name), the overload set is the UNION across all of them — real
     * pure's function matching collects across imports and signature
     * scoring picks. Single-referent calls keep the plain name path.
     */
    List<TypedFunction> functionCandidates(AppliedFunction af) {
        List<TypedFunction> found = candidatesOf(af);
        if (com.legend.builtin.DecisionProbe.INSTALLED != null) {
            com.legend.builtin.DecisionProbe.candidates(af.function(),
                    af.referents().isEmpty() ? "bare" : "node", found.stream().map(TypedFunction::definition));
        }
        return found;
    }

    private List<TypedFunction> candidatesOf(AppliedFunction af) {
        if (af.referents().isEmpty()) {
            com.legend.builtin.DecisionProbe.bareCall(af.function(), af.pos() != null, af.propertyCall(), af.infix());
            return functionCandidates(af.function());
        }
        List<TypedFunction> union = new ArrayList<>();
        RuntimeException firstBroken = null;
        for (String fqn : af.referents()) {
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
}
