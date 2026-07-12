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
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.TypeExpression;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CDate;
import com.legend.parser.spec.CLatestDate;
import com.legend.parser.spec.CTime;
import com.legend.parser.spec.CDecimal;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.NewInstanceCast;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.values.PureDateLiteral;
import com.legend.parser.spec.TypeAnnotation;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

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

    Typer(ModelContext ctx, InferenceKernel kernel) {
        this.ctx = ctx;
        this.kernel = kernel;
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
            case CInteger lit -> new TypedCInteger(lit.value(), ExprType.one(Type.Primitive.INTEGER));
            case CString lit -> new TypedCString(lit.value(), ExprType.one(Type.Primitive.STRING));
            case CBoolean lit -> new TypedCBoolean(lit.value(), ExprType.one(Type.Primitive.BOOLEAN));
            case CFloat lit -> new TypedCFloat(lit.value(), ExprType.one(Type.Primitive.FLOAT));
            case CDecimal lit -> new TypedCDecimal(lit.value(), ExprType.one(decimalType(lit.value())));
            // Date literals type by PRECISION (engine's CStrictDate/CDateTime split):
            // year/year-month -> Date, full day -> StrictDate, any time part -> DateTime.
            case CDate lit -> new TypedCDate(lit.value(), ExprType.one(dateType(lit.value())));
            case CTime lit -> new TypedCTime(lit.value(), ExprType.one(Type.Primitive.STRICT_TIME));
            case CLatestDate ignored -> new TypedCLatestDate(ExprType.one(Type.Primitive.LATEST_DATE));
            case TypeAnnotation ta -> typeRef(ta);
            case Variable v -> new TypedVariable(v.name(), env.lookup(v.name()).orElseThrow(
                    () -> new TypeInferenceException("unbound variable '$" + v.name() + "'")));
            case AppliedFunction af -> applyFunction(af, env);
            case AppliedProperty ap -> accessProperty(ap, env);
            case PureCollection coll -> collection(coll, env);
            case PackageableElementPtr ref -> classReference(ref);
            case NewInstance ni -> NewChecker.check(this, ni, env);
            case ColSpec cs -> typedColSpec(cs);
            case ColSpecArray arr -> typedColSpecArray(arr);
            case EnumValue ev -> enumValue(ev);
            // EXHAUSTIVE over sealed ValueSpecification — no default arm
            // (root package-info invariant): a new AST variant is a COMPILE
            // error here, not a runtime surprise. The two arms below are the
            // deliberate not-yet-implemented forms.
            case LambdaFunction lf -> throw new TypeInferenceException(
                    "a bare lambda has no type outside a call position"
                            + " (lambdas type against their call's signature)");
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
                                src.info().multiplicity()));
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
    private TypedSpec applyFunction(AppliedFunction af, Env env) {
        Optional<CoreFn> core = CoreFn.of(af.function());
        return core.isPresent() ? applyCore(core.get(), af, env) : applyGeneric(af, env);
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
        return switch (fn) {
            case LET -> LetChecker.check(this, af, env);
            case IF -> IfChecker.check(this, af, env);
            // ^Class(...) desugars to new(PackageableElementPtr, NewInstance); the inner node
            // carries the payload. Other arities/shapes of `new` ride the generic path.
            case NEW -> (af.parameters().size() == 2 && af.parameters().get(1) instanceof NewInstance ni)
                    ? NewChecker.check(this, ni, env)
                    : applyGeneric(af, env);
            case CAST, TO, TO_MANY -> CastChecker.check(this, af, env);
            case MATCH -> MatchChecker.check(this, af, env);
            case EVAL -> EvalChecker.check(this, af, env);
            case TDS -> TdsChecker.check(this, af, env);
            case SORT_BY -> SortChecker.sortBy(this, af, env, true);
            case SORT_BY_REVERSED -> SortChecker.sortBy(this, af, env, false);
            case GET_ALL -> GetAllChecker.check(this, af, env);
            case FROM -> FromChecker.check(this, af, env);
            case WRITE -> WriteChecker.check(this, af, env);
            case FOLD -> FoldChecker.check(this, af, env);
            case NAVIGATE -> NavigateChecker.check(this, af, env);
            // legacyNavigate: the pre-map rule under the legacy bridge's
            // name, with the target's table rows spelled into the call.
            case LEGACY_NAVIGATE -> NavigateChecker.legacy(this, af, env);
            case GRAPH_FETCH -> GraphFetchChecker.graphFetch(this, af, env);
            case SERIALIZE -> GraphFetchChecker.serialize(this, af, env);
            case OVER -> OverChecker.check(this, af, env);
            case SOURCE_URL -> SourceUrlChecker.check(this, af, env);
            case FLATTEN -> FlattenChecker.check(this, af, env);
            case PIVOT -> PivotChecker.check(this, af, env);
            case TABLE_REFERENCE -> TableReferenceChecker.check(this, af);
            case PROJECT -> ProjectChecker.check(this, af, env);
            case EXTEND -> ExtendChecker.check(this, af, env);
            case GROUP_BY -> GroupByChecker.check(this, af, env);
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
            case FILTER -> FilterChecker.check(this, af, env);
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
        Application a = checkGeneric(af, env);
        return emitCall(a.chosen(), a.args(), a.out());
    }

    /**
     * Run the generic application rule without emitting a node &mdash; the CHECK
     * half of the check/emit split ({@link Application}). Calls with
     * <em>deferred</em> arguments take {@link #checkWithDeferred}.
     */
    Application checkGeneric(AppliedFunction af, Env env) {
        if (af.parameters().stream().anyMatch(Typer::deferredArg)) {
            return checkWithDeferred(af, env);
        }
        List<TypedSpec> args = new ArrayList<>(af.parameters().size());
        for (ValueSpecification p : af.parameters()) {
            args.add(synth(p, env));
        }
        List<ExprType> argTypes = args.stream().map(TypedSpec::info).toList();

        List<TypedFunction> candidates = functionCandidates(af.function());
        if (candidates.isEmpty()) {
            throw new TypeInferenceException("unknown function '" + af.function() + "'");
        }
        InferenceKernel.Resolution r = kernel.resolveOverload(candidates, argTypes);
        return new Application(r.chosen(), args,
                refineParseDate(r.chosen(), args, refineDecimalCarrier(r.chosen(), r.output())));
    }

    /**
     * parseDate over a LITERAL refines its abstract Date output to the
     * concrete kind the string's shape determines ('...T...' is a DateTime,
     * a bare date is a StrictDate) — real pure's parseDate returns the
     * written kind, and the abstract-Date root otherwise cannot tell a
     * midnight DateTime from a StrictDate at the wire.
     */
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
        return chosen.isNative()
                ? new TypedNativeCall(chosen, args, out)
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
        List<TypedFunction> arity = functionCandidates(af.function()).stream()
                .filter(c -> c.parameters().size() == raw.size())
                .filter(c -> deferredShapesMatch(c, raw))
                .toList();
        if (arity.isEmpty()) {
            throw new TypeInferenceException("no overload of '" + af.function()
                    + "' matches " + raw.size() + " argument(s) of these shapes");
        }

        TypedSpec[] typed = new TypedSpec[raw.size()];
        for (int i = 0; i < raw.size(); i++) {
            if (!deferredArg(raw.get(i))) {
                typed[i] = synth(raw.get(i), env);   // value args first
            }
        }

        TypedFunction chosen = selectByPresentArgs(af.function(), arity, typed);

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
                typed[i] = raw.get(i) instanceof LambdaFunction lam
                        ? typeLambda(lam, chosen.parameters().get(i).type(), b, env)
                        : typeFuncColSpec(raw.get(i), chosen.parameters().get(i).type(), b, env);
            }
        }

        ExprType out = kernel.resolveOutput(chosen.returnType(), chosen.returnMultiplicity(), b);
        return new Application(chosen, List.of(typed), refineDecimalCarrier(chosen, out));
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

    /** An argument whose typing must wait for the chosen signature: a lambda, or a colspec carrying one. */
    private static boolean deferredArg(ValueSpecification p) {
        return p instanceof LambdaFunction
                || (p instanceof ColSpec cs && cs.function1() != null)
                || (p instanceof ColSpecArray arr
                        && arr.colSpecs().stream().anyMatch(c -> c.function1() != null));
    }

    /**
     * Prefilter candidates by the deferred arguments' <em>syntactic shape</em>
     * (engine dispatches its colspec overloads the same way): a lambda needs a
     * function-typed parameter; {@code ~a:x|…} needs {@code FuncColSpec} (or
     * {@code AggColSpec} when it carries a reducer {@code function2}); the array
     * forms need the {@code …Array} classes. Value-argument scoring cannot see
     * this, since deferred slots are not yet typed.
     */
    private static boolean deferredShapesMatch(TypedFunction c, List<ValueSpecification> raw) {
        for (int i = 0; i < raw.size(); i++) {
            ValueSpecification p = raw.get(i);
            if (!deferredArg(p)) {
                continue;
            }
            Type t = c.parameters().get(i).type();
            boolean ok = switch (p) {
                case LambdaFunction ignored -> isFunctionTyped(t);
                case ColSpec cs -> genericRawIs(t,
                        cs.function2() != null ? Pure.AGG_COL_SPEC : Pure.FUNC_COL_SPEC);
                case ColSpecArray arr -> genericRawIs(t,
                        arr.colSpecs().stream().anyMatch(x -> x.function2() != null)
                                ? Pure.AGG_COL_SPEC_ARRAY : Pure.FUNC_COL_SPEC_ARRAY);
                default -> true;
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static boolean isFunctionTyped(Type t) {
        return t instanceof Type.FunctionType
                || (t instanceof Type.GenericType g && g.arguments().size() == 1
                        && g.arguments().get(0) instanceof Type.FunctionType);
    }

    private static boolean genericRawIs(Type t, com.legend.parser.element.ClassDefinition def) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(def.qualifiedName());
    }

    /** Pick the best-scoring overload by its already-typed arguments (deferred slots are skipped). */
    private TypedFunction selectByPresentArgs(String name, List<TypedFunction> arity, TypedSpec[] typed) {
        List<ExprType> argTypes = new ArrayList<>(typed.length);
        for (TypedSpec t : typed) {
            argTypes.add(t == null ? null : t.info());   // null = deferred slot, not yet typed
        }
        TypedFunction best = null;
        long bestScore = -1;
        for (TypedFunction c : arity) {
            long s = kernel.scoreNonLambda(c, argTypes);
            if (s > bestScore) {
                best = c;
                bestScore = s;
            }
        }
        if (best == null) {
            throw new TypeInferenceException(
                    "no overload of '" + name + "' matches the argument types");
        }
        return best;
    }

    /** Type a lambda argument against its function-type parameter, with type vars partly solved in {@code b}. */
    TypedSpec typeLambda(LambdaFunction lam, Type functionParamType, Bindings b, Env env) {
        Type.FunctionType ftype = extractFunctionType(functionParamType);
        if (ftype.params().size() != lam.parameters().size()) {
            throw new TypeInferenceException("lambda has " + lam.parameters().size()
                    + " parameter(s) but the function type expects " + ftype.params().size());
        }
        if (lam.body().size() != 1) {
            throw new TypeInferenceException("only single-expression lambdas are supported yet");
        }

        Env lambdaScope = env;
        List<String> names = new ArrayList<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            Type paramType = kernel.resolve(ftype.params().get(i).type(), b);   // T -> the solved element type
            String name = lam.parameters().get(i).name();
            names.add(name);
            lambdaScope = lambdaScope.with(name, new ExprType(paramType, ftype.params().get(i).multiplicity()));
        }

        TypedSpec body = synth(lam.body().get(0), lambdaScope);

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
        kernel.unifyMult(ftype.result().multiplicity(), body.info().multiplicity(),
                body.info().type(), b);

        ExprType info = new ExprType(
                new Type.FunctionType(ftype.params(),
                        new Type.Param(body.info().type(), body.info().multiplicity())),
                Multiplicity.Bounded.ONE);
        return new TypedLambda(names, List.of(body), info);
    }

    /** Unwrap a {@code Function<{…}>} (or a bare {@code FunctionType}) parameter to its function type. */
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
        if (genericRawIs(formal, Pure.AGG_COL_SPEC) || genericRawIs(formal, Pure.AGG_COL_SPEC_ARRAY)) {
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
                throw new TypeInferenceException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            TypedLambda lam = (TypedLambda) typeLambda(cs.function1(), f, b, env);
            Type.Param result = ((Type.FunctionType) lam.info().type()).result();
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
                throw new TypeInferenceException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            Bindings local = b.copy();   // K/V are per-column (see javadoc)
            TypedLambda map = (TypedLambda) typeLambda(cs.function1(), mapF, local, env);
            TypedLambda reduce = (TypedLambda) typeLambda(cs.function2(), reduceF, local, env);
            Type.Param result = ((Type.FunctionType) reduce.info().type()).result();
            cols.add(new TypedAggCol(cs.name(), map, reduce));
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
    private List<TypedFunction> functionCandidates(String name) {
        List<TypedFunction> found = ctx.findFunction(name);
        if (!found.isEmpty()) {
            return found;
        }
        java.util.regex.Matcher m = MANGLED_TAIL.matcher(name);
        if (m.find()) {
            return ctx.findFunction(name.substring(0, m.start()));
        }
        return found;
    }

    /**
     * {@code _Type_mult} segments (mult: digits, MANY, or {@code $a_b$}
     * ranges) with a MANDATORY trailing underscore — engine manglings always
     * end with one, and requiring it keeps ordinary snake-case user names
     * ({@code transform_step_3}) from falsely demangling (audit).
     */
    private static final java.util.regex.Pattern MANGLED_TAIL = java.util.regex.Pattern
            .compile("(?:_?_[A-Za-z][A-Za-z0-9]*_(?:\\d+|MANY|\\$[^$]*\\$))+_$");

    /**
     * A packageable-element reference used as a value &mdash; currently a class
     * reference (the {@code Person} in {@code Person.all()}), typed as
     * {@code Class<Person>[1]} so {@code getAll<T>(Class<T>[1]):T[*]} resolves to
     * {@code Person[*]} via the generic native path.
     */
    private TypedSpec classReference(PackageableElementPtr ref) {
        var cls = ctx.findClass(ref.fullPath());
        if (cls.isPresent()) {
            // The node carries the RESOLVED FQN — a bare name accepted by
            // the simple-name fallback must not leak downstream (the H
            // resolver's mapping bindings are FQN-keyed).
            String fqn = cls.get().qualifiedName();
            Type classOf = new Type.GenericType(Pure.CLASS.qualifiedName(),
                    List.of(new Type.ClassType(fqn)));
            return new TypedPackageableRef(fqn, ExprType.one(classOf));
        }
        // An execution-context element (mapping/runtime/connection/database) is a value
        // of type Any[1] — exactly what from/write's signature parameters declare.
        if (ctx.isExecutionContextElement(ref.fullPath())) {
            return new TypedPackageableRef(ref.fullPath(), ExprType.one(InferenceKernel.anyType()));
        }
        // A FUNCTION REFERENCE used as a value (removeDuplicates(eq_Any_1__...))
        // ETA-EXPANDS: the reference becomes the lambda calling it — one
        // uniform function-value story, no new node kind. Only an
        // UNAMBIGUOUS (single-overload) target expands.
        List<TypedFunction> fns = functionCandidates(ref.fullPath());
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
            Type ft = new Type.FunctionType(ftParams,
                    new Type.FunctionType.Param(fn.returnType(), fn.returnMultiplicity()));
            return new TypedLambda(params, List.of(body), ExprType.one(ft));
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
            elements.add(synth(v, env));
        }
        Type elementType = elements.stream()
                .map(e -> e.info().type())
                .reduce(kernel::commonSupertype)
                // The empty collection [] types as Nil[0] — the BOTTOM type
                // (real pure), so it conforms to any expected element type
                // and vanishes in LUBs: if(c, {|Status}, {|[]}) is
                // Status[0..1], not Any.
                .orElseGet(() -> new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.NIL));
        Multiplicity mult = new Multiplicity.Bounded(elements.size(), elements.size());
        return new TypedCollection(elements, new ExprType(elementType, mult));
    }

    /**
     * Object-graph property access {@code $source.property}: type the receiver
     * (which must be a class), look up the property's signature via
     * {@link ModelContext#findProperty} (which walks inheritance and
     * association-injected properties), and <em>compose</em> the receiver's
     * multiplicity with the property's along the path.
     */
    private TypedSpec accessProperty(AppliedProperty ap, Env env) {
        TypedSpec source = synth(ap.receiver(), env);
        // The member is either a class property ($obj.prop) or a relation column ($row.col).
        ExprType member = switch (source.info().type()) {
            case Type.ClassType ct -> {
                Property prop = ctx.findProperty(ct.fqn(), ap.property()).orElseThrow(() ->
                        new TypeInferenceException("class " + ct.fqn() + " has no property '" + ap.property() + "'"));
                yield new ExprType(prop.type(), prop.multiplicity());
            }
            // A PARAMETERIZED class receiver (Pair<Integer,String>.first): the
            // property's declared type is written in the class's type parameters
            // — instantiate them at the receiver's arguments (positional, real
            // pure's generic instantiation).
            case Type.GenericType g -> {
                var cls = ctx.findClass(g.rawFqn()).orElseThrow(() -> new TypeInferenceException(
                        "unknown class '" + g.rawFqn() + "'"));
                Property prop = ctx.findProperty(g.rawFqn(), ap.property()).orElseThrow(() ->
                        new TypeInferenceException("class " + g.rawFqn()
                                + " has no property '" + ap.property() + "'"));
                if (cls.typeParameters().size() != g.arguments().size()) {
                    throw new TypeInferenceException("class " + g.rawFqn() + " declares "
                            + cls.typeParameters().size() + " type parameter(s) but the receiver "
                            + g.typeName() + " supplies " + g.arguments().size());
                }
                Bindings b = new Bindings();
                for (int i = 0; i < cls.typeParameters().size(); i++) {
                    b.bindType(cls.typeParameters().get(i), g.arguments().get(i));
                }
                yield new ExprType(kernel.resolve(prop.type(), b), prop.multiplicity());
            }
            case Type.RelationType rel -> {
                Type.Column col = rel.columns().stream()
                        .filter(c -> c.name().equals(ap.property()))
                        .findFirst()
                        .orElseThrow(() -> new TypeInferenceException(
                                "relation has no column '" + ap.property() + "'"));
                yield new ExprType(col.type(), col.multiplicity());
            }
            default -> throw new TypeInferenceException("cannot access '" + ap.property()
                    + "' on " + source.info().type().typeName());
        };
        Multiplicity mult = compose(source.info().multiplicity(), member.multiplicity());
        return new TypedPropertyAccess(source, ap.property(), new ExprType(member.type(), mult));
    }

    /**
     * Multiplicity composition along a navigation path: {@code [a..b] . [c..d] =
     * [a*c .. b*d]} (an unbounded upper on either side stays unbounded). So a
     * {@code [*]} hop makes everything after it {@code [*]}, and an optional hop
     * makes the result optional.
     */
    private static Multiplicity compose(Multiplicity outer, Multiplicity inner) {
        if (outer instanceof Multiplicity.Bounded a && inner instanceof Multiplicity.Bounded b) {
            int lower = a.lower() * b.lower();
            Integer upper = (a.upper() == null || b.upper() == null) ? null : a.upper() * b.upper();
            return new Multiplicity.Bounded(lower, upper);
        }
        return inner;   // multiplicity variables do not occur on object-graph paths
    }

    /** An enum value reference {@code Kind.VALUE}: both the enumeration and the value must exist. */
    private TypedSpec enumValue(EnumValue ev) {
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
        return new TypedTypeRef(target, ExprType.one(target));
    }

    private Type annotationType(TypeAnnotation ta) {
        return switch (ta) {
            case TypeAnnotation.Named n -> namedType(n.type());
            // A relation target is the BARE row-struct — the computed-value form (G-α),
            // so cast(@Relation<(…)>) yields the same representation every relation op emits.
            case TypeAnnotation.RelationShape rs -> relationShapeType(rs);
            case TypeAnnotation.Wildcard ignored -> throw new TypeInferenceException(
                    "the ? wildcard is only legal as a column type inside @Relation<(…)>");
        };
    }

    /**
     * A named type reference used in a value position ({@code @Integer},
     * {@code t:Person[1]|…} branch/parameter declarations). Names are FQN-resolved
     * by NameResolver in the full pipeline; for primitive short names (the prelude)
     * we fall back to the fixed primitive package, so direct query checking
     * ({@code @Integer}) works without an import scope. Package-private: the
     * checkers that read declared types ({@code match} branches, {@code eval}
     * lambda params) resolve through this single point.
     */
    Type namedType(TypeExpression te) {
        // GENERIC annotations (@Pair<String, Integer>): the base resolves
        // like a NameRef; arguments resolve recursively.
        if (te instanceof TypeExpression.Generic g) {
            Type base = namedType(new TypeExpression.NameRef(g.name()));
            java.util.List<Type> args = g.arguments().stream()
                    .map(this::namedType).toList();
            String fqn = base instanceof Type.ClassType ct ? ct.fqn()
                    : base instanceof Type.GenericType gt ? gt.rawFqn() : null;
            if (fqn == null) {
                throw new TypeInferenceException(
                        "generic annotation over a non-class type: " + g.name());
            }
            return new Type.GenericType(fqn, args);
        }
        if (!(te instanceof TypeExpression.NameRef nr)) {
            throw new TypeInferenceException(
                    "unsupported type annotation form: " + te.getClass().getSimpleName());
        }
        String name = nr.name();
        return ctx.findType(name)
                .or(() -> name.contains("::")
                        ? Optional.empty()
                        : ctx.findType("meta::pure::metamodel::type::" + name)
                                .or(() -> ctx.findType(Pure.VARIANT_PKG + "::" + name)))
                .orElseThrow(() -> new TypeInferenceException(
                        "unknown type '" + name + "' in @" + name));
    }

    /** {@code @Relation<(name:Type[m], …)>}: each column resolves recursively; multiplicity defaults to [1]. */
    private Type relationShapeType(TypeAnnotation.RelationShape rs) {
        List<Type.Column> cols = new ArrayList<>(rs.columns().size());
        for (TypeAnnotation.RelationShape.Column c : rs.columns()) {
            if (c.name() == null || c.type() instanceof TypeAnnotation.Wildcard) {
                throw new TypeInferenceException(
                        "wildcard columns in @Relation<(…)> are not implemented yet");
            }
            Multiplicity m = c.multiplicity() == null
                    ? Multiplicity.Bounded.ONE : Multiplicity.from(c.multiplicity());
            cols.add(new Type.Column(c.name(), annotationType(c.type()), m));
        }
        return new Type.RelationType(cols);
    }

    /** Date-literal precision: year/year-month = Date; a full day = StrictDate; any time part = DateTime. */
    private static Type dateType(PureDateLiteral lit) {
        return switch (lit) {
            case PureDateLiteral.Year ignored -> Type.Primitive.DATE;
            case PureDateLiteral.YearMonth ignored -> Type.Primitive.DATE;
            case PureDateLiteral.StrictDate ignored -> Type.Primitive.STRICT_DATE;
            default -> Type.Primitive.DATE_TIME;   // any variant carrying a time component
        };
    }

    /** A bare {@code ~col}: a first-class {@code ColSpec<(col:?)>[1]} value (see {@link TypedColSpec}). */
    private TypedSpec typedColSpec(ColSpec cs) {
        if (cs.function1() != null) {
            throw new TypeInferenceException("~" + cs.name()
                    + ": mapped/aggregate column specifications need an enclosing call to type against");
        }
        Type row = new Type.RelationType(List.of(unknownColumn(cs.name())));
        return new TypedColSpec(cs.name(),
                ExprType.one(new Type.GenericType(Pure.COL_SPEC.qualifiedName(), List.of(row))));
    }

    /** A bare {@code ~[a,b]}: a first-class {@code ColSpecArray<(a:?, b:?)>[1]} value. */
    private TypedSpec typedColSpecArray(ColSpecArray arr) {
        // ~[] is LEGAL where zero columns mean something: groupBy(~[], aggs)
        // is the whole-relation aggregate (the engine's empty-key grouping).
        if (arr.colSpecs().isEmpty()) {
            return new TypedColSpecArray(List.of(),
                    ExprType.one(new Type.GenericType(Pure.COL_SPEC_ARRAY.qualifiedName(),
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
                throw new TypeInferenceException("duplicate column '" + cs.name() + "' in ~[…]");
            }
            names.add(cs.name());
            cols.add(unknownColumn(cs.name()));
        }
        Type row = new Type.RelationType(cols);
        return new TypedColSpecArray(names,
                ExprType.one(new Type.GenericType(Pure.COL_SPEC_ARRAY.qualifiedName(), List.of(row))));
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
