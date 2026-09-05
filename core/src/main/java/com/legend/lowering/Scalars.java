package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.values.PureDateLiteral;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Scalar native dispatch, keyed by the RESOLVED overload's identity &mdash; the
 * {@link Pure} catalog constant Phase G chose ({@code TypedFunction.definition()}).
 * Dispatch never touches name strings; registration is catalog-driven: a
 * {@code family} maps EVERY overload of a Pure function to one semantic name
 * (all {@code lessThan} overloads mean {@code <}), and specific-overload
 * overrides land where the overload IS the decision &mdash; {@code plus} on
 * Strings is {@code ||}, on Numbers {@code +}; the type checker already chose.
 * An unregistered overload is a loud error naming the signature.
 */
final class Scalars {


    /** A rule receives the ALREADY-LOWERED argument expressions. */
    interface Rule extends BiFunction<TypedNativeCall, List<SqlExpr>, SqlExpr> {
    }

    private static final Map<String, Rule> RULES = new HashMap<>();

    private Scalars() {
    }

    /** Register every catalog overload of {@code pureName} under one semantic entry. */
    private static void family(SqlFn semantic, String pureName) {
        var overloads = Pure.nativeKeysAt(pureName);
        if (overloads.isEmpty()) {
            throw new IllegalStateException("no catalog overloads for '" + pureName + "'");
        }
        for (var f : overloads) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(semantic, args));
        }
    }

    static {
        // equal/eq are PRECISION-AWARE over dates: a partial-date
        // literal equals only a SAME-precision value (real pure, decided
        // statically — partial precision is literal-only; columns are
        // full-precision); same-precision partials compare as ISO-prefix
        // strings. eq = strict equality = the = operator here.
        // equal takes NO optionalOperandGuards DELIBERATELY (T1.4):
        // equal is a total NATIVE ([] == [] is TRUE; the guard would
        // spell it false). The residual both-NULL divergence (SQL NULL
        // vs pure true) is the reference engine's own behavior (bare =;
        // IS NOT DISTINCT FROM appears in no golden).
        for (String name : List.of("equal", "eq")) {
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    // ENGINE-VERBATIM empty ladder (pureToSQLQuery.pure
                    // nullSafeEqualsOperation: "a literal empty ([])
                    // operand degenerates to a null check on the other
                    // side ([] == [] is statically true)"; frontier
                    // witness testEqualEmpty — the engine's relational
                    // DuckDB executor passes it). Scoped to the LITERAL
                    // empty collection: the engine's [0..0] criterion is
                    // honest post-routing, but OUR checker types some
                    // subtype navigations [0..0] that still read real
                    // columns (the gate-4 mapping-family regression
                    // pinned that narrowing at burn slice 4).
                    boolean le = n.args().get(0) instanceof
                            com.legend.compiler.spec.typed.TypedCollection lc
                            && lc.elements().isEmpty();
                    boolean re = n.args().get(1) instanceof
                            com.legend.compiler.spec.typed.TypedCollection rc
                            && rc.elements().isEmpty();
                    if (le || re) {
                        if (le && re) {
                            return new SqlExpr.BoolLit(true);
                        }
                        return SqlExpr.Call.of(SqlFn.IS_NULL,
                                args.get(le ? 1 : 0));
                    }
                    Integer p0 = partialPrecision(n.args().get(0));
                    Integer p1 = partialPrecision(n.args().get(1));
                    if (p0 != null || p1 != null) {
                        if (Objects.equals(p0, p1)) {
                            return new SqlExpr.Call(SqlFn.EQUAL, args);
                        }
                        Type other = (p0 != null ? n.args().get(1) : n.args().get(0))
                                .info().type();
                        // Covers both a full-precision opposite side AND a
                        // different-precision partial (whose static type is
                        // also Date) — never equal.
                        if (isFullPrecisionDate(other)) {
                            return new SqlExpr.BoolLit(false);
                        }
                        // A Date is never equal to a NON-date kind — the
                        // string carrier must not leak into '2014'=='2014'
                        // being true (audit). Any stays dynamic (fall through).
                        if (!PlatformTypes.isAny(other)) {
                            return new SqlExpr.BoolLit(false);
                        }
                    }
                    List<SqlExpr> cargs = List.of(
                            CastPolicy.comparisonWireOperand(n.args().get(0),
                                    args.get(0), n.args().get(1)),
                            CastPolicy.comparisonWireOperand(n.args().get(1),
                                    args.get(1), n.args().get(0)));
                    // EQUALITY BY EMISSION (M4 re-land — the claim's eq
                    // lane, the burn-down doctrine): a LITERAL-marked
                    // side meeting a side of known SPELLABLE static kind
                    // SPELLS that side — both then byte-compare in the
                    // grammar, which IS pure equality (six kinds
                    // disjoint by spelling; §0.4 receipts). Never
                    // unspell the literal side; dynamic/unspellable
                    // other sides fall through to the existing lanes.
                    cargs = VariantShapes.alignLiteralToJson(n.args(), cargs);
                    cargs = MixedEncoding.equalityEmission(
                            n.args().get(0), n.args().get(1), cargs);
                    SqlExpr inv = EnumSourceValues.decodeInvert(
                            n.args().get(0), n.args().get(1),
                            cargs.get(0), cargs.get(1));
                    if (inv != null) {
                        return inv;
                    }
                    // nullable col-vs-col equality is NULL-SAFE (engine
                    // isEqualsFromFilter; task #62's equal-side arm)
                    return NullSemantics.equalNullArms(n, cargs);
                });
            }
        }
        // Ordering comparisons PAD partial-date literals to their instant
        // (dateArg) — the string carrier must never meet a DATE operand
        // (audit: '2014' < DATE '…' is a conversion error).
        for (var cmp : Map.of("lessThan", SqlFn.LESS, "lessThanEqual", SqlFn.LESS_EQUAL,
                "greaterThan", SqlFn.GREATER, "greaterThanEqual", SqlFn.GREATER_EQUAL)
                .entrySet()) {
            // the Any-typed Lite ordering shims (DynaFunc conditions,
            // ledger cluster 18) register by FQN — the bare-name index
            // deliberately excludes the lite package
            List<String> cmpKeys = new ArrayList<>(
                    Pure.nativeKeysAt(cmp.getKey()));
            cmpKeys.addAll(Pure.nativeKeysAt(
                    Pure.Lite.PKG + cmp.getKey()));
            for (String f : cmpKeys) {
                RULES.put(f, (n, args) -> {
                    List<SqlExpr> padded = new ArrayList<>(args.size());
                    for (int i = 0; i < args.size(); i++) {
                        padded.add(dateArg(n.args().get(i), args.get(i)));
                    }
                    // pure [0..1] overload bodies inline HERE (audit 20a H2)
                    return NullSemantics.optionalOperandGuards(n, padded,
                            new SqlExpr.Call(cmp.getValue(), padded));
                });
            }
        }
        // and(Boolean[*]) / or(Boolean[*]) are the COLLECTION reductions
        // (real pure) — the infix renderer would emit the lone list bare.
        // The EMPTY collection takes each reduction's IDENTITY (and([]) is
        // true, or([]) is false — list_aggregate over [] is NULL; audit).
        for (String f : Pure.nativeKeysAt("and")) {
            // DELETION LEG (invariant live): the ArrayLit escape was a
            // shape sniff — a to-one BOOLEAN operand cannot carry a
            // designed ArrayLit (List/instance/relation carriers never
            // type Boolean), and any other list would have THROWN at
            // the funnel. The stamp alone decides.
            // EMPTY-IDENTITY FORK FIX (audit §4, slice 4): the old
            // identity arm gated on upper==1, so a runtime-empty [0..1]
            // returned NULL where pure defines and([]) = true. Split:
            // exactlyOne = identity; [0..1] = coalesce to the identity.
            RULES.put(f, (n, args) -> args.size() == 1
                    ? (Stamps.exactlyOne(n.args().get(0))
                            ? args.get(0)
                            : isToOne(n.args().get(0))
                            ? SqlExpr.Call.of(SqlFn.COALESCE, args.get(0),
                                    new SqlExpr.BoolLit(true))
                            : SqlExpr.Call.of(SqlFn.COALESCE,
                                    new SqlExpr.Call(SqlFn.LIST_BOOL_AND, args),
                                    new SqlExpr.BoolLit(true)))
                    : Fold.mergeAnd(args.toArray(new SqlExpr[0])));
        }
        for (String f : Pure.nativeKeysAt("or")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? (Stamps.exactlyOne(n.args().get(0))
                            ? args.get(0)
                            : isToOne(n.args().get(0))
                            ? SqlExpr.Call.of(SqlFn.COALESCE, args.get(0),
                                    new SqlExpr.BoolLit(false))
                            : SqlExpr.Call.of(SqlFn.COALESCE,
                                    new SqlExpr.Call(SqlFn.LIST_BOOL_OR, args),
                                    new SqlExpr.BoolLit(false)))
                    : new SqlExpr.Call(SqlFn.OR, args));
        }
        // elementToPath: a REFERENCE is its path literal (rows: resolver)
        for (String f : Pure.nativeKeysAt("elementToPath")) {
            RULES.put(f, (n, args) -> {
                if (n.args().get(0) instanceof
                        com.legend.compiler.spec.typed.TypedPackageableRef pr) {
                    return new SqlExpr.StringLit(pr.fullPath());
                }
                throw new NotImplementedException("elementToPath over a "
                        + n.args().get(0).getClass().getSimpleName());
            });
        }
        // fail([message]) RAISES; in a VALUE position (Substitution.raise
        // types the call as the position) it casts to that carrier
        for (String f : Pure.nativeKeysAt("fail")) {
            RULES.put(f, (n, args) -> {
                SqlExpr raised = PureSql.raise(args.isEmpty()
                        ? new SqlExpr.StringLit("fail") : args.get(0), n.pos());
                return n.info().type() instanceof Type.Primitive p
                        && p != Type.Primitive.BOOLEAN
                        ? new SqlExpr.Cast(raised, PureSql.type(p))
                        : raised;
            });
        }
        // not(equal)/not(in) carry the engine's NULL ARMS (dbExtension.pure
        // processNotEqual/processNotIn): pure `x != v` MATCHES null x (eq
        // over empty is false) — bare SQL <> silently drops null rows
        // (testConsistencyWithNulls, task #62). See notEqualNullArms.
        for (String f : Pure.nativeKeysAt("not")) {
            RULES.put(f, (n, args) -> {
                SqlExpr negated = NullSemantics.negate(args.get(0),
                        NullSemantics.enumInvolved(n.args().get(0)));
                if (negated != null) {
                    return negated;
                }
                // everything else is BARE not (engine processNot default):
                // null tolerance for optional operands lives at the
                // COMPARISON SITE (NullSemantics.optionalOperandGuards —
                // audit 20a H2 removed the wrong-layer COALESCE wrap here)
                return new SqlExpr.Call(SqlFn.NOT, args);
            });
        }
        // UNARY plus/minus (the parser's -x => minus(x) desugar): a 1-arg
        // minus NEGATES — the binary operator renderer would silently DROP
        // the sign of a lone operand (audit: [-5, -3] executed as [5, 3]).
        for (String f : Pure.nativeKeysAt("plus")) {
            RULES.put(f, (n, rawArgs) -> {
                var args = decimalJoin(rawArgs);
                if (args.size() == 1 && isToOne(n.args().get(0))) {
                    return args.get(0);   // unary +x (stamp decides)
                }
                // plus<T>(values:T[*]) is the COLLECTION SUM (real pure) —
                // the infix renderer would emit a lone list bare (audit).
                // A NUMBER-LUB mixed literal rides the variant carrier:
                // numList unwraps it for the aggregate (sum(JSON) is a
                // Binder error; grammar witness testPlusNumber).
                if (args.size() == 1) {
                    return new SqlExpr.Call(SqlFn.LIST_SUM,
                            List.of(Numerics.numList(args.get(0))));
                }
                return new SqlExpr.Call(SqlFn.PLUS, hugeWiden(args));
            });
        }
        for (String f : Pure.nativeKeysAt("times")) {
            RULES.put(f, (n, rawArgs) -> {
                var args = decimalJoin(rawArgs);
                if (args.size() == 1 && isToOne(n.args().get(0))) {
                    return args.get(0);
                }
                // times<T>(values:T[*]) is the COLLECTION PRODUCT (real
                // pure); numList unwraps the mixed carrier (same defect
                // class as plus — the aggregate needs raw numerics).
                if (args.size() == 1) {
                    return new SqlExpr.Call(SqlFn.LIST_PRODUCT,
                            List.of(Numerics.numList(args.get(0))));
                }
                return new SqlExpr.Call(SqlFn.TIMES, hugeWiden(args));
            });
        }
        for (String f : Pure.nativeKeysAt("times")) {
            // DECIMAL-bearing LITERAL product: DuckDB's LIST_PRODUCT
            // degrades to DOUBLE (probed 2026-08-20: [19.905,17774] ->
            // 353791.47000000003) while BINARY decimal arithmetic is
            // exact (353791.470, witness testDecimalTimes) — fold the
            // literal list to a times chain BEFORE the aggregate.
            var base = java.util.Objects.requireNonNull(RULES.get(f),
                    "times rule registered above");
            RULES.put(f, (n, rawArgs) -> {
                var args = decimalJoin(rawArgs);
                if (args.size() == 1) {
                    SqlExpr chain = Numerics.decimalChain(Numerics.numList(args.get(0)),
                            SqlFn.TIMES);
                    if (chain != null) {
                        return chain;
                    }
                }
                return base.apply(n, rawArgs);
            });
        }
        for (String f : Pure.nativeKeysAt("minus")) {
            RULES.put(f, (n, rawArgs) -> {
                var args = decimalJoin(rawArgs);
                if (args.size() != 1) {
                    return new SqlExpr.Call(SqlFn.MINUS, hugeWiden(args));
                }
                // minus<T>(values:T[*]) LEFT-FOLDS subtraction (real pure:
                // [10,3,2] -> 5); the seed is the first element. A SINGLETON
                // LIST LITERAL is a list (the reduction of [x] is x, via the
                // fold), not a unary negate (audit).
                if (!isToOne(n.args().get(0))) {
                    // numList: the mixed-number VARIANT carrier unwraps
                    // for the reduction (-(JSON, JSON) does not bind;
                    // witness testDecimalMinus); a DECIMAL-bearing
                    // literal list folds to the exact BINARY chain
                    // (LIST_REDUCE, like the aggregates, runs DOUBLE)
                    SqlExpr list = Numerics.numList(args.get(0));
                    SqlExpr chain = Numerics.decimalChain(list, SqlFn.MINUS);
                    if (chain != null) {
                        return chain;
                    }
                    // RUNTIME size-1 NEGATES (real pure: interpreted
                    // Minus.java case-1 seeds 0, compiled delegates to
                    // unary — the first-element-seed fold returned +x
                    // for [x]; residue recorded at the C1 landing, fixed
                    // at the deletion-leg rebuild).
                    return new SqlExpr.Case(
                            List.of(new SqlExpr.Case.When(
                                    SqlExpr.Call.of(SqlFn.EQUAL,
                                            SqlExpr.Call.of(SqlFn.LIST_LENGTH, list),
                                            new SqlExpr.IntLit(1)),
                                    SqlExpr.Call.of(SqlFn.MINUS,
                                            new SqlExpr.IntLit(0),
                                            SqlExpr.Call.of(SqlFn.LIST_GET, list,
                                                    new SqlExpr.IntLit(1))))),
                            SqlExpr.Call.of(SqlFn.LIST_REDUCE, list,
                                    // params stamp as the list's element
                                    // (§4bZ-U leg 2 — the binding door;
                                    // the running difference stays in the
                                    // element's promoted domain)
                                    new SqlExpr.Lambda(List.of("_ma", "_mb"),
                                            SqlExpr.Call.of(SqlFn.MINUS,
                                                    SqlExpr.Column.param("_ma", list),
                                                    SqlExpr.Column.param("_mb", list)))));
                }
                return switch (args.get(0)) {
                    case SqlExpr.IntLit i -> new SqlExpr.IntLit(-i.value());
                    case SqlExpr.FloatLit fl -> new SqlExpr.FloatLit(-fl.value());
                    case SqlExpr.DecimalLit d -> new SqlExpr.DecimalLit(d.value().negate());
                    case SqlExpr e -> new SqlExpr.Call(SqlFn.MINUS,
                            List.of(new SqlExpr.IntLit(0), e));
                };
            });
        }
        // times registers ABOVE (collection-product overload needs its own rule).
        // Bit shifts: value casts to BIGINT (bare literals are INT32);
        // pure bounds the shift at 62 — beyond is a LOUD error.
        for (String name : List.of("bitShiftLeft", "bitShiftRight")) {
            SqlFn fn = name.equals("bitShiftLeft")
                    ? SqlFn.BIT_SHIFT_LEFT : SqlFn.BIT_SHIFT_RIGHT;
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    SqlExpr shifted = SqlExpr.Call.of(fn,
                            new SqlExpr.Cast(args.get(0), SqlType.Scalar.BIGINT),
                            args.get(1));
                    SqlExpr boundError = SqlExpr.Call.of(SqlFn.ERROR,
                            new SqlExpr.StringLit(
                                    "Unsupported number of bits to shift - max bits allowed is 62"));
                    if (args.get(1) instanceof SqlExpr.IntLit sh) {
                        return sh.value() < 0 || sh.value() > 62 ? boundError : shifted;
                    }
                    // non-literal shift: bound guards AT RUNTIME in SQL
                    // (deep-audit H4 — the laundering pct remap is deleted)
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.AND,
                                    SqlExpr.Call.of(SqlFn.GREATER_EQUAL, args.get(1), new SqlExpr.IntLit(0)),
                                    SqlExpr.Call.of(SqlFn.LESS_EQUAL, args.get(1), new SqlExpr.IntLit(62))),
                            shifted)), boundError);
                });
            }
        }
        // divide: the 3-arg overload carries a SCALE — BigDecimal HALF_UP
        // (SQL ROUND, half away from zero); plain division otherwise.
        // Integer arithmetic near the INT64 edge computes in HUGEINT
        // (2 * maxLong is a real PCT value).
        for (String f : Pure.nativeKeysAt("divide")) {
            // Decimal kind preservation lives in DecimalKindRules
            // (X-audit); the integer÷integer zero guard lives there too
            // (Part-1 fix — pure's BigDecimal lane raises)
            RULES.put(f, DecimalKindRules::divide);
        }
        family(SqlFn.MOD, "mod");
        // rem(a, 0): real pure raises 'Cannot divide 5 by zero'
        for (String f : Pure.nativeKeysAt("rem")) {
            RULES.put(f, (n, rawArgs) -> {
                var args = decimalJoin(rawArgs);
                return guarded(
                        SqlExpr.Call.of(SqlFn.EQUAL, args.get(1), new SqlExpr.IntLit(0)),
                        cat(new SqlExpr.StringLit("Cannot divide "), str(args.get(0)),
                                new SqlExpr.StringLit(" by zero")),
                        new SqlExpr.Call(SqlFn.REM, args));
            });
        }
        family(SqlFn.ABS, "abs");
        // isEmpty/isNotEmpty are TYPE-aware: a to-MANY argument is a SQL
        // LIST value (toMany(@T) et al.) — emptiness is length, not
        // NULL-ness (isEmpty([]) = true; IS NULL said false). Scalar
        // ([0..1]) stays the null test.
        Pure.nativeKeysAt("isTrue").forEach(f -> RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.COALESCE, args.get(0), new SqlExpr.BoolLit(false))));   // empty is false
        Pure.nativeKeysAt("defaultIfEmpty").forEach(f -> RULES.put(f, (n, args) -> listValued(n.args().get(0))   // col unless empty
                ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(new SqlExpr.Call(SqlFn.EQUAL, List.of(SqlExpr.Call.of(SqlFn.COALESCE,
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)), new SqlExpr.IntLit(0)), new SqlExpr.IntLit(0))), args.get(1))), args.get(0)) : SqlExpr.Call.of(SqlFn.COALESCE, args.get(0), args.get(1))));
        for (String f : Pure.nativeKeysAt("isEmpty")) {
            RULES.put(f, (n, args) -> listValued(n.args().get(0))
                    ? new SqlExpr.Call(SqlFn.EQUAL, List.of(
                            SqlExpr.Call.of(SqlFn.COALESCE,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                                    new SqlExpr.IntLit(0)),
                            new SqlExpr.IntLit(0)))
                    : new SqlExpr.Call(SqlFn.IS_NULL, args));
        }
        for (String f : Pure.nativeKeysAt("isNotEmpty")) {
            RULES.put(f, (n, args) -> listValued(n.args().get(0))
                    ? new SqlExpr.Call(SqlFn.GREATER, List.of(
                            SqlExpr.Call.of(SqlFn.COALESCE,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                                    new SqlExpr.IntLit(0)),
                            new SqlExpr.IntLit(0)))
                    : new SqlExpr.Call(SqlFn.IS_NOT_NULL, args));
        }
        family(SqlFn.LENGTH, "length");
        family(SqlFn.UPPER, "toUpper");
        family(SqlFn.LOWER, "toLower");

        // toOne/trustOne/toOneMany — the multiplicity-coercion family
        // (moved to Coercions at the shape limit; the seam split)
        Coercions.register(RULES);

        // evaluateAndDeactivate erases too (real pure: reflection-level
        // deactivation of expression wrappers — values here are already
        // values, so identity; evaluateAndDeactivate.pure:17).
        for (String f : Pure.nativeKeysAt("evaluateAndDeactivate")) {
            RULES.put(f, (n, args) -> args.get(0));
        }

        // exists/forAll over collections: SEMANTIC vocabulary entries whose
        // CONTRACT includes Pure's empty-collection semantics (exists([]) =
        // false, forAll([]) = true) — every dialect's expansion must honor
        // them (DuckDB: coalesce over list_bool_* lambdas).
        // c1-literal COLLECTION params box (DEEP_AUDIT §3: [7] bare)
        for (var fx : List.of(Map.entry(SqlFn.LIST_EXISTS, "exists"),
                Map.entry(SqlFn.LIST_FOR_ALL, "forAll"))) {
            for (var f : Pure.nativeKeysAt(fx.getValue())) {
                RULES.put(f, (n, args) -> new SqlExpr.Call(fx.getKey(),
                        List.of(PureSql.asList(args.get(0),
                                !CollectionLanes.c1Literal(n.args().get(0))),
                                args.get(1))));
            }
        }

        // ---- the registration grind (corpus-driven; MUST-honor templates) ----
        // Math (ROUND is banker's per the semantics contract).
        for (var e : Map.ofEntries(
                Map.entry("cbrt", SqlFn.CBRT),
                Map.entry("exp", SqlFn.EXP), Map.entry("log", SqlFn.LN),
                Map.entry("log10", SqlFn.LOG10), Map.entry("pow", SqlFn.POW),
                Map.entry("pi", SqlFn.PI),
                Map.entry("sin", SqlFn.SIN), Map.entry("cos", SqlFn.COS),
                Map.entry("tan", SqlFn.TAN), Map.entry("asin", SqlFn.ASIN),
                // acos/asin: the engine's spec cell is the BARE function
                // (extensionDefaults.pure 'acos(%s)'); out of domain H2
                // yields NaN and the row drops. A backend that raises
                // instead reaches the same answer through ITS dialect's
                // domain guard (DuckDb.call), never a semantic rule; Pure's
                // "Unable to compute acos" error is the interpreter's, and
                // every engine relational PCT adapter ledgers
                // testArcCosineError as an expected failure (batch 61).
                Map.entry("acos", SqlFn.ACOS),
                Map.entry("atan", SqlFn.ATAN),
                Map.entry("atan2", SqlFn.ATAN2), Map.entry("sinh", SqlFn.SINH),
                Map.entry("cosh", SqlFn.COSH), Map.entry("tanh", SqlFn.TANH),
                Map.entry("ceiling", SqlFn.CEILING), Map.entry("floor", SqlFn.FLOOR),
                Map.entry("sign", SqlFn.SIGN),
                Map.entry("xor", SqlFn.XOR),
                Map.entry("bitAnd", SqlFn.BIT_AND), Map.entry("bitOr", SqlFn.BIT_OR),
                Map.entry("bitXor", SqlFn.BIT_XOR),

                // Strings — plain families first; index-shifted below.
                // (startsWith/endsWith re-register with [0..1]-operand
                // guards right after this table — audit 20a H2)
                Map.entry("startsWith", SqlFn.STARTS_WITH),
                Map.entry("endsWith", SqlFn.ENDS_WITH),
                Map.entry("matches", SqlFn.REGEXP_FULL_MATCH),
                Map.entry("left", SqlFn.LEFT), Map.entry("right", SqlFn.RIGHT),

                Map.entry("trim", SqlFn.TRIM), Map.entry("ltrim", SqlFn.LTRIM),
                Map.entry("rtrim", SqlFn.RTRIM), Map.entry("replace", SqlFn.REPLACE),
                Map.entry("split", SqlFn.SPLIT),
                Map.entry("reverseString", SqlFn.REVERSE_STRING),
                Map.entry("ascii", SqlFn.ASCII_CODE), Map.entry("char", SqlFn.CHR),
                Map.entry("toUpperFirstCharacter", SqlFn.UC_FIRST),
                Map.entry("toLowerFirstCharacter", SqlFn.LC_FIRST),
                Map.entry("encodeBase64", SqlFn.ENCODE_BASE64),
                Map.entry("levenshteinDistance", SqlFn.LEVENSHTEIN),
                Map.entry("generateGuid", SqlFn.GUID),
                Map.entry("hash", SqlFn.HASH), Map.entry("hashCode", SqlFn.HASH),
                Map.entry("coalesce", SqlFn.COALESCE),
                // Temporal
                Map.entry("today", SqlFn.TODAY), Map.entry("now", SqlFn.NOW),

                // Lists / collections

                Map.entry("median", SqlFn.LIST_MEDIAN),

                Map.entry("toVariant", SqlFn.TO_VARIANT)).entrySet()) {
            familyIfPresent(e.getValue(), e.getKey());
        }
        // the 1-arg memsql-dialect 'hash' typing shim (the bare 'hash'
        // entry above registers only the REAL 2-arg hash::hash — the
        // shim is not user-resolvable and registers by exact identity)
        familyIfPresent(SqlFn.HASH, Pure.Lite.HASH);
        // startsWith/endsWith carry the pure [0..1]-overload guards
        // (stringExtension.pure: $source->isNotEmpty() && ...) — the
        // COMPARISON-SITE null tolerance (audit 20a H2), overriding the
        // plain-family registration above.
        for (var e : Map.of("startsWith", SqlFn.STARTS_WITH,
                "endsWith", SqlFn.ENDS_WITH).entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> NullSemantics.optionalOperandGuards(
                        n, args, new SqlExpr.Call(e.getValue(), args)));
            }
        }
        // ---- Date family (H-audit registrations bucket) ----
        // Numeric extractions ride EXTRACT with a part literal.
        for (var e : Map.of("dayOfYear", "doy", "weekOfYear", "week",
                "dayOfWeekNumber", "isodow", "quarterNumber", "quarter").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                        new SqlExpr.StringLit(e.getValue()),
                        dateArg(n.args().get(0), args.get(0)))));
            }
        }
        DateShifts.registerDayOfWeekNumber2(RULES);
        // Calendar-enum extractions: names match the Pure enum values
        // (Monday…, January… — the corpus's enum-by-name convention).
        // dayOfWeek()/month(): real pure returns calendar ENUMS (Monday…,
        // January…); the engine surface is NUMERIC (DuckDB dow: Sunday=0;
        // month 1-12) — the corpus reads both as Numbers.
        for (String f : Pure.nativeKeysAt("dayOfWeek")) {
            // real dayOfWeek():DayOfWeek — the value surface is the ENUM
            // NAME ('Saturday'), the enum-by-name convention every other
            // enum position uses (was DuckDB dow numbers, an engine-lite
            // relic the corpus no longer pins).
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.STRFTIME,
                    dateArg(n.args().get(0), args.get(0)),
                    new SqlExpr.FormatLit(java.util.List.of(com.legend.sql.DateFmt.Part.WEEKDAY_NAME))));
        }
        // month(): real pure returns the Month ENUM — the NAME ('January'),
        // same enum-by-name convention as dayOfWeek above (the engine's H2
        // emission is formatdatetime 'MMMM', the full month name; monthNumber
        // is the numeric surface and keeps EXTRACT below).
        for (String f : Pure.nativeKeysAt("month")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.STRFTIME,
                    dateArg(n.args().get(0), args.get(0)),
                    new SqlExpr.FormatLit(java.util.List.of(com.legend.sql.DateFmt.Part.MONTH_NAME))));
        }
        // quarter(): real pure returns the Quarter ENUM (Q1..Q4, with an
        // upstream TODO to make them numbers); the engine surface is the
        // bare integer — the corpus reads it as a Number.
        for (String f : Pure.nativeKeysAt("quarter")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                    new SqlExpr.StringLit("quarter"),
                    dateArg(n.args().get(0), args.get(0)))));
        }
        // Truncations moved to DateShifts (the 3500-line split
        // seam) — day-grained heads carry the Date-cast carrier
        // fix there.
        DateShifts.registerTruncationRules(RULES);
        // adjust(d, n, unit) / timeBucket(d, n, unit): the DurationUnit enum
        // literal selects DuckDB's interval-constructor function.
        // typeAsDeclared: type-only assertion — the VALUE passes through
        // (the mapping's declared-type coercion emits no SQL, engine
        // parity) AND the read takes the ENGINE-COMPAT provenance tag
        // (charter §4bZ): typeAsDeclared is emitted ONLY at a declared
        // property/column kind mismatch, so this rule IS the mapping
        // seam's tag door — reconciliation tolerates the label/wire
        // disagreement for tagged reads only.
        for (String f : Pure.nativeKeysAt("meta::legend::lite::typeAsDeclared")) {
            RULES.put(f, (n, args) ->
                    com.legend.sql.SqlTyping.tolerateRead(args.get(0)));
        }
        // castAsDeclared never reaches here — the Typer types it as a
        // WIRE-flagged TypedCast (the Lowerer's cast() reads the flag)

        // id() over an ENUM VALUE is its name — exactly the stored string
        // in relation-land. Any other instance's identifier is an engine
        // runtime concept with no SQL story: loud.
        for (String f : Pure.nativeKeysAt("id")) {
            RULES.put(f, (n, args) -> {
                if (n.args().get(0).info().type()
                        instanceof com.legend.compiler.element.type.Type.EnumType) {
                    return new SqlExpr.Cast(args.get(0), SqlType.Scalar.VARCHAR);
                }
                throw new com.legend.error.NotImplementedException(
                        "id() over a non-enum instance has no relation-land"
                                + " lowering");
            });
        }

        for (var sh : Map.of("mostRecentDayOfWeek", false,
                "previousDayOfWeek", true).entrySet()) {
            for (String f : Pure.nativeKeysAt(sh.getKey())) {
                RULES.put(f, (n, args) -> DateShifts.dayOfWeekShift(n, args,
                        enumName(n.args().get(n.args().size() == 1 ? 0 : 1)),
                        n.args().size() == 2
                                ? dateArg(n.args().get(0), args.get(0)) : null,
                        sh.getValue()));
            }
        }
        // (the old duplicate firstDayOfWeek registration is GONE — the
        // truncation table above owns it, WITH the Date cast)
        // adjust + its TEMPORAL channel twin live with the date-shift
        // machinery (DateShifts) — the 3500-line split seam.
        DateShifts.registerAdjustRules(RULES);
        // datePart of a PARTIAL literal is the IDENTITY (a year has no finer
        // date part); full-precision values truncate to the day.
        for (String f : Pure.nativeKeysAt("datePart")) {
            RULES.put(f, (n, args) -> partialPrecision(n.args().get(0)) != null
                    ? args.get(0)
                    : new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY, args));
        }
        for (String f : Pure.nativeKeysAt("timeBucket")) {
            RULES.put(f, (n, args) -> {
                boolean strict = n.args().get(0).info().type()
                        == Type.Primitive.STRICT_DATE;
                // real timeBucket REJECTS sub-day units on StrictDate —
                // message verbatim (strictDate assertError family)
                if (strict) {
                    switch (enumName(n.args().get(2))) {
                        case "HOURS", "MINUTES", "SECONDS", "MILLISECONDS",
                                "MICROSECONDS", "NANOSECONDS" ->
                            throw new ModelException(
                                    LegendCompileException.Phase.LOWER,
                                    "Unsupported duration unit for StrictDate. Units"
                                            + " can only be: [YEARS, DAYS, MONTHS, WEEKS]");
                        default -> { }
                    }
                }
                SqlExpr bucketed = new SqlExpr.Call(SqlFn.TIME_BUCKET, List.of(
                        new SqlExpr.StringLit(DateShifts.intervalFn(enumName(n.args().get(2)))),
                        args.get(1), dateArg(n.args().get(0), args.get(0))));
                if (strict) {
                    return new SqlExpr.Cast(bucketed, SqlType.Scalar.DATE);
                }
                // The result keeps the INPUT LITERAL's print precision: a
                // 9-digit-subsecond input buckets to a 9-digit-zero result
                // (real pure preserves subsecond DIGIT COUNT; bucketed
                // subseconds are always zero). Emitted as the precision-
                // faithful STRING — the wire's date convention.
                if (n.args().get(0) instanceof TypedCDate cd
                        && cd.value() instanceof
                                PureDateLiteral.DateWithSubsecond sub) {
                    return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.STRFTIME, bucketed,
                                    new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                            new SqlExpr.StringLit(
                                    "." + "0".repeat(sub.subsecond().length()))),
                            SqlType.Scalar.TEMPORAL_TEXT);
                }
                return bucketed;
            });
        }
        // dateDiff(d1, d2, unit): Pure semantics per unit (PCT-pinned) —
        // see dateDiffExpr.
        for (String f : Pure.nativeKeysAt("dateDiff")) {
            RULES.put(f, (n, args) -> dateDiffExpr(diffPart(n.args().get(2)),
                    dateArg(n.args().get(0), args.get(0)),
                    dateArg(n.args().get(1), args.get(1))));
        }
        // Epoch conversions: toEpochValue(d, unit) IS dateDiff(epoch, d,
        // unit) for EVERY DurationUnit (real pure dateExtension); the bare
        // form is SECONDS. (The audit: non-MILLISECONDS units were silently
        // epoch seconds.)
        for (String f : Pure.nativeKeysAt("toEpochValue")) {
            RULES.put(f, (n, args) -> dateDiffExpr(
                    n.args().size() > 1 ? diffPart(n.args().get(1)) : "second",
                    new SqlExpr.TimestampLit("1970-01-01 00:00:00"),
                    dateArg(n.args().get(0), args.get(0))));
        }
        // fromEpochValue(n, unit) = epoch + n unit-intervals.
        for (String f : Pure.nativeKeysAt("fromEpochValue")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                    new SqlExpr.StringLit(n.args().size() > 1
                            ? DateShifts.intervalFn(enumName(n.args().get(1))) : "to_seconds"),
                    args.get(0),
                    new SqlExpr.TimestampLit("1970-01-01 00:00:00"))));
        }
        // Day-granularity comparisons.
        for (String f : Pure.nativeKeysAt("isOnDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isAfterDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.GREATER,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isOnOrAfterDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        // Precision predicates: a LITERAL answers from its own written
        // precision; a column answers from its Pure type (StrictDate =
        // day precision, DateTime = SQL TIMESTAMP = full precision).
        for (var e : Map.of(
                "hasMonth", PureDateLiteral.Precision.MONTH,
                "hasDay", PureDateLiteral.Precision.DAY,
                "hasHour", PureDateLiteral.Precision.HOUR,
                "hasMinute", PureDateLiteral.Precision.MINUTE,
                "hasSecond", PureDateLiteral.Precision.SECOND,
                "hasSubsecond", PureDateLiteral.Precision.SUBSECOND).entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    // leg 7 D2 (A24/D92 retired): the answer is computed
                    // from the STAMP (datePrecision — ask the plan, never
                    // the cell) and the declared type is Boolean[1] — the
                    // carrier is Boolean UNCONDITIONALLY. The old
                    // TypedCDate fork emitted IntLit(1) for non-literal
                    // receivers under the Boolean stamp (HIR type dispatch
                    // + a silent kind default at the wire); its "engine's
                    // integer surface" justification was ungrounded — the
                    // reference has NO SQL surface for has* at all (six
                    // "No SQL translation exists" manifest rows).
                    boolean has = datePrecision(n.args().get(0)).atLeast(e.getValue());
                    return new SqlExpr.BoolLit(has);
                });
            }
        }
        for (String f : Pure.nativeKeysAt("hasSubsecondWithAtLeastPrecision")) {
            RULES.put(f, (n, args) -> {
                if (!(n.args().get(1)
                        instanceof TypedCInteger i)) {
                    throw new IllegalStateException("hasSubsecondWithAtLeastPrecision"
                            + " needs a literal precision");
                }
                long p2 = i.value().longValue();
                // A LITERAL answers from its WRITTEN digit count (PCT); a
                // TIMESTAMP column is microsecond-precision.
                if (n.args().get(0) instanceof TypedCDate d
                        && d.value() instanceof PureDateLiteral.DateWithSubsecond ds) {
                    return new SqlExpr.BoolLit(ds.subsecond().length() >= p2);
                }
                return new SqlExpr.BoolLit(datePrecision(n.args().get(0))
                        .atLeast(PureDateLiteral.Precision.SUBSECOND) && p2 <= 6);
            });
        }
        // ---- Misc (registrations bucket) ----
        for (String f : Pure.nativeKeysAt("between")) {
            // between IS the two guarded comparisons composed — every
            // operand is [0..1] and an EMPTY operand yields false, never
            // SQL NULL (C1.5a; same optionalOperandGuards the standalone
            // >=/<= rules apply). Partial-date literals pad like the
            // comparison family (dateArg).
            RULES.put(f, (n, args) -> {
                List<SqlExpr> padded = new ArrayList<>(args.size());
                for (int i = 0; i < args.size(); i++) {
                    padded.add(dateArg(n.args().get(i), args.get(i)));
                }
                return NullSemantics.optionalOperandGuards(n, padded,
                        new SqlExpr.Group(SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                                        padded.get(0), padded.get(1)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        padded.get(0), padded.get(2)))));
            });
        }
        for (String f : Pure.nativeKeysAt("compare")) {
            RULES.put(f, (n, args) -> {
                // CROSS-KIND compare is a CONSTANT: real Compare.java orders
                // Numbers < Dates < Booleans < Strings and never coerces —
                // SQL's coercion made compare(5, '5') zero.
                int k0 = Numerics.compareKind(n.args().get(0).info().type());
                int k1 = Numerics.compareKind(n.args().get(1).info().type());
                if (k0 >= 0 && k1 >= 0 && k0 != k1) {
                    return new SqlExpr.IntLit(Integer.compare(k0, k1));
                }
                // DATE operands compare CHRONOLOGICALLY on their padded
                // TIMESTAMP comparables — the partial-date STRING carrier
                // orders '2001' > '10999' lexically. Value work is SQL
                // (strptime pads; the compiler only names the format).
                SqlExpr lhs = MixedEncoding.dateComparableOrSelf(n.args().get(0), args.get(0));
                SqlExpr rhs = MixedEncoding.dateComparableOrSelf(n.args().get(1), args.get(1));
                return new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.LESS,
                                lhs, rhs), new SqlExpr.IntLit(-1)),
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER,
                                lhs, rhs), new SqlExpr.IntLit(1))),
                        new SqlExpr.IntLit(0));
            });
        }
        for (String f : Pure.nativeKeysAt("sqlTrue")) {
            RULES.put(f, (n, args) -> new SqlExpr.BoolLit(true));
        }
        for (String f : Pure.nativeKeysAt("sqlFalse")) {
            RULES.put(f, (n, args) -> new SqlExpr.BoolLit(false));
        }
        familyIfPresent(SqlFn.CURRENT_USER_FN, "currentUserId");
        familyIfPresent(SqlFn.COT, "cot");
        familyIfPresent(SqlFn.RADIANS, "toRadians");
        familyIfPresent(SqlFn.DEGREES, "toDegrees");
        // repeatString(s, n): an EMPTY/untyped first arg VARCHAR-casts —
        // DuckDB's binder otherwise picks the BLOB overload for a bare
        // NULL (wire BLOB under the String contract; §4bZ-V C
        // adjudication: fix-emitter, value identical)
        for (String f : Pure.nativeKeysAt("repeatString")) {
            RULES.put(f, (n, args) -> {
                SqlExpr s0 = args.get(0);
                if (!(s0.type() instanceof com.legend.sql.TypeFact.Typed)) {
                    s0 = new SqlExpr.Cast(s0, SqlType.Scalar.VARCHAR);
                }
                return SqlExpr.Call.of(SqlFn.REPEAT_STR, s0, args.get(1));
            });
        }
        familyIfPresent(SqlFn.JARO_WINKLER, "jaroWinklerSimilarity");
        // decodeBase64 accepts UNPADDED input (real pure; SQL from_base64
        // demands padding) — restore the '=' tail: literal-folded, or
        // s || repeat('=', (4 - length(s) % 4) % 4) at runtime.
        for (String f : Pure.nativeKeysAt("decodeBase64")) {
            RULES.put(f, (n, args) -> {
                SqlExpr in = args.get(0);
                if (in instanceof SqlExpr.StringLit lit) {
                    String v = lit.value();
                    in = new SqlExpr.StringLit(v + "=".repeat((4 - v.length() % 4) % 4));
                } else {
                    SqlExpr pad = SqlExpr.Call.of(SqlFn.MOD,
                            SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(4),
                                    SqlExpr.Call.of(SqlFn.MOD,
                                            SqlExpr.Call.of(SqlFn.LENGTH, in),
                                            new SqlExpr.IntLit(4))),
                            new SqlExpr.IntLit(4));
                    in = SqlExpr.Call.of(SqlFn.CONCAT, in,
                            SqlExpr.Call.of(SqlFn.REPEAT_STR,
                                    new SqlExpr.StringLit("="), pad));
                }
                return SqlExpr.Call.of(SqlFn.DECODE_BASE64, in);
            });
        }
        AsorReaders.register(RULES);
        // size(NULL list) is pure's EMPTY collection: 0, never NULL.
        // count stays OUT of these rules DELIBERATELY: the projection
        // sub-aggregation machinery owns it (the engine's group-by
        // subselect form) and dispatches via the absence — registering
        // it here hijacked testSubAggregationWithDeepAndOverlap
        // (measured 2026-08-21: 7 rows became 13).
        for (String f : Pure.nativeKeysAt("size")) {
            RULES.put(f, (n, args) -> {
                // a TO-ONE value is a 0/1-element collection: 'abc'->size()
                // is 1, never len('abc') (C1.5d — the same gate its 13
                // family siblings carry; the list encoding would
                // CHAR-INDEX a lone string)
                // deletion leg: stamp decides (a designed to-one
                // ArrayLit — a List OBJECT / struct instance — counts 1
                // in pure: size of one value).
                if (isToOne(n.args().get(0))) {
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.IS_NULL, args.get(0)),
                            new SqlExpr.IntLit(0))), new SqlExpr.IntLit(1));
                }
                // §5 at the counting consumer (R1 instrument catch:
                // size()=2 vs at(0)='a' over [[]->first(),'a'])
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                CollectionLanes.compactIfValueLane(
                                        n.args().get(0), args.get(0))),
                        new SqlExpr.IntLit(0));
            });
        }
        familyIfPresent(SqlFn.MINUS, Pure.Lite.SUB);
        // makeString: the Any[*] joiner. Elements stringify; a NULL element
        // prints 'TDSNull' (engine TDS-cell convention — ordinary pure
        // collections hold no empties, so the coalesce is unobservable
        // outside TDS rows).
        for (String f : Pure.nativeKeysAt("makeString")) {
            RULES.put(f, (n, args) -> {
                SqlExpr sep = args.size() == 2 ? args.get(1)
                        : args.size() == 4 ? args.get(2) : new SqlExpr.StringLit("");
                // LIST position, WRAP-BY-STAMP (burn-to-zero): stamps
                // are enforced-true; the h2 side is carried by the
                // CarrierStrategies list encodings (the 320 floor is
                // the referee).
                // audit §4: an HONEST [0..1] source that is empty at
                // runtime makeStrings to '' — pure's identity; the
                // 'TDSNull' spelling is the TDS CELL convention and
                // belongs to [1..1]-stamped (trust-wrapped) cell reads
                // and the many-element arm ONLY. The leak of the
                // sentinel as user data dies here.
                boolean optionalScalar = !Stamps.exactlyOne(n.args().get(0))
                        && isToOne(n.args().get(0));
                SqlExpr coll = PureSql.asList(args.get(0),
                        !isToOne(n.args().get(0)));
                SqlExpr strs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, coll,
                        new SqlExpr.Lambda(List.of("x"),
                                SqlExpr.Call.of(SqlFn.COALESCE,
                                        PureSql.elementText(n.args().get(0),
                                                coll, SqlExpr.Column.param(
                                                        "x", coll)),
                                        new SqlExpr.StringLit(optionalScalar
                                                ? ""
                                                : PlatformTypes.TDS_NULL_CELL))));
                SqlExpr joined = SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG, strs,
                                List.of(sep)),
                        new SqlExpr.StringLit(""));
                if (args.size() == 4) {
                    return SqlExpr.Call.of(SqlFn.CONCAT, args.get(1),
                            SqlExpr.Call.of(SqlFn.CONCAT, joined, args.get(3)));
                }
                return joined;
            });
        }
        // joinStrings over a LIST value: (list), (list, sep), or
        // (list, prefix, sep, suffix).
        for (String f : Pure.nativeKeysAt("joinStrings")) {
            RULES.put(f, (n, args) -> {
                // VALUE position over a literal element list: the engine
                // INTERLEAVES the separator (CONCAT_JOIN; the TDS channel
                // keeps the append-form STRING_AGG below)
                if (args.size() <= 2
                        && n.args().get(0) instanceof com.legend.compiler
                                .spec.typed.TypedCollection tcol
                        && !tcol.elements().isEmpty() && args.get(0) instanceof SqlExpr.ArrayLit jal
                        // a subquery element cannot ride the list literal
                        && (tcol.elements().stream().allMatch(el ->
                                el instanceof TypedNativeCall enc
                                && com.legend.builtin.Pure.isToOneCall(enc.callee().qualifiedName()))
                            || jal.elements().stream().anyMatch(SqlProbes::containsSubquery))) {
                    List<SqlExpr> parts = new ArrayList<>();
                    for (SqlExpr el : jal.elements()) {
                        if (!parts.isEmpty() && args.size() == 2) {
                            parts.add(args.get(1));
                        }
                        parts.add(el);
                    }
                    return new SqlExpr.Call(SqlFn.CONCAT_JOIN, parts);
                }
                // a TO-ONE source IS the joined string; an EMPTY list
                // joins to '' (list_aggregate over NULL/[] is NULL).
                SqlExpr joined;
                if (Stamps.exactlyOne(n.args().get(0))) {
                    joined = args.get(0);   // stamp decides (String args)
                } else if (isToOne(n.args().get(0))) {
                    // audit §4: a runtime-empty [0..1] joins to '' —
                    // pure's empty identity, not NULL
                    joined = SqlExpr.Call.of(SqlFn.COALESCE, args.get(0),
                            new SqlExpr.StringLit(""));
                } else {
                    SqlExpr sep = args.size() == 2 ? args.get(1)
                            : args.size() == 4 ? args.get(2) : new SqlExpr.StringLit("");
                    joined = SqlExpr.Call.of(SqlFn.COALESCE,
                            new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG,
                                    // empty/NULL lists conform to the
                                    // pure element's array (§4bZ-U leg 2
                                    // — the typedList door)
                                    PureSql.typedList(args.get(0),
                                            n.args().get(0).info().type()),
                                    List.of(sep)),
                            new SqlExpr.StringLit(""));
                }
                if (args.size() == 4) {
                    return SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.CONCAT, args.get(1), joined),
                            args.get(3));
                }
                return joined;
            });
        }
        // percentile family over LIST values; the 4-arg overload's
        // ascending/continuous flags choose the quantile flavor, and a
        // DESCENDING percentile is the 1-p quantile.
        for (String f : Pure.nativeKeysAt("percentile")) {
            RULES.put(f, (n, args) -> {
                boolean asc = true;
                boolean cont = true;
                if (n.args().size() == 4) {
                    asc = boolLiteral(n.args().get(2), "percentile ascending");
                    cont = boolLiteral(n.args().get(3), "percentile continuous");
                }
                if (cont) {
                    SqlExpr p2 = asc ? args.get(1)
                            : SqlExpr.Call.of(SqlFn.MINUS,
                                    new SqlExpr.IntLit(1), args.get(1));
                    return new SqlExpr.ReduceCollection(SqlAgg.Fn.QUANTILE_CONT,
                            args.get(0), List.of(p2));
                }
                return pureDiscretePercentile(args.get(0), args.get(1), asc);
            });
        }
        // collection sort: bare list_sort; a COMPARATOR must be a bare
        // compare over the two parameters (its argument order IS the
        // direction); a KEY function sorts {k, i, v} structs by key —
        // index second, so equal keys stay stable — then unwraps.
        for (String f : Pure.nativeKeysAt("sort")) {
            RULES.put(f, (n, args) -> {
                // STAMP-READ identity: sort over <=1 values IS the
                // operand ([0..0] included: sort([]) is []).
                if (Stamps.atMostOne(n.args().get(0))) {
                    return args.get(0);
                }
                if (n.args().size() == 1) {
                    TypedSpec sortOperand = n.args().get(0);
                    SqlExpr sortLowered = args.get(0);
                    // SEE-THROUGH a plain removeDuplicates (callee-FQN
                    // identified, never a name string): sort∘dedup =
                    // dedup∘sort over identical (id, comparable) pairs, so
                    // the encoding reads the INNER collection and the pair
                    // subselect dedups with DISTINCT
                    // (testRemoveDuplicates...MixedTypes sorts a dedup).
                    boolean dedup = false;
                    if (sortOperand instanceof
                                com.legend.compiler.spec.typed.TypedNativeCall dd
                            && Pure.nativeKeysAt("removeDuplicates")
                                    .contains(dd.callee().signatureKey())
                            && !dd.args().isEmpty()
                            && sortLowered instanceof SqlExpr.Call dc
                            && dc.fn() == SqlFn.LIST_FILTER
                            && unwrapArrayCast(dc.args().get(0))
                                    instanceof SqlExpr.ArrayLit innerLa) {
                        sortOperand = dd.args().get(0);
                        sortLowered = innerLa;
                        dedup = true;
                    }
                    MixedEncoding.MixedElems mx = MixedEncoding.mixedElems(sortOperand, sortLowered);
                    if (mx == null) {
                        // ANY-LUB mixed kinds (ints + strings): the rank-
                        // struct comparable orders by the engine's compare
                        // groups; the identity channel is the carrier's own
                        // literal lane (testRemoveDuplicates...MixedTypes)
                        mx = MixedEncoding.rankedElems(sortOperand, sortLowered);
                    }
                    if (mx == null && dedup) {
                        // dedup detected but the inner shape isn't
                        // encodable — fall back to the ORIGINAL operand
                        sortOperand = n.args().get(0);
                        sortLowered = args.get(0);
                        dedup = false;
                    }
                    if (mx != null) {
                        // identity-preserving mixed sort: order the ids by
                        // their comparables (parallel select-list unnests)
                        var inner = new SqlSelect(List.of(
                                new SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, mx.idList()), "i", null),
                                new SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, mx.valList()), "v", null)),
                                dedup, new com.legend.sql.SqlSource.Dual(), null,
                                List.of(), null, null, List.of(),
                                null, null, List.of());
                        var src = new SqlSource.Subselect(inner, "_mx", null);
                        var outer = new SqlSelect(List.of(
                                new SqlSelect.Projection(
                                        new SqlExpr.OrderedListAgg(
                                                SqlExpr.Column.derived("_mx", "i"),
                                                SqlExpr.Column.derived("_mx", "v")), "s", null)),
                                false, src, null, List.of(), null, null, List.of(),
                                null, null, List.of());
                        // F10 slice 2/3: the sorted ids ARE pure-literal
                        // spellings (temporals joined in slice 3 via
                        // the %-forms) — the Array(LITERAL) cast is the
                        // construction-site label (scalarRoot reads it;
                        // VARCHAR[] physically, an identity cast).
                        return new SqlExpr.Cast(
                                new SqlExpr.ScalarSubquery(outer),
                                new SqlType.Array(SqlType.Scalar.LITERAL));
                    }
                    // STAMP-read (pair-#4 eliminated): only many-
                    // stamped operands reach here, and a many-stamped
                    // value's SQL is a list (the invariant's contract).
                    return new SqlExpr.Call(SqlFn.LIST_SORT,
                            List.of(args.get(0)));
                }
                Boolean asc = Comparators.direction(
                        n.args().get(n.args().size() - 1));
                if (asc == null) {
                    throw new com.legend.error.NotImplementedException(
                            "sort comparators beyond a bare compare over the"
                            + " two parameters are not modeled");
                }
                if (n.args().size() == 2) {
                    return new SqlExpr.Call(
                            asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC,
                            List.of(args.get(0)));
                }
                if (!(args.get(1) instanceof SqlExpr.Lambda key)
                        || key.params().size() != 1) {
                    throw new IllegalStateException(
                            "sort expects (values, key-function, comparator)");
                }
                SqlExpr range = SqlExpr.Call.of(SqlFn.RANGE_FN,
                        new SqlExpr.IntLit(1),
                        plusOne(SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                args.get(0))));
                SqlExpr i = SqlExpr.Column.param("_st_i", range);
                SqlExpr valAt = SqlExpr.Call.of(SqlFn.LIST_GET, args.get(0), i);
                SqlExpr keyExpr = substituteRef(key.body(), key.params().get(0), valAt);
                SqlExpr idxField = asc ? i
                        : SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(0), i);
                SqlExpr pairs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, range,
                        new SqlExpr.Lambda(List.of("_st_i"),
                                new SqlExpr.StructLit(List.of(
                                        new SqlExpr.StructLit.Field("k", keyExpr),
                                        new SqlExpr.StructLit.Field("i", idxField),
                                        new SqlExpr.StructLit.Field("v", valAt)))));
                SqlExpr sorted = new SqlExpr.Call(
                        asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(pairs));
                return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, sorted,
                        new SqlExpr.Lambda(List.of("_st_e"),
                                new SqlExpr.StructGet(
                                        SqlExpr.Column.param("_st_e", sorted),
                                        "v")));
            });
        }
        for (String f : Pure.nativeKeysAt("isBeforeDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LESS,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isOnOrBeforeDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("toDecimal")) {
            // literal folds + input-stamp-driven scale (DecimalKindRules)
            RULES.put(f, DecimalKindRules::toDecimal);
        }
        for (String f : Pure.nativeKeysAt(Pure.Lite.DIVIDE_ROUND)) {
            RULES.put(f, DecimalKindRules::divideRound);
        }
        for (String f : Pure.nativeKeysAt(Pure.Lite.NOT_EQUAL_ANSI)) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                    args.get(0), args.get(1)));
        }

        // Temporal EXTRACT parts: one SqlFn entry, part-name literal first.
        for (var e : Map.of(
                "year", "year", "monthNumber", "month", "dayOfMonth", "day",
                "hour", "hour", "minute", "minute", "second", "second").entrySet()) {
            // a PARTIAL date lacking the component RAISES real pure's
            // message ('Cannot get day of month for 2017') — statically
            // decidable from the precision; the message composes in SQL
            PureDateLiteral.Precision needed = switch (e.getValue()) {
                case "month" -> PureDateLiteral.Precision.MONTH;
                case "day" -> PureDateLiteral.Precision.DAY;
                case "hour" -> PureDateLiteral.Precision.HOUR;
                case "minute" -> PureDateLiteral.Precision.MINUTE;
                case "second" -> PureDateLiteral.Precision.SECOND;
                default -> PureDateLiteral.Precision.YEAR;
            };
            String label = switch (e.getValue()) {
                case "day" -> "day of month";
                default -> e.getValue();
            };
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    PureDateLiteral.Precision prec =
                            datePrecisionOrUnknown(n.args().get(0));
                    if (prec != null && !prec.atLeast(needed)) {
                        return PureSql.raise(
                                cat(new SqlExpr.StringLit(
                                                "Cannot get " + label + " for "),
                                        str(args.get(0))), n.pos());
                    }
                    // A PARTIAL date that HAS the component carries as its
                    // print-form string ('2015-04') — the component is a
                    // split_part read, in SQL (date_part can't bind VARCHAR).
                    Integer pp = partialPrecision(n.args().get(0));
                    if (pp != null) {
                        int field = e.getValue().equals("year") ? 1 : 2;
                        return new SqlExpr.Cast(
                                SqlExpr.Call.of(SqlFn.SPLIT_PART, args.get(0),
                                        new SqlExpr.StringLit("-"),
                                        new SqlExpr.IntLit(field)),
                                SqlType.Scalar.BIGINT);
                    }
                    List<SqlExpr> withPart = new ArrayList<>();
                    withPart.add(new SqlExpr.StringLit(e.getValue()));
                    withPart.addAll(args);
                    return new SqlExpr.Call(SqlFn.EXTRACT, withPart);
                });
            }
        }
        // Collection min/max/sum: 1-arg = over a LIST; 2-arg = least/greatest.
        // A TO-ONE argument (sum(7), average of one value) is the IDENTITY —
        // the list encodings choke on scalars.
        for (String f : Pure.nativeKeysAt("min")) {
            RULES.put(f, (n, args) -> {
                MixedEncoding.MixedElems mx = args.size() == 1 ? MixedEncoding.mixedElems(n.args().get(0), args.get(0)) : null;
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MIN, mx.valList()));
                }
                if (args.size() == 2 && args.get(1) instanceof SqlExpr.Lambda cmp) {
                    // a TO-ONE collection is its own extreme — but a
                    // SINGLETON LIST LITERAL is a list; its reduction is
                    // the element (the minus rule's convention)
                    return isToOne(n.args().get(0))
                            ? args.get(0)
                            : Comparators.select(args.get(0), cmp, false);
                }
                if (args.size() > 1) {
                    MixedEncoding.MixedElems ma = MixedEncoding.mixedArgs(n.args(), args);
                    return ma != null
                            ? ma.select(SqlExpr.Call.of(SqlFn.LIST_MIN, ma.valList()))
                            : new SqlExpr.Call(SqlFn.LEAST, args);
                }
                return isToOne(n.args().get(0))
                        ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MIN, args);
            });
        }
        for (String f : Pure.nativeKeysAt("max")) {
            RULES.put(f, (n, args) -> {
                MixedEncoding.MixedElems mx = args.size() == 1 ? MixedEncoding.mixedElems(n.args().get(0), args.get(0)) : null;
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MAX, mx.valList()));
                }
                if (args.size() == 2 && args.get(1) instanceof SqlExpr.Lambda cmp) {
                    // (same singleton-list-literal guard as min)
                    return isToOne(n.args().get(0))
                            ? args.get(0)
                            : Comparators.select(args.get(0), cmp, true);
                }
                if (args.size() > 1) {
                    MixedEncoding.MixedElems ma = MixedEncoding.mixedArgs(n.args(), args);
                    return ma != null
                            ? ma.select(SqlExpr.Call.of(SqlFn.LIST_MAX, ma.valList()))
                            : new SqlExpr.Call(SqlFn.GREATEST, args);
                }
                return isToOne(n.args().get(0))
                        ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MAX, args);
            });
        }
        // THE SINGLETON-LIST-LITERAL RULE (burn slice 2 closed the CLASS,
        // not the instance): a TO-ONE argument is its own reduction, but a
        // SINGLETON LIST LITERAL is a LIST — the reduction of [x] is x,
        // via the list op (the minus rule's convention; witnesses
        // testAverage_Integers, testLeast_Single). DELETION LEG: the
        // ArrayLit guards are gone — the invariant makes non-designed
        // lists under to-one stamps impossible, and for the designed
        // List-OBJECT carrier the identity arm IS pure semantics
        // (first(aList) is the List, not its inner first).
        for (String f : Pure.nativeKeysAt("sum")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    ? args.get(0)
                    : SqlExpr.Call.of(SqlFn.LIST_SUM, Numerics.numList(args.get(0))));
        }
        // round(Number[1]) RETURNS Integer (real pure) — banker's round,
        // then the integral cast the signature promises; round(x, scale)
        // keeps its operand's type.
        for (String f : Pure.nativeKeysAt("round")) {
            RULES.put(f, DecimalKindRules::round);
        }
        // greatest/least/mode take ONE collection argument (real pure: values:X[*]);
        // like min/max/sum, a to-one argument is the identity and a list reduces
        // with the list encoding — SQL's variadic GREATEST/LEAST never applies.
        for (String f : Pure.nativeKeysAt("greatest")) {
            RULES.put(f, (n, args) -> {
                MixedEncoding.MixedElems mx = MixedEncoding.mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MAX, mx.valList()));
                }
                return isToOne(n.args().get(0))
                        ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MAX, args);
            });
        }
        for (String f : Pure.nativeKeysAt("least")) {
            RULES.put(f, (n, args) -> {
                MixedEncoding.MixedElems mx = MixedEncoding.mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MIN, mx.valList()));
                }
                return isToOne(n.args().get(0))
                        ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MIN, args);
            });
        }
        for (String f : Pure.nativeKeysAt("mode")) {
            RULES.put(f, (n, args) -> {
                MixedEncoding.MixedElems mx = MixedEncoding.mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    // real mode.pure SORTS then folds runs: the representative
                    // is the LAST-ENCOUNTERED equal element (stable sort keeps
                    // encounter order) — the winner's last position in vals
                    SqlExpr winner = SqlExpr.Call.of(SqlFn.LIST_MODE, mx.valList());
                    SqlExpr lastPos = SqlExpr.Call.of(SqlFn.MINUS,
                            SqlExpr.Call.of(SqlFn.PLUS,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, mx.valList()),
                                    new SqlExpr.IntLit(1)),
                            SqlExpr.Call.of(SqlFn.LIST_POSITION,
                                    SqlExpr.Call.of(SqlFn.LIST_REVERSE, mx.valList()),
                                    winner));
                    return mx.markLiteral(SqlExpr.Call.of(
                            SqlFn.LIST_GET, mx.idList(), lastPos));
                }
                return isToOne(n.args().get(0))
                        ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MODE, args);
            });
        }
        // zip(a, b) -> Pair<T,U>[*]: index over the SHORTER list (real pure
        // truncates; DuckDB's native list_zip PADS with NULL — wrong
        // semantics), each element a struct with Pair's first/second layout.
        // zip: c1-literal sides box (DEEP_AUDIT §3); ListEncodings.zip
        ListEncodings.registerZip(RULES);   // zip: the encoding's owner
        for (String name : List.of("mean", "average")) {
            for (String f : Pure.nativeKeysAt(name)) {
                // a to-one value is its own mean but the KIND is Float
                // (pure average: Float[1]) — the bare identity kept the
                // column's INTEGER, the wrong declared kind on the wire
                // (adjudication ledger cluster 10)
                RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                        ? new SqlExpr.Cast(args.get(0), SqlType.Scalar.DOUBLE)
                        : SqlExpr.Call.of(SqlFn.LIST_AVG, Numerics.numList(args.get(0))));
            }
        }
        // median overrides its plain-family registration: the mixed-Number
        // carrier must unwrap (json ordering is lexicographic — the wrong
        // middle) and a to-one value is its own median.
        for (String f : Pure.nativeKeysAt("median")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    ? args.get(0)
                    : SqlExpr.Call.of(SqlFn.LIST_MEDIAN, Numerics.numList(args.get(0))));
        }
        ScalarStats.register(RULES);   // stat reductions
        // variance(list, isBiasCorrected): true => sample, false => population.
        for (String f : Pure.nativeKeysAt("variance")) {
            RULES.put(f, (n, args) -> {
                boolean sample = n.args().size() <= 1
                        || boolLiteral(n.args().get(1), "variance isBiasCorrected");
                return new SqlExpr.ReduceCollection(
                        sample ? SqlAgg.Fn.VAR_SAMP : SqlAgg.Fn.VAR_POP,
                        Numerics.numList(args.get(0)), List.of());
            });
        }
        // first/head/last over a TO-ONE value are the IDENTITY — the list
        // encoding CHAR-INDEXES a lone string ('Doe'[1] = 'D', the at()/last()
        // trap; audit made the family uniform).
        for (String f : Pure.nativeKeysAt("first")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("head")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("last")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(-1))));
        }
        // RELATIONAL substring = VERBATIM SQL substring(start, length)
        // passthrough (engine goldens pass args unshifted; diverges from
        // platform pure's 0-based). ONE verbatim emission (Phase 1
        // audit): the DuckDB start-clamp is SubstringClamp, that
        // dialect's own rewrite pass.
        for (String f : Pure.nativeKeysAt("substring")) {
            RULES.put(f, (n, args) ->
                    new SqlExpr.Call(SqlFn.SUBSTRING, args));
        }
        for (String f : Pure.nativeKeysAt("indexOf")) {
            RULES.put(f, (n, args) -> {
                // Dispatch on the RESOLVED CALLEE's declared param: a [*]
                // set is the LIST search (collection::indexOf), a [1]
                // string the SUBSTRING search — never the operand's SQL
                // shape (C1: scalar-stamped singletons lower scalar; the
                // old type+ArrayLit sniff sent ['a']->indexOf to strpos).
                boolean listCallee = !(n.callee().parameters().get(0)
                        .multiplicity() instanceof Multiplicity.Bounded pb
                        && pb.upper() != null && pb.upper() <= 1);
                if (listCallee || n.args().get(0).info().type() != Type.Primitive.STRING) {
                    // LIST indexOf: 0-based, -1 on a miss; the operand
                    // conforms to the list contract by stamp (asList)
                    // and COMPACTS on the value lane (§5 positional
                    // consumer — R1 instrument).
                    return new SqlExpr.Call(SqlFn.MINUS, List.of(
                            new SqlExpr.Call(SqlFn.COALESCE, List.of(
                                    new SqlExpr.Call(SqlFn.LIST_POSITION,
                                            List.of(CollectionLanes
                                                    .compactIfValueLane(
                                                            n.args().get(0),
                                                    PureSql.asList(args.get(0),
                                                    !isToOne(n.args().get(0)))),
                                                    args.get(1))),
                                    new SqlExpr.IntLit(0))),
                            new SqlExpr.IntLit(1)));
                }
                if (args.size() == 3) {
                    // indexOf(s, sub, from): H2 LOCATE(sub, s, from)
                    // semantics — 1-BASED from and result, miss 0 (the
                    // engine has NO translation for this overload; the
                    // convention follows the 2-arg golden). Search the
                    // suffix, re-base hits.
                    SqlExpr from1 = args.get(2) instanceof SqlExpr.IntLit il
                            ? new SqlExpr.IntLit(Math.max(il.value(), 1))
                            : SqlExpr.Call.of(SqlFn.GREATEST, args.get(2),
                                    new SqlExpr.IntLit(1));
                    SqlExpr suffix = new SqlExpr.Call(SqlFn.SUBSTRING, List.of(
                            args.get(0), from1));
                    SqlExpr k = new SqlExpr.Call(SqlFn.STRPOS,
                            List.of(suffix, args.get(1)));
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.GREATER, k, new SqlExpr.IntLit(0)),
                            SqlExpr.Call.of(SqlFn.MINUS,
                                    SqlExpr.Call.of(SqlFn.PLUS, k, from1),
                                    new SqlExpr.IntLit(1)))),
                            new SqlExpr.IntLit(0));
                }
                // 1-BASED, raw strpos: the engine translates indexOf to
                // locate() verbatim (testSqlFunctionsInMapping golden
                // 'select locate(...)' with rows [12,12] — C1.5c; the
                // reference DuckDB PCT adapter ledgers the same platform
                // divergence, "expected: 4 actual: 5"). Composes with the
                // 1-based verbatim substring above. Miss = 0.
                return new SqlExpr.Call(SqlFn.STRPOS, args);
            });
        }
        for (String f : Pure.nativeKeysAt("at")) {
            // at(x, 0) over a TO-ONE value is the IDENTITY — the list encoding
            // would CHAR-INDEX a lone string ('Doe'[1] = 'D' in DuckDB).
            RULES.put(f, (n, args) -> {
                if (isToOne(n.args().get(0))
                        && args.get(1) instanceof SqlExpr.IntLit i && i.value() == 0) {
                    return args.get(0);
                }
                // OUT-OF-BOUNDS raises pure's message in the database;
                // §5: the positional read consumes the COMPACTED carrier
                SqlExpr op = CollectionLanes.compactIfValueLane(
                        n.args().get(0), args.get(0));
                SqlExpr size = SqlExpr.Call.of(SqlFn.LIST_LENGTH, op);
                SqlExpr oob = SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.GREATER_EQUAL, args.get(1), size),
                        SqlExpr.Call.of(SqlFn.LESS, args.get(1), new SqlExpr.IntLit(0)));
                return guarded(oob,
                        cat(new SqlExpr.StringLit(
                                        "The system is trying to get an element at offset "),
                                str(args.get(1)),
                                new SqlExpr.StringLit(" where the collection is of size "),
                                str(size)),
                        new SqlExpr.Call(SqlFn.LIST_GET,
                                List.of(op, plusOne(args.get(1)))),
                        n.pos());
            });
        }
        // list(items): the List<T> CARRIER — at SQL level the list value
        // itself (a to-one item wraps as a singleton).
        for (String f : Pure.nativeKeysAt("list")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    ? new SqlExpr.ArrayLit(List.of(args.get(0)))
                    : args.get(0));
        }
        // add(set, val) appends; add(set, index, val) INSERTS at the 0-based
        // index: prefix || [val] || suffix.
        for (String f : Pure.nativeKeysAt("add")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 2) {
                    // audit §4: a to-one first operand carriers as its
                    // one-element list (list_append(VARCHAR, x) is a
                    // binder error — the missing asList wrap)
                    return new SqlExpr.Call(SqlFn.LIST_APPEND, List.of(
                            PureSql.asList(args.get(0),
                                    !isToOne(n.args().get(0))),
                            args.get(1)));
                }
                SqlExpr l = args.get(0);
                SqlExpr idx = args.get(1);
                SqlExpr inserted = SqlExpr.Call.of(SqlFn.LIST_CONCAT,
                        SqlExpr.Call.of(SqlFn.LIST_CONCAT,
                                SqlExpr.Call.of(SqlFn.LIST_SLICE, l,
                                        new SqlExpr.IntLit(1), idx),
                                new SqlExpr.ArrayLit(List.of(args.get(2)))),
                        SqlExpr.Call.of(SqlFn.LIST_SLICE, l, plusOne(idx),
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, l)));
                // An out-of-range index ERRORS (real pure) — the slice
                // recipe would silently clamp to an append (audit).
                return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER, idx,
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, l)),
                        SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(
                                "add(set, index, value): index out of bounds")))),
                        inserted);
            });
        }
        // removeDuplicates: bare distinct for the plain / equality-comparator
        // forms; a CUSTOM comparator (or key + comparator) folds real pure's
        // accumulate-then-compare-against-KEPT semantics — a list_reduce over
        // singleton-wrapped elements (the accumulator IS the kept list), the
        // candidate dropped when any KEPT element satisfies eq(kept, candidate).
        for (String f : Pure.nativeKeysAt("removeDuplicates")) {
            RULES.put(f, (n, args) -> {
                // a TO-ONE value is its own dedup — but the output is
                // [*]-typed, so it must stay LIST-shaPED for consumers
                // (the root UNNEST, downstream list ops)
                if (isToOne(n.args().get(0))) {
                    return new SqlExpr.ArrayLit(List.of(args.get(0)));
                }
                if (args.size() < 2 || isEqualityComparator(n.args().get(n.args().size() - 1))) {
                    return ListEncodings.orderedDedup(args.get(0));
                }
                if (!(args.get(args.size() - 1) instanceof SqlExpr.Lambda eq)
                        || eq.params().size() != 2) {
                    throw new IllegalStateException("removeDuplicates comparator"
                            + " must be a 2-parameter function");
                }
                UnaryOperator<SqlExpr> key =
                        args.size() == 3 && args.get(1) instanceof SqlExpr.Lambda k
                                && k.params().size() == 1
                        ? v -> substituteRef(k.body(), k.params().get(0), v)
                        : UnaryOperator.identity();
                // NESTED dedups reuse these accumulator names — an inner
                // comparator's lambdas would CAPTURE the outer's refs
                // (audit). The suffix is the count of dedup calls inside
                // this one's own subtree: deterministic, and strictly
                // larger for the outer of any nested pair.
                int depth = Dedup.countDedups(n.args().get(n.args().size() - 1));
                // empty/NULL lists conform to the pure element's array
                // (§4bZ-U — the typedList door: the dedup encoding's own
                // param doors then stamp everything downstream)
                return Dedup.keptDedup(
                        PureSql.typedList(args.get(0),
                                n.args().get(0).info().type()),
                        depth, (prior, cand) -> substituteRef(
                        substituteRef(eq.body(), eq.params().get(0), key.apply(prior)),
                        eq.params().get(1), key.apply(cand)));
            });
        }
        // collection::distinct = removeDuplicates (real distinct.pure) —
        // registered by the EXACT collection overload key.
        RULES.put(Pure.DISTINCT_COLLECTION_KEY,
                // audit §4: the same to-one guard its synonym
                // removeDuplicates always had (a [0..1] value hit the
                // list-lambda binder)
                (n, args) -> isToOne(n.args().get(0))
                        ? new SqlExpr.ArrayLit(List.of(args.get(0)))
                        : ListEncodings.orderedDedup(args.get(0)));
        // print/println inside an expression (map(r|println(...))): the
        // NO-OP doctrine (StatementExecutor's print arm) — the value is
        // the Nil[0] cell, NULL; the argument is pure SQL computation and
        // drops with it (effectful args cannot type into scalar position)
        for (String f : Pure.nativeKeysAt("print")) {
            RULES.put(f, (n, args) -> new SqlExpr.NullLit());
        }
        for (String f : Pure.nativeKeysAt("println")) {
            RULES.put(f, (n, args) -> new SqlExpr.NullLit());
        }
        // regexp family (real regex/*.pure): DuckDB regexp_* with the
        // RegexpParameter enums translated to RE2 option chars —
        // CASE_SENSITIVE 'c', CASE_INSENSITIVE 'i', MULTILINE 'm',
        // NON_NEWLINE_SENSITIVE 's' (POSIX '.' matches newline).
        // toRepresentation (Phase 4 platform native; host owner
        // PureAsserts.repr, THIS is the SQL owner): the pure-source
        // spelling — strings quote+escape, dates take the % form,
        // Decimal the D suffix, numbers/booleans their text. Statically
        // typed args emit exactly; an Any-typed arg takes the carrier's
        // best-effort text (message-position cosmetics only — pass/fail
        // never rides this).
        for (String f : Pure.nativeKeysAt("toRepresentation")) {
            RULES.put(f, (n, args) -> Repr.of(
                    n.args().get(0).info().type(), args.get(0)));
        }
        // chunk(s, n): fixed-size chunking IS the regex '.{1,n}' swept
        // globally (chunk.pure spec: 'abcdefghijklmnop'->chunk(5) =
        // abcde|fghij|klmno|p; a short string is one chunk) — the
        // pattern composes in SQL so a computed n works too.
        for (String f : Pure.nativeKeysAt("chunk")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(
                    SqlFn.REGEXP_EXTRACT_ALL, args.get(0),
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            new SqlExpr.StringLit(".{1,"),
                            new SqlExpr.Cast(args.get(1),
                                    com.legend.sql.SqlType.Scalar.VARCHAR),
                            new SqlExpr.StringLit("}"))));
        }
        for (String f : Pure.nativeKeysAt("regexpLike")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.MATCHES, List.of(
                    args.get(0),
                    n.args().size() > 2
                            ? RegexpRules.inlineFlags(args.get(1), RegexpRules.regexpFlags(n.args().get(2)))
                            : args.get(1))));
        }
        for (String f : Pure.nativeKeysAt("regexpCount")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                    RegexpRules.regexpAll(n, args, 2)));
        }
        for (String f : Pure.nativeKeysAt("regexpExtract")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(2) instanceof SqlExpr.BoolLit all)) {
                    throw new IllegalStateException("regexpExtract extractAll must be literal");
                }
                SqlExpr allMatches = RegexpRules.regexpAll(n, args, 3);
                // extract-one stays LIST-shaped — String[*] unnests it
                return all.value() ? allMatches
                        : SqlExpr.Call.of(SqlFn.LIST_SLICE, allMatches,
                                new SqlExpr.IntLit(1), new SqlExpr.IntLit(1));
            });
        }
        // regexpIndexOf is 0-based Matcher.start(group); no match -> -1.
        // POSITIONAL, never lexical (the audit unwound a strpos-of-match-text
        // shape that mislocated anchored/repeated matches): the position is
        // the length of the LAZY ANCHORED PREFIX group '^(.*?)P' — measured
        // by the regex engine itself, in SQL. For a group argument the
        // (static, literal) pattern splits at that group's capturing paren:
        // '^(.*?  P-before-group )( P-from-group ...' — our prefix group is
        // always #1 (the first paren), later renumbering is irrelevant.
        for (String f : Pure.nativeKeysAt("regexpIndexOf")) {
            RULES.put(f, (n, args) -> {
                int group = 0;
                String flags = "";
                for (int i = 2; i < n.args().size(); i++) {
                    if (args.get(i) instanceof SqlExpr.IntLit g) {
                        group = (int) g.value();
                    } else {
                        flags = RegexpRules.regexpFlags(n.args().get(i));
                    }
                }
                if (!(args.get(1) instanceof SqlExpr.StringLit pat)) {
                    if (group > 0) {
                        throw new IllegalStateException("regexpIndexOf with a group"
                                + " needs a literal pattern (the pattern splits at"
                                + " the group's paren statically)");
                    }
                }
                String p = args.get(1) instanceof SqlExpr.StringLit lit ? lit.value() : null;
                String before = "", from = null;
                if (group > 0) {
                    // gate-found NPE: a NON-LITERAL pattern with group > 0
                    // reached capturingParen(null, ...) — the paren split
                    // needs the literal text; wall instead of NPE
                    if (p == null) {
                        throw new com.legend.error.NotImplementedException(
                                "regexp group extraction over a non-literal"
                                + " pattern is not supported");
                    }
                    int idx = RegexpRules.capturingParen(p, group);
                    before = p.substring(0, idx);
                    from = p.substring(idx);
                }
                SqlExpr prefixPattern = p != null
                        ? new SqlExpr.StringLit("(?s)^((?:.*?)" + before + ")"
                                + (from != null ? from : "(?:" + p + ")"))
                        : cat(new SqlExpr.StringLit("(?s)^((?:.*?))(?:"),
                                args.get(1), new SqlExpr.StringLit(")"));
                SqlExpr prefix = new SqlExpr.Call(SqlFn.REGEXP_EXTRACT, List.of(
                        args.get(0), RegexpRules.inlineFlags(prefixPattern, flags),
                        new SqlExpr.IntLit(1)));
                // a match where the GROUP did not participate is -1 in real
                // pure (Matcher.start(group)); regexp_extract yields '' there
                SqlExpr matched = SqlExpr.Call.of(SqlFn.LIST_GET,
                        RegexpRules.regexpAll(n, args, 2), new SqlExpr.IntLit(1));
                return new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.OR,
                                        SqlExpr.Call.of(SqlFn.IS_NULL, matched),
                                        SqlExpr.Call.of(SqlFn.IS_NULL, prefix)),
                                new SqlExpr.IntLit(-1))),
                        SqlExpr.Call.of(SqlFn.LENGTH, prefix));
            });
        }
        for (String f : Pure.nativeKeysAt("regexpReplace")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(3) instanceof SqlExpr.BoolLit all)) {
                    throw new IllegalStateException("regexpReplace replaceAll must be literal");
                }
                String flags = n.args().size() > 4 ? RegexpRules.regexpFlags(n.args().get(4)) : "";
                SqlExpr pattern = RegexpRules.inlineFlags(args.get(1), flags);
                // 'g' (global) is a true OPTION, not an inline flag
                return new SqlExpr.Call(SqlFn.REGEXP_REPLACE, List.of(
                        args.get(0), pattern, args.get(2),
                        new SqlExpr.StringLit(all.value() ? "g" : "")));
            });
        }
        // lpad/rpad: an EMPTY pad char returns the subject unchanged (real
        // testLpadEmptyChar) — DuckDB raises 'Insufficient padding' instead.
        for (String name : List.of("lpad", "rpad")) {
            SqlFn padFn = name.equals("lpad") ? SqlFn.LPAD : SqlFn.RPAD;
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) ->
                        args.size() == 3 && args.get(2) instanceof SqlExpr.StringLit lit
                                && lit.value().isEmpty()
                        ? args.get(0)
                        : new SqlExpr.Call(padFn, args));
            }
        }
        // ---- Map<U,V>: the DuckDB MAP carrier ----
        // pair(a,b) travels as STRUCT(first, second) — map_from_entries
        // takes exactly that shape.
        RULES.put(Pure.PAIR_KEY, (n, args) ->
                new SqlExpr.StructLit(List.of(
                        new SqlExpr.StructLit.Field("first", args.get(0)),
                        new SqlExpr.StructLit.Field("second", args.get(1)))));
        for (String f : Pure.nativeKeysAt("newMap")) {
            RULES.put(f, (n, args) -> mapFromPairs(n, args.get(0)));
        }
        for (String f : Pure.nativeKeysAt("put")) {
            // both operands cast to the RESOLVED map type — DuckDB's
            // map_concat rejects INTEGER-vs-BIGINT value mismatches
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_CONCAT,
                    castToMapType(n, args.get(0)),
                    castToMapType(n, SqlExpr.Call.of(SqlFn.MAP_FROM_LISTS,
                            new SqlExpr.ArrayLit(List.of(args.get(1))),
                            new SqlExpr.ArrayLit(List.of(args.get(2)))))));
        }
        for (String f : Pure.nativeKeysAt("putAll")) {
            RULES.put(f, (n, args) -> {
                boolean mapArg = PlatformTypes
                        .isMapCarrier(n.args().get(1).info().type());
                SqlExpr other = mapArg ? args.get(1) : mapFromPairs(n, args.get(1));
                return SqlExpr.Call.of(SqlFn.MAP_CONCAT,
                        castToMapType(n, args.get(0)), castToMapType(n, other));
            });
        }
        for (String f : Pure.nativeKeysAt("keys")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_KEYS, args.get(0)));
        }
        for (String f : Pure.nativeKeysAt("values")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_VALUES, args.get(0)));
        }
        // get: the MAP overload only — the bare-name set is shared with
        // variant get(v, key), whose rule is registered separately above.
        RULES.put(Pure.MAP_GET_KEY, (n, args) ->
                SqlExpr.Call.of(SqlFn.LIST_GET,
                        SqlExpr.Call.of(SqlFn.MAP_EXTRACT, args.get(0), args.get(1)),
                        new SqlExpr.IntLit(1)));
        ListRules.register(RULES);
        // DOMAIN guards RAISED IN SQL with real pure's messages (error()
        // runs in the database — literal AND runtime values alike).
        for (String f : Pure.nativeKeysAt("sqrt")) {
            RULES.put(f, (n, args) -> {
                SqlExpr x = new SqlExpr.Cast(args.get(0), SqlType.Scalar.DOUBLE);
                return guarded(
                        SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
                        cat(new SqlExpr.StringLit("Unable to compute sqrt of "), floatRepr(x)),
                        SqlExpr.Call.of(SqlFn.SQRT, args.get(0)));
            });
        }
        family(SqlFn.BIT_NOT, "bitNot");
        // formatDate(date, Strict/DateTimeFormat): the two real ISO forms.
        for (String f : Pure.nativeKeysAt("formatDate")) {
            RULES.put(f, (n, args) -> switch (enumName(n.args().get(1))) {
                case "ISO8601" -> SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                        new SqlExpr.FormatLit(com.legend.sql.DateFmt.DATE));
                // 9-digit nanos. A LITERAL prints its own WRITTEN subsecond
                // digits right-padded to 9 (static text — digits beyond the
                // TIMESTAMP carrier's 6 exist only in literals). A runtime
                // value holds at most 6 subsecond digits, so %f + '000' is
                // EXACT for everything the carrier can represent (audit:
                // the pad is faithful, not fabricated — but only because
                // the literal path takes the written digits first).
                case "ISO8601_NanoSecondPrecision" -> {
                    if (n.args().get(0) instanceof TypedCDate cd
                            && cd.value() instanceof
                                    PureDateLiteral.DateWithSubsecond sub
                            && sub.subsecond().length() > 6) {
                        String nanos = (sub.subsecond() + "000000000").substring(0, 9);
                        yield SqlExpr.Call.of(SqlFn.CONCAT,
                                SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                                        new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_DOT)),
                                new SqlExpr.StringLit(nanos));
                    }
                    yield SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                                    new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_MICRO)),
                            new SqlExpr.StringLit("000"));
                }
                default -> throw new IllegalStateException(
                        "unsupported date format " + enumName(n.args().get(1)));
            });
        }
        JsonLane.register(RULES);   // fromJson + meta::json: the variant lane
        ListEncodings.registerConcatenate(RULES);
        // tail/init of a TO-ONE value = EMPTY (all-but-first/-last of 1).
        for (String f : Pure.nativeKeysAt("tail")) {
            RULES.put(f, (n, args) -> args.get(0) instanceof SqlExpr.NullLit
                    || (isToOne(n.args().get(0)))
                    ? new SqlExpr.NullLit()
                    : new SqlExpr.Call(SqlFn.LIST_TAIL, args));
        }
        for (String f : Pure.nativeKeysAt("init")) {
            RULES.put(f, (n, args) -> args.get(0) instanceof SqlExpr.NullLit
                    || (isToOne(n.args().get(0)))
                    ? new SqlExpr.NullLit()
                    : new SqlExpr.Call(SqlFn.LIST_INIT, args));
        }
        // reverse(T[*]): the list reversed; a to-one value is its own reverse.
        for (String f : Pure.nativeKeysAt("reverse")) {
            // <=1 values reverse to themselves ([0..0] included).
            RULES.put(f, (n, args) -> Stamps.atMostOne(n.args().get(0))
                    ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_REVERSE, args));
        }
        // type(x): real pure returns THE Type instance ('Integer', not
        // DuckDB's 'INTEGER'). A CONCRETE static type is the runtime type —
        // emit its pure name; the wire resolves it to the canonical Type
        // instance (assertIs checks identity). Abstract statics (Number,
        // Date, Any) fall back to DuckDB's typeof — honest, still a name.
        for (String f : Pure.nativeKeysAt("type")) {
            RULES.put(f, (n, args) -> {
                Type t = n.args().get(0).info().type();
                String name = switch (t) {
                    case Type.Primitive p when p != Type.Primitive.NUMBER
                            && p != Type.Primitive.DATE ->
                            p.qualifiedName().substring(p.qualifiedName().lastIndexOf(':') + 1);
                    case Type.PrecisionDecimal ignored -> "Decimal";
                    case Type.ClassType ct -> ct.fqn();
                    case Type.EnumType et -> et.fqn();
                    case Type.GenericType g -> g.rawFqn();
                    default -> null;
                };
                return name != null ? new SqlExpr.StringLit(name)
                        : new SqlExpr.Call(SqlFn.TYPEOF, args);
            });
        }
        // minBy/maxBy(values, key[, count]): sort {k,v} structs by key (list
        // sort over structs orders by the FIRST field), take the head or the
        // top count, then unwrap the values.
        for (String name : List.of("minBy", "maxBy")) {
            boolean asc = name.equals("minBy");
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    if (args.size() < 2) {
                        throw new IllegalStateException(name
                                + " expects (values, key-function|keys[, count]) here");
                    }
                    // Pair BY INDEX for both forms so ties resolve to the
                    // FIRST occurrence (real pure): the middle sort field is
                    // the original position — negated under the descending
                    // sort so ties still come out first-occurrence.
                    SqlExpr range = SqlExpr.Call.of(SqlFn.RANGE_FN,
                            new SqlExpr.IntLit(1),
                            plusOne(SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                    args.get(0))));
                    SqlExpr i = SqlExpr.Column.param("_by_i", range);
                    SqlExpr valAt = SqlExpr.Call.of(SqlFn.LIST_GET, args.get(0), i);
                    SqlExpr keyExpr = args.get(1) instanceof SqlExpr.Lambda key
                            && key.params().size() == 1
                            ? substituteRef(key.body(), key.params().get(0), valAt)
                            : SqlExpr.Call.of(SqlFn.LIST_GET, args.get(1), i);
                    SqlExpr idxField = asc ? i
                            : SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(0), i);
                    SqlExpr pairs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, range,
                            new SqlExpr.Lambda(List.of("_by_i"),
                                    new SqlExpr.StructLit(List.of(
                                            new SqlExpr.StructLit.Field("k", keyExpr),
                                            new SqlExpr.StructLit.Field("i", idxField),
                                            new SqlExpr.StructLit.Field("v", valAt)))));
                    SqlExpr sorted = new SqlExpr.Call(
                            asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(pairs));
                    if (args.size() == 3) {
                        String e = "_by_e";
                        SqlExpr sliced = SqlExpr.Call.of(SqlFn.LIST_SLICE,
                                sorted, new SqlExpr.IntLit(1), args.get(2));
                        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, sliced,
                                new SqlExpr.Lambda(List.of(e),
                                        new SqlExpr.StructGet(
                                                SqlExpr.Column.param(e, sliced),
                                                "v")));
                    }
                    return new SqlExpr.StructGet(
                            SqlExpr.Call.of(SqlFn.LIST_GET, sorted, new SqlExpr.IntLit(1)), "v");
                });
            }
        }
        // removeDuplicatesBy(values, key): keep each key's FIRST occurrence —
        // an element survives iff the first position of its key is its own.
        for (String f : Pure.nativeKeysAt("removeDuplicatesBy")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(1) instanceof SqlExpr.Lambda key && key.params().size() == 1)) {
                    throw new IllegalStateException(
                            "removeDuplicatesBy expects (values, key-function)");
                }
                String x = key.params().get(0);
                SqlExpr keys = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        args.get(0), SqlExpr.Lambda.bind(key, args.get(0)));
                return SqlExpr.Call.of(SqlFn.LIST_FILTER, args.get(0),
                        new SqlExpr.Lambda(List.of(x, "_rd_i"),
                                SqlExpr.Call.of(SqlFn.EQUAL,
                                        SqlExpr.Call.of(SqlFn.LIST_POSITION, keys, key.body()),
                                        SqlExpr.Column.derived(null, "_rd_i"))));
            });
        }
        // corr/covarPopulation/covarSample over two LISTS: the paired-unnest
        // subquery recipe — (SELECT CORR(a, b) FROM (SELECT unnest(x) AS a,
        // unnest(y) AS b)); DuckDB zips parallel select-list unnests.
        for (var e : Map.of("corr", SqlAgg.Fn.CORR,
                "covarPopulation", SqlAgg.Fn.COVAR_POP,
                "covarSample", SqlAgg.Fn.COVAR_SAMP).entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    if (args.size() != 2) {
                        throw new IllegalStateException(e.getKey()
                                + " expects two value lists in scalar position");
                    }
                    // An EMPTY side has no pairs: the statistic is empty
                    // (NULL) — and unnest(NULL) can't correlate anyway.
                    if (args.get(0) instanceof SqlExpr.NullLit
                            || args.get(1) instanceof SqlExpr.NullLit) {
                        return new SqlExpr.NullLit();
                    }
                    // A TO-ONE side is the single-element list ([1] fits
                    // Number[*]) — unnest needs the list shape.
                    // deletion leg: the stamp decides the wrap (a
                    // many-stamped scalar or to-one list would have
                    // thrown at the funnel; Number sides carry no
                    // designed ArrayLit).
                    SqlExpr xs = Numerics.numList(n.args().get(0).info().multiplicity().isMany()
                            ? args.get(0) : new SqlExpr.ArrayLit(List.of(args.get(0))));
                    SqlExpr ys = Numerics.numList(n.args().get(1).info().multiplicity().isMany()
                            ? args.get(1) : new SqlExpr.ArrayLit(List.of(args.get(1))));
                    var inner = new SqlSelect(List.of(
                            new SqlSelect.Projection(
                                    SqlExpr.Call.of(SqlFn.UNNEST, xs), "a", null),
                            new SqlSelect.Projection(
                                    SqlExpr.Call.of(SqlFn.UNNEST, ys), "b", null)),
                            false, new com.legend.sql.SqlSource.Dual(), null,
                            List.of(), null, null, List.of(), null, null,
                            List.of());
                    var outer = new SqlSelect(List.of(
                            new SqlSelect.Projection(
                                    new SqlAgg.Reducer(e.getValue(),
                                            List.of(SqlExpr.Column.derived(null, "a"),
                                                    SqlExpr.Column.derived(null, "b")), false, java.util.List.of()),
                                    null, null)),
                            false, new SqlSource.Subselect(inner, "_uz", null),
                            null, List.of(), null, null, List.of(), null, null, List.of());
                    // MISMATCHED lengths would zip-pad with NULLs and the
                    // reducer would silently drop the unpaired tail (audit:
                    // corr([1,2,3],[2,4]) said 1.0) — unpaired data is LOUD.
                    // (the guard measures the WRAPPED sides — a to-one side
                    // is a 1-element list, never a bare scalar under len())
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, xs),
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, ys)),
                            SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(
                                    e.getKey() + ": the two value lists differ"
                                            + " in length")))),
                            new SqlExpr.ScalarSubquery(outer));
                });
            }
        }
        // find(coll, pred): the FIRST satisfying element, [0..1] — filter, then head.
        for (String f : Pure.nativeKeysAt("find")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_GET, List.of(
                    new SqlExpr.Call(SqlFn.LIST_FILTER, args), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("splitPart")) {
            RULES.put(f, (n, args) -> {
                // An EMPTY delimiter never splits: index 0 IS the whole
                // string (PCT; SQL split_part('', …) returns '' instead).
                if (args.get(1) instanceof SqlExpr.StringLit d && d.value().isEmpty()) {
                    return args.get(2) instanceof SqlExpr.IntLit i && i.value() == 0
                            ? args.get(0) : new SqlExpr.NullLit();
                }
                List<SqlExpr> shifted = new ArrayList<>(args);
                shifted.set(2, plusOne(args.get(2)));
                return new SqlExpr.Call(SqlFn.SPLIT_PART, shifted);
            });
        }
        // contains on a TO-ONE STRING: strpos > 0. A String[*] source is a
        // LIST of strings — list containment, not substring search (the
        // to-one gate; audit: ['x','y']->contains('x') hit strpos).
        for (String f : Pure.nativeKeysAt("uniqueValueOnly")) {
            RULES.put(f, (n, args) -> DateShifts.uniqueValueOnly(args));
        }
        for (String f : Pure.nativeKeysAt("contains")) {
            RULES.put(f, (n, args) -> {
                // COLLECTION-callee c1-LITERALS box (DEEP_AUDIT §3);
                // dispatch by the RESOLVED CALLEE's param mult (C1
                // indexOf pattern) — string::contains keeps the scalar.
                boolean collCallee = !(n.callee().parameters().get(0)
                        .multiplicity() instanceof Multiplicity.Bounded pb0
                        && pb0.upper() != null && pb0.upper() <= 1);
                if (collCallee) {
                    args = new java.util.ArrayList<>(args);
                    args.set(0, PureSql.asList(args.get(0),
                            !CollectionLanes.c1Literal(n.args().get(0))));
                }
                // contains(coll, val, comparator): filter by the comparator
                // against the needle, then non-empty. SQL lambdas are
                // positional and list_filter is 1-param — the needle
                // parameter closes over by SUBSTITUTION.
                if (args.size() == 3 && args.get(2) instanceof SqlExpr.Lambda comp
                        && comp.params().size() == 2) {
                    // pure contains.pure: exists(x | $comparator->eval($value, $x))
                    // — the FIRST comparator param binds the NEEDLE, the
                    // second each element (C1.5b; the reversed binding sent
                    // [1,2,3]->contains(5, {v,e|$v>$e}) the wrong way)
                    // carried collection: the needle enters SPELLED
                    // AND MARKED (MixedEncoding.markedNeedle — the
                    // comparator-form needle wrap; makes the param-0
                    // element stamp honest and the compare kind-true)
                    SqlExpr needle = MixedEncoding.markedNeedle(
                            n.args().get(1), args.get(1), args.get(0));
                    SqlExpr body = substituteRef(comp.body(), comp.params().get(0), needle);
                    return new SqlExpr.Call(SqlFn.GREATER, List.of(
                            SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                    SqlExpr.Call.of(SqlFn.LIST_FILTER, args.get(0),
                                            new SqlExpr.Lambda(
                                                    List.of(comp.params().get(1)), body))),
                            new SqlExpr.IntLit(0)));
                }
                // TO-ONE singleton-literal needle (['ISIN2']) unwraps
                if (args.get(1) instanceof SqlExpr.ArrayLit al
                        && al.elements().size() == 1
                        && isToOne(n.args().get(1))) {
                    args = List.of(args.get(0), al.elements().get(0));
                }
                Type elem = n.args().get(0).info().type();
                Type val = n.args().get(1).info().type();
                // a ^TDSNull()-TYPED needle (TDSNull is DATA): membership
                // of the null cell is an IS NULL scan — the ctor lowers
                // to the SQL NULL literal, and list containment of NULL
                // is never true under three-valued equality (resolver-bug
                // burn follow-on: the right-outer-join goldens assert
                // contains(^TDSNull()) over the null-fanned key column)
                if (val instanceof Type.ClassType nc0
                        && PlatformTypes.TDS_NULL_FQN.equals(nc0.fqn())) {
                    return CollectionLanes.nullMembership(args.get(0));
                }
                // the TDSNull sentinel (^TDSNull() travels as the literal
                // string 'TDSNull' on the wire) probes for a NULL cell —
                // against a NON-string element list the string could never
                // match anyway (today it dies as a cross-type comparison)
                if (args.get(1) instanceof SqlExpr.StringLit snl
                        && "TDSNull".equals(snl.value())
                        && elem != Type.Primitive.STRING) {
                    return CollectionLanes.nullMembership(args.get(0));
                }
                if (elem == Type.Primitive.STRING && isToOne(n.args().get(0))) {
                    // pure [0..1] overload body inlines HERE (engine
                    // stringExtension.pure:21 contains(String[0..1],
                    // String[1]) = isNotEmpty && contains) — same guard as
                    // startsWith/endsWith; STRPOS' accidental NULL>0 was
                    // filter-equivalent but NULL != false in value position
                    return NullSemantics.optionalOperandGuards(n, args,
                            new SqlExpr.Call(SqlFn.GREATER, List.of(
                                    new SqlExpr.Call(SqlFn.STRPOS, args),
                                    new SqlExpr.IntLit(0))));
                }
                // A heterogeneous (Any) list is variant-wrapped — wrap the
                // needle the same way so containment compares JSON to JSON.
                // This MUST precede the cross-kind rule: an Any list can
                // legitimately contain an instance (audit: class-in-mixed-list
                // containment was constant FALSE).
                if (PlatformTypes.isAny(elem)) {
                    // F10 slice 3b: against a LITERAL-carried collection
                    // the needle SPELLS (same grammar, byte-comparable);
                    // an unspellable needle (instance) is statically
                    // outside the six scalar kinds — pure equality
                    // instance-vs-primitive is FALSE
                    if (args.get(0) instanceof SqlExpr.Cast lm
                            && lm.target() instanceof SqlType.Array lma
                            && lma.element() == SqlType.Scalar.LITERAL) {
                        SqlExpr sn = MixedEncoding.elementLiteral(
                                n.args().get(1), args.get(1));
                        return sn == null ? new SqlExpr.BoolLit(false)
                                : new SqlExpr.Membership(sn, args.get(0));
                    }
                    return new SqlExpr.Membership(
                            SqlExpr.Call.of(SqlFn.TO_VARIANT, args.get(1)),
                            args.get(0));
                }
                // Pure equality never relates an instance to a primitive —
                // CONCRETE cross-kind containment is statically FALSE (SQL
                // list_contains would refuse to even type it).
                if (isClassish(elem) != isClassish(val)) {
                    return new SqlExpr.BoolLit(false);
                }
                // NULL-safe: containment in a NULL list (toMany over JSON
                // null) is pure's empty-collection FALSE, not SQL NULL.
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.Membership(args.get(1), args.get(0)),
                        new SqlExpr.BoolLit(false));
            });
        }
        // format('%s...', [args]) -> printf(fmt, args...): the array spreads.
        // Two directives printf cannot honor rewrite to %s over a literal
        // format string: %t{javaDatePattern} formats its date argument
        // (strftime, pattern converted), and bare %f is pure's MINIMAL float
        // repr, not printf's fixed six decimals.
        for (String f : Pure.nativeKeysAt("format")) {
            RULES.put(f, (n, args) -> {
                List<SqlExpr> spread = new ArrayList<>();
                spread.add(args.get(0));
                // typed elements ride along so %s can print CLASS-typed
                // values (Pair -> '<f, s>') by their STATIC type
                List<TypedSpec> typedElems =
                        n.args().get(1) instanceof TypedCollection tc
                                ? tc.elements() : List.of(n.args().get(1));
                // A LITERAL-carried argument list (M4 re-land): each
                // slot re-emits as its spelling->PRINT projection by
                // its STATIC kind (MixedEncoding.printedFormatSlots)
                SqlExpr argColl = MixedEncoding.printedFormatSlots(
                        args.get(1), typedElems);
                if (argColl instanceof SqlExpr.ArrayLit arr) {
                    // A MIXED argument list arrives variant-wrapped (its LUB
                    // is Any) — printf wants the raw values back, each
                    // substitution slot carries its own kind already.
                    for (int i = 0; i < arr.elements().size(); i++) {
                        SqlExpr e = MixedEncoding.unwrapVariant(
                                arr.elements().get(i));
                        Type et = i < typedElems.size()
                                ? typedElems.get(i).info().type() : null;
                        // class-typed slots pre-print via the pure toString
                        // (printf's %s would show the raw struct)
                        if (et != null
                                && (PlatformTypes.isPairCarrier(et)
                                        || PlatformTypes
                                                .isListCarrier(et))) {
                            e = pureToString(et, e);
                        }
                        spread.add(e);
                    }
                } else {
                    spread.add(argColl);
                }
                if (spread.get(0) instanceof SqlExpr.StringLit fmt) {
                    rewriteFormatDirectives(fmt.value(), spread, typedElems);
                }
                return new SqlExpr.Call(SqlFn.FORMAT, spread);
            });
        }
        // REAL pure hash(text, HashType.X): the enum value picks the digest
        // (the relational md5/sha dynafunctions translate here — the lite
        // md5/sha natives are gone).
        for (String f : Pure.nativeKeysAt("meta::pure::functions::hash::hash")) {
            RULES.put(f, (n, args) -> {
                if (!(n.args().get(1) instanceof TypedEnumValue ev)) {
                    throw new IllegalStateException("hash(text, hashType) needs a HashType literal");
                }
                SqlFn digest = switch (ev.value()) {
                    case "MD5" -> SqlFn.MD5;
                    case "SHA1" -> SqlFn.SHA1;
                    case "SHA256" -> SqlFn.SHA256;
                    default -> throw new IllegalStateException(
                            "unknown HashType." + ev.value());
                };
                return new SqlExpr.Call(digest, List.of(args.get(0)));
            });
        }

        // Parses and toString are CASTS (the Type rides the IR).
        // toString of a DATETIME prints Pure's ISO form
        // (2014-01-01T00:00:00.000+0000) — SQL's VARCHAR cast uses a space
        // separator and no offset. Other types keep the plain cast.
        for (String f : Pure.nativeKeysAt("toString")) {
            RULES.put(f, (n, args) -> {
                Type t = n.args().get(0).info().type();
                // A LATE-BOUND grid cell read (Phase 1c) is PHYSICAL —
                // present it as the map-binder scalar cell (the
                // single-cell collapse convention below)
                if (com.legend.compiler.spec.typed.TypedRawSqlRelation
                        .lateBoundCellRead(n.args().get(0)) != null) {
                    t = new Type.RelationType(java.util.List.of(
                            Type.RelationType.trustedColumn(
                                    com.legend.sql.SqlSelect.SYNTH_MAP_COL)));
                }
                // A DATE LITERAL's print form is fully static — subsecond
                // DIGIT COUNT is part of the value (%2014-01-01T00:00:00.00
                // prints '.00', which no timestamp carrier can retain).
                SqlExpr lit = dateLiteralPrint(n.args().get(0), t);
                if (lit != null) {
                    return lit;
                }
                if (t == Type.Primitive.DATE_TIME) {
                    return SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                            new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_PURE_UTC));
                }
                if (t == Type.Primitive.FLOAT) {
                    return floatRepr(args.get(0));
                }
                return pureToString(t, args.get(0));
            });
        }
        // isDistinct (DEEP_AUDIT §5k): 2-ARG = SQL IS DISTINCT FROM;
        // 1-ARG = the ALL_DISTINCT semantic node (a blanket family()
        // routed it into the binary SQL — AIOOBE on any input).
        for (String f : Pure.nativeKeysAt("isDistinct", 2)) {
            RULES.put(f, (n, args) ->
                    new SqlExpr.Call(SqlFn.IS_DISTINCT, args));
        }
        for (String f : Pure.nativeKeysAt("isDistinct", 1)) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    ? new SqlExpr.BoolLit(true)
                    : SqlExpr.Call.of(SqlFn.ALL_DISTINCT,
                            PureSql.asList(args.get(0),
                                    !CollectionLanes.c1Literal(
                                            n.args().get(0)))));
        }
        // parseInteger is 64-BIT (PCT pins Long.MIN/MAX round-trips) —
        // the SqlFn.PARSE_INT semantic entry lets each dialect spell it:
        // BIGINT cast in execution, the golden 'integer' in engine style
        // (the per-dynafunction origin tag audit 19 F3 called for).
        for (String f : Pure.nativeKeysAt("parseInteger")) {
            RULES.put(f, (n, args) ->
                    SqlExpr.Call.of(SqlFn.PARSE_INT, args.get(0)));
        }
        castFamily("parseFloat", Type.Primitive.FLOAT);
        castFamily("toFloat", Type.Primitive.FLOAT);
        // parseDecimal accepts the 'd'/'D' Pure-literal suffix ('3.14159d');
        // SQL DECIMAL casts do not — strip it (literal-folded or RTRIM).
        // Real pure is new BigDecimal(s): the SCALE comes from the string
        // ('0.0' is 0.0D, never 0.000000000000000000D). A literal's scale is
        // static program text — the cast targets DECIMAL(38, that scale).
        // The 3-arg overload is setScale(scale, HALF_UP) with a precision
        // bound: DuckDB's string→DECIMAL(p,s) cast rounds half away from
        // zero and raises on overflow, both matching.
        for (String f : Pure.nativeKeysAt("parseDecimal")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 3) {
                    if (!(args.get(1) instanceof SqlExpr.IntLit p
                            && args.get(2) instanceof SqlExpr.IntLit s)) {
                        throw new IllegalStateException(
                                "parseDecimal precision/scale must be literal integers");
                    }
                    SqlExpr in = args.get(0) instanceof SqlExpr.StringLit lit
                            ? new SqlExpr.StringLit(lit.value().replaceAll("[dD]$", ""))
                            : SqlExpr.Call.of(SqlFn.RTRIM, args.get(0),
                                    new SqlExpr.StringLit("dD"));
                    return new SqlExpr.Cast(in, new SqlType.Decimal(
                            (int) p.value(), (int) s.value()));
                }
                if (args.get(0) instanceof SqlExpr.StringLit lit) {
                    String clean = lit.value().replaceAll("[dD]$", "");
                    return new SqlExpr.Cast(new SqlExpr.StringLit(clean),
                            new SqlType.Decimal(38, literalScale(clean)));
                }
                // NON-LITERAL (column) argument: the scale of the string
                // is runtime data SQL cannot carry — the engine's OWN
                // 1-arg contract is a hardcoded decimal(5, 2)
                // (h2Extension2_1_214.pure transformParseDecimalH2), and
                // its goldens round through it (123.450021 -> 123.45).
                return new SqlExpr.Cast(
                        SqlExpr.Call.of(SqlFn.RTRIM, args.get(0), new SqlExpr.StringLit("dD")),
                        new SqlType.Decimal(5, 2));
            });
        }
        castFamily("parseBoolean", Type.Primitive.BOOLEAN);
        // parseDate accepts PARTIAL-time text ('2015-04-15T17') — pad the
        // literal to a full timestamp shape (SQL's cast demands one).
        for (String f : Pure.nativeKeysAt("parseDate")) {
            RULES.put(f, (n, args) -> {
                SqlExpr in = args.get(0);
                if (in instanceof SqlExpr.StringLit lit) {
                    // A ZONE-carrying input keeps its INSTANT, normalized
                    // to naive UTC — the platform's ONE temporal carrier
                    // (PureDateLiteral mirrors the engine: timezones
                    // normalise to GMT at parse, offset discarded; a
                    // TIMESTAMPTZ egress here leaked OffsetDateTime cells
                    // the verdict channel then had to absorb — the
                    // compensation smell, 2026-08-20).
                    if (lit.value().matches(".*([+-]\\d{4}|[+-]\\d{2}:\\d{2}|Z)$")) {
                        return SqlExpr.Call.of(SqlFn.TIMEZONE,
                                new SqlExpr.StringLit("UTC"),
                                new SqlExpr.Cast(in,
                                        SqlType.Scalar.TIMESTAMPTZ));
                    }
                    String v = lit.value().replace('T', ' ');
                    if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}")) {
                        v += ":00:00";
                    } else if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}")) {
                        v += ":00";
                    }
                    in = new SqlExpr.StringLit(v);
                }
                // Conform-by-emission (slice-4 J8a): the Typer refines a
                // bare-date literal to StrictDate (refineParseDate); the
                // emission speaks that SAME fact — SQL DATE, day-precise
                // on the wire. Casting everything to TIMESTAMP delivered a
                // midnight DateTime under a StrictDate stamp, which only
                // the PCT adapter's narrowing arm absorbed (now deleted).
                Type rt = n.info().type();
                return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.PARSE_DATE, in), PureSql.type(
                        rt == Type.Primitive.STRICT_DATE
                                ? Type.Primitive.STRICT_DATE
                                : Type.Primitive.DATE_TIME));
            });
        }
        // date(y,m,d[,h,mi,s]) constructors.
        // date(y[,m[,d[,h[,mi[,s]]]]]): every arity SHORT of seconds is a
        // PARTIAL date — the ISO-prefix string carrier at that precision
        // (real pure prints date(1973,11,13,23) as 1973-11-13T23). Only the
        // full six-part form is a real timestamp; three parts is make_date.
        // FORMAT dynafunctions: strptime with engine tokens translated to
        // C-style (the format must be a LITERAL — mapping expressions always
        // spell it inline; anything else is loud)
        for (String f : Pure.nativeKeysAt(Pure.Lite.PARSE_DATE_FORMAT)) {
            RULES.put(f, (n, args) -> strptimeOf(args, false));
        }
        for (String f : Pure.nativeKeysAt(Pure.Lite.CONVERT_DATE_TIME_FORMAT)) {
            RULES.put(f, (n, args) -> strptimeOf(args, false));
        }
        for (String f : Pure.nativeKeysAt(Pure.Lite.CONVERT_DATE_FORMAT)) {
            RULES.put(f, (n, args) -> strptimeOf(args, true));
        }
        // isNumeric(str): PINNED to the engine's H2 emission
        // lower(x) = upper(x) — true iff the text has no cased letters
        // (h2Extension2_1_214.pure:230). Semantically loose ('', '$5' and
        // '1.2.3' are all "numeric") but it is what generated every corpus
        // expectation; a tighter regex silently diverges on those inputs.
        for (String f : Pure.nativeKeysAt(Pure.Lite.IS_NUMERIC)) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.EQUAL,
                    SqlExpr.Call.of(SqlFn.LOWER, args.get(0)),
                    SqlExpr.Call.of(SqlFn.UPPER, args.get(0))));
        }
        // convertTimeZone(dt, tz, fmt): the input is UTC, printed in the
        // target zone (engine H2 UDF: utcTime.withZoneSameInstant(target)).
        // DuckDB's timezone(tz, naive_ts) goes the OTHER way (interprets
        // the naive value as tz-local), so pin the instant first:
        // timezone('UTC', dt) tags the naive value AS UTC, then
        // timezone(tz, ...) renders that instant in the target zone.
        for (String f : Pure.nativeKeysAt(Pure.Lite.CONVERT_TIME_ZONE_FORMAT)) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(2) instanceof SqlExpr.StringLit fmt)) {
                    throw new NotImplementedException(
                            "convertTimeZone needs a LITERAL format string");
                }
                SqlExpr asUtc = new SqlExpr.Call(SqlFn.TIMEZONE,
                        List.of(new SqlExpr.StringLit("UTC"), args.get(0)));
                SqlExpr shifted = new SqlExpr.Call(SqlFn.TIMEZONE,
                        List.of(args.get(1), asUtc));
                return new SqlExpr.Call(SqlFn.STRFTIME, List.of(shifted,
                        new SqlExpr.FormatLit(DateFormats.pureToParts(fmt.value()))));
            });
        }
        // sqlNull() — the relational store's NULL literal dynafunction
        for (String f : Pure.nativeKeysAt("sqlNull")) {
            RULES.put(f, (n, args) -> new SqlExpr.NullLit());
        }
        for (String f : Pure.nativeKeysAt("date")) {
            // component-validated date/timestamp construction —
            // DateCtorRule owns the spelling (file guardrail split)
            RULES.put(f, DateCtorRule::lower);
        }

        // Overload-specific overrides — the resolved signature IS the decision.
        RULES.put(Pure.keyPlusString(), (n, args) -> new SqlExpr.Call(SqlFn.CONCAT, args));
        // real pure declares BOTH in(Any[1], ...) and in(Any[0..1], ...):
        // the optional-needle overload is FALSE for the empty needle
        // (COALESCE — a NULL needle must never say NULL).
        RULES.put(Pure.keyInOptional(), (n, args) ->
                args.get(0) instanceof SqlExpr.NullLit
                        ? new SqlExpr.BoolLit(false)
                        : SqlExpr.Call.of(SqlFn.COALESCE,
                                java.util.Objects.requireNonNull(
                                        RULES.get(Pure.keyIn()),
                                        "in-rule must be registered")
                                        .apply(n, args),
                                new SqlExpr.BoolLit(false)));
        RULES.put(Pure.keyIn(), (n, args) -> {
            // TYPE-aware membership: a kind-mismatched needle is never a
            // member — static FALSE, not a DB error.
            if (MixedEncoding.kindMismatch(n.args().get(0).info().type(),
                    n.args().get(1).info().type())) {
                return new SqlExpr.BoolLit(false);
            }
            // in(x, []) is FALSE in pure; empty lowers to NULL and
            // `x IN (NULL)` would drop rows under negation (audit).
            if (args.get(1) instanceof SqlExpr.NullLit) {
                return new SqlExpr.BoolLit(false);
            }
            // Any-collection variant wrap decided by LOWERED shape
            // (ListShapes rule): plain-lowered literal lists compare plain.
            boolean collVariant = PlatformTypes.isAny(
                    n.args().get(1).info().type())
                    && !(args.get(1) instanceof SqlExpr.ArrayLit al
                            && al.elements().stream().noneMatch(
                                    MixedEncoding::variantWrapped))
                    // a SCALAR-STAMPED RHS compares PLAIN — one element,
                    // no variant harmonization (to_json(needle) IN
                    // ('John') did not bind). Stamp-read (burn-to-zero;
                    // was wrap-by-proof).
                    && !isToOne(n.args().get(1));
            SqlExpr raw = CastPolicy.comparisonWireOperand(n.args().get(0), args.get(0),
                    n.args().get(1));
            // F10 slice 3b: a LITERAL-carried collection compares in
            // the spelling grammar — the needle spells by its static
            // kind (an unspellable needle is statically not a member)
            if (args.get(1) instanceof SqlExpr.Cast inLm
                    && inLm.target() instanceof SqlType.Array inLa
                    && inLa.element() == SqlType.Scalar.LITERAL) {
                SqlExpr sn = MixedEncoding.elementLiteral(
                        n.args().get(0), raw);
                if (sn == null) {
                    return new SqlExpr.BoolLit(false);
                }
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.Membership(sn, args.get(1)),
                        new SqlExpr.BoolLit(false));
            }
            SqlExpr needle = collVariant
                    ? SqlExpr.Call.of(SqlFn.TO_VARIANT, raw) : raw;
            // A RELATION-shaped collection = LIST-aggregated subquery;
            // membership is list containment (NULL list = empty = FALSE).
            if (Type.relationValued(n.args().get(1).info())) {
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.Membership(needle, args.get(1)),
                        new SqlExpr.BoolLit(false));
            }
            // a COLLECTION-VALUED expression RHS (split(...) etc.) is
            // MEMBERSHIP, never a 2-element literal list — the flat IN
            // collapsed to '=' downstream (ledger cluster 35: silent
            // wrong rows, 'LEGALNAME = string_split(...)')
            if (!(args.get(1) instanceof SqlExpr.ArrayLit)
                    && !(args.get(1) instanceof SqlExpr.PlanParam)
                    && n.args().get(1).info().multiplicity().isMany()) {
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.Membership(needle, args.get(1)),
                        new SqlExpr.BoolLit(false));
            }
            List<SqlExpr> flat = new ArrayList<>();
            flat.add(needle);
            if (args.get(1) instanceof SqlExpr.ArrayLit arr) {
                flat.addAll(arr.elements());
            } else {
                flat.add(args.get(1));
            }
            // pure in() is TOTAL (Boolean[1] — false over an empty needle,
            // never empty): coalesce the SQL three-valued NULL to false so
            // a [0..1] read's in() projects pure's false, not TDSNull.
            // (not(in) stays correct: not(coalesce(NULL->false)) = true =
            // the engine's processNotIn 'OR IS NULL' outcome.)
            return SqlExpr.Call.of(SqlFn.COALESCE,
                    new SqlExpr.Call(SqlFn.IN, flat),
                    new SqlExpr.BoolLit(false));
        });
    }

    /**
     * Names known to be ABSENT from our catalog (engine-only or not yet
     * signed). Anything else missing at registration is a TYPO and dies.
     */
    private static final Set<String> KNOWN_ABSENT = Set.of(
            "cbrt", "log10", "atan2", "sinh", "cosh", "tanh", "ascii", "char",
            "encodeBase64", "levenshteinDistance", "generateGuid", "hashCode",
            "toUpperFirstCharacter", "toLowerFirstCharacter", "matches",
            "lpad", "rpad", "ltrim", "rtrim", "reverseString", "splitPart",
            "left", "right", "mode", "median", "mean", "datePart", "today",
            "now", "hash", "zip", "toVariant", "split", "xor",
            "bitAnd", "bitOr", "bitXor", "bitShiftLeft", "bitShiftRight");

    private static void familyIfPresent(SqlFn semantic, String pureName) {
        if (!Pure.nativeFunctionsAt(pureName).isEmpty()) {
            family(semantic, pureName);
        } else if (!KNOWN_ABSENT.contains(pureName)) {
            throw new IllegalStateException("registration typo: no catalog overloads for '"
                    + pureName + "' and it is not in KNOWN_ABSENT");
        }
    }

    private static void castFamily(String pureName, Type target) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            RULES.put(f, (n, args) -> new SqlExpr.Cast(args.get(0), PureSql.type(target)));
        }
    }

    private static SqlExpr strptimeOf(List<SqlExpr> args, boolean toDate) {
        if (!(args.get(1) instanceof SqlExpr.StringLit fmt)) {
            throw new NotImplementedException(
                    "format dynafunctions need a LITERAL format string");
        }
        SqlExpr parsed = new SqlExpr.Call(SqlFn.STRPTIME,
                List.of(args.get(0), new SqlExpr.FormatLit(DateFormats.pureToParts(fmt.value()))));
        return toDate
                ? new SqlExpr.Cast(parsed, PureSql.type(
                        Type.Primitive.STRICT_DATE))
                : parsed;
    }

    /** {@code i + 1} — constant-folded for literals (the common case). */

    private static SqlExpr plusOne(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(i.value() + 1)
                : new SqlExpr.Call(SqlFn.PLUS, List.of(e, new SqlExpr.IntLit(1)));
    }

    /** {@code instanceOf} with a STATICALLY-DECIDED answer folds to a
     * literal (the corpus's loadAndTestExecution tail guards a TDS read
     * with {@code instanceOf(TabularDataSet)} — the frame's static type
     * already decides it). A dynamically-undecidable check stays a loud
     * wall — never a guessed boolean. */
    static SqlExpr instanceOfFold(TypedNativeCall n) {
        // the type argument: @Type annotation (TypedTypeRef) or a bare
        // class reference in value position (TypedPackageableRef)
        String target = switch (n.args().get(1)) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr ->
                    tr.target() instanceof Type.ClassType c ? c.fqn() : null;
            case com.legend.compiler.spec.typed.TypedPackageableRef pr ->
                    pr.fullPath();
            default -> throw new NotImplementedException(
                    "instanceOf with a non-literal type argument ("
                    + n.args().get(1).getClass().getSimpleName() + ")");
        };
        Type actual = n.args().get(0).info().type();
        boolean sure = target != null
                && (actual instanceof Type.ClassType a && a.fqn().equals(target)
                    || com.legend.compiler.element.type.PlatformTypes
                            .TABULAR_DATA_SET.equals(target)
                       && Type.isRelation(actual));
        if (!sure) {
            throw new NotImplementedException(
                    "instanceOf undecidable statically: " + actual
                    + " vs '" + target + "'");
        }
        return new SqlExpr.BoolLit(true);
    }

    /** The lowering for {@code call}'s resolved overload; loud error when unregistered. */
    static SqlExpr lower(TypedNativeCall call, List<SqlExpr> loweredArgs) {
        Rule rule = RULES.get(call.callee().signatureKey());
        if (rule == null) {
            // A REDUCER reaching scalar rules always means WRONG CONTEXT —
            // the aggregation machinery owns it (projection sub-agg
            // synthesis, group-by). Signal the TRIAL boundary
            // (Resolution.attempt) so its fallback claims the column;
            // pre-C1 the trial failed earlier by accident (an
            // UnfoldableRef from the boxed literal), C1's honest
            // singletons let the trial reach the reducer — the contract
            // belongs here, not to the accident (witness
            // testSubAggregationWithDeepAndOverlap).
            if (Aggregates.reducerOrNull(call.callee()) != null) {
                throw new Resolvers.UnfoldableRef("aggregate '"
                        + call.callee().qualifiedName()
                        + "' in scalar position (aggregation machinery owns it)");
            }
            throw new IllegalStateException("no scalar lowering registered for resolved overload '"
                    + call.callee().qualifiedName() + "' with " + call.callee().parameters().size()
                    + " parameter(s)");
        }
        return rule.apply(call, loweredArgs);
    }

    /**
     * Integer arithmetic NEAR THE INT64 EDGE computes in HUGEINT (real
     * pure's 2 * maxLong PCT value): a literal within a factor of ~2 of
     * overflow widens the first operand, and DuckDB propagates.
     */
    private static List<SqlExpr> hugeWiden(List<SqlExpr> args) {
        // Widen the near-edge INTEGER LITERAL itself — never a float
        // operand (CAST(2.5 AS HUGEINT) rounds to 3 and poisons the
        // product; audit). DuckDB propagates HUGEINT from either side.
        List<SqlExpr> out = null;
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof SqlExpr.IntLit lit
                    && (lit.value() > (Long.MAX_VALUE >> 2)
                            || lit.value() < (Long.MIN_VALUE >> 2))) {
                if (out == null) {
                    out = new ArrayList<>(args);
                }
                out.set(i, new SqlExpr.Cast(lit, SqlType.Scalar.HUGEINT));
            }
        }
        return out == null ? args : out;
    }

    /**
     * Whether a typed argument lowers to a SQL LIST value: an upper bound
     * beyond one. Relation columns are at most [0..1], so to-many here means
     * a collection expression (toMany(@T), literal lists, split, ...).
     */
    private static boolean listValued(TypedSpec arg) {
        return arg.info().multiplicity().isMany();
    }




    /** {@code ', '}-joined string list ('' for empty) — composed in SQL. */
    private static SqlExpr joinList(SqlExpr strings) {
        return SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG, strings,
                        List.of(new SqlExpr.StringLit(", "))),
                new SqlExpr.StringLit(""));
    }

    /**
     * The pure PRINT of a value by its STATIC type, composed IN SQL —
     * Pair prints {@code '<first, second>'} (real anonymousCollections
     * toString), recursively; everything else is the VARCHAR cast.
     */
    /** Package-visible for the RENDER phase (F4.2): the CSV cell rule is
     *  {@code toString()->escapeCSVString()} — one print form, one owner. */
    static SqlExpr pureToString(Type t, SqlExpr x) {
        if (t == Type.Primitive.FLOAT) {
            return floatRepr(x);
        }
        if (t instanceof Type.ClassType ac
                && PlatformTypes.isAny(ac)) {
            // an ANY slot is variant-carried: root TEXT extraction strips
            // the JSON quoting ('b', not '"b"'); a variant-carried LIST
            // (a nested ^List under Any) prints pure's '[a, b]', its
            // ELEMENTS as root text — composed in SQL from the JSON array
            // F10 slice-3 AUDIT (2026-08-24): the spelling-print path
            // gates on the LITERAL wire the TREE carries (M3 flip:
            // .type() read) — first-char dispatch on arbitrary Any
            // wires was SNIFFING (a raw VARCHAR cell whose text starts
            // with ' or % would mis-print); the engine's rule is
            // declared-type-decides. Non-LITERAL wires keep the
            // pre-carrier variant path unchanged.
            boolean literalWire = x.type()
                    instanceof
                            com.legend.sql.TypeFact.Typed jt
                    && (jt.type() == SqlType.Scalar.LITERAL
                            || (jt.type() instanceof SqlType.Array ja
                                    && ja.element()
                                            == SqlType.Scalar.LITERAL));
            if (literalWire) {
                // ONE recipe, one owner (M4 re-land): the spelling->
                // PRINT projection moved to LiteralSpelling.printForm,
                // byte-identical to the inline block it replaces
                return literalPrint(x);
            }
            return new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL,
                            SqlExpr.Call.of(SqlFn.JSON_TYPE, x),
                            new SqlExpr.StringLit("ARRAY")),
                    cat(new SqlExpr.StringLit("["),
                            joinList(new SqlExpr.Cast(x, new SqlType.Array(
                                    PureSql.type(Type.Primitive.STRING)))),
                            new SqlExpr.StringLit("]")))),
                    new SqlExpr.Cast(
                            SqlExpr.Call.of(SqlFn.VARIANT_GET, x, new SqlExpr.StringLit("$")),
                            PureSql.type(Type.Primitive.STRING)));
        }
        if (PlatformTypes.isListCarrier(t)) {
            // real anonymousCollections List.toString(): '[v1, v2, ...]'
            Type et = t instanceof Type.GenericType g && !g.arguments().isEmpty()
                    ? g.arguments().get(0)
                    : new Type.ClassType(PlatformTypes.ANY);
            SqlExpr elem = SqlExpr.Column.param("_ts", x);
            return cat(new SqlExpr.StringLit("["),
                    joinList(SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, x,
                            new SqlExpr.Lambda(List.of("_ts"), pureToString(et, elem)))),
                    new SqlExpr.StringLit("]"));
        }
        if (PlatformTypes.isPairCarrier(t)) {
            Type ft = ((Type.GenericType) t).arguments().get(0);
            Type st = ((Type.GenericType) t).arguments().get(1);
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("<"),
                                    pureToString(ft, new SqlExpr.StructGet(x, "first"))),
                            new SqlExpr.StringLit(", ")),
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            pureToString(st, new SqlExpr.StructGet(x, "second")),
                            new SqlExpr.StringLit(">")));
        }
        // a SINGLE-scalar-column relation in scalar position IS its cell
        // (the scalar-subquery collapse) — the cast is that cell's
        // toString and stays; anything wider is fabrication
        boolean scalarCell = Type.schemaView(t) instanceof Type.RelationType rt
                && rt.columns().size() == 1
                && rt.dynamicColumns().isEmpty()
                && (rt.columns().get(0).type() instanceof Type.Primitive
                        // the map-binder value column over a LATE-BOUND
                        // grid cell is Any-typed (Phase 1c) — still ONE
                        // cell, the same collapse
                        || (rt.columns().get(0).name().startsWith(
                                com.legend.sql.SqlSelect.SYNTH_MAP_COL)
                            && rt.columns().get(0).type()
                                    instanceof Type.ClassType ac
                            && PlatformTypes.isAny(ac)));
        // Nil (the []-born bottom) has no inhabitants: the value is
        // provably EMPTY — SQL NULL, cast for the string context
        if (t instanceof Type.ClassType nil0 && PlatformTypes.isNil(nil0)) {
            return new SqlExpr.Cast(new SqlExpr.NullLit(),
                    com.legend.sql.SqlType.Scalar.VARCHAR);
        }
        // Variant.toString IS its CANONICAL JSON text — compact, source
        // whitespace normalized away, leaf quoting PRESERVED (witness
        // testVariantColumn_keyExtraction: the engine prints {"a":1},
        // never the source's {"a": 1}; a leaf string stays '"hello"').
        // to_json over the JSON-cast value is the canonicalizer — the
        // '$'-extract strips leaf-string quotes, the plain VARCHAR cast
        // keeps source spacing.
        if (t instanceof Type.ClassType vc && PlatformTypes.isVariant(vc)) {
            return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.TO_VARIANT,
                    new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.JSON)),
                    com.legend.sql.SqlType.Scalar.VARCHAR);
        }
        // Default INSTANCE print (leg 6a, testPersonToString): a user
        // class with no toString() qualifier (the Typer's derivedShadow
        // dispatched any declared one before this arm can see the call)
        // prints its REPOSITORY ID — interpreted pure's anonymous-
        // instance name; the accepted spellings are Anonymous_*/@_*,
        // and our minted F13 site id rides the @_ form. The id is the
        // struct's own __id field, read IN SQL — no Java formatting.
        // ClassType ONLY — a GenericType here is a metamodel CARRIER
        // (Class<T>/Enumeration<T> element refs print their NAME via
        // their own arm), never an instance struct
        if (t instanceof Type.ClassType && Type.schemaView(t) == null
                && InstanceEquality.userClass(t)) {
            return cat(new SqlExpr.StringLit("@_"),
                    new SqlExpr.StructGet(x,
                            com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID));
        }
        if ((Type.schemaView(t) != null && !scalarCell)
                || com.legend.compiler.element.type.PlatformTypes
                        .functionTypeOf(t) != null
                || t instanceof Type.SchemaAlgebra
                || (t instanceof Type.ClassType tc
                        && !PlatformTypes.isAny(tc))) {
            // engine parity: toString(any:Any[1]) TYPES over a relation or
            // instance, but a blanket VARCHAR cast fabricates output the
            // engine never produces — loud until modeled (TENET #10.1)
            throw new com.legend.error.NotImplementedException(
                    "toString over " + t + " is not modeled");
        }
        // boolean text is a SEMANTIC node (P7): the reference prints
        // 'true'/'false', H2's VARCHAR cast prints 'TRUE' — the arg
        // type is known HERE, the spelling is each dialect's.
        if (t == Type.Primitive.BOOLEAN) {
            return SqlExpr.Call.of(SqlFn.BOOL_TO_TEXT, x);
        }
        return new SqlExpr.Cast(x, PureSql.type(Type.Primitive.STRING));
    }

    /** datePrecision, or null where the abstract Date makes it undecidable. */
    private static PureDateLiteral.@com.legend.Nullable Precision datePrecisionOrUnknown(TypedSpec arg) {
        try {
            return datePrecision(arg);
        } catch (IllegalStateException undecidable) {
            return null;
        }
    }

    /** Real pure computes in BigDecimal the moment ONE arithmetic
     * operand is Decimal — the whole SQL expression must stay DECIMAL
     * (one CAST AS DOUBLE poisons DuckDB's resolution and loses the
     * scale surface, 6.0D not 6.000000000000000000D). With a decimal
     * operand present, FLOAT LITERALS join as native DECIMAL literals
     * at their printed scale (BigDecimal.valueOf repr); DuckDB's DECIMAL
     * arithmetic then reproduces BigDecimal's scale rules. Only literals
     * transform: a runtime DOUBLE stays DOUBLE. */
    /** new BigDecimal(s)'s scale: digits after the point (exponent forms fall back to 18). */
    private static int literalScale(String s) {
        if (s.indexOf('e') >= 0 || s.indexOf('E') >= 0) {
            return 18;
        }
        int dot = s.indexOf('.');
        return dot < 0 ? 0 : s.length() - dot - 1;
    }

    private static List<SqlExpr> decimalJoin(List<SqlExpr> args) {
        return args.stream().anyMatch(Scalars::decimalKind)
                ? args.stream().map(Scalars::undoubled).toList()
                : args;
    }

    private static boolean decimalKind(SqlExpr e) {
        return switch (e) {
            case SqlExpr.DecimalLit ignored -> true;
            case SqlExpr.ArrayLit a -> a.elements().stream().anyMatch(Scalars::decimalKind);
            case SqlExpr.Cast c -> c.target() instanceof SqlType.Decimal
                    || decimalKind(c.value());
            // a chain like 1.0D - 2 - 3.0 nests the decimal inside the
            // first subtraction — the detector looks through calls/cases
            case SqlExpr.Call c -> c.args().stream().anyMatch(Scalars::decimalKind);
            case SqlExpr.Case c -> c.otherwise() != null && decimalKind(c.otherwise());
            default -> false;
        };
    }

    private static SqlExpr undoubled(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Cast c when c.value() instanceof SqlExpr.FloatLit f
                    && c.target() == SqlType.Scalar.DOUBLE ->
                    new SqlExpr.DecimalLit(java.math.BigDecimal.valueOf(f.value()));
            case SqlExpr.FloatLit f ->
                    new SqlExpr.DecimalLit(java.math.BigDecimal.valueOf(f.value()));
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(
                    a.elements().stream().map(Scalars::undoubled).toList());
            default -> e.mapChildren(Scalars::undoubled);
        };
    }

    /** A typed-list conformance cast peels off (PureSql.typedList wraps
     * the literal carrier in CAST(x AS T[]) — shape-transparent). */
    static SqlExpr unwrapArrayCast(SqlExpr e) {
        return e instanceof SqlExpr.Cast c
                && c.target() instanceof SqlType.Array ? c.value() : e;
    }

    /** {@code CASE WHEN cond THEN error(msg) ELSE value END} — a DATABASE-raised guard. */
    static SqlExpr guarded(SqlExpr cond, SqlExpr msg, SqlExpr value) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(cond,
                SqlExpr.Call.of(SqlFn.ERROR, msg))), value);
    }

    /** The provenance-carrying guard: the raising call's source span rides
     * the raise ({@link PureSql#raise}) — assertError's position channel. */
    static SqlExpr guarded(SqlExpr cond, SqlExpr msg, SqlExpr value,
            com.legend.protocol.@com.legend.Nullable SourceInfo pos) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(cond,
                PureSql.raise(msg, pos))), value);
    }

    static SqlExpr str(SqlExpr x) {
        return new SqlExpr.Cast(x, PureSql.type(Type.Primitive.STRING));
    }

    static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    /** Cast a map operand to the call's RESOLVED Map(K, V) SQL type. */
    private static SqlExpr castToMapType(TypedNativeCall n,
                                         SqlExpr m) {
        return n.info().type() instanceof Type.GenericType g && g.arguments().size() == 2
                ? new SqlExpr.Cast(m, new SqlType.Map(
                        PureSql.type(g.arguments().get(0)),
                        PureSql.type(g.arguments().get(1))))
                : m;
    }

    /**
     * A PAIR COLLECTION as a MAP value: map_from_entries over the lowered
     * STRUCT(first, second) list; the statically-EMPTY collection is the
     * typed empty map (CAST(MAP {{}} AS MAP(K, V)) from the resolved output).
     */
    private static SqlExpr mapFromPairs(TypedNativeCall n,
                                        SqlExpr pairs) {
        // a SINGLE pair ([1] fits Pair[*]) wraps into the entry list
        if (pairs instanceof SqlExpr.StructLit) {
            pairs = new SqlExpr.ArrayLit(List.of(pairs));
        }
        if (pairs instanceof SqlExpr.NullLit) {
            Type out = n.info().type();
            if (out instanceof Type.GenericType g && g.arguments().size() == 2) {
                return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.MAP_EMPTY),
                        new SqlType.Map(
                                PureSql.type(g.arguments().get(0)),
                                PureSql.type(g.arguments().get(1))));
            }
            return SqlExpr.Call.of(SqlFn.MAP_EMPTY);
        }
        return SqlExpr.Call.of(SqlFn.MAP_FROM_ENTRIES, pairs);
    }


    /** Literal cell of a TDS row → typed SQL literal, by the column's Pure type. */
    /** pure DECIMAL-suffix cells (21d) carry the marker in the TEXT. */
    private static String stripDecimalSuffix(String cell) {
        return cell.matches("[+-]?\\d+(\\.\\d+)?[dD]")
                ? cell.substring(0, cell.length() - 1) : cell;
    }

    static SqlExpr tdsCell(String cell, Type type) {
        if (cell == null || cell.isEmpty()
                || cell.equals(PlatformTypes.TDS_NULL_CELL)
                || (cell.equals("null") && !PlatformTypes.isVariant(type))) {
            // A bare 'null' cell is SQL NULL for EVERY non-variant type —
            // String included (a 'null' name must vanish from joinStrings
            // window collections, pure's empty semantics). A VARIANT 'null'
            // is the JSON null VALUE (variant arm below). 'TDSNull' is real
            // pure's TDS null-cell INSTANCE (^TDSNull) — SQL NULL for every
            // type; it is never a string payload in a TDS literal.
            return new SqlExpr.NullLit();
        }
        if (type == Type.Primitive.INTEGER) {
            return new SqlExpr.IntLit(Long.parseLong(cell));
        }
        if (type == Type.Primitive.FLOAT || type == Type.Primitive.NUMBER) {
            // a FLOAT-declared cell seeds a DOUBLE literal (§4bZ-V C
            // adjudication: DecimalLit here made DuckDB type the whole
            // Values column DECIMAL(p,s) under the DOUBLE label — the
            // star-covered head-column diverge family; the declared
            // contract owns the seed)
            return new SqlExpr.FloatLit(
                    Double.parseDouble(stripDecimalSuffix(cell)));
        }
        if (type == Type.Primitive.DECIMAL || type instanceof Type.PrecisionDecimal) {
            return new SqlExpr.DecimalLit(
                    new java.math.BigDecimal(stripDecimalSuffix(cell)));
        }
        if (type == Type.Primitive.BOOLEAN) {
            return new SqlExpr.BoolLit(Boolean.parseBoolean(cell));
        }
        if (type == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.DateLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        if (type == Type.Primitive.DATE_TIME || type == Type.Primitive.DATE) {
            String v = cell.startsWith("%") ? cell.substring(1) : cell;
            // Normalize the PCT fixture spelling: a +0000/Z suffix drops
            // (values are UTC) and sub-second digits truncate to DuckDB's
            // microsecond precision.
            v = v.replaceFirst("(\\+0000|Z)$", "");
            Matcher frac = Pattern
                    .compile("\\.(\\d{7,9})$").matcher(v);
            if (frac.find()) {
                v = v.substring(0, frac.start()) + "." + frac.group(1).substring(0, 6);
            }
            // pure Date is VALUE-polymorphic (cluster 40 companion): a
            // date-only cell stays a DATE literal — without this a
            // Date[1] column of date-only cells would render
            // '2014-12-04 00:00:00' in toString compares
            if (type == Type.Primitive.DATE
                    && v.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return new SqlExpr.DateLit(v);
            }
            return new SqlExpr.TimestampLit(v);
        }
        if (type == Type.Primitive.STRING) {
            return new SqlExpr.StringLit(cell);
        }
        // A Variant cell is JSON TEXT (the TDS literal wraps it in quotes).
        if (PlatformTypes.isVariant(type)) {
            String json = cell.length() >= 2 && cell.startsWith("\"") && cell.endsWith("\"")
                    ? cell.substring(1, cell.length() - 1) : cell;
            return new SqlExpr.Cast(new SqlExpr.StringLit(json),
                    SqlType.Scalar.JSON);
        }
        throw new IllegalStateException(
                "no TDS cell rendering for Pure type " + type.typeName());
    }
    /** The DuckDB interval-constructor for a DurationUnit enum literal. */
        /** The date_diff part name for a DurationUnit enum literal. */
    private static String diffPart(TypedSpec unit) {
        return switch (enumName(unit)) {
            case "YEARS" -> "year";
            case "MONTHS" -> "month";
            case "WEEKS" -> "week";
            case "DAYS" -> "day";
            case "HOURS" -> "hour";
            case "MINUTES" -> "minute";
            case "SECONDS" -> "second";
            case "MILLISECONDS" -> "millisecond";
            case "MICROSECONDS" -> "microsecond";
            default -> throw new IllegalStateException(
                    "unknown DurationUnit for dateDiff: " + enumName(unit));
        };
    }

    /**
     * A DATE-ARITHMETIC argument: partial-date LITERALS (year, year-month
     * — globally string-typed for the pinned string-comparison semantics)
     * pad to the first of their period as real DATE literals.
     */
    static SqlExpr dateArg(TypedSpec typed,
                                   SqlExpr lowered) {
        if (typed instanceof TypedCDate d) {
            if (d.value() instanceof PureDateLiteral.Year y) {
                return new SqlExpr.DateLit(y.toEngineString() + "-01-01");
            }
            if (d.value() instanceof PureDateLiteral.YearMonth ym) {
                return new SqlExpr.DateLit(ym.toEngineString() + "-01");
            }
        }
        return lowered;
    }

    /** {@code dateDiff} with REAL pure's per-unit semantics (PCT-pinned):
     * WEEKS counts Sunday-boundary crossings — (d1,d2] forward, [d2,d1)
     * backward (not the negation); HOURS/MINUTES/SECONDS are truncated
     * ELAPSED time (SQL date_diff counts crossings); calendar parts
     * match SQL date_diff. */
    private static SqlExpr dateDiffExpr(String part, SqlExpr d1, SqlExpr d2) {
        switch (part) {
            case "week" -> {
                SqlExpr forward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(d2), sundayIndex(d1));
                SqlExpr backward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(backOneDay(d2)), sundayIndex(backOneDay(d1)));
                return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                                        new SqlExpr.StringLit("day"), d2, d1)),
                                new SqlExpr.IntLit(0)),
                        forward)), backward);
            }
            case "hour" -> {
                return elapsed(d1, d2, 3_600_000L);
            }
            case "minute" -> {
                return elapsed(d1, d2, 60_000L);
            }
            case "second" -> {
                return elapsed(d1, d2, 1_000L);
            }
            default -> {
                return new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit(part), d1, d2));
            }
        }
    }

    /** Truncated elapsed time in {@code unitMs} chunks (Java toHours-style). */
    private static SqlExpr elapsed(SqlExpr d1, SqlExpr d2, long unitMs) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                SqlExpr.Call.of(SqlFn.MINUS,
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d2)),
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d1))),
                new SqlExpr.IntLit(unitMs));
    }

    /**
     * Floored week index counted from an ANCIENT Sunday epoch (0001-01-07,
     * proleptic Gregorian) — always positive for real dates, so DuckDB's
     * truncating {@code //} IS floor division (the audit's pre-1970 case).
     */
    private static SqlExpr sundayIndex(SqlExpr d) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit("day"),
                        new SqlExpr.DateLit("0001-01-07"), d)),
                new SqlExpr.IntLit(7));
    }

    private static SqlExpr backOneDay(SqlExpr d) {
        return new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                new SqlExpr.StringLit("to_days"),
                new SqlExpr.IntLit(-1), d));
    }

    /**
     * Pure's DISCRETE percentile (engine percentile.pure): over the sorted
     * data, {@code ip = floor(p*(n-1))}; pick {@code data[ip]} when
     * {@code (ip+1)/n > p}, else {@code data[ip+1]}. quantile_disc computes
     * a DIFFERENT rank at exact-rank points — the audit's divergence.
     */
    private static SqlExpr pureDiscretePercentile(SqlExpr list, SqlExpr p,
            boolean ascending) {
        SqlExpr sorted = new SqlExpr.Call(
                ascending ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(list));
        SqlExpr n = new SqlExpr.Call(SqlFn.LIST_LENGTH, List.of(list));
        SqlExpr ip = new SqlExpr.Call(SqlFn.FLOOR, List.of(
                SqlExpr.Call.of(SqlFn.TIMES, p,
                        SqlExpr.Call.of(SqlFn.MINUS, n, new SqlExpr.IntLit(1)))));
        // real percentile.pure guards the BOUNDARIES before the rank rule:
        // pos == 0 takes the first element, pos >= n-1 the last (p=1.0
        // otherwise indexes past the end)
        SqlExpr pos = SqlExpr.Call.of(SqlFn.TIMES, p,
                SqlExpr.Call.of(SqlFn.MINUS, n, new SqlExpr.IntLit(1)));
        SqlExpr pick = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.LESS_EQUAL, pos, new SqlExpr.IntLit(0)),
                        new SqlExpr.IntLit(1)),
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER_EQUAL, pos,
                                SqlExpr.Call.of(SqlFn.MINUS, n, new SqlExpr.IntLit(1))),
                        n),
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER,
                                SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(1)),
                                SqlExpr.Call.of(SqlFn.TIMES, p, n)),
                        SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(1)))),
                SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(2)));
        return new SqlExpr.Call(SqlFn.LIST_GET, List.of(sorted,
                new SqlExpr.Cast(pick, SqlType.Scalar.BIGINT)));
    }

    /** Replace bare references to {@code name} with {@code replacement}
     * across an expression tree (a 2-param comparator closing over the
     * needle in a 1-param SQL lambda); rebinding lambdas SHADOW. */
    /** A comparator whose body is bare eq/equal over its two parameters. */
    private static boolean isEqualityComparator(TypedSpec spec) {
        if (!(spec instanceof TypedLambda cmp)
                || cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || cc.args().size() != 2) {
            return false;
        }
        String fqn = cc.callee().qualifiedName();
        if (!fqn.equals("meta::pure::functions::boolean::eq")
                && !fqn.equals("meta::pure::functions::boolean::equal")) {
            return false;
        }
        // BARE parameter references only ({x,y|eq($x,$y)}) — a body like
        // {x,y|$x == 2+$y} is a CUSTOM comparator, not plain equality
        return cc.args().stream().allMatch(arg ->
                arg instanceof TypedVariable v
                        && cmp.parameters().contains(v.name()));
    }


    static SqlExpr substituteRef(SqlExpr e, String name, SqlExpr replacement) {
        return switch (e) {
            case SqlExpr.Column c when c.table() == null && name.equals(c.name()) -> replacement;
            case SqlExpr.Column c when name.equals(c.table()) ->
                    new SqlExpr.StructGet(replacement, c.name());   // $b.field over the needle
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(),
                    c.args().stream().map(a -> substituteRef(a, name, replacement)).toList());
            case SqlExpr.Cast c ->
                    new SqlExpr.Cast(substituteRef(c.value(), name,
                            replacement), c.target(), c.conform());
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(a.elements().stream()
                    .map(x -> substituteRef(x, name, replacement)).toList());
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(fl -> new SqlExpr.StructLit.Field(fl.name(),
                            substituteRef(fl.value(), name, replacement),
                            fl.declared())).toList());
            case SqlExpr.StructGet g ->
                    new SqlExpr.StructGet(substituteRef(g.source(), name, replacement), g.field());
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            substituteRef(w.condition(), name, replacement),
                            substituteRef(w.then(), name, replacement))).toList(),
                    cs.otherwise() == null ? null
                            : substituteRef(cs.otherwise(), name, replacement));
            case SqlExpr.Lambda l -> l.params().contains(name)
                    ? l
                    : new SqlExpr.Lambda(l.params(), substituteRef(l.body(), name, replacement));
            // leaves pass; every other composite recurses structurally
            // (query-carrying nodes own their traversal — children() is
            // empty for them by contract)
            default -> e.mapChildren(x -> substituteRef(x, name, replacement));
        };
    }

    /** Whether a type is an instance kind (a user class or parameterized class), not a primitive. */
    /** Pure Float minimal-decimal PRINT form — owned by
     * {@link LiteralSpelling#floatPrint} (F10 proper slice 1). */
    static SqlExpr floatRepr(SqlExpr x) {
        return LiteralSpelling.floatPrint(x);
    }

    /** The spelling->PRINT projection — owned by
     * {@link LiteralSpelling#printForm} (one grammar owner, both
     * directions of the label seam). */
    static SqlExpr literalPrint(SqlExpr x) {
        return LiteralSpelling.printForm(x);
    }

    /**
     * Scans a LITERAL printf format string for the pure-only directives,
     * rewriting each to %s and wrapping the matching spread argument
     * (spread = [fmt, arg1, ...]; directive order maps to argument order).
     */
    private static void rewriteFormatDirectives(String fmt, List<SqlExpr> spread,
            List<TypedSpec> typedElems) {
        StringBuilder out = new StringBuilder();
        int argIdx = 1;
        int i = 0;
        while (i < fmt.length()) {
            char c = fmt.charAt(i);
            if (c != '%' || i + 1 >= fmt.length()) {
                out.append(c);
                i++;
                continue;
            }
            char d = fmt.charAt(i + 1);
            if (d == '%') {
                out.append("%%");
                i += 2;
                continue;
            }
            var typed = argIdx - 1 < typedElems.size() ? typedElems.get(argIdx - 1) : null;
            if (d == 't' && i + 2 < fmt.length() && fmt.charAt(i + 2) == '{') {
                int close = fmt.indexOf('}', i + 3);
                if (close < 0) {
                    throw new IllegalStateException("unterminated %t{ in format: " + fmt);
                }
                spread.set(argIdx, dateWithPattern(fmt.substring(i + 3, close), spread.get(argIdx)));
                out.append("%s");
                argIdx++;
                i = close + 1;
                continue;
            }
            if (d == 'f') {
                spread.set(argIdx, floatRepr(spread.get(argIdx)));
                out.append("%s");
                argIdx++;
                i += 2;
                continue;
            }
            // %r: pure's REPR — a string in quotes with \-escapes, a date
            // with its % literal prefix.
            if (d == 'r') {
                spread.set(argIdx, Repr.of(typed, spread.get(argIdx)));
                out.append("%s");
                argIdx++;
                i += 2;
                continue;
            }
            // %0<width>d: pure pads the DIGITS to width and then signs
            // (-3 at width 5 is '-00003'); printf's width includes the
            // sign ('-0003').
            if (d == '0') {
                int j = i + 2;
                while (j < fmt.length() && Character.isDigit(fmt.charAt(j))) {
                    j++;
                }
                if (j > i + 2 && j < fmt.length() && fmt.charAt(j) == 'd') {
                    long width = Long.parseLong(fmt.substring(i + 2, j));
                    spread.set(argIdx, signedZeroPad(spread.get(argIdx), width));
                    out.append("%s");
                    argIdx++;
                    i = j + 1;
                    continue;
                }
            }
            // %s / bare %t over a DATE argument: pure's default date print
            // (the ISO T-form with +0000), not SQL's space-separated cast.
            if ((d == 's' || d == 't') && typed != null) {
                SqlExpr dp = datePrintOf(typed, spread.get(argIdx));
                if (dp != null) {
                    spread.set(argIdx, dp);
                    out.append("%s");
                    argIdx++;
                    i += 2;
                    continue;
                }
            }
            out.append('%').append(d == 't' ? 's' : d);
            argIdx++;
            i += 2;
        }
        spread.set(0, new SqlExpr.StringLit(out.toString()));
    }

    /**
     * %t{pattern}: an optional leading {@code [Zone]} formats the value in
     * that zone — the shift and the {@code Z} offset suffix both compute IN
     * SQL (ICU timezone()); without a zone, values are UTC and {@code Z}
     * renders the literal +0000 (via the token table).
     */
    private static SqlExpr dateWithPattern(String pattern, SqlExpr arg) {
        if (!pattern.startsWith("[") || pattern.indexOf(']') < 0) {
            return SqlExpr.Call.of(SqlFn.STRFTIME, arg,
                    new SqlExpr.FormatLit(DateFormats.javaDateToParts(pattern)));
        }
        int zb = pattern.indexOf(']');
        String zone = pattern.substring(1, zb);
        String pat = pattern.substring(zb + 1);
        boolean offsetSuffix = pat.endsWith("Z");
        if (offsetSuffix) {
            pat = pat.substring(0, pat.length() - 1);
        }
        if (pat.contains("Z")) {
            throw new IllegalStateException(
                    "a zone-shifted date pattern supports Z only as a suffix: " + pattern);
        }
        SqlExpr wall = SqlExpr.Call.of(SqlFn.TIMEZONE, new SqlExpr.StringLit(zone),
                SqlExpr.Call.of(SqlFn.TIMEZONE, new SqlExpr.StringLit("UTC"), arg));
        SqlExpr shifted = SqlExpr.Call.of(SqlFn.STRFTIME, wall,
                new SqlExpr.FormatLit(DateFormats.javaDateToParts(pat)));
        if (!offsetSuffix) {
            return shifted;
        }
        SqlExpr off = SqlExpr.Call.of(SqlFn.DATE_DIFF,
                new SqlExpr.StringLit("minute"), arg, wall);
        SqlExpr absOff = SqlExpr.Call.of(SqlFn.ABS, off);
        SqlExpr hh = SqlExpr.Call.of(SqlFn.LPAD,
                str(SqlExpr.Call.of(SqlFn.INT_DIVIDE, absOff, new SqlExpr.IntLit(60))),
                new SqlExpr.IntLit(2), new SqlExpr.StringLit("0"));
        SqlExpr mm = SqlExpr.Call.of(SqlFn.LPAD,
                str(SqlExpr.Call.of(SqlFn.MOD, absOff, new SqlExpr.IntLit(60))),
                new SqlExpr.IntLit(2), new SqlExpr.StringLit("0"));
        SqlExpr sign = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, off, new SqlExpr.IntLit(0)),
                new SqlExpr.StringLit("-"))), new SqlExpr.StringLit("+"));
        return cat(shifted, sign, hh, mm);
    }

    /** {@code -3 @ width 5 → '-00003'}: pad the digits, then sign (pure's format). */
    private static SqlExpr signedZeroPad(SqlExpr x, long width) {
        SqlExpr padded = SqlExpr.Call.of(SqlFn.LPAD,
                str(SqlExpr.Call.of(SqlFn.ABS, x)),
                new SqlExpr.IntLit(width), new SqlExpr.StringLit("0"));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("-"), padded))),
                padded);
    }

    /** Pure's default date print for a format slot, or null when not a date. */
    static @com.legend.Nullable SqlExpr datePrintOf(TypedSpec typed, SqlExpr e) {
        Type t = typed.info().type();
        SqlExpr lit = dateLiteralPrint(typed, t);
        if (lit != null) {
            return lit;
        }
        if (t == Type.Primitive.DATE_TIME) {
            return SqlExpr.Call.of(SqlFn.STRFTIME, e,
                    new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_PURE_UTC));
        }
        return null;
    }

    static boolean isClassish(Type t) {
        // a WRAPPED RELATION is a GenericType but NOT an instance kind —
        // a relation-valued collection's containment is real membership
        // over its column values (Row-vs-Relation: the wrapped table
        // must classify exactly like the bare struct always did).
        return (t instanceof Type.ClassType && !PlatformTypes.isVariant(t)
                        && !PlatformTypes.isAny(t) && !PlatformTypes.isNil(t))
                || (t instanceof Type.GenericType && !Type.isRelation(t));
    }

    /**
     * The STATIC print form of a date literal (real pure's toString):
     * components padded, subsecond digits exactly as written, DateTime
     * normalized to +0000 (the parser already shifted zone-carrying
     * literals to GMT). {@code null} for non-literal args.
     */
    private static @com.legend.Nullable SqlExpr dateLiteralPrint(TypedSpec spec, Type t) {
        if (!(spec instanceof TypedCDate cd)) {
            return null;
        }
        String s = cd.value().toEngineString();
        return new SqlExpr.StringLit(t == Type.Primitive.DATE_TIME ? s + "+0000" : s);
    }

    /** Partial-date-literal precision: 1 = year, 2 = year-month; null otherwise. */
    /** Split-part FIELD COUNT of a partial (year / year-month) literal —
     * derived from the one precision ladder, not a second scale. */
    static @com.legend.Nullable Integer partialPrecision(TypedSpec t) {
        if (t instanceof TypedCDate d) {
            return switch (d.value().precision()) {
                case YEAR -> 1;
                case MONTH -> 2;
                default -> null;
            };
        }
        return null;
    }

    /** A date type whose VALUES are always full-precision (columns, full literals). */
    private static boolean isFullPrecisionDate(Type t) {
        return t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE;
    }

    /** The toOne AGG-STRIP (STAMP_DISCIPLINE_PROGRAM, C2 key insight):
     * dropping the LIST collect on a subquery operand yields SQL's
     * NATIVE scalar-subquery semantics — pure's checked toOne (>1 rows
     * raises, 1 yields the value, 0 rows NULL, the engine-noOp empty
     * the corpus pins). Only the EXACT plain single-projection
     * non-distinct no-groupBy shape strips. Moved from the dissolved
     * ListShapes. Package-private: {@code Lowerer#scalarRoot} uses the
     * recognizer half to spot the SAME shape at the statement root,
     * where it keeps the LIST instead (egress slice A). */
    static @com.legend.Nullable SqlExpr aggStrip(SqlExpr e) {
        if (!(e instanceof SqlExpr.ScalarSubquery sq
                && sq.subquery() instanceof SqlSelect ss
                && ss.projections().size() == 1
                && ss.projections().get(0).expr()
                        instanceof SqlAgg.Reducer r
                && r.fn() == SqlAgg.Fn.LIST
                && !r.distinct()
                && r.args().size() == 1
                && ss.groupBy().isEmpty())) {
            return null;
        }
        return new SqlExpr.ScalarSubquery(ss.withProjections(
                List.of(new SqlSelect.Projection(
                        r.args().get(0),
                        ss.projections().get(0).alias(),
                        ss.projections().get(0).out()))));
    }

    /** The reduction rules' identity-arm guard — Stamps.toOne, the
     * historical upper==1 reading preserved verbatim (see Stamps for
     * the empty-identity fork this deliberately does NOT change). */
    static boolean isToOne(TypedSpec arg) {
        return Stamps.toOne(arg);
    }


    /** A literal boolean argument; LOUD otherwise (never a silent default). */
    private static boolean boolLiteral(TypedSpec arg,
            String what) {
        if (arg instanceof TypedCBoolean b) {
            return b.value();
        }
        throw new IllegalStateException(what + " must be a literal boolean, got "
                + arg.getClass().getSimpleName());
    }





    /** ISO day numbers of the pure {@code DayOfWeek} enum (Monday=1). */
        /** {@code mostRecentDayOfWeek}/{@code previousDayOfWeek}: the anchored
     * shift per the engine H2 formula. {@code strict} excludes the anchor
     * day itself (previous). */
        static String enumName(TypedSpec arg) {
        if (arg instanceof TypedEnumValue ev) {
            return ev.value();
        }
        // a VARIABLE unit is valid pure — an unimplemented form, not a
        // resolver bug (C1.5 crash-on-valid)
        throw new com.legend.error.NotImplementedException(
                "a non-literal DurationUnit argument ("
                + arg.getClass().getSimpleName() + ") is not modeled");
    }

    /**
     * The precision RANK of a date argument (0=year .. 6=subsecond): a
     * LITERAL answers from its own written precision; a column from its
     * Pure type (StrictDate = day, DateTime = SQL TIMESTAMP = full); the
     * abstract Date is undecidable and refuses loudly.
     */
    static PureDateLiteral.Precision datePrecision(TypedSpec arg) {
        if (arg instanceof TypedCDate d) {
            return d.value().precision();
        }
        // A date() CONSTRUCTOR call's precision is its ARITY — the static
        // return type says DateTime for every arity (audit: hasMinute of
        // date(y,mo,d,h) answered true).
        if (arg instanceof TypedNativeCall dc
                && dc.callee().qualifiedName().equals("meta::pure::functions::date::date")) {
            return switch (dc.args().size()) {
                case 1 -> PureDateLiteral.Precision.YEAR;
                case 2 -> PureDateLiteral.Precision.MONTH;
                case 3 -> PureDateLiteral.Precision.DAY;
                case 4 -> PureDateLiteral.Precision.HOUR;
                case 5 -> PureDateLiteral.Precision.MINUTE;
                default -> dc.args().get(5).info().type()
                        == Type.Primitive.FLOAT
                        || dc.args().get(5).info().type()
                                == Type.Primitive.DECIMAL
                        ? PureDateLiteral.Precision.SUBSECOND
                        : PureDateLiteral.Precision.SECOND;
            };
        }
        var t = arg.info().type();
        if (t == Type.Primitive.DATE_TIME) {
            return PureDateLiteral.Precision.SUBSECOND;
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return PureDateLiteral.Precision.DAY;
        }
        throw new IllegalStateException("a date-precision predicate over the"
                + " abstract Date type is not statically decidable — declare"
                + " the value StrictDate or DateTime");
    }


    /** A STRING-target WIRE conformance cast ({@code castAsDeclared}) at
     * a projected CELL ROOT unwraps — the engine's TDS cell keeps the RAW
     * column value there (tree.pure asserts Long over a String-declared
     * property; the goldens never spell wire casts). Non-String targets
     * keep the cast (boolean.pure asserts true over a 'true'/'false'
     * STRING mapping — the engine converts TOWARD Boolean, referee-
     * proven: unscoped unwrap regressed tests/mapping 9->7). CONSUMED
     * positions keep the cast always (audit 19 F7: DuckDB does not
     * wire-convert where H2 does). */
}
