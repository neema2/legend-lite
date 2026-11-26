// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.element.type.PlatformTypes;
import java.util.ArrayList;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * The K-arm verdict channel's QUERY SYNTHESIS (Invariant 7: minting
 * typed nodes is compiler work): the assert family's in-database
 * evaluations are BUILT here; {@code AssertVerdicts} fetches the
 * results and keeps the JUDGMENT host-side (Clause 2c — arguments
 * execute in the database, the verdict is World 1's). This class is the
 * seed of the canonical-render verdicts leg: when asserts move to
 * compiler-known canonical serialization with byte-compare, that
 * emission joins this owner.
 */
public final class VerdictQueries {


    private VerdictQueries() {
    }

    /** The SQL-TEXT arm's OUR-ROWS query (SQLTEXT charter §3.5c —
     * Appendix A): the producer's own query lambda body wrapped in
     * {@code from(<the producer's mapping>)}, runtime left to the
     * executing env. SqlTextVerdicts fetches and judges; the mint is
     * compiler emission (Invariant 7). */

    /** {@code assertEquals(expected, actual)} as a typed call — the meaning
     * of a dual-golden assert once its golden is chosen (the verdict arm
     * adjudicates it as the plain verdict). Null when the catalog has no
     * two-argument assertEquals (never, in a platform build). */
    public static @com.legend.base.Nullable TypedSpec assertEqualsOf(TypedSpec expected,
            TypedSpec actual, SpecCompiler specs) {
        return specs.ctx().findFunction(
                        com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS)
                .stream().filter(f -> f.parameters().size() == 2).findFirst()
                .map(f -> (TypedSpec) new com.legend.compiler.spec.typed.TypedNativeCall(f,
                        java.util.List.of(expected, actual),
                        new com.legend.compiler.element.type.ExprType(
                                com.legend.compiler.element.type.Type.Primitive.BOOLEAN,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE)))
                .orElse(null);
    }

    /** assertSameSQL's OUR-ROWS read (§8.3b): {@code $result.values} —
     * the envelope splice swaps it for the frame's typed chain
     * (ResultEnvelopeSplice.valuesRead), which carries the REAL
     * result type; the minted info is a pre-splice placeholder. */
    public static TypedSpec valuesRead(TypedSpec resultArg) {
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                resultArg, "values", resultArg.info());
    }

    private static ExprType scalar(Type t) {
        return new ExprType(t, Multiplicity.Bounded.ONE);
    }

    /** The predicate VECTOR for a quantified assert
     * ({@code source->map(binder|assert(pred, msg))}): same source, same
     * binder, the assert's CONDITION as the mapper body — one boolean
     * per row, computed in the database. */
    public static TypedSpec predicateVector(TypedMap quantified,
            TypedLambda lam, TypedSpec condition) {
        TypedLambda predLam = new TypedLambda(lam.parameters(),
                List.of(condition), lam.info());
        return new TypedMap(quantified.source(), predLam,
                new ExprType(Type.Primitive.BOOLEAN,
                        Multiplicity.Bounded.ZERO_MANY));
    }

    /** THE VECTOR CONTRACT (2026-09-22, read off every body that failed the first cut — all 47
     * through {@code createTableRowIdentifiers}): a {@code forAll} over a collection is a
     * ROW-WISE SQL CONDITION only when the whole per-element check is a function of that row.
     * (1) the source is rows the relation lane plans — a relation read, or a zip whose arms are
     * single-column relations or literal collections (a computed list is the list lane's, a
     * class query nobody's yet); (2) the predicate is ROW-LOCAL — the binder's fields, literals,
     * natives over those; any other variable, class query, helper call or instance means it
     * reaches outside the row; (3) the message is literal or absent (the vector raises it once,
     * host-side — the caller's own rule for {@code assert(pred)}). Anything else keeps the
     * unroll: each element a literal substituted into the check, judged by the ordinary arms. */
    public static boolean vectorContract(TypedSpec source, TypedLambda lam, TypedSpec root, TypedSpec predicate) {
        return literalMessage(root) && rowSource(source)
                && lam.parameters().size() == 1
                && rowLocal(predicate, lam.parameters().get(0));
    }

    /** (3): the verdict call's message — the argument AFTER its value arguments (one for
     * assert / assertFalse, two for assertEquals, three for assertEqWithinTolerance) — is a
     * literal or absent. */
    static boolean literalMessage(TypedSpec root) {
        String fqn = com.legend.compiler.spec.typed.Calls.calleeOf(root);
        List<TypedSpec> a = com.legend.compiler.spec.typed.Calls.argsOf(root);
        int values;
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT.equals(fqn)
                || com.legend.compiler.element.type.PlatformTypes.ASSERT_FALSE.equals(fqn)) {
            values = 1;
        } else if (com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS.equals(fqn)) {
            values = 2;
        } else if ("meta::pure::functions::asserts::assertEqWithinTolerance".equals(fqn)) {
            values = 3;
        } else {
            return false;
        }
        return a.size() <= values || a.get(values) instanceof com.legend.compiler.spec.typed.TypedCString;
    }

    /**
     * THE PER-ELEMENT OPERANDS of a quantified equality (the asserts-as-plan-nodes program, step 5, 2026-11-04):
     * {@code source->map(x | pair(expected, actual))} -- both operands of each element computed by the database in one
     * ordered value, never in Java -- or null where the lambda is not one statement or an operand is not one value per
     * element (the pairing would not align). Compiler-owned synthesis over the typed tree, nothing evaluated.
     */
    public static @com.legend.base.Nullable TypedMap pairsVector(TypedSpec source, TypedLambda lam, TypedSpec expected,
            TypedSpec actual, com.legend.compiler.element.ModelContext ctx) {
        if (lam.parameters().size() != 1 || lam.body().size() != 1 || !one(expected) || !one(actual)) {
            return null;
        }
        var pairFns = ctx.findFunction(com.legend.compiler.element.type.PlatformTypes.PAIR_FN).stream()
                .filter(f -> f.parameters().size() == 2).toList();
        if (pairFns.size() != 1) {
            throw new IllegalStateException("verdict synthesis bug: expected one 2-arg collection::pair");
        }
        ExprType pairType = new ExprType(new Type.GenericType(com.legend.compiler.element.type.PlatformTypes.PAIR,
                List.of(expected.info().type(), actual.info().type()), List.of()), Multiplicity.Bounded.ONE);
        TypedLambda pairLam = new TypedLambda(lam.parameters(),
                List.of(new TypedNativeCall(pairFns.get(0), List.of(expected, actual), pairType)), lam.info());
        return new TypedMap(source, pairLam, new ExprType(pairType.type(), Multiplicity.Bounded.ZERO_MANY));
    }

    private static boolean one(TypedSpec n) {
        return n.info().multiplicity() instanceof Multiplicity.Bounded b && b.lower() == 1 && b.upper() != null
                && b.upper() == 1;
    }

    /** The message an {@code assert}/{@code assertFalse} under the vector contract fails with: its literal message
     *  ({@code (3)} admits no other), else {@code otherwise}, the run's words of a condition's failure (the user's R1,
     *  2026-11-18, PR #118: lite's sentence, legend-pure's the edge's alone; 2026-11-04: both judges printed the default
     *  over a caller's message -- AssertBarTest's forAll-zip-assert-message). */
    public static String assertMessage(TypedSpec root, String otherwise) {
        String m = callerMessage(root);
        return m != null ? m : otherwise;
    }

    /** The literal message of an {@code assert}/{@code assertFalse} under the vector contract, or null where it has
     *  none ({@code (3)} admits no other). */
    public static @com.legend.base.Nullable String callerMessage(TypedSpec root) {
        List<TypedSpec> a = com.legend.compiler.spec.typed.Calls.argsOf(root);
        return a.size() >= 2 && a.get(1) instanceof com.legend.compiler.spec.typed.TypedCString m ? m.value() : null;
    }

    /** (1): rows the relation lane plans. */
    static boolean rowSource(TypedSpec source) {
        if (Type.relationValued(source.info())) {
            return true;
        }
        if (source instanceof TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())
                && z.args().size() == 2) {
            return rowArm(z.args().get(0)) && rowArm(z.args().get(1));
        }
        return false;
    }

    private static boolean rowArm(TypedSpec arm) {
        if (arm instanceof com.legend.compiler.spec.typed.TypedCollection) {
            return true;
        }
        if (arm instanceof TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())) {
            return rowSource(arm);
        }
        return Type.relationValued(arm.info())
                && Type.schemaView(arm.info().type()) instanceof Type.RelationType rt
                && rt.columns().size() == 1;
    }

    /** (2): a function of the row — the binder, its fields, literals, natives over those. */
    static boolean rowLocal(TypedSpec e, String binder) {
        return switch (e) {
            case com.legend.compiler.spec.typed.TypedCString s -> true;
            case com.legend.compiler.spec.typed.TypedCInteger i -> true;
            case com.legend.compiler.spec.typed.TypedCFloat f -> true;
            case com.legend.compiler.spec.typed.TypedCDecimal d -> true;
            case com.legend.compiler.spec.typed.TypedCBoolean b -> true;
            case com.legend.compiler.spec.typed.TypedVariable v -> v.name().equals(binder);
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa -> rowLocal(pa.source(), binder);
            case com.legend.compiler.spec.typed.TypedCast c -> rowLocal(c.source(), binder);
            case com.legend.compiler.spec.typed.TypedCollection c -> c.elements().stream().allMatch(x -> rowLocal(x, binder));
            case TypedNativeCall n -> n.args().stream().allMatch(x -> rowLocal(x, binder));
            default -> false;
        };
    }

    /** The predicate vector over a REBOUND source ({@code source->map(binder | pred)}):
     * the quantified map's own info, the caller's resolved source (through its lets),
     * the same binder — minted here, the compiler layer (Invariant 7). */
    public static TypedSpec predicateVectorOver(TypedSpec source, TypedMap quantified,
            TypedLambda lam, TypedSpec condition) {
        return predicateVector(new TypedMap(source, lam, quantified.info()), lam, condition);
    }

    /** {@code equal(distinct(<map>), [true])} → the map; else the node
     * (the toSQLString dialect-table idiom's outer wrapper). */
    public static TypedSpec distinctTrueWrapper(TypedSpec bare) {
        if (bare instanceof TypedNativeCall eq
                && (eq.callee().qualifiedName().equals("meta::pure::functions::boolean::equal")
                        || eq.callee().qualifiedName().equals("meta::pure::functions::boolean::eq"))
                && eq.args().size() == 2
                && eq.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCollection tc
                && tc.elements().size() == 1
                && tc.elements().get(0) instanceof com.legend.compiler.spec.typed.TypedCBoolean b
                && b.value()
                && eq.args().get(0) instanceof TypedNativeCall d
                && d.callee().qualifiedName().equals("meta::pure::functions::collection::distinct")
                && d.args().size() == 1) {
            return d.args().get(0);
        }
        return bare;
    }

    /** One element of an UNROLLED quantified assert: the caller's lets,
     * the lambda's parameter bound to {@code element} as a let, then the
     * lambda's own statements — reduced by the inliner (the one
     * substitution engine). The last statement is the element's assert;
     * a message-carrying assert ({@code assertEquals(e, a, fmt, args)})
     * normalizes to its two-argument form (the message is failure text,
     * never part of the verdict). */
    /** The frame variable's {@code .activities} read — the node the
     * splice hook resolves to the frame's own execute() call (the
     * verdict arms recover a frame's mapping through it). */
    // (extentSubset, the referee's replay gate: moved to the edge's ReplayGates, 2026-11-18, the edge audit's J40)

    /** {@code forAll(coll, x | <assert>)} — the engine's per-element
     * assert idiom (stringToFloat testProject: {@code [123.456, 100.001]
     * ->zip($tds.rows.values)->forAll(pair | assertEqWithinTolerance(
     * ...))}) IS the quantified assert: every element's assert holds (an
     * assert never yields false — it raises), so it unrolls exactly as
     * the map form does. Null = not that shape. */
    public static @com.legend.base.Nullable TypedMap forAllAsQuantified(TypedSpec bare) {
        if (bare instanceof TypedNativeCall fa
                && fa.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::forAll")
                && fa.args().size() == 2
                && fa.args().get(1) instanceof TypedLambda flam
                && flam.parameters().size() == 1
                && !flam.body().isEmpty()) {
            return new TypedMap(fa.args().get(0), flam, fa.info());
        }
        return null;
    }

    /** The elements a quantified assert unrolls over: a LITERAL
     * collection's elements, or — {@code zip(A, B)} — pairs of the two
     * arms' elements, each arm a literal collection or a side the
     * database evaluates ({@code fetch}); its values become literal
     * specs (the unroll COMPARES, never computes — the pairing is
     * orchestration, every arithmetic stays in the assert's own side
     * evaluation). Null = not an unrollable shape (a runtime
     * collection, a value with no literal spelling). */
    public static @com.legend.base.Nullable List<TypedSpec> unrollElements(
            TypedSpec source, List<TypedSpec> letPrefix,
            com.legend.compiler.element.ModelContext ctx) {
        if (source instanceof TypedCollection coll) {
            // elements that are let-bound values ([$_s1_hoisted, $_s2_hoisted]
            // — hoisted constructor programs) read through the caller's lets
            List<TypedSpec> out = new java.util.ArrayList<>(coll.elements().size());
            for (TypedSpec e : coll.elements()) {
                out.add(com.legend.compiler.spec.typed.Lets.bound(e, letPrefix));
            }
            return out;
        }
        // a [1] instance literal (a let-bound constructor value) is the
        // one-element collection pure's [x] == x law makes it
        if (source instanceof com.legend.compiler.spec.typed.TypedNewInstance ni) {
            return List.of(ni);
        }
        if (source instanceof TypedNativeCall z
                && z.callee().qualifiedName().equals(
                        com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP)
                && z.args().size() == 2) {
            List<TypedSpec> left = armElements(z.args().get(0), letPrefix);
            List<TypedSpec> right = armElements(z.args().get(1), letPrefix);
            if (left == null || right == null) {
                return null;
            }
            var pairFns = ctx.findFunction("meta::pure::functions::collection::pair")
                    .stream().filter(f -> f.parameters().size() == 2).toList();
            if (pairFns.size() != 1) {
                throw new IllegalStateException(
                        "verdict synthesis bug: expected one 2-arg collection::pair");
            }
            // zip pairs by position and stops at the shorter arm (zip.pure)
            int n = Math.min(left.size(), right.size());
            List<TypedSpec> out = new java.util.ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                TypedSpec l = left.get(i);
                TypedSpec r = right.get(i);
                out.add(new TypedNativeCall(pairFns.get(0), List.of(l, r),
                        new ExprType(new Type.GenericType(
                                "meta::pure::functions::collection::Pair",
                                List.of(l.info().type(), r.info().type()), List.of()),
                                Multiplicity.Bounded.ONE)));
            }
            return out;
        }
        return null;
    }

    /** A zip arm's elements as the program SPELLED them: a literal collection's, or a bare literal as the
     *  one-element collection pure's {@code [x] == x} law makes it; an arm the database would answer is null here --
     *  the vector form reads it as rows (the seam program's step after S2f, 2026-10-30: the unroll fetches nothing). */
    private static @com.legend.base.Nullable List<TypedSpec> armElements(TypedSpec arm0,
            List<TypedSpec> letPrefix) {
        TypedSpec arm = com.legend.compiler.spec.typed.Lets.bound(arm0, letPrefix);
        if (arm instanceof TypedCollection c) {
            return c.elements();
        }
        if (arm instanceof com.legend.compiler.spec.typed.TypedCString
                || arm instanceof com.legend.compiler.spec.typed.TypedCInteger
                || arm instanceof com.legend.compiler.spec.typed.TypedCFloat
                || arm instanceof com.legend.compiler.spec.typed.TypedCDecimal
                || arm instanceof com.legend.compiler.spec.typed.TypedCBoolean) {
            return List.of(arm);
        }
        return null;
    }

    public static List<TypedSpec> unrolledElement(SpecCompiler specs,
            List<TypedSpec> letPrefix, TypedLambda lam, TypedSpec element,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        List<TypedSpec> seq = new java.util.ArrayList<>(letPrefix);
        seq.add(new com.legend.compiler.spec.typed.TypedLet(lam.parameters().get(0),
                element, element.info()));
        seq.addAll(lam.body());
        var inliner = hook == null ? new UserCallInliner(specs)
                : new UserCallInliner(specs, hook);
        List<TypedSpec> reduced = new java.util.ArrayList<>(inliner.inlineBody(seq));
        int last = reduced.size() - 1;
        TypedSpec stmt = reduced.get(last);
        // the element's assert as written, its caller's MESSAGE kept: each judge plans it as any assert's and reads it
        // only where that element fails (item 3's S4 step 3, the user's decision 4, 2026-11-22: until then the message
        // arguments dropped, both judges failing an element with the family's default where upstream raises the
        // element's own message)
        reduced.set(last, com.legend.compiler.spec.typed.Lets.bare(stmt));
        return reduced;
    }

    /** An expected literal collection's {@code ^TDSNull()} elements as
     * present null cells ({@code TypedNullCell}, C5 step C, C-2: the String
     * {@code 'TDSNull'} until then; the canon writes lite's bare null). All-string
     * afterwards → a String collection; otherwise the collection keeps
     * its mixed stamp (the literal channel). Minted HERE (Invariant 7: the
     * compiler layers own typed nodes), for the database-mode verdict. */
    /** A String the program spells as one literal token -- through the pretty-print natives, which are the identity up
     *  to whitespace on a JSON text -- or null. A chain of literals joined by {@code +} is null: a COMPARED text is the
     *  database's to compute, never the compiler's join (item 3's S7, the user's Q-D of 2026-11-21 and Q-S7b of
     *  2026-11-22: a joined golden refused by name until a use appears; none on any database judge's lane). */
    public static @com.legend.base.Nullable String stringToken(TypedSpec s) {
        s = throughJsonPrettyPrint(s);
        return s instanceof com.legend.compiler.spec.typed.TypedCString c ? c.value() : null;
    }

    /** {@code x->parseJSON()->toPrettyJSONString()} and {@code x->toPrettyJSONString()}
     * are the identity up to whitespace on a JSON text — the canonical form
     * erases exactly that, so the verdict reads through them to {@code x}. */
    public static TypedSpec throughJsonPrettyPrint(TypedSpec s) {
        TypedSpec cur = s;
        while (cur instanceof TypedNativeCall n && n.args().size() == 1
                && (n.callee().qualifiedName().equals("meta::json::toPrettyJSONString")
                        || n.callee().qualifiedName().equals("meta::json::parseJSON"))) {
            cur = n.args().get(0);
        }
        return cur;
    }

    /** A JSON golden, as its judge read it ({@code testexecutionplan.JudgeJson}, register S144), in its CANONICAL text (compact,
     * keys sorted — {@code Json.canonical}). */
    public static com.legend.compiler.spec.typed.TypedCString canonicalJsonGolden(@com.legend.base.Nullable Object parsed) {
        String canon = com.legend.sql.Json.canonical(parsed);
        return new com.legend.compiler.spec.typed.TypedCString(canon,
                new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE));
    }

    /** A JSON golden's ROOT ARRAY as its elements' canonical texts (a
     * literal String collection) — the peer of a document whose root order
     * the chain does not define (an unsorted graph fetch: the root objects
     * are a multiset). Null when the golden is not an array. */
    public static @com.legend.base.Nullable TypedSpec jsonRootElements(@com.legend.base.Nullable Object parsed) {
        if (!(parsed instanceof List<?> elements)) {
            return null;
        }
        List<TypedSpec> out = new ArrayList<>(elements.size());
        for (Object e : elements) {
            out.add(new TypedCString(com.legend.sql.Json.canonical(e), scalar(Type.Primitive.STRING)));
        }
        return new TypedCollection(out, new ExprType(Type.Primitive.STRING,
                new Multiplicity.Bounded(out.size(), out.size())));
    }

    /** {@code instanceOf(value, Type)} as a typed call — the meaning of
     * {@code assertInstanceOf} (the model's own subtype relation, lowered by
     * Scalars.instanceOfFold), judged as a condition. Null when the catalog
     * has no two-argument instanceOf. */
    public static @com.legend.base.Nullable TypedSpec instanceOfCondition(TypedSpec value,
            TypedSpec typeArg, SpecCompiler specs) {
        // a value of a CONCRETE primitive static type is of that type and no other at run time (no primitive but Number
        // and Date has a subtype), so instanceOf is the type system's own subtype relation, decided here by the
        // compiler -- legend-pure's answer for any value of that type (2026-11-03: the lowering's fold decides a class
        // type alone, so assertInstanceOf($dt, DateTime) and testHashCode's Integer walled under the database judge)
        String target = typeTarget(typeArg);
        if (target != null && value.info().multiplicity().equals(Multiplicity.Bounded.ONE)
                && value.info().type() instanceof Type.Primitive p && CONCRETE_PRIMITIVES.contains(p)) {
            return new com.legend.compiler.spec.typed.TypedCBoolean(
                    specs.ctx().isSubtype(p.qualifiedName(), target),
                    new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
        }
        return specs.ctx().findFunction(
                        com.legend.compiler.element.type.PlatformTypes.INSTANCE_OF)
                .stream().filter(f -> f.parameters().size() == 2).findFirst()
                .map(f -> (TypedSpec) new TypedNativeCall(f, List.of(value, typeArg),
                        new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)))
                .orElse(null);
    }

    /** The primitives no other primitive specializes: a value typed one of them is of exactly it at run time. Number
     *  and Date are abstract (Integer, Float, Decimal; StrictDate, DateTime), and LatestDate is a date's marker. */
    private static final java.util.Set<Type.Primitive> CONCRETE_PRIMITIVES = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(Type.Primitive.INTEGER, Type.Primitive.FLOAT, Type.Primitive.DECIMAL,
                    Type.Primitive.STRING, Type.Primitive.BOOLEAN, Type.Primitive.BYTE, Type.Primitive.STRICT_DATE,
                    Type.Primitive.DATE_TIME, Type.Primitive.STRICT_TIME));

    /** The type an {@code instanceOf}'s type argument names: a class or primitive {@code @Type}, or a bare type
     *  reference; null for any other argument. */
    private static @com.legend.base.Nullable String typeTarget(TypedSpec typeArg) {
        return switch (typeArg) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr -> tr.target() instanceof Type.ClassType c ? c.fqn()
                    : tr.target() instanceof Type.Primitive p ? p.qualifiedName() : null;
            case com.legend.compiler.spec.typed.TypedPackageableRef pr -> pr.fullPath();
            default -> null;
        };
    }

    /** A VERDICT FUNCTION CALL AS THE PREDICATE IT MEANS (2026-09-22): inside a
     * quantified lambda ({@code coll->forAll(x | assertEqWithinTolerance(...))}) an
     * assert is not a verdict of its own, it is the per-element condition —
     * {@code assert(p)} is {@code p}, {@code assertFalse(p)} is {@code not p},
     * {@code assertEquals(a, b)} is {@code a == b}, {@code assertEqWithinTolerance(a,
     * b, t)} is {@code abs(a - b) <= t}: the same conditions the verdict SQL spells for
     * the statement-root forms, minted here as typed natives so the quantified vector
     * plans them like any predicate. Null = not a verdict call with a predicate
     * spelling (the unroll stays the road). */
    public static @com.legend.base.Nullable TypedSpec assertAsPredicate(TypedSpec root, SpecCompiler specs) {
        String fqn = com.legend.compiler.spec.typed.Calls.calleeOf(root);
        List<TypedSpec> a = com.legend.compiler.spec.typed.Calls.argsOf(root);
        if (fqn == null) {
            return null;
        }
        var ctx = specs.ctx();
        if (fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT) && !a.isEmpty()) {
            return a.get(0);
        }
        if (fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT_FALSE) && !a.isEmpty()) {
            return native1(ctx, "meta::pure::functions::boolean::not", a.get(0), Type.Primitive.BOOLEAN);
        }
        if (fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS) && a.size() >= 2) {
            return native2(ctx, "meta::pure::functions::boolean::equal", a.get(0), a.get(1), Type.Primitive.BOOLEAN);
        }
        if (fqn.equals("meta::pure::functions::asserts::assertEqWithinTolerance") && a.size() >= 3) {
            // abs(a - b) <= t — the operator run is the parser's one-collection carrier
            TypedSpec diff = minus(ctx, a.get(0), a.get(1));
            TypedSpec abs = diff == null ? null : native1(ctx, "meta::pure::functions::math::abs", diff, Type.Primitive.NUMBER);
            return abs == null ? null
                    : native2(ctx, "meta::pure::functions::boolean::lessThanEqual", abs, a.get(2), Type.Primitive.BOOLEAN);
        }
        return null;
    }

    /** The name an assert's EXPECTED side stands under in the condition it means ({@link #sameValue}). */
    public static final String EXPECTED = "e";
    /** The name an assert's ACTUAL side stands under. */
    public static final String ACTUAL = "a";

    /** {@code collection->size()}: {@code assertSize}'s count as the PRODUCT counts it, the platform's own
     *  {@code size} applied to the side as it is typed (the equality program's first slice, 2026-11-26: an assert is
     *  the Pure condition it means, planned by the product's own road; until then the database judge counted the
     *  side's rows itself). The call is built BY DECLARATION over the side the typer already typed as the assert's
     *  argument, as the typer's own desugars build one ({@code TdsDesugars}): the exact declared function, never an
     *  overload picked by its arity. A rewrite the typer makes of a WRITTEN {@code size} call (a grid's cells,
     *  rows times columns) is not made here; the judge refuses such a side before it asks (a grid's envelope read). */
    public static TypedSpec sizeOf(TypedSpec collection, SpecCompiler specs) {
        return declared(specs, com.legend.builtin.Pure.SIZE__ANY_MANY, List.of(collection), Type.Primitive.INTEGER);
    }

    /** {@code $a == $e} ({@code want}) or {@code !($a == $e)}: the condition an assert of two values means, over its
     *  sides as the named values {@link #EXPECTED} and {@link #ACTUAL} of the types given, the comparison answered by
     *  the door's comparison setting as one written in a query is ({@link SpecCompiler#comparedAsInAQuery}): two
     *  sides of different types are lite's comparison across types, which its planner refuses by name. */
    public static TypedSpec sameValue(ExprType expected, ExprType actual, boolean want, SpecCompiler specs) {
        TypedSpec same = specs.comparedAsInAQuery(declared(specs, com.legend.builtin.Pure.EQUAL__ANY_MANY__ANY_MANY,
                List.of(new com.legend.compiler.spec.typed.TypedVariable(ACTUAL, actual),
                        new com.legend.compiler.spec.typed.TypedVariable(EXPECTED, expected)),
                Type.Primitive.BOOLEAN));
        return want ? same : declared(specs, com.legend.builtin.Pure.NOT__BOOLEAN_1, List.of(same),
                Type.Primitive.BOOLEAN);
    }

    /** A call of exactly {@code declaration}, a function the platform declares, by its identity: never absent, never
     *  another overload of its name (NO FALLBACKS, invariant 4; the audit of 2026-11-26, S1). */
    private static TypedNativeCall declared(SpecCompiler specs, com.legend.model.Function declaration,
            List<TypedSpec> args, Type out) {
        com.legend.model.FunctionId id = com.legend.model.FunctionId.of(declaration);
        var function = specs.ctx().findFunction(declaration.qualifiedName()).stream()
                .filter(f -> f.id().equals(id)).findFirst().orElseThrow(() -> new IllegalStateException(
                        "the platform declares no " + id + ": an assert's condition needs it"));
        return built(function, args, out);
    }

    /** The one place this class builds a call of a declared function over typed arguments, answering {@code out}. */
    private static TypedNativeCall built(com.legend.compiler.element.TypedFunction function, List<TypedSpec> args,
            Type out) {
        return new TypedNativeCall(function, args, scalar(out));
    }

    private static @com.legend.base.Nullable TypedSpec minus(com.legend.compiler.element.ModelContext ctx,
            TypedSpec l, TypedSpec r) {
        var fn = ctx.findFunction(com.legend.compiler.element.type.PlatformTypes.MINUS).stream()
                .filter(f -> f.parameters().size() == 1
                        && f.parameters().get(0).type() == Type.Primitive.NUMBER).findFirst().orElse(null);
        if (fn == null) {
            return null;
        }
        TypedSpec run = new com.legend.compiler.spec.typed.TypedCollection(List.of(l, r),
                new ExprType(Type.Primitive.NUMBER, Multiplicity.Bounded.ZERO_MANY), false, true);
        return new TypedNativeCall(fn, List.of(run), scalar(Type.Primitive.NUMBER));
    }

    private static @com.legend.base.Nullable TypedSpec native1(com.legend.compiler.element.ModelContext ctx,
            String fqn, TypedSpec x, Type out) {
        return ctx.findFunction(fqn).stream().filter(f -> f.parameters().size() == 1).findFirst()
                .map(f -> (TypedSpec) built(f, List.of(x), out)).orElse(null);
    }

    private static @com.legend.base.Nullable TypedSpec native2(com.legend.compiler.element.ModelContext ctx,
            String fqn, TypedSpec x, TypedSpec y, Type out) {
        return ctx.findFunction(fqn).stream().filter(f -> f.parameters().size() == 2).findFirst()
                .map(f -> (TypedSpec) built(f, List.of(x, y), out)).orElse(null);
    }

    /** The literal {@code 0} — assertTdsEquivalent's absent time delta. */
    public static TypedSpec zeroLiteral() {
        return new com.legend.compiler.spec.typed.TypedCInteger(0L,
                new ExprType(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE));
    }

    public static com.legend.compiler.spec.typed.TypedSpec tdsNullSentinel(
            com.legend.compiler.spec.typed.TypedSpec spec) {
        if (!(spec instanceof com.legend.compiler.spec.typed.TypedCollection c)) {
            return spec;
        }
        boolean any = false;
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> out = new java.util.ArrayList<>(c.elements().size());
        for (com.legend.compiler.spec.typed.TypedSpec e : c.elements()) {
            if (e instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                    && com.legend.compiler.element.type.PlatformTypes.TDS_NULL_FQN
                            .equals(ni.classFqn())
) {
                any = true;
                // a present null cell, typed (C5 step C, C-2: never the String 'TDSNull' it was spelled as)
                out.add(new com.legend.compiler.spec.typed.TypedNullCell(
                        new com.legend.compiler.element.type.ExprType(
                                com.legend.compiler.element.type.Type.Primitive.STRING,
                                new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1))));
            } else {
                out.add(e);
            }
        }
        if (!any) {
            return spec;
        }
        boolean allStrings = out.stream().allMatch(
                e -> e instanceof com.legend.compiler.spec.typed.TypedCString);
        com.legend.compiler.element.type.ExprType info = allStrings
                ? new com.legend.compiler.element.type.ExprType(
                        com.legend.compiler.element.type.Type.Primitive.STRING,
                        new com.legend.compiler.element.type.Multiplicity.Bounded(
                                out.size(), out.size()))
                : c.info();
        return new com.legend.compiler.spec.typed.TypedCollection(out, info,
                c.rowCells(), c.operatorRun());
    }


    // ── a rendered side (item 3's S4, phase 2, 2026-11-22): the database judge compares a rendered text as the String it
    // is (Q-A); what is read of the render is the VALUE it renders and whether that is a relation (AssertVerdicts
    // .renderedStatic). The render grammars that brought a golden to rows, and the golden's reader, went with the rows
    // comparison.

    /** A rendered side: the VALUE that was rendered; {@code grid}, whether it is a relation. */
    public record RenderedSide(TypedSpec value, boolean grid) {
    }

    /** The rendered value of a side, or null when the side is not a render
     * the grammar names; {@code chase} reads a let-bound variable through
     * to its value. Typed-tree navigation only. */
    public static @com.legend.base.Nullable RenderedSide renderedSide(TypedSpec s0,
            java.util.function.UnaryOperator<TypedSpec> chase) {
        TypedSpec s = chase.apply(s0);
        if (s instanceof TypedNativeCall rep
                && PlatformTypes.STRING_REPLACE.equals(rep.callee().qualifiedName())
                && rep.args().size() == 3
                && chase.apply(rep.args().get(0)) instanceof TypedNativeCall csv
                && PlatformTypes.TO_CSV.equals(csv.callee().qualifiedName())
                && csv.args().size() == 1
                && rep.args().get(1) instanceof TypedCString from && "\n".equals(from.value())
                && rep.args().get(2) instanceof TypedCString) {
            return new RenderedSide(csv.args().get(0), true);
        }
        if (s instanceof TypedNativeCall csv2
                && PlatformTypes.TO_CSV.equals(csv2.callee().qualifiedName())
                && csv2.args().size() == 1) {
            return new RenderedSide(csv2.args().get(0), true);
        }
        if (s instanceof TypedNativeCall ts
                && PlatformTypes.TO_STRING.equals(ts.callee().qualifiedName())
                && ts.args().size() == 1 && Type.isRelation(ts.args().get(0).info().type())) {
            return new RenderedSide(ts.args().get(0), true);
        }
        if (s instanceof TypedNativeCall j && isJoin(j) && j.args().size() == 2
                && j.args().get(1) instanceof TypedCString) {
            TypedSpec coll = chase.apply(j.args().get(0));
            // rows->map(r | $r.values->makeString(cs)) — the grid itself; or
            // rows->map(r | $r.values) — the cells flattened row-major
            if (coll instanceof com.legend.compiler.spec.typed.TypedMap map
                    && map.mapper() instanceof TypedLambda lam && lam.body().size() == 1
                    && rowsOf(chase.apply(map.source())) instanceof TypedSpec relation) {
                TypedSpec body = lam.body().get(0);
                if (body instanceof TypedNativeCall one && one.args().size() == 1
                        && one.callee().qualifiedName().equals(com.legend.builtin.Pure.Lite.TRUST_ONE)) {
                    body = one.args().get(0);   // the Typer's one-value read wrap
                }
                if (rowCellsRead(body)) {
                    return new RenderedSide(relation, true);
                }
                // rows->map(r | $r.<col>) (columnValues): the relation
                // restricted to that one column — a null cell is a NULL cell
                // there, the sentinel on both sides
                if (body instanceof TypedPropertyAccess pa
                        && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable pv
                        && lam.parameters().size() == 1 && lam.parameters().get(0).equals(pv.name())) {
                    return new RenderedSide(relation, true);
                }
                if (body instanceof TypedNativeCall inner && isJoin(inner) && inner.args().size() == 2
                        && inner.args().get(1) instanceof TypedCString
                        && rowCellsRead(inner.args().get(0))) {
                    return new RenderedSide(relation, true);
                }
            }
            if (coll.info().type() instanceof Type.Primitive
                    || coll.info().type() instanceof Type.EnumType) {
                return new RenderedSide(coll, false);
            }
        }
        return null;
    }

    /** The relation whose rows a {@code map} walks: {@code rel.rows} (the
     * Typer's marker), or the relation itself once the executor's splice
     * has erased the marker. Null when the source is not a relation. */
    private static @com.legend.base.Nullable TypedSpec rowsOf(TypedSpec source) {
        if (source instanceof TypedPropertyAccess rows && rows.property().equals("rows")
                && Type.isRelation(rows.source().info().type())) {
            return rows.source();
        }
        return Type.isRelation(source.info().type()) ? source : null;
    }

    private static boolean isJoin(TypedNativeCall c) {
        String fqn = c.callee().qualifiedName();
        return PlatformTypes.STRING_MAKE_STRING.equals(fqn) || PlatformTypes.STRING_JOIN_STRINGS.equals(fqn);
    }

    /** {@code $r.values} (the Typer's row-cells collection), or the same
     * through {@code ->map(x | $x->toString())}. */
    private static boolean rowCellsRead(TypedSpec s) {
        if (s instanceof TypedCollection c && c.rowCells()) {
            return true;
        }
        // the Typer's IDENTITY form of a row's values: the row itself (a
        // relation-typed variable), or the values read off it
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v
                && Type.isRelation(v.info().type())) {
            return true;
        }
        if (s instanceof TypedPropertyAccess pa && pa.property().equals("values")
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable) {
            return true;
        }
        return s instanceof com.legend.compiler.spec.typed.TypedMap m
                && m.mapper() instanceof TypedLambda lam && lam.body().size() == 1
                && lam.body().get(0) instanceof TypedNativeCall ts
                && PlatformTypes.TO_STRING.equals(ts.callee().qualifiedName())
                && rowCellsRead(m.source());
    }

    /** The declared schema with its WIRE-DECIDED columns (String / unrefined
     * Number / Any — {@code dataTypeTransformer}'s identity arm) typed by the
     * plan's output SLOTS (the slot is the wire): a rendered golden's cell is
     * what the wire printed, so its kind is the wire's — {@code addressId :
     * String} over {@code addressTable.ID INT} prints {@code 12}, an Integer
     * cell (leg 3.3). Every other declaration keeps its kind (a Float
     * declaration converts the wire cell and carries the grid leniency). */
    public static Type.RelationType wireDecidedKinds(Type.RelationType declared,
            List<com.legend.sql.OutputCol> outputs) {
        if (outputs.size() != declared.columns().size()) {
            return declared;
        }
        List<Type.Column> cols = new ArrayList<>(declared.columns().size());
        boolean changed = false;
        for (int i = 0; i < outputs.size(); i++) {
            Type.Column c = declared.columns().get(i);
            Type wire = Type.kindOfSqlType(outputs.get(i).type());
            if (Type.wireDecided(c.type()) && wire != null && wire != c.type()) {
                cols.add(new Type.Column(c.name(), wire, c.multiplicity()));
                changed = true;
            } else {
                cols.add(c);
            }
        }
        return changed ? new Type.RelationType(cols, declared.dynamicColumns()) : declared;
    }

    /** RFC4180 cells: a quoted cell may hold the separator and doubled quotes. */
    static List<String> csvCells(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (quoted) {
                if (ch == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cur.append(ch);
                }
            } else if (ch == '"' && cur.length() == 0) {
                quoted = true;
            } else if (ch == ',') {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }

}
