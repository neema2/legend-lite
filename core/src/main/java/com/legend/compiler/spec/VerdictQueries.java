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
    /** The FIRST-statement form of an indexed SQL read ({@code
     * sqlRemoveFormatting($res, n)} → {@code sqlRemoveFormatting($res)}):
     * the verdict arm reads our one statement where the engine's plan
     * names its n-th (batch 67). Compiler-layer minting (Invariant 7). */
    public static com.legend.compiler.spec.typed.TypedUserCall firstStatementRead(
            com.legend.compiler.spec.typed.TypedUserCall read) {
        return new com.legend.compiler.spec.typed.TypedUserCall(
                read.callee(), List.of(read.args().get(0)), read.info());
    }

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

    public static TypedSpec fromWrapped(TypedSpec query,
            com.legend.compiler.spec.typed.TypedPackageableRef mapping) {
        return fromWrapped(query, mapping,
                com.legend.compiler.spec.typed.ExecutionContext.NONE);
    }

    /** The verdict's read wrapped in the FRAME's own bound context (the
     * producer's post-processors, time zone, options) under the mapping. */
    public static TypedSpec fromWrapped(TypedSpec query,
            com.legend.compiler.spec.typed.TypedPackageableRef mapping,
            com.legend.compiler.spec.typed.ExecutionContext base) {
        return new com.legend.compiler.spec.typed.TypedFrom(query,
                base.withMapping(java.util.Optional.of(mapping)), query.info());
    }

    /** assertSameSQL's OUR-TEXT read (charter §8.3b): the
     * {@code sqlRemoveFormatting($result)} call over the assert's own
     * Result argument — the envelope splice folds it to the frame's
     * EXECUTED SQL text (ResultEnvelopeSplice.sqlProducerCall). Null
     * when the model does not know the Result overload (the arm then
     * leaves the shape on its current path). */
    public static @com.legend.base.Nullable TypedSpec sqlStripRead(
            TypedSpec resultArg,
            com.legend.compiler.element.ModelContext ctx) {
        for (var f : ctx.findFunction(
                ResultEnvelopeSplice.SQL_REMOVE_FORMATTING_FQN)) {
            if (f.parameters().size() == 1
                    && f.parameters().get(0).type()
                            != Type.Primitive.STRING) {
                return new com.legend.compiler.spec.typed.TypedUserCall(f,
                        List.of(resultArg),
                        new ExprType(Type.Primitive.STRING,
                                Multiplicity.Bounded.ONE));
            }
        }
        return null;
    }

    /** assertSameSQL's OUR-ROWS read (§8.3b): {@code $result.values} —
     * the envelope splice swaps it for the frame's typed chain
     * (ResultEnvelopeSplice.valuesRead), which carries the REAL
     * result type; the minted info is a pre-splice placeholder. */
    public static TypedSpec valuesRead(TypedSpec resultArg) {
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                resultArg, "values", resultArg.info());
    }

    /** SQLTEXT charter §5 (the plan replayer, slice 4) — REFEREE
     * PARAMETER BINDINGS for a plan lambda: each scalar parameter
     * binds a fixed referee value as a minted {@code TypedLet} (the
     * verdict layer appends them to the let prefix — parameters
     * resolve exactly like test-body lets, no substitution walk).
     * {@code spellings} pairs each name with the SQL literal TEXT the
     * golden's <code>${'$'}{name}</code> hole fills with (the golden
     * supplies its own quoting). Null when any parameter is not a
     * bindable scalar (enum/class/collection — the arm declines
     * COUNTED; the charter's measure-first residue). */
    public record PlanBindings(List<TypedSpec> lets,
            java.util.Map<String, String> spellings,
            java.util.Map<String, List<String>> lists) {
        public PlanBindings(List<TypedSpec> lets,
                java.util.Map<String, String> spellings) {
            this(lets, spellings, singletons(spellings));
        }

        private static java.util.Map<String, List<String>> singletons(
                java.util.Map<String, String> spellings) {
            java.util.Map<String, List<String>> out = new java.util.LinkedHashMap<>();
            spellings.forEach((k, v) -> out.put(k, List.of(v)));
            return out;
        }
    }

    public static @com.legend.base.Nullable PlanBindings refereeBindings(
            com.legend.compiler.spec.typed.TypedLambda lam) {
        Type.FunctionType ft = com.legend.compiler.element.type
                .PlatformTypes.functionTypeOf(lam.info().type());
        if (ft == null || ft.params().size() != lam.parameters().size()) {
            return null;
        }
        List<TypedSpec> lets = new java.util.ArrayList<>();
        java.util.Map<String, String> spellings =
                new java.util.LinkedHashMap<>();
        java.util.Map<String, List<String>> lists =
                new java.util.LinkedHashMap<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            String name = lam.parameters().get(i);
            Type pt = ft.params().get(i).type();
            TypedSpec value;
            String spelling;
            if (ft.params().get(i).multiplicity().isMany()
                    && (pt == Type.Primitive.STRING
                            || pt == Type.Primitive.INTEGER)) {
                // a COLLECTION parameter (batch 66): two fixed referee
                // elements — the plan's template operations
                // (collectionSize, renderCollection) evaluate over them
                // at the oracle; our side runs the same two
                boolean str = pt == Type.Primitive.STRING;
                List<TypedSpec> elems = str
                        ? List.of(new com.legend.compiler.spec.typed.TypedCString(
                                        "A", scalar(pt)),
                                new com.legend.compiler.spec.typed.TypedCString(
                                        "B", scalar(pt)))
                        : List.of(new com.legend.compiler.spec.typed.TypedCInteger(
                                        22L, scalar(pt)),
                                new com.legend.compiler.spec.typed.TypedCInteger(
                                        23L, scalar(pt)));
                ExprType many = new ExprType(pt, Multiplicity.Bounded.ZERO_MANY);
                lets.add(new com.legend.compiler.spec.typed.TypedLet(name,
                        new com.legend.compiler.spec.typed.TypedCollection(
                                elems, many), many));
                lists.put(name, str ? List.of("A", "B") : List.of("22", "23"));
                continue;
            }
            if (pt == Type.Primitive.STRING) {
                value = new com.legend.compiler.spec.typed.TypedCString(
                        "A", scalar(Type.Primitive.STRING));
                spelling = "A";
            } else if (pt == Type.Primitive.INTEGER) {
                value = new com.legend.compiler.spec.typed.TypedCInteger(
                        22L, scalar(Type.Primitive.INTEGER));
                spelling = "22";
            } else if (pt == Type.Primitive.FLOAT
                    || pt == Type.Primitive.NUMBER) {
                value = new com.legend.compiler.spec.typed.TypedCFloat(
                        1.0, new java.math.BigDecimal("1.0"),
                        scalar(Type.Primitive.FLOAT));
                spelling = "1.0";
            } else if (pt == Type.Primitive.BOOLEAN) {
                value = new com.legend.compiler.spec.typed.TypedCBoolean(
                        true, scalar(Type.Primitive.BOOLEAN));
                spelling = "true";
            } else if (pt == Type.Primitive.DATE
                    || pt == Type.Primitive.STRICT_DATE) {
                value = new com.legend.compiler.spec.typed.TypedCDate(
                        com.legend.values.PureDateLiteral.parse(
                                "2015-10-16"), scalar(pt));
                spelling = "2015-10-16";
            } else if (pt == Type.Primitive.DATE_TIME) {
                value = new com.legend.compiler.spec.typed.TypedCDate(
                        com.legend.values.PureDateLiteral.parse(
                                "2015-10-16T00:00:00"), scalar(pt));
                spelling = "2015-10-16 00:00:00";
            } else {
                return null;
            }
            lets.add(new com.legend.compiler.spec.typed.TypedLet(
                    name, value, value.info()));
            spellings.put(name, spelling);
            lists.put(name, List.of(spelling));
        }
        return new PlanBindings(lets, spellings, lists);
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
    /** Whether a typed chain is a SUB-COLLECTION of a class extent:
     * getAll through filter/sort/limit/slice/drop/from/first/last/toOne
     * (the walk lane's extentSubset, on the typed tree). */
    public static boolean extentSubset(TypedSpec n) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedGetAll g -> true;
            case com.legend.compiler.spec.typed.TypedFilter f ->
                    extentSubset(f.source());
            case com.legend.compiler.spec.typed.TypedSort s ->
                    extentSubset(s.source());
            case com.legend.compiler.spec.typed.TypedSortBy s ->
                    extentSubset(s.source());
            case com.legend.compiler.spec.typed.TypedLimit l ->
                    extentSubset(l.source());
            case com.legend.compiler.spec.typed.TypedSlice l ->
                    extentSubset(l.source());
            case com.legend.compiler.spec.typed.TypedDrop d ->
                    extentSubset(d.source());
            case com.legend.compiler.spec.typed.TypedFrom f ->
                    extentSubset(f.source());
            case com.legend.compiler.spec.typed.TypedNativeCall c when !c.args().isEmpty() -> {
                String q = c.callee().qualifiedName();
                String simple = q.substring(q.lastIndexOf(':') + 1);
                yield switch (simple) {
                    case "first", "last", "toOne", "take", "limit", "drop",
                            "slice" -> extentSubset(c.args().get(0));
                    default -> false;
                };
            }
            default -> false;
        };
    }

    public static TypedSpec activitiesRead(TypedSpec frameVar) {
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                frameVar, "activities", frameVar.info());
    }

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
            com.legend.compiler.element.ModelContext ctx,
            java.util.function.Function<TypedSpec, List<Object>> fetch) {
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
            List<TypedSpec> left = armElements(z.args().get(0), letPrefix, fetch);
            List<TypedSpec> right = armElements(z.args().get(1), letPrefix, fetch);
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

    private static @com.legend.base.Nullable List<TypedSpec> armElements(TypedSpec arm0,
            List<TypedSpec> letPrefix,
            java.util.function.Function<TypedSpec, List<Object>> fetch) {
        TypedSpec arm = com.legend.compiler.spec.typed.Lets.bound(arm0, letPrefix);
        if (arm instanceof TypedCollection c) {
            return c.elements();
        }
        List<TypedSpec> out = new java.util.ArrayList<>();
        for (Object v : fetch.apply(arm)) {
            TypedSpec lit = literalSpec(v);
            if (lit == null) {
                return null;
            }
            out.add(lit);
        }
        return out;
    }

    /** A database value as the literal spec that spells it; null when
     * the value has no literal spelling (dates, structures). */
    public static @com.legend.base.Nullable TypedSpec literalSpec(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case Long l -> new com.legend.compiler.spec.typed.TypedCInteger(l,
                    ExprType.one(Type.Primitive.INTEGER));
            case Integer i -> new com.legend.compiler.spec.typed.TypedCInteger((long) i,
                    ExprType.one(Type.Primitive.INTEGER));
            case Double d -> new com.legend.compiler.spec.typed.TypedCFloat(d, null,
                    ExprType.one(Type.Primitive.FLOAT));
            case Float f -> new com.legend.compiler.spec.typed.TypedCFloat(f, null,
                    ExprType.one(Type.Primitive.FLOAT));
            case java.math.BigDecimal bd -> new com.legend.compiler.spec.typed.TypedCDecimal(bd,
                    ExprType.one(Type.Primitive.DECIMAL));
            case String str -> new com.legend.compiler.spec.typed.TypedCString(str,
                    ExprType.one(Type.Primitive.STRING));
            case Boolean b -> new com.legend.compiler.spec.typed.TypedCBoolean(b,
                    ExprType.one(Type.Primitive.BOOLEAN));
            case null, default -> null;
        };
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
        TypedSpec bare = com.legend.compiler.spec.typed.Lets.bare(stmt);
        if (bare instanceof TypedNativeCall an
                && an.callee().qualifiedName().startsWith(com.legend.compiler.element.type.PlatformTypes.ASSERTS_PACKAGE)) {
            // the MESSAGE arguments drop; the value arity is the assert's
            // own (assertEqWithinTolerance carries its delta as a third
            // VALUE — assertEqWithinTolerance.pure:22)
            int keep = "meta::pure::functions::asserts::assertEqWithinTolerance"
                    .equals(an.callee().qualifiedName()) ? 3 : 2;
            if (an.args().size() > keep) {
                bare = new TypedNativeCall(an.callee(), an.args().subList(0, keep), an.info(), an.pos());
            }
        }
        reduced.set(last, bare);
        return reduced;
    }

    /** An expected literal collection's {@code ^TDSNull()} elements as the
     * string {@code 'TDSNull'} (the golden convention's null-cell value;
     * the peer rule spells it bare on the expected side). All-string
     * afterwards → a String collection; otherwise the collection keeps
     * its mixed stamp (the literal channel). Minted HERE (Invariant 7: the
     * compiler layers own typed nodes), for the database-mode verdict. */
    /** A String constant written as a chain of literals joined by {@code +}
     * ({@code '[' + '{...},' + ']'} — the corpus's golden spelling), folded
     * to ONE literal at compile time; null when any piece is not a literal
     * (the value is not a constant). A constant fold over literals — no
     * data touched. */
    public static @com.legend.base.Nullable String foldedStringLiteral(TypedSpec s) {
        s = throughJsonPrettyPrint(s);
        if (s instanceof com.legend.compiler.spec.typed.TypedCString c) {
            return c.value();
        }
        if (s instanceof TypedNativeCall n
                && com.legend.compiler.element.type.PlatformTypes.STRING_PLUS
                        .equals(n.callee().qualifiedName())) {
            StringBuilder b = new StringBuilder();
            for (TypedSpec a : n.args()) {
                if (a instanceof TypedCollection col) {
                    for (TypedSpec e : col.elements()) {
                        String piece = foldedStringLiteral(e);
                        if (piece == null) {
                            return null;
                        }
                        b.append(piece);
                    }
                } else {
                    String piece = foldedStringLiteral(a);
                    if (piece == null) {
                        return null;
                    }
                    b.append(piece);
                }
            }
            return b.toString();
        }
        return null;
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

    /** A JSON golden in its CANONICAL text (compact, keys sorted —
     * {@code Json.canonical}); {@code rootMany} = the query's root is
     * many-valued, so a golden written as a bare object stands for the
     * engine's single-result print of a one-element array and is wrapped
     * {@code [...]} — the same verdict as the engine's {@code [x] ≡ x}
     * root rule, decided at compile time. Null when the text does not
     * parse (the golden itself is defective — named, never guessed). */
    public static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedCString
            canonicalJsonGolden(String text, boolean rootMany) {
        Object parsed;
        try {
            parsed = com.legend.sql.Json.parseOne(text);
        } catch (IllegalStateException malformed) {
            return null;   // the parser's own refusal; anything else stays loud
        }
        String canon = com.legend.sql.Json.canonical(parsed);
        if (rootMany && parsed instanceof java.util.Map) {
            canon = "[" + canon + "]";
        }
        return new com.legend.compiler.spec.typed.TypedCString(canon,
                new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE));
    }

    /** A JSON golden's ROOT ARRAY as its elements' canonical texts (a
     * literal String collection) — the peer of a document whose root order
     * the chain does not define (an unsorted graph fetch: the root objects
     * are a multiset). Null when the golden is not an array. */
    public static @com.legend.base.Nullable TypedSpec jsonRootElements(String text) {
        Object parsed;
        try {
            parsed = com.legend.sql.Json.parseOne(text);
        } catch (IllegalStateException malformed) {
            return null;
        }
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
        return specs.ctx().findFunction(
                        com.legend.compiler.element.type.PlatformTypes.INSTANCE_OF)
                .stream().filter(f -> f.parameters().size() == 2).findFirst()
                .map(f -> (TypedSpec) new TypedNativeCall(f, List.of(value, typeArg),
                        new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)))
                .orElse(null);
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
                .map(f -> (TypedSpec) new TypedNativeCall(f, List.of(x), scalar(out))).orElse(null);
    }

    private static @com.legend.base.Nullable TypedSpec native2(com.legend.compiler.element.ModelContext ctx,
            String fqn, TypedSpec x, TypedSpec y, Type out) {
        return ctx.findFunction(fqn).stream().filter(f -> f.parameters().size() == 2).findFirst()
                .map(f -> (TypedSpec) new TypedNativeCall(f, List.of(x, y), scalar(out))).orElse(null);
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
                            .equals(ni.classFqn())) {
                any = true;
                out.add(new com.legend.compiler.spec.typed.TypedCString("TDSNull",
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


    // ── bucket 8 (homework §4s): the rendered-text LAW — a render function's
    // text equals a golden iff the rendered VALUE equals the golden parsed by
    // that function's own grammar. The golden is a compile-time constant and
    // is brought to ROWS here, typed by the rendered relation's declared
    // column kinds (the peer of the grid verdict) or a flat join's element
    // kind. Nothing about the text is judged at run time.

    /** The grammar a rendered side was produced by. */
    public sealed interface RenderGrammar {
        /** {@code toCSV}: a header line, data lines, a trailing newline
         * ({@code rowSep} is the newline, or the replacement of a
         * {@code ->replace('\n', sep)}); RFC4180 cells. */
        record Csv(String rowSep) implements RenderGrammar {
        }
        /** {@code toString} over a relation: the {@code #TDS} frame, a
         * three-space-prefixed header line and rows, {@code #}. */
        record Tds() implements RenderGrammar {
        }
        /** {@code rows->map(r | $r.values->makeString(cellSep))->makeString(rowSep)}. */
        record Rows(String rowSep, String cellSep) implements RenderGrammar {
        }
        /** {@code collection->makeString(sep)} over a primitive collection. */
        record Flat(String sep) implements RenderGrammar {
        }
    }

    /** A rendered side: the VALUE that was rendered and the grammar that
     * rendered it. {@code grid} = the value is a relation. */
    public record RenderedSide(TypedSpec value, RenderGrammar grammar, boolean grid,
            @com.legend.base.Nullable String restrictTo) {
        public RenderedSide(TypedSpec value, RenderGrammar grammar, boolean grid) {
            this(value, grammar, grid, null);
        }
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
                && rep.args().get(2) instanceof TypedCString to) {
            return new RenderedSide(csv.args().get(0), new RenderGrammar.Csv(to.value()), true);
        }
        if (s instanceof TypedNativeCall csv2
                && PlatformTypes.TO_CSV.equals(csv2.callee().qualifiedName())
                && csv2.args().size() == 1) {
            return new RenderedSide(csv2.args().get(0), new RenderGrammar.Csv("\n"), true);
        }
        if (s instanceof TypedNativeCall ts
                && PlatformTypes.TO_STRING.equals(ts.callee().qualifiedName())
                && ts.args().size() == 1 && Type.isRelation(ts.args().get(0).info().type())) {
            return new RenderedSide(ts.args().get(0), new RenderGrammar.Tds(), true);
        }
        if (s instanceof TypedNativeCall j && isJoin(j) && j.args().size() == 2
                && j.args().get(1) instanceof TypedCString sep) {
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
                    return new RenderedSide(relation,
                            new RenderGrammar.Rows(sep.value(), sep.value()), true);
                }
                // rows->map(r | $r.<col>) (columnValues): the relation
                // restricted to that one column — a null cell is a NULL cell
                // there, the sentinel on both sides
                if (body instanceof TypedPropertyAccess pa
                        && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable pv
                        && lam.parameters().size() == 1 && lam.parameters().get(0).equals(pv.name())) {
                    return new RenderedSide(relation,
                            new RenderGrammar.Rows(sep.value(), sep.value()), true, pa.property());
                }
                if (body instanceof TypedNativeCall inner && isJoin(inner) && inner.args().size() == 2
                        && inner.args().get(1) instanceof TypedCString cs
                        && rowCellsRead(inner.args().get(0))) {
                    return new RenderedSide(relation,
                            new RenderGrammar.Rows(sep.value(), cs.value()), true);
                }
            }
            if (coll.info().type() instanceof Type.Primitive
                    || coll.info().type() instanceof Type.EnumType) {
                return new RenderedSide(coll, new RenderGrammar.Flat(sep.value()), false);
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

    /** The relation restricted to one declared column (the Typer's own
     * {@code select} node) — {@code schema} the PLANNED side's schema; null
     * when the column is not declared. */
    public static @com.legend.base.Nullable TypedSpec restrictedTo(TypedSpec relation,
            @com.legend.base.Nullable Type.RelationType schema, String column) {
        if (schema == null) {
            return null;
        }
        Type.Column col = schema.columns().stream().filter(c -> c.name().equals(column))
                .findFirst().orElse(null);
        if (col == null) {
            return null;
        }
        Type one = new Type.RelationType(List.of(col));
        Type t = relation.info().type() instanceof Type.GenericType g
                ? new Type.GenericType(g.rawFqn(), List.of(one), g.multArguments()) : one;
        return new com.legend.compiler.spec.typed.TypedSelect(relation, List.of(column),
                new ExprType(t, relation.info().multiplicity()));
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

    /** The golden text of a side: a string constant, a one-element list of
     * one, or a folded {@code +} chain of constants. */
    public static @com.legend.base.Nullable String goldenText(TypedSpec s0,
            java.util.function.UnaryOperator<TypedSpec> chase) {
        TypedSpec s = chase.apply(s0);
        if (s instanceof TypedCollection c && c.elements().size() == 1) {
            s = chase.apply(c.elements().get(0));
        }
        return foldedStringLiteral(s);
    }

    /** The parsed golden: the language's own TDS literal (a typed VALUES
     * relation of the rendered relation's schema) for a grid grammar, a
     * typed literal collection for a flat join; or the reason it could not
     * be brought to rows. {@code headerMismatch} is a STATIC verdict (both
     * sides compile-time facts), never an unjudged. */
    public record ParsedGolden(@com.legend.base.Nullable TypedSpec literal,
            @com.legend.base.Nullable String reason, boolean headerMismatch) {
        public static ParsedGolden of(TypedSpec literal) {
            return new ParsedGolden(literal, null, false);
        }
        public static ParsedGolden declined(String reason) {
            return new ParsedGolden(null, reason, false);
        }
    }

    /** Brings a rendered golden to rows. {@code schema} is the rendered
     * relation's schema for the grid grammars ({@code elementKind} unused);
     * {@code elementKind} the collection's element kind for a flat join. */
    public static ParsedGolden parseRendered(String text, RenderGrammar grammar,
            @com.legend.base.Nullable Type.RelationType schema, @com.legend.base.Nullable Type elementKind) {
        if (grammar instanceof RenderGrammar.Flat f) {
            Type kind = elementKind;
            if (!(kind instanceof Type.Primitive) && !(kind instanceof Type.EnumType)) {
                return ParsedGolden.declined("rendered-text: flat join over a non-primitive element kind "
                        + elementKind);
            }
            List<String> parts = text.isEmpty() ? List.of()
                    : List.of(text.split(java.util.regex.Pattern.quote(f.sep()), -1));
            List<TypedSpec> out = new ArrayList<>(parts.size());
            for (String part : parts) {
                if (part.equals("TDSNull")) {
                    // a null element: the collection statement reads the
                    // non-null elements, so the null's count is not judged
                    return ParsedGolden.declined("rendered-text: a null element in a flat join");
                }
                TypedSpec e = cellLiteral(part, kind, false);
                if (e == null) {
                    return ParsedGolden.declined("rendered-text: element '" + part + "' is not a " + kind);
                }
                out.add(e);
            }
            return ParsedGolden.of(collectionOf(out));
        }
        if (schema == null || schema.columns().isEmpty() || !schema.dynamicColumns().isEmpty()) {
            return ParsedGolden.declined("rendered-text: the rendered relation's columns are late-bound ("
                    + (schema == null ? "no schema" : schema.columns().size() + " static, "
                    + schema.dynamicColumns().size() + " dynamic") + ")");
        }
        List<String> names = schema.columns().stream().map(Type.Column::name).toList();
        List<List<String>> rows = new ArrayList<>();
        List<String> header;
        if (grammar instanceof RenderGrammar.Csv csv && csv.rowSep().equals(",")) {
            // toCSV->replace('\n', ','): the row boundaries are gone — the
            // text is one cell sequence (RFC4180), header first, '' last
            List<String> tokens = csvCells(text);
            int width = names.size();
            if (tokens.size() < width + 1 || !tokens.get(tokens.size() - 1).isEmpty()
                    || (tokens.size() - 1) % width != 0) {
                return ParsedGolden.declined("rendered-text: toCSV cells joined by ',' do not chunk"
                        + " by the width " + width + " (" + tokens.size() + " cells)");
            }
            header = tokens.subList(0, width);
            for (int i = width; i + width <= tokens.size() - 1; i += width) {
                rows.add(tokens.subList(i, i + width));
            }
        } else if (grammar instanceof RenderGrammar.Csv csv) {
            List<String> lines = List.of(text.split(java.util.regex.Pattern.quote(csv.rowSep()), -1));
            if (lines.size() < 2 || !lines.get(lines.size() - 1).isEmpty()) {
                return ParsedGolden.declined(
                        "rendered-text: toCSV text without its header line and trailing newline");
            }
            header = csvCells(lines.get(0));
            List<String> data = lines.subList(1, lines.size() - 1);
            if (data.size() == 1 && data.get(0).isEmpty()) {
                // the EMPTY relation prints one blank data line (the rows
                // join's '\n' prefix); for one column the same text is one
                // NULL cell — undecidable from the text
                if (names.size() == 1) {
                    return ParsedGolden.declined("rendered-text: one-column toCSV text with a blank line —"
                            + " an empty relation or one NULL cell");
                }
            } else {
                for (String line : data) {
                    rows.add(csvCells(line));
                }
            }
        } else if (grammar instanceof RenderGrammar.Tds) {
            List<String> lines = List.of(text.split("\n", -1));
            if (lines.size() < 3 || !lines.get(0).equals("#TDS")
                    || !lines.get(lines.size() - 1).equals("#")) {
                return ParsedGolden.declined("rendered-text: TDS text without its #TDS frame");
            }
            header = tdsHeader(lines.get(1));
            for (String line : lines.subList(2, lines.size() - 1)) {
                if (line.isEmpty()) {
                    continue;   // an empty relation prints one blank rows segment
                }
                rows.add(List.of(stripIndent(line).split(",", -1)));
            }
        } else {
            RenderGrammar.Rows r = (RenderGrammar.Rows) grammar;
            header = names;
            if (!text.isEmpty()) {
                if (r.rowSep().equals(r.cellSep())) {
                    // the cells flattened row-major: chunk by the width
                    List<String> tokens = List.of(text.split(java.util.regex.Pattern.quote(r.cellSep()), -1));
                    int width = names.size();
                    if (tokens.size() % width != 0) {
                        return ParsedGolden.declined("rendered-text: " + tokens.size()
                                + " flattened cells do not chunk by the width " + width);
                    }
                    for (int i = 0; i < tokens.size(); i += width) {
                        rows.add(tokens.subList(i, i + width));
                    }
                } else {
                    for (String line : text.split(java.util.regex.Pattern.quote(r.rowSep()), -1)) {
                        rows.add(List.of(line.split(java.util.regex.Pattern.quote(r.cellSep()), -1)));
                    }
                }
            }
        }
        if (!header.equals(names)) {
            return new ParsedGolden(null, "rendered-text: header " + header
                    + " differs from the columns " + names, true);
        }
        List<Type> kinds = new ArrayList<>(names.size());
        for (int c = 0; c < names.size(); c++) {
            Type k = schema.columns().get(c).type();
            kinds.add(k instanceof Type.ClassType ct && PlatformTypes.isAny(ct) && !rows.isEmpty()
                    ? TdsChecker.inferredType(rows, c) : k);
        }
        List<List<String>> cells = new ArrayList<>();
        List<Boolean> nullable = new ArrayList<>(java.util.Collections.nCopies(names.size(), false));
        for (List<String> row : rows) {
            if (row.size() != names.size()) {
                return ParsedGolden.declined("rendered-text: a golden row has " + row.size()
                        + " cells for " + names.size() + " columns (" + row + ")");
            }
            List<String> out = new ArrayList<>(row.size());
            for (int c = 0; c < row.size(); c++) {
                String cell = tdsCellText(row.get(c), kinds.get(c));
                if (cell == null) {
                    return ParsedGolden.declined("rendered-text: cell '" + row.get(c) + "' is not a "
                            + kinds.get(c) + " (column " + names.get(c) + ")");
                }
                if (cell.isEmpty()) {
                    nullable.set(c, true);
                }
                out.add(cell);
            }
            cells.add(out);
        }
        List<Type.Column> columns = new ArrayList<>(names.size());
        for (int c = 0; c < names.size(); c++) {
            Type.Column col = schema.columns().get(c);
            columns.add(new Type.Column(col.name(), kinds.get(c),
                    nullable.get(c) ? Multiplicity.Bounded.ZERO_ONE : col.multiplicity()));
        }
        return ParsedGolden.of(new com.legend.compiler.spec.typed.TypedTds(cells,
                new ExprType(new Type.GenericType(PlatformTypes.TDS_RELATION_CLASS,
                        List.of(new Type.RelationType(columns))), Multiplicity.Bounded.ONE)));
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

    /** A late-bound grid's schema read off its PLAN's outputs (a raw
     * executeInDb relation has no static columns; the plan's output kinds
     * are wire facts). Null when an output's kind has no pure kind. */
    public static @com.legend.base.Nullable Type.RelationType wireSchema(
            List<com.legend.sql.OutputCol> outputs) {
        List<Type.Column> cols = new ArrayList<>(outputs.size());
        for (com.legend.sql.OutputCol o : outputs) {
            Type kind = Type.kindOfSqlType(o.type());
            // an output the wire does not kind (a raw SQL grid's column) is
            // typed by the golden's own cells, as an unannotated TDS literal
            // column is (TdsChecker.inferredType) — marked Any here
            cols.add(new Type.Column(o.name(),
                    kind == null ? new Type.ClassType(PlatformTypes.ANY) : kind,
                    o.nullable() ? Multiplicity.Bounded.ZERO_ONE : Multiplicity.Bounded.ONE));
        }
        return cols.isEmpty() ? null : new Type.RelationType(cols);
    }

    /** A grid cell's text for the TDS literal: an empty / {@code TDSNull} /
     * {@code null} cell is the null cell (empty text); a cell that is not of
     * its column's kind is {@code null}. The literal's lowering types the
     * text by the column ({@code Scalars.tdsCell}). */
    private static @com.legend.base.Nullable String tdsCellText(String cell, Type kind) {
        if (cell.isEmpty() || cell.equals("TDSNull") || cell.equals("null")) {
            return "";
        }
        return cellLiteral(cell, kind, false) == null ? null : cell;
    }

    /** One cell as the typed literal of its declared kind; {@code null} when
     * the text is not of that kind. A grid's empty / {@code TDSNull} /
     * {@code null} cell is the TDSNull sentinel string (the peer rule spells
     * it bare on the expected side). */
    private static @com.legend.base.Nullable TypedSpec cellLiteral(String cell, Type kind, boolean gridCell) {
        if (gridCell && (cell.isEmpty() || cell.equals("TDSNull") || cell.equals("null"))) {
            return new com.legend.compiler.spec.typed.TypedCString("TDSNull", scalar(Type.Primitive.STRING));
        }
        if (kind instanceof Type.EnumType et) {
            return new com.legend.compiler.spec.typed.TypedEnumValue(et.fqn(), cell, scalar(kind));
        }
        try {
            if (kind == Type.Primitive.STRING) {
                return new com.legend.compiler.spec.typed.TypedCString(cell, scalar(kind));
            }
            if (kind == Type.Primitive.INTEGER) {
                return new com.legend.compiler.spec.typed.TypedCInteger(Long.parseLong(cell), scalar(kind));
            }
            if (kind == Type.Primitive.FLOAT || kind == Type.Primitive.NUMBER) {
                return new com.legend.compiler.spec.typed.TypedCFloat(Double.parseDouble(cell),
                        new java.math.BigDecimal(cell), scalar(Type.Primitive.FLOAT));
            }
            if (kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal) {
                String d = cell.endsWith("D") || cell.endsWith("d")
                        ? cell.substring(0, cell.length() - 1) : cell;
                return new com.legend.compiler.spec.typed.TypedCDecimal(new java.math.BigDecimal(d),
                        scalar(Type.Primitive.DECIMAL));
            }
            if (kind == Type.Primitive.BOOLEAN) {
                if (!cell.equals("true") && !cell.equals("false")) {
                    return null;
                }
                return new com.legend.compiler.spec.typed.TypedCBoolean(Boolean.parseBoolean(cell), scalar(kind));
            }
            if (kind == Type.Primitive.DATE || kind == Type.Primitive.STRICT_DATE
                    || kind == Type.Primitive.DATE_TIME) {
                String d = cell.startsWith("%") ? cell.substring(1) : cell;
                return new com.legend.compiler.spec.typed.TypedCDate(
                        com.legend.values.PureDateLiteral.parse(d.replace(' ', 'T')), scalar(kind));
            }
        } catch (NumberFormatException | java.time.DateTimeException e) {
            return null;
        }
        return null;
    }

    /** A flat join's elements as a literal collection, typed as the Typer
     * types {@code [a, b, c]}: one element kind, or Any for a mix. */
    private static TypedSpec collectionOf(List<TypedSpec> elements) {
        Type kind = null;
        boolean mixed = false;
        for (TypedSpec e : elements) {
            Type t = e.info().type();
            if (kind == null) {
                kind = t;
            } else if (!kind.equals(t)) {
                mixed = true;
            }
        }
        Type element = kind == null || mixed ? new Type.ClassType(PlatformTypes.ANY) : kind;
        return new com.legend.compiler.spec.typed.TypedCollection(elements,
                new ExprType(element, new Multiplicity.Bounded(elements.size(), elements.size())));
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

    /** The {@code #TDS} header: three-space indent, names quoted unless simple. */
    private static List<String> tdsHeader(String line) {
        List<String> out = new ArrayList<>();
        for (String n : stripIndent(line).split(",", -1)) {
            String t = n.strip();
            out.add(t.length() >= 2 && t.startsWith("'") && t.endsWith("'")
                    ? t.substring(1, t.length() - 1) : t);
        }
        return out;
    }

    private static String stripIndent(String line) {
        return line.startsWith("   ") ? line.substring(3) : line;
    }
}
