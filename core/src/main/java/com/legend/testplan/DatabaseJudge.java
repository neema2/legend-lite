// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;


import com.legend.testexecutionplan.Verdicts;
import com.legend.testexecutionplan.JsonGrammar;
import com.legend.testexecutionplan.JudgeJson;
import com.legend.testexecutionplan.KindClass;
import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;

/**
 * THE DATABASE JUDGE (task #27, 2026-09-21 — split out of AssertVerdicts): both sides
 * of an assert are PLANNED, composed by {@link com.legend.lowering.VerdictSql} into one
 * verdict statement (deferred into the body's batch), and the database returns the
 * verdict row. No value is compared in Java here; an undecidable pair is UNJUDGED,
 * named. The router ({@link AssertVerdicts}) classifies the assert and hands the sides
 * over; the host judge ({@link HostJudge}) is the other arm of the same router.
 */

public final class DatabaseJudge {
    private DatabaseJudge() {
    }

    static com.legend.lowering.VerdictSql.GridSide gridSide(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.Type.RelationType schema, boolean csvStrings) {
        List<Boolean> floatCols = new ArrayList<>();
        List<Boolean> emptyIsNull = new ArrayList<>();
        for (var colT : schema.columns()) {
            floatCols.add(colT.type() == com.legend.compiler.element.type.Type.Primitive.FLOAT);
            emptyIsNull.add(csvStrings
                    && colT.type() == com.legend.compiler.element.type.Type.Primitive.STRING);
        }
        return new com.legend.lowering.VerdictSql.GridSide(plan, schema.columns().size(), floatCols,
                emptyIsNull);
    }

    static TypedSide databaseVerdict(String name, boolean wantEqual,
            TypedSpec eSpec, TypedSpec aSpec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean canonicalOrder,
            boolean cellPool) {
        return databaseVerdict(name, wantEqual, eSpec, aSpec, letPrefix, specs, env, hook,
                canonicalOrder, cellPool, false);
    }

    /** {@code csvStrings}: the pair is judged under toCSV's own equivalence
     * (a String column's empty string and NULL print alike — bucket 8). */
    static TypedSide databaseVerdict(String name, boolean wantEqual,
            TypedSpec eSpec, TypedSpec aSpec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean canonicalOrder,
            boolean cellPool, boolean csvStrings) {
        return databaseVerdict(name, wantEqual, eSpec, aSpec, letPrefix, specs, env, hook, canonicalOrder, cellPool,
                csvStrings, false);
    }

    /** {@code renderedStrings}: the pair is the rendered-text family's (a golden String against a relation's text),
     *  judged by their rows, its message the two Strings upstream prints ({@code VerdictSql.messageAsStrings}). */
    static TypedSide databaseVerdict(String name, boolean wantEqual,
            TypedSpec eSpec, TypedSpec aSpec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean canonicalOrder,
            boolean cellPool, boolean csvStrings, boolean renderedStrings) {
        return databaseVerdict(name, wantEqual, eSpec, aSpec, letPrefix, specs, env, hook, canonicalOrder, cellPool,
                csvStrings, renderedStrings, false);
    }

    /** {@code gridPairArm}: the pair is the GRID PAIR family's (two relation values compared as grids, the grid
     *  owner's rule): its schemas decide where they differ -- names or widths false at planning, kinds spelled in the
     *  expected's. Another family that reaches two grids (a flat cells compare) compares their cells alone
     *  (2026-11-04: the corpus's validation tests, whose cells agree under other column names). */
    static TypedSide databaseVerdict(String name, boolean wantEqual,
            TypedSpec eSpec, TypedSpec aSpec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean canonicalOrder,
            boolean cellPool, boolean csvStrings, boolean renderedStrings, boolean gridPairArm) {
        java.util.function.UnaryOperator<com.legend.sql.SqlQuery> message = q -> renderedStrings
                ? com.legend.lowering.VerdictSql.messageAsStrings(q) : q;
        KindClass ke = AssertVerdicts.kindKey(eSpec, letPrefix, env);
        KindClass ka = AssertVerdicts.kindKey(aSpec, letPrefix, env);
        boolean anyNil = com.legend.compiler.element.type.PlatformTypes.isNil(eSpec.info().type())
                || com.legend.compiler.element.type.PlatformTypes.isNil(aSpec.info().type());
        boolean anyAny = AssertVerdicts.isAnyStamped(eSpec) || AssertVerdicts.isAnyStamped(aSpec);
        boolean bothGrids = com.legend.compiler.element.type.Type.isRelation(eSpec.info().type())
                && com.legend.compiler.element.type.Type.isRelation(aSpec.info().type());
        if (ke != null && ka != null && !anyNil && !anyAny && !bothGrids && !ke.equals(ka)) {
            if (ke.primitive() && ka.primitive()) {
                // X4: the engine has no cross-kind PRIMITIVE equality — a
                // VERDICT (false), decided statically, the one comparison SQL
                // never sees
                staticallyDecided(env);
                return wantEqual ? com.legend.testexecutionplan.Verdicts.fail(name + ": kinds differ (" + AssertVerdicts.typeName(eSpec)
                        + " vs " + AssertVerdicts.typeName(aSpec) + ")") : com.legend.testexecutionplan.Verdicts.ok();
            }
            // a class / generic side's static class is a declaration, not
            // the value's kind (a supertype stamp over equal instances):
            // leg 3.2's instance canon decides; unjudged until then
            return unjudged(name, "kind-gate: non-primitive", " ("
                    + AssertVerdicts.typeName(eSpec) + " vs " + AssertVerdicts.typeName(aSpec) + ")");
        }
        // the riders plan WITHOUT the canon-text sort: the statement orders
        // (string_agg ORDER BY __c for a multiset form) and a grid's value
        // peer is chunked into rows in its ARRIVAL order — a wrap-time sort
        // would scramble the cells across rows (3.1b lane witness)
        // an enum against an UNTYPED wire ($row.values->at(0), toDomainValue):
        // the carrier holds the NAME and no enumeration — the pair's ONE
        // declared enumeration frames the untyped AssertVerdicts.side (Rule 2: at the
        // boundary the declared kind is assigned; the host compares the names)
        String frameE = null;
        String frameA = null;
        if (!anyNil && anyAny && ke instanceof KindClass.Enum ^ ka instanceof KindClass.Enum) {
            if (ke instanceof KindClass.Enum en) {
                frameA = en.fqn();
            } else if (ka instanceof KindClass.Enum en) {
                frameE = en.fqn();
            }
        }
        var re = new com.legend.lowering.CanonRider(false, false, frameE);
        var ra = new com.legend.lowering.CanonRider(false, false, frameA);
        // the golden's ^TDSNull() cells spell the bare sentinel (direction-
        // aware: the EXPECTED side only, as TdsCompare.peerElementCanons) —
        // rewritten at plan time so the cell rides the literal channel as
        // the string 'TDSNull' the peer rule maps, never a JSON tree
        eSpec = com.legend.compiler.spec.VerdictQueries.tdsNullSentinel(eSpec);
        BodyDriver.PlannedValue pe = BodyDriver.planValue(eSpec, letPrefix, specs, env, re, hook);
        BodyDriver.PlannedValue pa = BodyDriver.planValue(aSpec, letPrefix, specs, env, ra, hook);
        // a HOST-CONSTANT side is bound on the OTHER side's database (a
        // literal evaluates anywhere; a planned side reads where its tables
        // are — the system database for a metamodel read)
        BodyDriver.ExecEnv on = pe.side() != null && !pe.side().storeFree()
                ? env.withTarget(pe.side().target())
                : pa.side() != null && !pa.side().storeFree() ? env.withTarget(pa.side().target())
                : env;
        BodyDriver.WrappedSide we = pe.side() != null ? pe.side()
                : constantSide(pe.answered(), eSpec, re, false, env, on);
        BodyDriver.WrappedSide wa = pa.side() != null ? pa.side()
                : constantSide(pa.answered(), aSpec, ra, false, env, on);
        String why = we == null ? "host-value AssertVerdicts.side (expected): " + describe(pe.answered())
                : wa == null ? "host-value AssertVerdicts.side (actual): " + describe(pa.answered())
                : re.declined() != null ? "side-e: " + re.declined()
                : ra.declined() != null ? "side-a: " + ra.declined()
                : !we.target().equals(wa.target()) && !we.storeFree() && !wa.storeFree()
                        ? "sides on different databases"
                : null;
        // leg 3.1b — a GRID AssertVerdicts.side (the tabular wrap): the statement compares
        // its row canons against the peer's cells chunked by the grid's
        // width (assertEquals) or the loose cell pool (assertSameElements)
        boolean gridE = why == null && re.tdsWrapped();
        boolean gridA = why == null && ra.tdsWrapped();
        if (gridE || gridA) {
            if (gridE && gridA) {
                // two grids: their row canons against each other, the cells
                // walked for the declared-Float leniency when the schemas agree
                var se = AssertVerdicts.effectiveSchema(java.util.Objects.requireNonNull(we));
                var sa = AssertVerdicts.effectiveSchema(java.util.Objects.requireNonNull(wa));
                com.legend.sql.SqlQuery pq;
                if (se != null && sa != null && se.columns().size() == sa.columns().size()) {
                    // both spelled in the expected's kinds where the columns' kinds differ (the grid owner's rule)
                    pq = com.legend.lowering.VerdictSql.gridPair(gridSide(we.plan(), se, csvStrings),
                            gridSide(wa.plan(), gridPairArm && sameNames(se, sa) ? se : sa, csvStrings),
                            canonicalOrder, words(env));
                } else {
                    pq = com.legend.lowering.VerdictSql.gridPair(we.plan(), wa.plan(), canonicalOrder);
                }
                if (gridPairArm && AssertVerdicts.gridSchemasDiffer(eSpec, aSpec)) {
                    // names or widths differ: false by the schemas (decided at planning), the rows read for the message
                    pq = com.legend.lowering.VerdictSql.decidedFalse(pq);
                }
                return runVerdict(name, wantEqual, message.apply(pq),
                        env.withTarget(we.storeFree() ? wa.target() : we.target()));
            } else {
                BodyDriver.WrappedSide gw = gridE ? we : wa;
                com.legend.lowering.CanonRider pr = gridE ? ra : re;
                BodyDriver.WrappedSide pw = gridE ? wa : we;
                var schema = AssertVerdicts.effectiveSchema(java.util.Objects.requireNonNull(gw));
                int width = schema == null ? 0 : schema.columns().size();
                if (width <= 0) {
                    why = "grid side without a schema view";
                } else if (!pr.wrapped() || pr.literalIndex() < 0) {
                    // the peer's own state rides the reason: WHY it has no
                    // literal channel is the bucket's diagnosis
                    why = "tds-peer: no literal channel (peer "
                            + (!pr.wrapped() ? "unwrapped: " + pr.declined()
                                    : "kinds " + pr.kinds() + ", no literal candidate")
                            + ")";
                } else {
                    var grid = gridSide(gw.plan(), java.util.Objects.requireNonNull(schema), csvStrings);
                    boolean peerFloat = pr.kinds().size() == 1
                            ? pr.kinds().get(0) == com.legend.compiler.element.type.Type.Primitive.FLOAT
                            : pr.literalIndex() >= 0 && pr.kinds().get(pr.literalIndex())
                                    == com.legend.compiler.element.type.Type.Primitive.FLOAT;
                    var peer = new com.legend.lowering.VerdictSql.PeerSide(
                            java.util.Objects.requireNonNull(pw).plan(),
                            "__canon" + pr.literalIndex(), !gridE, peerFloat);
                    com.legend.sql.SqlQuery gq = cellPool
                            ? com.legend.lowering.VerdictSql.gridCells(grid, peer, gridE, words(env))
                            : com.legend.lowering.VerdictSql.gridRows(grid, peer, gridE,
                                    canonicalOrder, words(env));
                    return runVerdict(name, wantEqual, message.apply(gq),
                            env.withTarget(gw.storeFree() ? pw.target() : gw.target()));
                }
            }
        }
        // a store-free side rides the store-reading side's database
        BodyDriver.ExecEnv runOn = we != null && wa != null
                ? env.withTarget(we.storeFree() ? wa.target() : we.target()) : env;
        int ie = -1;
        int ia = -1;
        if (why == null) {
            boolean literal = literal(re, ra, anyNil, anyAny);
            ie = literal ? re.literalIndex() : 0;
            ia = literal ? ra.literalIndex() : 0;
            int bareE = re.kinds().size() - (re.literalIndex() >= 0 ? 1 : 0);
            int bareA = ra.kinds().size() - (ra.literalIndex() >= 0 ? 1 : 0);
            if (literal && (ie < 0 || ia < 0)) {
                why = "no literal channel";
            } else if (!literal && (bareE > 1 || bareA > 1)) {
                why = "unrefined-number: multi-candidate side";
            }
        }
        if (why != null) {
            return unjudged(name, why);
        }
        // TYPED (item 3's S4, 2026-11-21, the user's Q-A): two sides of one scalar kind the database answers as the
        // host reads it are compared as typed values, their failure recorded by the host's builders from the values
        com.legend.compiler.element.type.Type tke = re.kinds().get(ie);
        if (!literal(re, ra, anyNil, anyAny) && !renderedStrings && typedKind(tke) && tke.equals(ra.kinds().get(ia))
                && !(eSpec instanceof com.legend.compiler.spec.typed.TypedCDate)
                && !(aSpec instanceof com.legend.compiler.spec.typed.TypedCDate)) {
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedEquals(
                    java.util.Objects.requireNonNull(we).plan(), tke, java.util.Objects.requireNonNull(wa).plan(),
                    tke, canonicalOrder, wantEqual);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(canonicalOrder
                        ? com.legend.testexecutionplan.RootLoop.Family.SAME_ELEMENTS
                        : com.legend.testexecutionplan.RootLoop.Family.EQUALS, wantEqual,
                        JudgmentFacts.sideFacts(eSpec), JudgmentFacts.sideFacts(aSpec), null, -1), runOn);
            }
        }
        // each side WRITTEN in the failure message by lite's own rules (VerdictSql.Message), where its elements are of
        // one known primitive kind (2026-11-04, the assert bar: the message read the canons, Strings unquoted)
        com.legend.lowering.VerdictSql.Message me = written(re, ie, cellPool, java.util.Objects.requireNonNull(we).plan(), eSpec);
        com.legend.lowering.VerdictSql.Message ma = written(ra, ia, cellPool, java.util.Objects.requireNonNull(wa).plan(), aSpec);
        if (me != null && ma != null && !me.canon() && !ma.canon()) {
            // each side's kind, for lite's sentence where the two texts tie; two DateTime literals each with its
            // written precision (the orchestrator's ruling, 2026-11-17)
            String ek = com.legend.lowering.VerdictSql.kindText(re.kinds().get(ie));
            String ak = com.legend.lowering.VerdictSql.kindText(ra.kinds().get(ia));
            if (ek.equals("DateTime") && ak.equals("DateTime") && eSpec instanceof com.legend.compiler.spec.typed.TypedCDate ed
                    && aSpec instanceof com.legend.compiler.spec.typed.TypedCDate ad) {
                ek = ek + ", " + ed.value().precision().name().toLowerCase(java.util.Locale.ROOT);
                ak = ak + ", " + ad.value().precision().name().toLowerCase(java.util.Locale.ROOT);
            }
            me = me.withKindText(ek);
            ma = ma.withKindText(ak);
        }
        com.legend.sql.SqlQuery vq = com.legend.lowering.VerdictSql.equality(
                new com.legend.lowering.VerdictSql.Side(
                        java.util.Objects.requireNonNull(we).plan(), "__canon" + ie,
                        re.many(), canonicalOrder,
                        re.kinds().get(ie) == com.legend.compiler.element.type.Type.Primitive.FLOAT, me,
                        com.legend.lowering.VerdictSql.decimalByValue(re.kinds().get(ie))),
                new com.legend.lowering.VerdictSql.Side(
                        java.util.Objects.requireNonNull(wa).plan(), "__canon" + ia,
                        ra.many(), canonicalOrder,
                        ra.kinds().get(ia) == com.legend.compiler.element.type.Type.Primitive.FLOAT, ma,
                        com.legend.lowering.VerdictSql.decimalByValue(ra.kinds().get(ia))));
        // (legend-pure's readings of the canons -- two raw Strings quoted as %r, two class sides as their instances, a
        // Decimal's D -- left with A4 of the compatibility program, stage A ratified by the user 2026-11-19: the
        // database judge plans for the product alone, its words lite's) two single numbers of different kinds (register
        // S127, the user's ruling of 2026-11-04): never equal -- decided false at planning, the rows read for the
        // message
        if (!re.many() && !ra.many() && crossNumberKinds(re.kinds().get(ie), ra.kinds().get(ia))) {
            vq = com.legend.lowering.VerdictSql.decidedFalse(vq);
        }
        return runVerdict(name, wantEqual, message.apply(vq), runOn);
    }

    /** Whether the pair compares in the literal channel (a mixed-kind or Any-stamped side). */
    private static boolean literal(com.legend.lowering.CanonRider re, com.legend.lowering.CanonRider ra, boolean anyNil,
            boolean anyAny) {
        return !anyNil && (anyAny || re.literalOnly() || ra.literalOnly());
    }

    /** How a side's canon {@code i} is written in the failure message, by lite's own rules (A4 of the compatibility
     *  program, stage A ratified by the user 2026-11-19: the database judge plans for the product alone, rule 4): a
     *  date literal's text spelled at planning; instances of a generic class carried as structs; its typed kind's (a
     *  literal channel's kind is no element's, but an empty side's, Nil, which writes no element); else, no one
     *  primitive kind (a mixed-kind collection, a literal channel, an instance carried as its JSON), each element from
     *  its canon's mark, an instance's class-prefixed. */
    private static com.legend.lowering.VerdictSql.Message written(com.legend.lowering.CanonRider r, int i,
            boolean sorted, com.legend.sql.SqlQuery plan, TypedSpec side) {
        com.legend.compiler.element.type.Type kind = r.kinds().get(i);
        if (side instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            // a date LITERAL: its text spelled once at planning, lite's own -- the database carries the literal's
            // precision in no value it could print (2026-11-05, the user: "clean datetime for the clean plan")
            return com.legend.lowering.VerdictSql.Message.literal(kind, liteDate(d.value()));
        }
        if (kind instanceof com.legend.compiler.element.type.Type.GenericType g && !com.legend.compiler.element.type
                .Type.isRelation(g) && plan instanceof com.legend.sql.SqlSelect ps
                && com.legend.lowering.VerdictSql.primitiveStruct(ps) != null) {
            // instances of a generic class carried as structs (a Pair): each written as the host writes one
            return com.legend.lowering.VerdictSql.Message.instances(g, sorted);
        }
        com.legend.lowering.VerdictSql.@com.legend.base.Nullable Message m = i != r.literalIndex()
                || com.legend.compiler.element.type.PlatformTypes.isNil(kind)
                ? com.legend.lowering.VerdictSql.Message.of(kind, sorted) : null;
        return m != null ? m : com.legend.lowering.VerdictSql.Message.fromCanon(sorted, classOf(side));
    }

    /** A date's lite text (the host's {@code LiteWords.dateText}): its day where it has no time of day (its own text
     *  where coarser than a day), else ISO 8601 in UTC with six fraction digits and Z. */
    static String liteDate(com.legend.values.PureDateLiteral d) {
        java.time.LocalDateTime t = d.toInstantFloor();
        if (!d.precision().atLeast(com.legend.values.PureDateLiteral.Precision.HOUR)) {
            return d.precision().atLeast(com.legend.values.PureDateLiteral.Precision.DAY)
                    ? String.format(java.util.Locale.ROOT, "%d-%02d-%02d", t.getYear(), t.getMonthValue(),
                            t.getDayOfMonth())
                    : d.canonicalText();
        }
        return String.format(java.util.Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ", t.getYear(),
                t.getMonthValue(), t.getDayOfMonth(), t.getHour(), t.getMinute(), t.getSecond(), t.getNano() / 1000);
    }

    /** The database judge's words of a failure, lite's: it plans for lite's flow alone (A4 of the compatibility
     *  program, stage A ratified by the user 2026-11-19; rule 1 of item 3), so it asks the plan style nothing (item
     *  3's S3, 2026-11-21: until then the style's two-form VerdictWords kind, whose edge form only the host judge's
     *  planner reached). Until S4, which carries the database judge's values to the record. */
    static com.legend.lowering.VerdictWords words(BodyDriver.ExecEnv env) {
        return com.legend.lowering.LiteVerdictWords.INSTANCE;
    }

    /** Whether {@code e} and {@code a} are two numbers of different known kinds, never equal (S127's lite side). An
     *  unrefined Number is no known kind. (Compiled legend-pure's Integer-and-Decimal-by-text, the two-form kind
     *  CrossKindNumbers, left the database judge with A4 of the compatibility program, stage A ratified by the user
     *  2026-11-19: the database judge plans for the product alone, rule 4 of the rules of record.) */
    private static boolean crossNumberKinds(com.legend.compiler.element.type.Type e,
            com.legend.compiler.element.type.Type a) {
        com.legend.compiler.element.type.Type fe = fineNumber(e);
        com.legend.compiler.element.type.Type fa = fineNumber(a);
        return fe != null && fa != null && fe != fa;
    }


    /** {@code kind}'s fine number kind -- Integer, Float, Decimal (a declared precision one's too) -- or null. */
    private static com.legend.compiler.element.type.@com.legend.base.Nullable Type fineNumber(
            com.legend.compiler.element.type.Type kind) {
        if (decimal(kind)) {
            return com.legend.compiler.element.type.Type.Primitive.DECIMAL;
        }
        return kind == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || kind == com.legend.compiler.element.type.Type.Primitive.FLOAT ? kind : null;
    }

    /** Whether {@code kind} is a Decimal's (a declared precision one's too): its {@code %r} ends in {@code D}. */
    private static boolean decimal(com.legend.compiler.element.type.Type kind) {
        return kind == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || kind instanceof com.legend.compiler.element.type.Type.PrecisionDecimal;
    }

    private static @com.legend.base.Nullable String classOf(TypedSpec s) {
        com.legend.compiler.element.type.Type t = s.info().type();
        // a generic class's instance (a Pair<Integer, String>) is of its raw class (the host's PureAsserts.classFqnOf)
        return t instanceof com.legend.compiler.element.type.Type.ClassType ct
                && !com.legend.compiler.element.type.PlatformTypes.isAny(ct) ? ct.fqn()
                : t instanceof com.legend.compiler.element.type.Type.GenericType g ? g.rawFqn() : null;
    }

    private static boolean sameNames(com.legend.compiler.element.type.Type.RelationType e,
            com.legend.compiler.element.type.Type.RelationType a) {
        for (int i = 0; i < e.columns().size(); i++) {
            if (!e.columns().get(i).name().equals(a.columns().get(i).name())) {
                return false;
            }
        }
        return true;
    }

    /** {@code spec}, a rendered TEXT, as a one-row relation of its {@code value} for a verdict row (2026-11-04, the
     *  core program: a rendered text's generic comparison) -- planned as the host judge plans the same side, a plain
     *  value with no canon wrap (a canon would encode the render's own reduction, which a dialect without a list
     *  encoding refuses: H2's {@code joinStrings}); null where the side is answered with no plan. */
    /** {@code message}, a caller's failure text, planned as the verdict carries it -- a constant the compiler folds (a
     *  literal, a join of literals), else the one-row relation the database computes -- with the session its row reads
     *  (the body's where it reads no store). */
    record PlannedMessage(com.legend.lowering.VerdictSql.FailureMessage message, com.legend.executionplan.ExecutionPlan.Target on) {
    }

    static PlannedMessage plannedMessage(TypedSpec message, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        Object folded = com.legend.compiler.spec.typed.Literals.fold(message, letPrefix);
        if (folded instanceof String s) {
            return new PlannedMessage(new com.legend.lowering.VerdictSql.FailureMessage(s, null), env.target());
        }
        BodyDriver.PlannedValue pv = BodyDriver.planHostSide(message, letPrefix, specs, env, false, false,
                hook == null ? null : hook::apply);
        BodyDriver.WrappedSide side = pv.side();
        if (side == null) {
            throw new IllegalStateException("an assert's message planned no side and folds to no String: "
                    + message.getClass().getSimpleName());
        }
        return new PlannedMessage(new com.legend.lowering.VerdictSql.FailureMessage(null,
                com.legend.lowering.VerdictSql.firstValueRow(side.plan())),
                side.storeFree() ? env.target() : side.target());
    }

    static com.legend.sql.@com.legend.base.Nullable SqlQuery textRow(TypedSpec spec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        BodyDriver.PlannedValue pv = BodyDriver.planHostSide(spec, letPrefix, specs, env, false, false,
                hook == null ? null : hook::apply);
        BodyDriver.WrappedSide side = pv.side();
        return side == null ? null : com.legend.lowering.VerdictSql.firstValueRow(side.plan());
    }

    /** Execute a verdict statement on {@code on} and read its one row:
     * unjudged (counted, the assert fails with the reason), or the verdict
     * (never NULL) with the two framed canons as the message. */
    static TypedSide runVerdict(String name, boolean wantEqual,
            com.legend.sql.SqlQuery vq, BodyDriver.ExecEnv runOn) {
        if (runOn.failureMessage() != null) {
            // a caller's failure message (the asserts-as-plan-nodes program, step 2): the row's message the caller's
            vq = com.legend.lowering.VerdictSql.withFailureMessage(vq, runOn.failureMessage());
        }
        com.legend.testplan.VerdictBatch batch = runOn.verdictBatch();
        if (batch != null && batch.active()) {
            batch.defer(name, wantEqual, vq, runOn.target());   // leg 3.4: judged at the flush
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        // a verdict statement is a step of its body's plan, deferred on the body's batch and judged at its verdict step
        // (I2, 2026-11-07: the walk's verdict sent on its own, outside a batch, deleted -- measured unreached)
        throw new IllegalStateException(name + ": a verdict statement outside its body's verdict batch");
    }

    /** A TYPED member deferred on the body's verdict batch (item 3's S4, 2026-11-21): its row and schema, its failure
     *  recorded at run by {@code report} -- a caller's literal message its failure fixed at planning. */
    static TypedSide runTyped(String name, com.legend.lowering.VerdictSql.TypedRow member,
            com.legend.testexecutionplan.RootLoop.Report report, BodyDriver.ExecEnv runOn) {
        com.legend.lowering.VerdictSql.FailureMessage m = runOn.failureMessage();
        if (m != null) {
            if (m.literal() == null) {
                throw new IllegalStateException(name + ": a caller's message that reads is the batch's (VerdictBatch"
                        + ".message), never the row's");
            }
            report = report.withFixed(com.legend.error.AssertKind.MESSAGE.record(
                    java.util.Map.of("message", m.literal())));
        }
        com.legend.testplan.VerdictBatch batch = runOn.verdictBatch();
        if (batch != null && batch.active()) {
            batch.deferTyped(name, member.row(), member.schema(), report, runOn.target());
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        throw new IllegalStateException(name + ": a verdict statement outside its body's verdict batch");
    }

    /** A side's one number kind where it states one (an Integer, a Float, a Decimal), else null. */
    private static com.legend.compiler.element.type.@com.legend.base.Nullable Type numberKind(SideRows s) {
        com.legend.lowering.CanonRider r = s.rider();
        if (r == null || r.literalOnly() || r.kinds().size() != 1 || s.side() == null) {
            return null;
        }
        com.legend.compiler.element.type.Type k = r.kinds().get(0);
        return k == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || k == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || k == com.legend.compiler.element.type.Type.Primitive.DECIMAL ? k : null;
    }

    /** Whether the database judge compares {@code kind}'s values as typed values today (item 3's S4, phase 1: the
     *  scalar kinds whose value the database answers as the host reads it; a date's written precision, an enum, an
     *  instance and a table stay the seven-column row until S4 converts them). */
    private static boolean typedKind(com.legend.compiler.element.type.Type kind) {
        return kind == com.legend.compiler.element.type.Type.Primitive.STRING
                || kind == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || kind == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || kind == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || kind == com.legend.compiler.element.type.Type.Primitive.BOOLEAN;
    }

    static SideRows planSide(TypedSpec spec, boolean expected, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return planSide(spec, expected, true, letPrefix, specs, env, hook);
    }

    /** {@code needCanon} false = the family only COUNTS (size / emptiness):
     * a declined canon is not a reason, the plan's rows are. */
    static SideRows planSide(TypedSpec spec, boolean expected, boolean needCanon,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return planSide(spec, expected, needCanon, false, letPrefix, specs, env, hook);
    }

    /** {@code canonicalJson} = the side's JSON objects are written with
     * their keys sorted (the JSON verdict's plan). */
    static SideRows planSide(TypedSpec spec, boolean expected, boolean needCanon,
            boolean canonicalJson, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return planSide(spec, expected, needCanon, canonicalJson, null, letPrefix, specs, env, hook);
    }

    /** {@code enumFrame} = the pair's declared enumeration framing an untyped
     * or abstract-Enum AssertVerdicts.side (CanonRider.enumFrame). */
    static SideRows planSide(TypedSpec spec, boolean expected, boolean needCanon,
            boolean canonicalJson, @com.legend.base.Nullable String enumFrame, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        var rider = new com.legend.lowering.CanonRider(false, canonicalJson, enumFrame);
        TypedSpec s = expected ? com.legend.compiler.spec.VerdictQueries.tdsNullSentinel(spec) : spec;
        BodyDriver.PlannedValue pv = BodyDriver.planValue(s, letPrefix, specs, env, rider, hook);
        BodyDriver.WrappedSide w = pv.side() != null ? pv.side()
                : constantSide(pv.answered(), s, rider, false, env, env);
        if (w == null) {
            return new SideRows(null, rider, "host-value side: " + describe(pv.answered()));
        }
        if (needCanon && (rider.declined() != null || w.shape() == com.legend.plan.ResultShape.GRAPH)) {
            // a decline is the database judge's own word only for a side the database could run: a side the dialect
            // cannot write raises its refusal here, as the host judge's read of the same side raises it (2026-11-03:
            // H2's Variant sides declined their canon before the side was written, so the database judge alone read
            // UNJUDGED where the host judge, and the ledger, read the dialect's UNNEST refusal); planning only, a
            // render with nothing sent
            env.dialect().renderStatement(env.read(w.plan()));
            return new SideRows(w, rider, rider.declined() != null ? "side: " + rider.declined()
                    : "graph AssertVerdicts.side (leg 3.2)");
        }
        return new SideRows(w, rider, null);
    }

    /** THE ONE UNJUDGED OUTCOME (leg 3.3, audit §4y): the database judge
     * declined the shape — counted, then raised as a verdict failure that
     * CARRIES its reason (the runner's ledger reads the type, never the
     * text). {@code detail} rides the message only. */
    static TypedSide unjudged(String name, String why) {
        return unjudged(name, why, "");
    }

    static TypedSide unjudged(String name, String why, String detail) {
        // counted UNJUDGED at the root's turn, where the plan's failure is raised (RootLoops); planning counts nothing
        // (the slice 3 invariant audit, 2026-11-08)
        throw com.legend.error.AssertFailed.unjudged(why,
                name + ": UNJUDGED in database mode — " + why + detail);
    }

    static TypedSide databaseSize(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return databaseSize(name, args, letPrefix, specs, env, hook, false);
    }

    /** {@code negated}: {@code assertNotSize} (#28): the size row with the polarity flipped, its negated sentence. */
    static TypedSide databaseSize(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean negated) {
        SideRows coll = planSide(args.get(0), false, false, letPrefix, specs, env, hook);
        if (coll.why() != null) {
            return unjudged(name, coll.why());
        }
        SideRows n = planSide(args.get(1), false, letPrefix, specs, env, hook);
        if (n.why() != null) {
            return unjudged(name, "size arg: " + n.why());
        }
        boolean envelope = java.util.Objects.requireNonNull(coll.rider()).tdsWrapped()
                && AssertVerdicts.envelopeValuesRead(args.get(0), letPrefix);
        var cw = java.util.Objects.requireNonNull(coll.side());
        if (negated && cw.shape() == com.legend.plan.ResultShape.GRAPH) {
            return unjudged(name, "assertNotSize over a graph: no negated form planned");
        }
        com.legend.lowering.CanonRider nr = n.rider();
        if (cw.shape() != com.legend.plan.ResultShape.GRAPH && !envelope && nr != null && !nr.literalOnly()
                && nr.kinds().size() == 1 && nr.kinds().get(0) == com.legend.compiler.element.type.Type.Primitive.INTEGER
                && n.side() != null) {
            // TYPED (item 3's S4): the collection's count against the size's one Integer, both riding the failure
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedSize(coll.countRows(),
                    n.side().plan(), !negated);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.SIZE, !negated, null, null, null, -1), coll.on(env));
            }
        }
        com.legend.sql.SqlQuery vq = cw.shape() == com.legend.plan.ResultShape.GRAPH
                ? com.legend.lowering.VerdictSql.sizeOfGraph(cw.plan(), n.scalarRow(false))
                : com.legend.lowering.VerdictSql.size(coll.countRows(), n.scalarRow(false), envelope, words(env),
                        negated);
        return runVerdict(name, !negated, vq, coll.on(env));
    }

    static TypedSide databaseEmpty(String name, TypedSpec arg, boolean wantEmpty,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        SideRows side = planSide(arg, false, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
        }
        var sw = java.util.Objects.requireNonNull(side.side());
        // the elements written as lite's sentence writes them where the side's canon names one primitive kind
        com.legend.lowering.CanonRider r = side.rider();
        if (sw.shape() != com.legend.plan.ResultShape.GRAPH && r != null && r.declined() == null && !r.tdsWrapped()
                && !r.literalOnly() && r.kinds().size() == 1 && typedKind(r.kinds().get(0))) {
            // TYPED (item 3's S4): the side's values as one list, its emptiness the verdict, the list its failure's
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedEmpty(sw.plan(),
                    r.kinds().get(0), wantEmpty);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.EMPTY, wantEmpty, null, null, null, -1),
                        side.on(env));
            }
        }
        com.legend.lowering.VerdictSql.Message written = r == null || r.declined() != null || r.tdsWrapped()
                || r.kinds().isEmpty() ? null : com.legend.lowering.VerdictSql.Message.of(r.kinds().get(0), false);
        com.legend.sql.SqlQuery vq = sw.shape() == com.legend.plan.ResultShape.GRAPH
                ? com.legend.lowering.VerdictSql.emptyOfGraph(sw.plan(), wantEmpty)
                : written != null ? com.legend.lowering.VerdictSql.empty(com.legend.lowering.VerdictSql.writtenRows(
                        sw.plan(), "__canon0", java.util.Objects.requireNonNull(r).many(), written), wantEmpty, true,
                        words(env))
                : com.legend.lowering.VerdictSql.empty(side.countRows(), wantEmpty, false, words(env));
        return runVerdict(name, true, vq, side.on(env));
    }

    static TypedSide databaseContains(String name, TypedSpec coll, TypedSpec val,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return databaseContains(name, coll, val, letPrefix, specs, env, hook, false);
    }

    /** {@code negated}: {@code assertNotContains} (#28): the written membership row with the polarity flipped, its
     *  negated sentence; a side not written is no negated form planned, refused by name. */
    static TypedSide databaseContains(String name, TypedSpec coll, TypedSpec val,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean negated) {
        SideRows c = planSide(coll, false, letPrefix, specs, env, hook);
        if (c.why() != null) {
            return unjudged(name, c.why());
        }
        SideRows v = planSide(val, false, letPrefix, specs, env, hook);
        if (v.why() != null) {
            return unjudged(name, "value: " + v.why());
        }
        var cr = java.util.Objects.requireNonNull(c.rider());
        var vr = java.util.Objects.requireNonNull(v.rider());
        boolean literal = cr.literalOnly() || vr.literalOnly();
        if (literal && (cr.literalIndex() < 0 || vr.literalIndex() < 0)) {
            return unjudged(name, "no literal channel");
        }
        if (!literal && !cr.tdsWrapped() && cr.kinds().size() == 1 && typedKind(cr.kinds().get(0))
                && cr.kinds().equals(vr.kinds())) {
            // TYPED (item 3's S4): some element of the collection IS NOT DISTINCT FROM the value, both of one kind
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedContains(
                    java.util.Objects.requireNonNull(c.side()).plan(), cr.kinds().get(0),
                    java.util.Objects.requireNonNull(v.side()).plan(), vr.kinds().get(0), !negated);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.CONTAINS, !negated, JudgmentFacts.sideFacts(coll),
                        JudgmentFacts.sideFacts(val), null, -1), c.on(env));
            }
        }
        // both written by lite's own rules where each names one primitive kind (2026-11-04, the assert bar: the canons
        // left a String unquoted)
        com.legend.lowering.VerdictSql.Message cm = literal ? null
                : written(cr, 0, false, java.util.Objects.requireNonNull(c.side()).plan(), coll);
        com.legend.lowering.VerdictSql.Message vm = literal ? null
                : written(vr, 0, false, java.util.Objects.requireNonNull(v.side()).plan(), val);
        if (cm != null && vm != null && !cr.tdsWrapped()) {
            return runVerdict(name, !negated, com.legend.lowering.VerdictSql.contains(
                    com.legend.lowering.VerdictSql.writtenRows(java.util.Objects.requireNonNull(c.side()).plan(),
                            "__canon0", cr.many(), cm), v.scalarRow(false), vm, negated, words(env)), c.on(env));
        }
        if (negated) {
            return unjudged(name, "assertNotContains over sides not written: no negated form planned");
        }
        return runVerdict(name, true, com.legend.lowering.VerdictSql.contains(c.rows(literal), v.scalarRow(literal),
                null, false, words(env)), c.on(env));
    }

    /** {@code assertTdsEquivalent} in database mode (bucket 5): both grids
     * planned, the column names checked statically (the schemas), the cells
     * aligned by position in one statement with the numeric / temporal
     * tolerances (VerdictSql.gridTolerance). */
    static TypedSide databaseTdsEquivalent(String name, List<TypedSpec> targs,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        SideRows one = planSide(targs.get(0), true, letPrefix, specs, env, hook);
        SideRows two = planSide(targs.get(1), false, letPrefix, specs, env, hook);
        if (one.why() != null || two.why() != null) {
            return unjudged(name, "tdsEquivalent side: " + (one.why() != null ? one.why() : two.why()));
        }
        var r1 = java.util.Objects.requireNonNull(one.rider());
        var r2 = java.util.Objects.requireNonNull(two.rider());
        var w1 = java.util.Objects.requireNonNull(one.side());
        var w2 = java.util.Objects.requireNonNull(two.side());
        var s1 = com.legend.compiler.element.type.Type.schemaView(w1.shapeInfo().type());
        var s2 = com.legend.compiler.element.type.Type.schemaView(w2.shapeInfo().type());
        if (!r1.tdsWrapped() || !r2.tdsWrapped() || s1 == null || s2 == null) {
            return unjudged(name, "tdsEquivalent: a side is not a grid");
        }
        List<String> n1 = s1.columns().stream().map(c -> c.name()).toList();
        List<String> n2 = s2.columns().stream().map(c -> c.name()).toList();
        if (!n1.equals(n2)) {
            staticallyDecided(env);
            return com.legend.testexecutionplan.Verdicts.fail(name + ": columns differ " + n1 + " vs " + n2);   // a static verdict
        }
        List<com.legend.compiler.element.type.Type> kinds = new ArrayList<>();
        List<Boolean> floats = new ArrayList<>();
        for (var c : s1.columns()) {
            kinds.add(c.type());
            floats.add(c.type() == com.legend.compiler.element.type.Type.Primitive.FLOAT);
        }
        var g1 = new com.legend.lowering.VerdictSql.GridSide(w1.plan(), n1.size(), floats);
        var g2 = new com.legend.lowering.VerdictSql.GridSide(w2.plan(), n2.size(), floats);
        SideRows delta = planSide(targs.get(2), false, letPrefix, specs, env, hook);
        SideRows timeDelta = planSide(targs.size() == 4 ? targs.get(3)
                : com.legend.compiler.spec.VerdictQueries.zeroLiteral(), false, letPrefix, specs, env, hook);
        if (delta.why() != null || timeDelta.why() != null) {
            return unjudged(name, "tdsEquivalent delta: "
                    + (delta.why() != null ? delta.why() : timeDelta.why()));
        }
        return runVerdict(name, true, com.legend.lowering.VerdictSql.gridTolerance(g1, g2, kinds,
                        delta.rows(false), timeDelta.rows(false), n1, words(env)),
                env.withTarget(w1.storeFree() ? w2.target() : w1.target()));
    }

    static TypedSide databaseCondition(String name, TypedSpec cond, boolean wantTrue,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return databaseCondition(name, cond, wantTrue, letPrefix, specs, env, hook, null);
    }

    /** {@code failure}: the failure's record where the assert fixes it (an {@code assert}'s, an element pair's
     *  {@code assertIs}), the host's; null, the condition's canons framed (an assert judged by a condition of its own
     *  making). */
    static TypedSide databaseCondition(String name, TypedSpec cond, boolean wantTrue,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook,
            com.legend.error.@com.legend.base.Nullable ErrorRecord failure) {
        @com.legend.base.Nullable String message = failure == null ? null : failure.sentence();
        TypedSpec[] fc = AssertVerdicts.forAllContains(cond);
        if (fc != null) {
            SideRows need = planSide(fc[0], false, letPrefix, specs, env, hook);
            SideRows have = planSide(fc[1], false, letPrefix, specs, env, hook);
            if (need.why() != null || have.why() != null) {
                return unjudged(name, "forAll-contains: " + (need.why() != null ? need.why() : have.why()));
            }
            var nr = java.util.Objects.requireNonNull(need.rider());
            var hr = java.util.Objects.requireNonNull(have.rider());
            boolean literal = nr.literalOnly() || hr.literalOnly();
            if (literal && (nr.literalIndex() < 0 || hr.literalIndex() < 0)) {
                return unjudged(name, "forAll-contains: no literal channel");
            }
            if (!literal && failure != null && nr.kinds().size() == 1 && typedKind(nr.kinds().get(0))
                    && nr.kinds().equals(hr.kinds()) && need.side() != null && have.side() != null) {
                // TYPED (item 3's S4): no needed value without a have not distinct from it; the condition's record
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedSubset(
                        need.side().plan(), have.side().plan(), wantTrue);
                if (typed != null) {
                    return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.CONDITION, wantTrue, null, null, failure, -1),
                            need.on(env));
                }
            }
            // the condition's own failure (its message), as the host's subset judges it (the S2+S3 fixes' audit)
            return runVerdict(name, true, com.legend.lowering.VerdictSql.subset(need.rows(literal), have.rows(literal),
                    wantTrue, message), need.on(env));
        }
        SideRows side = planSide(cond, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
        }
        com.legend.lowering.CanonRider cr = side.rider();
        if (failure != null && cr != null && !cr.literalOnly() && cr.kinds().size() == 1
                && cr.kinds().get(0) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN && side.side() != null) {
            // TYPED (item 3's S4): the condition's one Boolean, its failure the record fixed here
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedCondition(
                    side.side().plan(), wantTrue);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.CONDITION, wantTrue, null, null, failure, -1),
                        side.on(env));
            }
        }
        return runVerdict(name, true, com.legend.lowering.VerdictSql.condition(side.scalarRow(false), wantTrue,
                message), side.on(env));
    }

    static TypedSide databaseTolerance(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        SideRows e = planSide(args.get(0), false, letPrefix, specs, env, hook);
        SideRows a = planSide(args.get(1), false, letPrefix, specs, env, hook);
        SideRows t = planSide(args.get(2), false, letPrefix, specs, env, hook);
        String why = e.why() != null ? e.why() : a.why() != null ? a.why() : t.why();
        if (why != null) {
            return unjudged(name, why);
        }
        com.legend.compiler.element.type.Type ek = numberKind(e);
        com.legend.compiler.element.type.Type ak = numberKind(a);
        com.legend.compiler.element.type.Type tk = numberKind(t);
        if (ek != null && ak != null && tk != null) {
            // TYPED (item 3's S4): the three numbers as their plans type them, the arithmetic the database's
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedTolerance(
                    java.util.Objects.requireNonNull(e.side()).plan(), ek, java.util.Objects.requireNonNull(a.side())
                            .plan(), ak, java.util.Objects.requireNonNull(t.side()).plan(), tk,
                    toleranceArithmetic(args));
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.TOLERANCE, true, null, null, null, -1), a.on(env));
            }
        }
        return runVerdict(name, true, com.legend.lowering.VerdictSql.tolerance(e.scalarRow(false), a.scalarRow(false),
                t.scalarRow(false), toleranceArithmetic(args), words(env)), a.on(env));
    }

    /** The kind {@code assertEqWithinTolerance}'s three sides compute in: BIGINT where all three are Integers, DOUBLE
     *  where one is a Float (the database's Float), else DECIMAL (a Decimal, or a Number the database holds exactly)
     *  -- never DOUBLE for exact kinds (the user's ruling, 2026-11-03). */
    private static com.legend.sql.SqlType toleranceArithmetic(List<TypedSpec> args) {
        boolean allInteger = true;
        for (TypedSpec s : args.subList(0, 3)) {
            com.legend.compiler.element.type.Type t = s.info().type();
            if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
                return com.legend.sql.SqlType.Scalar.DOUBLE;
            }
            allInteger &= t == com.legend.compiler.element.type.Type.Primitive.INTEGER;
        }
        return allInteger ? com.legend.sql.SqlType.Scalar.BIGINT : new com.legend.sql.SqlType.Decimal(38, 18);
    }

    /** A side the pipeline ANSWERED as a host constant (a TDG seed string, a
     * rendered DDL text, a folded literal): bound as a VALUES relation and
     * canon-wrapped like any side, so the database still compares
     * (database-ADJUDICATED). Null = no literal spelling for the value's
     * kind (the caller reports it unjudged by kind). */
    static BodyDriver.@com.legend.base.Nullable WrappedSide constantSide(
            @com.legend.base.Nullable TypedSide answered, TypedSpec spec,
            com.legend.lowering.CanonRider rider, boolean canonicalOrder,
            BodyDriver.ExecEnv env, BodyDriver.ExecEnv on) {
        if (answered == null) {
            return null;
        }
        List<Object> values = com.legend.testexecutionplan.Verdicts.decodeSideValues(answered);
        com.legend.sql.SqlQuery plan = com.legend.lowering.VerdictSql.constantPlan(values);
        if (plan == null) {
            return null;
        }
        var w = com.legend.lowering.CanonicalRenderSql.wrapWithCanon(plan, spec.info(),
                canonicalOrder, com.legend.compiler.element.EqualityKeys
                        .resolve(env.ctx(), spec.info().type()),
                true, com.legend.lowering.CanonicalRenderSql.nameValued(
                        spec.info().type(), env.ctx()::tracksClassifier),
                rider.enumFrame());
        if (w.declineReason() != null) {
            rider.decline(w.declineReason());
            return new BodyDriver.WrappedSide(plan, spec.info(),
                    com.legend.plan.ResultShape.COLLECTION, on.target(), true);
        }
        rider.wrap(w.kinds(), w.many(), w.literalIndex());
        return new BodyDriver.WrappedSide(w.plan(), spec.info(),
                com.legend.plan.ResultShape.COLLECTION, on.target(), true);
    }

    static String describe(@com.legend.base.Nullable TypedSide r) {
        return r == null ? "no plan" : r.getClass().getSimpleName();
    }


    // ── leg 3.1c: the one-line families in database mode. Each side is
    // planned like an equality side (planSide); the predicate statement
    // returns the same verdict row; unjudged fails by name.

    /** A planned side for the predicate forms, or a reason it could not be. */
    public record SideRows(BodyDriver.@com.legend.base.Nullable WrappedSide side,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider,
            @com.legend.base.Nullable String why) {
        /** The side's canon rows; {@code literal} = the pair compares in the
         * literal channel (decided for BOTH sides — one channel per pair). */
        com.legend.sql.SqlQuery rows(boolean literal) {
            var r = java.util.Objects.requireNonNull(rider);
            var w = java.util.Objects.requireNonNull(side);
            int idx = literal && r.literalIndex() >= 0 ? r.literalIndex() : 0;
            return com.legend.lowering.VerdictSql.sideRows(w.plan(), r.tdsWrapped(),
                    "__canon" + idx, r.many() || r.tdsWrapped());
        }
        /** The side's own TEXT column as its rows (a database-built JSON
         * document, a golden bound as VALUES): the plan's first output,
         * read raw — no canon needed, none may exist (a graph plan's
         * canon declines by kind while its document is one VARCHAR). */
        com.legend.sql.SqlQuery textRows() {
            var w = java.util.Objects.requireNonNull(side);
            return com.legend.lowering.VerdictSql.sideRows(w.plan(), false,
                    w.plan().outputs().get(0).name(), false);
        }
        /** A literal collection's TEXTS as rows (one per element), raw. */
        com.legend.sql.SqlQuery textRowsMany() {
            var w = java.util.Objects.requireNonNull(side);
            return com.legend.lowering.VerdictSql.sideRows(w.plan(), false,
                    w.plan().outputs().get(0).name(), true);
        }
        /** The side's rows for counting: the canon may have declined. */
        /** A side DECLARED exactly one as a one-row relation {@code (__c,
         * value)} straight over its plan — no rows CTE, no limited read
         * (lean ladder rung 4). */
        com.legend.sql.SqlQuery scalarRow(boolean literal) {
            var r = java.util.Objects.requireNonNull(rider);
            var w = java.util.Objects.requireNonNull(side);
            int idx = literal && r.literalIndex() >= 0 ? r.literalIndex() : 0;
            return com.legend.lowering.VerdictSql.scalarRow(w.plan(), "__canon" + idx);
        }
        com.legend.sql.SqlQuery countRows() {
            var r = java.util.Objects.requireNonNull(rider);
            var w = java.util.Objects.requireNonNull(side);
            return r.tdsWrapped() ? com.legend.lowering.VerdictSql.gridCountRows(w.plan())
                    : com.legend.lowering.VerdictSql.countRows(w.plan());
        }
        /** The environment a statement over this side runs in: the side's own
         * session when it reads a store, the body's otherwise. */
        BodyDriver.ExecEnv on(BodyDriver.ExecEnv env) {
            return side != null && !side.storeFree() ? env.withTarget(side.target()) : env;
        }
    }

    /** A verdict decided at planning (a static decision: a statically identified {@code is} pair, the static kind
     *  gate, a grid's columns, a rendered header), planned as a step of the open root that the census counts at its
     *  turn (J39, 2026-11-18, the orchestrator's ruling (b): until then counted here, as it was planned). The database
     *  judge plans under its body's verdict batch, always. */
    static void staticallyDecided(BodyDriver.ExecEnv env) {
        java.util.Objects.requireNonNull(env.verdictBatch(), "a static verdict planned with no verdict batch").judged();
    }

    /** The database arm, chosen once per adjudication by the router. */
    static final VerdictArm ARM = new VerdictArm() {
        @Override public TypedSide rendered(String name, boolean wantEqual, List<TypedSpec> args, String form, TypedSpec rendered, @com.legend.base.Nullable String eForm, @com.legend.base.Nullable String aForm, boolean orderedForm, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // bucket 8 (homework §4s): the rendered VALUE against the golden
            // brought to rows by the render function's own grammar — the
            // grid / collection statements judge; ordered only when the
            // chain ends in a sort and the assert is ordered
            return AssertVerdicts.renderedValueVerdict(name, wantEqual, args, letPrefix, specs, env, hook,
                    orderedForm);
        }
        @Override public void staticallyDecided(BodyDriver.ExecEnv env) {
            DatabaseJudge.staticallyDecided(env);
        }
        @Override public TypedSide jsonStringsEqual(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return DatabaseJudge.jsonStringsEqual(name, args, letPrefix, specs, env, hook);
        }
        @Override public TypedSide equals(String name, boolean wantEqual, List<TypedSpec> args, boolean incidental, boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // 3.1b: grid sides route too (the statement frames the
            // peer by the grid's width; a grid PAIR is unjudged there)
            return databaseVerdict(name, wantEqual, args.get(0), args.get(1),
                    letPrefix, specs, env, hook, incidental, false);
        }
        @Override public TypedSide gridPair(String name, boolean wantEqual, List<TypedSpec> args, boolean sorted, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // the two planned grids' row canons against each other (VerdictSql.gridPair: the cells walked for the
            // declared-Float leniency where the schemas agree), a row multiset unless the actual side is sorted --
            // the grid owner's own rule (TdsCompare.grids), in the statement (the test-is-the-product plan's M10c
            // item 2)
            return databaseVerdict(name, wantEqual, args.get(0), args.get(1), letPrefix, specs, env, hook, !sorted,
                    false, false, false, true);
        }
        @Override public TypedSide quantified(String fqn, TypedSpec predMap, boolean wantTrue,
                com.legend.error.ErrorRecord failure, List<TypedSpec> letPrefix, SpecCompiler specs,
                BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // task #14 leg 2 (2026-09-21): the predicate vector is planned and the
            // database returns the one verdict row — no element true/false is read
            // in Java
            SideRows vector = planSide(predMap, false, letPrefix, specs, env, hook);
            if (vector.why() != null) {
                return unjudged(fqn, vector.why());
            }
            com.legend.lowering.CanonRider vr = vector.rider();
            if (vr != null && !vr.literalOnly() && vr.kinds().size() == 1
                    && vr.kinds().get(0) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN
                    && vector.side() != null) {
                // TYPED (item 3's S4): the vector's Booleans, its record the one fixed at planning
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedAllOf(
                        vector.side().plan(), wantTrue);
                if (typed != null) {
                    return runTyped(fqn, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.CONDITION, wantTrue, null, null, failure, -1),
                            vector.on(env));
                }
            }
            return runVerdict(fqn, true, com.legend.lowering.VerdictSql.allOf(vector.rows(false),
                    wantTrue, failure.sentence()), vector.on(env));
        }
        @Override public @com.legend.base.Nullable TypedSide quantifiedVector(String fqn, TypedSpec predMap, TypedSpec source,
                com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, List<TypedSpec> letPrefix,
                SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            com.legend.builtin.NativeFn.Verdict family = com.legend.builtin.NativeFn.Verdict
                    .of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
            if (family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS) {
                // an equality is judged element by element, by the judge's own spelling of the operands' kinds: the
                // unroll over a literal source, the per-element form over any other (step 5, 2026-11-04) -- never the
                // database's SQL equal over a predicate vector, which compares across kinds and drops a Decimal's scale
                return null;
            }
            if (family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQ_WITHIN_TOLERANCE
                    && com.legend.compiler.spec.VerdictQueries.unrollElements(source, letPrefix, env.ctx()) != null) {
                // a literal source (2026-11-04, the core program: every assert on its own merit): each element's assert
                // judged as itself -- the spelled tokens rearranged, nothing computed -- each a row of the segment's
                // statement, the first failing element's message upstream's own, as the host's row read words it
                return null;
            }
            SideRows vector = planSide(predMap, false, letPrefix, specs, env, hook);
            if (vector.why() != null) {
                return unjudged(fqn, vector.why());   // the contract held and the plan declined: loud, as the vector form is
            }
            // the vector contract's assert/assertFalse: the host's words (HostJudge.quantifiedRows), the call's literal
            // message or the run's words of a condition's failure; another family's rows keep the count's frame
            boolean assertFamily = family == com.legend.builtin.NativeFn.Verdict.ASSERT
                    || family == com.legend.builtin.NativeFn.Verdict.ASSERT_FALSE;
            com.legend.lowering.CanonRider vr = vector.rider();
            if (assertFamily && vr != null && !vr.literalOnly() && vr.kinds().size() == 1
                    && vr.kinds().get(0) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN
                    && vector.side() != null) {
                // TYPED (item 3's S4): the vector already carries the polarity; its record the host's vector judgment's
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedAllOf(
                        vector.side().plan(), true);
                if (typed != null) {
                    return runTyped(fqn, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.CONDITION, true, null, null,
                            com.legend.error.AssertKind.condition(family == com.legend.builtin.NativeFn.Verdict.ASSERT,
                                    com.legend.compiler.spec.VerdictQueries.callerMessage(root)), -1), vector.on(env));
                }
            }
            return runVerdict(fqn, true, com.legend.lowering.VerdictSql.allOf(vector.rows(false), true,
                    assertFamily ? com.legend.compiler.spec.VerdictQueries.assertMessage(root,
                            AssertVerdicts.defaultConditionMessage(family == com.legend.builtin.NativeFn.Verdict.ASSERT))
                            : null),
                    vector.on(env));
        }
        @Override public TypedSide sameElements(String name, List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseVerdict(name, true, args.get(0), args.get(1),
                    letPrefix, specs, env, hook, true, true);
        }
        @Override public TypedSide perElement(String name, TypedSpec pairs,
                com.legend.compiler.element.type.ExprType expected, com.legend.compiler.element.type.ExprType actual,
                List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // the operand pairs planned as any side the database computes (no canon over the pair itself: each
            // operand is spelled by its own kind inside the verdict); one statement, the first failing element's words
            SideRows vector = planSide(pairs, false, false, letPrefix, specs, env, hook);
            if (vector.side() == null) {
                return unjudged(name, java.util.Objects.requireNonNull(vector.why()));
            }
            com.legend.compiler.element.type.Type ek = expected.type();
            com.legend.compiler.element.type.Type ak = actual.type();
            if (typedKind(ek) && typedKind(ak)) {
                // TYPED (item 3's S4): each element's two operands as their kinds type them, the first failing one's
                // index and operands riding the failure; its record the host's row arm's
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedPerElement(
                        vector.side().plan(), ek, ak, crossNumberKinds(ek, ak) || !ek.equals(ak));
                if (typed != null) {
                    return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.PER_ELEMENT, true, null, null, null, -1),
                            vector.on(env));
                }
            }
            com.legend.sql.SqlQuery row = com.legend.lowering.VerdictSql.perElement(vector.side().plan(), ek, ak,
                    crossNumberKinds(ek, ak), com.legend.lowering.VerdictSql.decimalByValue(ek), words(env),
                    env.plan().styled());
            if (row == null) {
                return unjudged(name, "per-element: a kind with no spelling (" + ek.typeName() + " vs "
                        + ak.typeName() + ")");
            }
            return runVerdict(name, true, row, vector.on(env));
        }
        @Override public TypedSide is(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            KindClass ki = AssertVerdicts.kindKey(args.get(0), letPrefix, env);
            KindClass kj = AssertVerdicts.kindKey(args.get(1), letPrefix, env);
            if (ki instanceof KindClass.Enum && kj instanceof KindClass.Enum) {
                // an enum's identity IS its value: the equality statement
                // (Enumeration.NAME on both sides — bucket 1)
                return databaseVerdict(name, true, args.get(0), args.get(1),
                        letPrefix, specs, env, hook, false, false);
            }
            if (AssertVerdicts.elementTyped(args.get(0), specs) && AssertVerdicts.elementTyped(args.get(1), specs)) {
                TypedSpec cond = com.legend.resolver.ChainNormalizer.identityCondition(
                        specs.ctx(), args.get(0), args.get(1));
                return databaseCondition(name, cond, true, letPrefix, specs, env, hook,
                        com.legend.error.AssertKind.SAME_INSTANCE.record());
            }
            return unjudged(name, "is: neither an enum pair nor a tracked element pair");
        }
        @Override public TypedSide instanceOf(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // bucket 5: the model's subtype relation IS instanceOf —
            // minted as the native call, judged as a condition
            TypedSpec cond = com.legend.compiler.spec.VerdictQueries
                    .instanceOfCondition(args.get(0), args.get(1), specs);
            if (cond == null) {
                return unjudged(name, "instanceOf: no two-argument native in the catalog");
            }
            String type = AssertVerdicts.typeRefName(args.get(1));
            if (cond instanceof com.legend.compiler.spec.typed.TypedCBoolean decided && type != null
                    && args.get(0).info().type() instanceof com.legend.compiler.element.type.Type.Primitive p) {
                // the value's static type decided it: the value read for upstream's message, the host's words
                SideRows side = planSide(args.get(0), false, letPrefix, specs, env, hook);
                if (side.why() != null) {
                    return unjudged(name, side.why());
                }
                // a primitive type is named as upstream's elementToPath names it, by its name alone (the type's own
                // declaration read, never its spelling)
                String typePath = java.util.Arrays.stream(com.legend.compiler.element.type.Type.Primitive.values())
                        .filter(q -> q.qualifiedName().equals(type)).findFirst()
                        .map(com.legend.compiler.element.type.Type.Primitive::typeName).orElse(type);
                return runVerdict(name, true, com.legend.lowering.VerdictSql.instanceOfDecided(side.scalarRow(false),
                        decided.value(), typePath, p.typeName(),
                        p == com.legend.compiler.element.type.Type.Primitive.STRING, words(env)), side.on(env));
            }
            return databaseCondition(name, cond, true, letPrefix, specs, env, hook);
        }
        @Override public TypedSide eq(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // task #14 leg 2 (2026-09-21): eq over PRIMITIVES is equals — the database
            // verdict decides; a class-instance pair keeps the host's LOUD identity wall. One side a primitive: the
            // kind and the value, the database's kind-faithful canon decides (2026-11-03, the static gate's own rule)
            if (!(AssertVerdicts.classKind(args.get(0)) && AssertVerdicts.classKind(args.get(1)))) {
                return databaseVerdict(name, true, args.get(0), args.get(1),
                        letPrefix, specs, env, hook, false, false);
            }
            if (AssertVerdicts.keylessInstances(args.get(0), args.get(1), env.ctx())) {
                // two keyless instances: identity, the database's identity canon (each construction's __id)
                return databaseVerdict(name, true, args.get(0), args.get(1),
                        letPrefix, specs, env, hook, false, false);
            }
            // no host fallback (the deep audit, 2026-11-05): a keyed pair has no identity form in SQL
            return unjudged(name, "assertEq over keyed class instances: the database judge has no form for their identity");
        }
        @Override public @com.legend.base.Nullable TypedSide tdsEquivalent(String name, List<TypedSpec> targs, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseTdsEquivalent(name, targs, letPrefix, specs, env, hook);
        }
        @Override public TypedSide size(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseSize(name, args, letPrefix, specs, env, hook);
        }
        @Override public TypedSide contains(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseContains(name, args.get(0), args.get(1), letPrefix, specs, env, hook);
        }
        @Override public TypedSide tolerance(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseTolerance(name, args, letPrefix, specs, env, hook);
        }
        @Override public TypedSide condition(String name, TypedSpec cond, boolean wantTrue, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // the host's words (com.legend.testexec.HostJudgments.conditionOf): the catalog's, until S4 carries the record
            return databaseCondition(name, cond, wantTrue, letPrefix, specs, env, hook,
                    com.legend.error.AssertKind.condition(wantTrue, null));
        }
        @Override public TypedSide empty(String name, TypedSpec arg, boolean wantEmpty, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseEmpty(name, arg, wantEmpty, letPrefix, specs, env, hook);
        }
    };

    /** {@code assertJsonStringsEqual(golden, actual)}: the document the database built against
     * the golden's canonical text (compact, keys sorted on both sides), the engine's root
     * {@code [x] ≡ x} applied to the golden at compile time when the query root is
     * many-valued; an unsorted many-valued root is a MULTISET of root objects. */
    static TypedSide jsonStringsEqual(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // bucket 3: the document the database built against the
        // golden's CANONICAL text — compact, keys sorted on both
        // sides (the verdict plan's objects through JsonKeyOrder),
        // the engine's root [x] ≡ x applied to the golden at
        // compile time when the query root is many-valued; bytes
        // decide. Measured first (2026-09-19): 81 byte-equal as
        // built, 69 key order, 15 root envelope, 9 whitespace, 1
        // root order (unsorted chain — the collections leg).
        // the golden, through its let and the pretty-print natives
        // (identity up to whitespace), to the literal chain
        String goldenText = com.legend.compiler.spec.VerdictQueries
                .foldedStringLiteral(AssertVerdicts.chaseLets(
                        com.legend.compiler.spec.VerdictQueries.throughJsonPrettyPrint(
                                AssertVerdicts.chaseLets(args.get(0), letPrefix)), letPrefix));
        if (goldenText == null) {
            return unjudged(name, "json golden is not a literal");
        }
        // the golden read as strict JSON, the product's (json-simple, register S144, is the host judge's at the edge:
        // the two-form kind's planning read left with A4 of the compatibility program, stage A ratified by the user
        // 2026-11-19: the database judge plans for the product alone, rule 4 of the rules of record)
        Object goldenRead;
        try {
            goldenRead = JudgeJson.STRICT.read(goldenText);
        } catch (IllegalStateException malformed) {
            // the host judge's words for the same golden (com.legend.testexec.HostJudgments.jsonOf): a golden the rules refuse fails the test
            throw new IllegalStateException("golden JSON does not parse: " + malformed.getMessage(), malformed);
        }
        TypedSpec actualJson = com.legend.compiler.spec.VerdictQueries
                .throughJsonPrettyPrint(AssertVerdicts.chaseLets(args.get(1), letPrefix));
        SideRows ja = planSide(actualJson, false, false, true, letPrefix, specs, env, hook);
        // bucket 9: a root the query does not order is a MULTISET of root objects -- the golden's root elements against
        // the document's -- under the host's own test (com.legend.testexec.HostJudgments.jsonOf: the actual's OrderView INCIDENTAL), so the two
        // judges decide the same documents the same way (2026-11-03: the deleted rootMany sniff had gated it, and its
        // deletion left these unjudged here while the host judged them). A dialect that cannot place the element rows
        // falls to the byte compare below, which equal sequences pass and others fail
        if (ja.why() == null && com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                == com.legend.compiler.spec.OrderView.INCIDENTAL) {
            TypedSpec elements = com.legend.compiler.spec.VerdictQueries.jsonRootElements(goldenRead);
            if (elements != null) {
                SideRows jr = planSide(elements, true, false, false, letPrefix, specs, env, hook);
                if (jr.why() == null) {
                    com.legend.sql.SqlQuery mq = com.legend.lowering.VerdictSql.jsonRootMultiset(
                            jr.textRowsMany(), ja.textRows());
                    boolean placeable;
                    try {
                        env.dialect().renderStatement(env.read(mq));
                        placeable = true;
                    } catch (com.legend.sql.DialectCapability wall) {
                        placeable = false;
                    }
                    if (placeable) {
                        com.legend.diagnostics.VerdictCensus.sqlRoute(name, "json-bytes");
                        return runVerdict(name, true, mq, ja.on(env));
                    }
                }
            }
        }
        // the golden as written otherwise (the user's ruling, 2026-11-03: no [x] for x -- a document serialized
        // otherwise than the engine serializes it is the serializer's to fix)
        var golden = com.legend.compiler.spec.VerdictQueries.canonicalJsonGolden(goldenRead);
        SideRows je = planSide(golden, true, false, false, letPrefix, specs, env, hook);
        if (je.why() != null || ja.why() != null) {
            return unjudged(name, "json side: " + (je.why() != null ? je.why() : ja.why()));
        }
        com.legend.diagnostics.VerdictCensus.sqlRoute(name, "json-bytes");
        return runVerdict(name, true,
                com.legend.lowering.VerdictSql.jsonText(je.textRows(), ja.textRows()),
                ja.on(env));
    }
}
