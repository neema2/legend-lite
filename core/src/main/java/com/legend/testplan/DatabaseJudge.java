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

    /** {@code gridPairArm}: the pair is the GRID PAIR family's (two relation values compared as grids, the grid
     *  owner's rule): its schemas decide where they differ -- names or widths false at planning, kinds spelled in the
     *  expected's. Another family that reaches two grids (a flat cells compare) compares their cells alone
     *  (2026-11-04: the corpus's validation tests, whose cells agree under other column names). */
    static TypedSide databaseVerdict(String name, boolean wantEqual,
            TypedSpec eSpec, TypedSpec aSpec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, boolean canonicalOrder,
            boolean cellPool, boolean csvStrings, boolean gridPairArm) {
        KindClass ke = AssertVerdicts.kindKey(eSpec, letPrefix, env);
        KindClass ka = AssertVerdicts.kindKey(aSpec, letPrefix, env);
        boolean anyNil = com.legend.compiler.element.type.PlatformTypes.isNil(eSpec.info().type())
                || com.legend.compiler.element.type.PlatformTypes.isNil(aSpec.info().type());
        boolean anyAny = AssertVerdicts.isAnyStamped(eSpec) || AssertVerdicts.isAnyStamped(aSpec);
        boolean bothGrids = com.legend.compiler.element.type.Type.isRelation(eSpec.info().type())
                && com.legend.compiler.element.type.Type.isRelation(aSpec.info().type());
        // register S401: refused by name where only identity decides; a Variant against values of other types alone
        // is the typed pair's (the same only where both are empty, JudgmentFacts.typedPair), never the kind gate's
        // two primitives of different kinds (X4: never equal, S127) are the typed pair's too, its record the host's (the
        // final audit of c758fe95: a sentence planned here had been the one failure no record carried)
        if (ke != null && ka != null && !anyNil && !anyAny && !bothGrids && !ke.equals(ka)
                && !JudgmentFacts.variantDisjoint(eSpec, aSpec) && !(ke.primitive() && ka.primitive())) {
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
        // a GRID side (the tabular wrap): two grids compared as typed rows; a grid against a list as the list the
        // product reads it as; any other pair refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        if (why == null && re.tdsWrapped() != ra.tdsWrapped()) {
            // a grid against a list (item 3's S4, the last families, and the audit of 946d16f2): a one-column grid's
            // values, a wider grid's cells row-major, as any side's
            TypedSide one = gridColumnPair(name, wantEqual, eSpec, aSpec, re, ra, java.util.Objects.requireNonNull(we),
                    java.util.Objects.requireNonNull(wa), canonicalOrder, env);
            if (one != null) {
                return one;
            }
        }
        if (why == null && (re.tdsWrapped() || ra.tdsWrapped())) {
            var se = re.tdsWrapped() && ra.tdsWrapped() ? AssertVerdicts.effectiveSchema(java.util.Objects.requireNonNull(we))
                    : null;
            var sa = se == null ? null : AssertVerdicts.effectiveSchema(java.util.Objects.requireNonNull(wa));
            // TYPED (item 3's S4, tables): each grid's rows as typed cells, each by its own declared columns (the
            // user's ruling of 2026-11-03, the host's TdsCompare.grids), compared in order where the actual is
            // sorted, else as a multiset of rows; names or widths that differ false by the schemas
            var typed = !gridPairArm || cellPool || se == null || sa == null ? null : com.legend.lowering.VerdictSql
                    .typedGridPair(gridSide(java.util.Objects.requireNonNull(we).plan(), se, csvStrings), kinds(se),
                    gridSide(java.util.Objects.requireNonNull(wa).plan(), sa, csvStrings), kinds(sa), canonicalOrder,
                    wantEqual, AssertVerdicts.gridSchemasDiffer(se, sa), se, sa);
            if (typed == null) {
                return unjudged(name, se == null || sa == null ? "a grid against a side of another shape: no typed form"
                        : "two grids with no typed form (" + (cellPool ? "a cell pool" : "columns with no typed kind")
                        + ")");
            }
            return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                    com.legend.testexecutionplan.RootLoop.Family.GRIDS, wantEqual, null, null, null, -1),
                    env.withTarget(java.util.Objects.requireNonNull(we).storeFree() ? java.util.Objects
                            .requireNonNull(wa).target() : we.target()));
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
        // the pair's typed kinds (JudgmentFacts.typedPair: two number kinds unequal unless empty, S127; a date by its
        // kind and its instant, S399)
        JudgmentFacts.TypedPair tp = JudgmentFacts.typedPair(eSpec, re.kinds().get(ie), java.util.Objects.requireNonNull(we)
                .plan(), aSpec, ra.kinds().get(ia), java.util.Objects.requireNonNull(wa).plan(), env.ctx());
        if (tp != null && (tp.dates() || tp.tagged() || !literal(re, ra, anyNil, anyAny)
                || tp.form() == JudgmentFacts.TypedPair.Form.INSTANCE_TEXTS || tp.kindsDiffer()
                || tp.oneScalar(eSpec, we.plan(), aSpec, wa.plan(), env.ctx()))) {
            com.legend.lowering.VerdictSql.TypedRow typed = tp.row(we.plan(), wa.plan(), canonicalOrder, wantEqual);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(tp.family(canonicalOrder,
                        name), wantEqual,
                        JudgmentFacts.sideFacts(eSpec), JudgmentFacts.sideFacts(aSpec), null, -1), runOn);
            }
        }
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, "a pair with no typed form (" + re.kinds().get(ie).typeName() + " against "
                + ra.kinds().get(ia).typeName() + ")");
    }

    /** A one-column grid against a list (item 3's S4, the last families): the grid's column as a side of its declared
     *  kind, the peer its one kind (its literal channel's where it rides one), compared as any typed pair; null where
     *  the grid has another width or a side no typed form. */
    private static @com.legend.base.Nullable TypedSide gridColumnPair(String name, boolean wantEqual, TypedSpec eSpec,
            TypedSpec aSpec, com.legend.lowering.CanonRider re, com.legend.lowering.CanonRider ra,
            BodyDriver.WrappedSide we, BodyDriver.WrappedSide wa, boolean canonicalOrder, BodyDriver.ExecEnv env) {
        boolean gridE = re.tdsWrapped();
        var schema = AssertVerdicts.effectiveSchema(gridE ? we : wa);
        com.legend.lowering.CanonRider peer = gridE ? ra : re;
        com.legend.compiler.element.type.Type pk = peer.literalOnly() && peer.literalIndex() >= 0
                ? peer.kinds().get(peer.literalIndex()) : oneKind(peer);
        if (schema == null || schema.columns().isEmpty()) {
            return null;
        }
        com.legend.compiler.element.type.Type ck;
        com.legend.sql.SqlQuery column;
        if (schema.columns().size() > 1) {
            // a wider grid: its cells row-major, each of its column's kind (the product's flatten), a tagged list
            column = com.legend.lowering.VerdictSql.gridCells((gridE ? we : wa).plan(), schema);
            ck = new com.legend.compiler.element.type.Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY);
            pk = pk != null ? pk : (gridE ? aSpec : eSpec).info().type();
        } else {
            column = com.legend.lowering.VerdictSql.gridColumn((gridE ? we : wa).plan(), schema.columns().get(0).name());
            ck = schema.columns().get(0).type();
        }
        if (column == null || pk == null) {
            return null;
        }
        com.legend.sql.SqlQuery ep = gridE ? column : we.plan();
        com.legend.sql.SqlQuery ap = gridE ? wa.plan() : column;
        JudgmentFacts.TypedPair tp = JudgmentFacts.typedPair(eSpec, gridE ? ck : pk, ep, aSpec, gridE ? pk : ck, ap,
                env.ctx());
        com.legend.lowering.VerdictSql.TypedRow typed = tp == null ? null : tp.row(ep, ap, canonicalOrder, wantEqual);
        return tp == null || typed == null ? null : runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                tp.family(canonicalOrder, name), wantEqual, JudgmentFacts.sideFacts(eSpec), JudgmentFacts.sideFacts(aSpec),
                null, -1), env.withTarget(we.storeFree() ? wa.target() : we.target()));
    }

    /** Whether the pair compares in the literal channel (a mixed-kind or Any-stamped side). */
    private static boolean literal(com.legend.lowering.CanonRider re, com.legend.lowering.CanonRider ra, boolean anyNil,
            boolean anyAny) {
        return !anyNil && (anyAny || re.literalOnly() || ra.literalOnly());
    }

    /** A grid's declared kinds, column by column. */
    private static List<com.legend.compiler.element.type.Type> kinds(
            com.legend.compiler.element.type.Type.RelationType schema) {
        return schema.columns().stream().map(c -> c.type()).toList();
    }

    /** {@code message}, a caller's failure text, planned as the verdict carries it -- a constant the compiler folds (a
     *  literal, a join of literals), else the one-row relation the database computes -- with the session its row reads
     *  (the body's where it reads no store). */
    record PlannedMessage(com.legend.lowering.VerdictSql.FailureMessage message, com.legend.executionplan.ExecutionPlan.Target on) {
    }

    static PlannedMessage plannedMessage(TypedSpec message, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // a caller's message: a final print, its literals' join the compiler's (a ruled carve-out: Q-S7b, 2026-11-22)
        Object folded = com.legend.compiler.spec.typed.Literals.joined(message, letPrefix);
        if (folded instanceof String s) {
            return new PlannedMessage(new com.legend.lowering.VerdictSql.FailureMessage(s, null), env.target());
        }
        BodyDriver.PlannedValue pv = BodyDriver.planHostSide(message, letPrefix, specs, env, false, HostSides.Fold.NONE,
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

    /** A side's one kind, its literal channel aside (a rider lists its kinds, then its literal channel's where it has
     *  one), or null where it has another count or rides the literal channel alone (S4's measurement, 2026-11-22:
     *  until then the typed gates asked for one kind in all, and every side carrying a literal channel -- most of the
     *  conditions, tolerances and subsets -- declined). */
    private static com.legend.compiler.element.type.@com.legend.base.Nullable Type oneKind(com.legend.lowering.CanonRider r) {
        return !r.literalOnly() && r.kinds().size() - (r.literalIndex() >= 0 ? 1 : 0) == 1 ? r.kinds().get(0) : null;
    }

    /** A side's one number kind where it states one (an Integer, a Float, a Decimal), else null. */
    private static com.legend.compiler.element.type.@com.legend.base.Nullable Type numberKind(SideRows s) {
        com.legend.lowering.CanonRider r = s.rider();
        com.legend.compiler.element.type.@com.legend.base.Nullable Type k = r == null ? null : oneKind(r);
        if (k == null || s.side() == null) {
            return null;
        }
        // (a Decimal of a declared precision, every {@code ...D} literal's type, is a Decimal: the audit of 946d16f2)
        return k instanceof com.legend.compiler.element.type.Type.PrecisionDecimal
                ? com.legend.compiler.element.type.Type.Primitive.DECIMAL
                : k == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || k == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || k == com.legend.compiler.element.type.Type.Primitive.DECIMAL ? k : null;
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

    /** Whether a side is a Map or a List: a value with no identity on any carrier ({@code eq}'s refusal). */
    private static boolean collectionKind(TypedSpec s) {
        String f = com.legend.compiler.element.EqualityKeys.fqnOf(s.info().type());
        return com.legend.compiler.element.type.PlatformTypes.LIST.equals(f)
                || "meta::pure::functions::collection::Map".equals(f);
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
        com.legend.lowering.CanonRider nr = n.rider();
        if (cw.shape() != com.legend.plan.ResultShape.GRAPH && !envelope && nr != null && !nr.literalOnly()
                && oneKind(nr) == com.legend.compiler.element.type.Type.Primitive.INTEGER
                && n.side() != null) {
            // THE CONDITION THE ASSERT MEANS, size(c) == n (the equality program's first slice, 2026-11-26): the
            // product's own count and the product's own ==, each side's value read once and riding the failure
            TypedSpec size = com.legend.compiler.spec.VerdictQueries.sizeOf(args.get(0), specs);
            SideRows count = planSide(size, false, letPrefix, specs, env, hook);
            if (count.why() != null) {
                return unjudged(name, "the collection's size: " + count.why());
            }
            TypedSpec same = com.legend.compiler.spec.VerdictQueries.sameValue(args.get(1).info(), size.info(),
                    !negated, specs);
            com.legend.lowering.VerdictSql.TypedRow typed = count.side() == null ? null
                    : com.legend.lowering.VerdictSql.typedOverValues(List.of(
                    new com.legend.lowering.VerdictSql.ValueSide(com.legend.lowering.VerdictSql.TYPED_EXPECTED,
                            n.side().plan(), com.legend.compiler.element.type.Type.Primitive.INTEGER),
                    new com.legend.lowering.VerdictSql.ValueSide(com.legend.lowering.VerdictSql.TYPED_ACTUAL,
                            count.side().plan(), com.legend.compiler.element.type.Type.Primitive.INTEGER)),
                    bound -> com.legend.planner.SidePlanner.over(same, java.util.Map.of(
                            com.legend.compiler.spec.VerdictQueries.EXPECTED, bound.get(0),
                            com.legend.compiler.spec.VerdictQueries.ACTUAL, bound.get(1)), env.plan()));
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.SIZE, !negated, null, null, null, -1),
                        count.on(env));
            }
        }
        if (cw.shape() == com.legend.plan.ResultShape.GRAPH && nr != null && !nr.literalOnly()
                && oneKind(nr) == com.legend.compiler.element.type.Type.Primitive.INTEGER && n.side() != null) {
            // TYPED (item 3's S4, the last families): a graph's count of root objects against the size's one Integer
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedSizeOfGraph(cw.plan(),
                    n.side().plan(), !negated);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.SIZE, !negated, null, null, null, -1), coll.on(env));
            }
        }
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, envelope ? "a grid's envelope size: no typed form"
                : "a size that is no one Integer: no typed form");
    }

    static TypedSide databaseEmpty(String name, TypedSpec arg, boolean wantEmpty,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        SideRows side = planSide(arg, false, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
        }
        var sw = java.util.Objects.requireNonNull(side.side());
        // the elements recorded by their kind where the side's canon names one primitive kind
        com.legend.lowering.CanonRider r = side.rider();
        // (a side the types say is empty -- Nil, an empty literal's kind on its literal channel alone -- its own kind)
        if (sw.shape() != com.legend.plan.ResultShape.GRAPH && r != null && !r.tdsWrapped() && JudgmentFacts.emptyKind(
                r.kinds().isEmpty() ? arg.info().type() : r.kinds().stream().allMatch(
                com.legend.compiler.element.type.PlatformTypes::isNil) ? r.kinds().get(0) : oneKind(r), sw.plan())
                instanceof com.legend.compiler.element.type.Type rk) {
            // TYPED (item 3's S4): the side's values as one list, its emptiness the verdict, the list its failure's
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedEmpty(sw.plan(),
                    rk, wantEmpty);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.EMPTY, wantEmpty, null, null, null, -1),
                        side.on(env));
            }
        }
        // TYPED (item 3's S4, the last families): a graph's count of root objects; assertEmpty's failure its root
        // objects, assertNotEmpty's none (so it reads no element: every database spells it)
        if (sw.shape() != com.legend.plan.ResultShape.GRAPH && !sw.plan().outputs().isEmpty()
                && sw.plan().outputs().get(0).type() instanceof com.legend.sql.SqlType.Tagged) {
            // TYPED (the audit of 946d16f2): a tagged side's rows, its values each its kind and its value
            com.legend.lowering.VerdictSql.TypedRow tagged = com.legend.lowering.VerdictSql.typedEmptyTagged(sw.plan(),
                    arg.info().type(), wantEmpty);
            if (tagged != null) {
                return runTyped(name, tagged, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.EMPTY_TAGGED, wantEmpty, null, null, null, -1),
                        side.on(env));
            }
        }
        com.legend.lowering.VerdictSql.TypedRow graph = sw.shape() != com.legend.plan.ResultShape.GRAPH ? null
                : wantEmpty ? com.legend.lowering.VerdictSql.typedEmptyGraph(sw.plan(), arg.info().type())
                : com.legend.lowering.VerdictSql.typedNotEmptyGraph(sw.plan());
        if (graph != null) {
            return runTyped(name, graph, new com.legend.testexecutionplan.RootLoop.Report(wantEmpty
                    ? com.legend.testexecutionplan.RootLoop.Family.EMPTY_GRAPH
                    : com.legend.testexecutionplan.RootLoop.Family.EMPTY, wantEmpty, null, null, null, -1), side.on(env));
        }
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, sw.shape() == com.legend.plan.ResultShape.GRAPH
                ? "a graph with no value column: no typed form"
                : "an emptiness with no typed form (" + (r == null ? "no rider" : r.kinds()) + ")");
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
        // among Variants (a mixed collection's member too), register S401's refusal, as the host judge's
        if (JudgmentFacts.variantMembership(coll, val)) {
            // a Variant's membership among values of other types: no typed form, refused by name (the audit of
            // c2217801, 2026-11-23: a scalar Variant's membership compared the JSON texts)
            return unjudged(name, "a Variant membership: no typed form");
        }
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
        // TYPED (item 3's S4): some element of the collection IS NOT DISTINCT FROM the value, both of one kind, or
        // instances of one class each read as its typed leaves (as equality reads them)
        com.legend.compiler.element.type.Type ck = com.legend.lowering.VerdictSql.typedClass(oneKind(cr));
        String cf = com.legend.compiler.element.EqualityKeys.fqnOf(coll.info().type());
        com.legend.lowering.VerdictSql.TypedRow typed = literal || cr.tdsWrapped() ? null
                : ck != null && ck.equals(com.legend.lowering.VerdictSql.typedClass(oneKind(vr)))
                ? com.legend.lowering.VerdictSql.typedContains(java.util.Objects.requireNonNull(c.side()).plan(), ck,
                        java.util.Objects.requireNonNull(v.side()).plan(), ck, !negated)
                : cf != null && cf.equals(com.legend.compiler.element.EqualityKeys.fqnOf(val.info().type()))
                ? com.legend.lowering.VerdictSql.typedContainsInstances(java.util.Objects.requireNonNull(c.side()).plan(),
                        coll.info().type(), java.util.Objects.requireNonNull(v.side()).plan(), val.info().type(),
                        com.legend.compiler.element.EqualityKeys.resolve(env.ctx(), coll.info().type()), !negated) : null;
        if (typed != null) {
            return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                    com.legend.testexecutionplan.RootLoop.Family.CONTAINS, !negated, JudgmentFacts.sideFacts(coll),
                    JudgmentFacts.sideFacts(val), null, -1), c.on(env));
        }
        if (!cr.tdsWrapped()) {
            // TYPED (the audit of 946d16f2): tagged values -- a mixed list, an Any, a literal channel's -- each its kind
            // and its value
            typed = com.legend.lowering.VerdictSql.typedContainsTagged(java.util.Objects.requireNonNull(c.side()).plan(),
                    coll.info().type(), java.util.Objects.requireNonNull(v.side()).plan(), val.info().type(), !negated);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.CONTAINS_TAGGED, !negated,
                        JudgmentFacts.sideFacts(coll), JudgmentFacts.sideFacts(val), null, -1), c.on(env));
            }
        }
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, literal ? "a literal channel's membership: no typed form"
                : "a membership with no typed form (" + cr.kinds() + " against " + vr.kinds() + ")");
    }

    /** {@code assertTdsEquivalent} in database mode (bucket 5): both grids
     * planned, the column names checked statically (the schemas), the cells
     * aligned by position in one statement with the numeric / temporal
     * tolerances (VerdictSql.typedTdsEquivalent). */
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
        List<com.legend.compiler.element.type.Type> kinds = kinds(s1);
        // the cells by each grid's own kinds (toleranceCells); no grid-against-peer leniency reads its Float columns
        var g1 = new com.legend.lowering.VerdictSql.GridSide(w1.plan(), s1.columns().size(), List.of());
        var g2 = new com.legend.lowering.VerdictSql.GridSide(w2.plan(), s2.columns().size(), List.of());
        SideRows delta = planSide(targs.get(2), false, letPrefix, specs, env, hook);
        SideRows timeDelta = planSide(targs.size() == 4 ? targs.get(3)
                : com.legend.compiler.spec.VerdictQueries.zeroLiteral(), false, letPrefix, specs, env, hook);
        if (delta.why() != null || timeDelta.why() != null) {
            return unjudged(name, "tdsEquivalent delta: "
                    + (delta.why() != null ? delta.why() : timeDelta.why()));
        }
        // TYPED (item 3's S4, tables): the verdict over the typed cells, each grid's rows riding a failure for the
        // host's record (until then the row's sentence, and a sentence of its own where the columns differ)
        var typed = com.legend.lowering.VerdictSql.typedTdsEquivalent(g1, g2, kinds, kinds(s2), delta.rows(false),
                timeDelta.rows(false), AssertVerdicts.gridSchemasDiffer(s1, s2), s1, s2);
        return typed == null ? unjudged(name, "tdsEquivalent: a grid whose plan names no cells") : runTyped(name,
                typed, new com.legend.testexecutionplan.RootLoop.Report(
                com.legend.testexecutionplan.RootLoop.Family.TDS_EQUIVALENT, true, null, null, null, -1),
                env.withTarget(w1.storeFree() ? w2.target() : w1.target()));
    }

    /** {@code failure}: the failure's record the assert fixes (an {@code assert}'s, an element pair's
     *  {@code assertIs}), the host's. */
    static TypedSide databaseCondition(String name, TypedSpec cond, boolean wantTrue,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, com.legend.error.ErrorRecord failure) {
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
            if (!literal && com.legend.lowering.VerdictSql.typedClass(oneKind(nr)) instanceof com.legend.compiler.element.type.Type nk
                    && nk.equals(com.legend.lowering.VerdictSql.typedClass(oneKind(hr))) && need.side() != null && have.side() != null) {
                // TYPED (item 3's S4): no needed value without a have not distinct from it; the condition's record
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedSubset(
                        need.side().plan(), have.side().plan(), nk, wantTrue);
                if (typed != null) {
                    return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.CONDITION, wantTrue, null, null, failure, -1),
                            need.on(env));
                }
            }
            // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
            return unjudged(name, "forAll-contains with no typed form (" + nr.kinds() + " in " + hr.kinds() + ")");
        }
        SideRows side = planSide(cond, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
        }
        com.legend.lowering.CanonRider cr = side.rider();
        if (cr != null && oneKind(cr) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN && side.side() != null) {
            // TYPED (item 3's S4): the condition's one Boolean, its failure the record fixed here
            com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedCondition(
                    side.side().plan(), wantTrue);
            if (typed != null) {
                return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                        com.legend.testexecutionplan.RootLoop.Family.CONDITION, wantTrue, null, null, failure, -1),
                        side.on(env));
            }
        }
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, "a condition with no typed form (" + (cr == null ? "no rider" : cr.kinds()) + ")");
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
        // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
        return unjudged(name, "a tolerance over sides that are no one number each");
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
            // a rendered text is a String, compared as one (item 3's S4, phase 2)
            return AssertVerdicts.renderedValueVerdict(name, wantEqual, args, letPrefix, specs, env, hook);
        }
        @Override public void staticallyDecided(BodyDriver.ExecEnv env) {
            DatabaseJudge.staticallyDecided(env);
        }
        @Override public TypedSide jsonStringsEqual(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return DatabaseJudge.jsonStringsEqual(name, args, letPrefix, specs, env, hook);
        }
        @Override public TypedSide equals(String name, boolean wantEqual, List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // 3.1b: grid sides route too (the statement frames the
            // peer by the grid's width; a grid PAIR is unjudged there)
            return databaseVerdict(name, wantEqual, args.get(0), args.get(1),
                    letPrefix, specs, env, hook, false, false);
        }
        @Override public TypedSide gridPair(String name, boolean wantEqual, List<TypedSpec> args, boolean sorted, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // the two planned grids' row canons against each other (VerdictSql.gridPair: the cells walked for the
            // declared-Float leniency where the schemas agree), a row multiset unless the actual side is sorted --
            // the grid owner's own rule (TdsCompare.grids), in the statement (the test-is-the-product plan's M10c
            // item 2)
            return databaseVerdict(name, wantEqual, args.get(0), args.get(1), letPrefix, specs, env, hook, !sorted,
                    false, false, true);
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
            if (vr != null && oneKind(vr) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN
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
            // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
            return unjudged(fqn, "a quantifier over values that are no Booleans: no typed form");
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
            if (assertFamily && vr != null && oneKind(vr) == com.legend.compiler.element.type.Type.Primitive.BOOLEAN
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
            // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
            return unjudged(fqn, "a quantified " + (assertFamily ? "condition" : "family") + " over a vector with no"
                    + " typed form");
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
            if (com.legend.lowering.VerdictSql.typedClass(ek) instanceof com.legend.compiler.element.type.Type.Primitive pe && com.legend.lowering.VerdictSql.typedClass(ak) instanceof com.legend.compiler.element.type.Type.Primitive pa) {
                // TYPED (item 3's S4): each element's two operands as their kinds type them, the first failing one's
                // index and operands riding the failure; its record the host's row arm's
                com.legend.lowering.VerdictSql.TypedRow typed = com.legend.lowering.VerdictSql.typedPerElement(
                        vector.side().plan(), pe, pa, !pe.equals(pa));
                if (typed != null) {
                    return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(
                            com.legend.testexecutionplan.RootLoop.Family.PER_ELEMENT, true, null, null, null, -1),
                            vector.on(env));
                }
            }
            // no typed form: refused by name (item 3's S4, the sentence rows' deletion, 2026-11-22)
            return unjudged(name, "per-element: kinds with no typed form (" + ek.typeName() + " vs " + ak.typeName()
                    + ")");
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
            com.legend.compiler.element.type.Type vk = args.get(0).info().type();
            if (cond instanceof com.legend.compiler.spec.typed.TypedCBoolean decided && type != null
                    && (vk instanceof com.legend.compiler.element.type.Type.Primitive
                    || com.legend.compiler.element.EqualityKeys.fqnOf(vk) != null)) {
                // the value's static type decided it: the value read for upstream's message, the host's words
                SideRows side = planSide(args.get(0), false, letPrefix, specs, env, hook);
                if (side.why() != null) {
                    return unjudged(name, side.why());
                }
                // TYPED (item 3's S4): the decided verdict, the value (a primitive, or an instance carried as a
                // struct) and the type's name riding its failure, the record the host's (AssertRecords.notInstanceOf:
                // the type named by its name alone)
                var typed = com.legend.lowering.VerdictSql.typedInstanceOf(java.util.Objects.requireNonNull(side.side())
                        .plan(), vk, decided.value(), type);
                return typed == null ? unjudged(name, "instanceOf: a value with no typed column") : runTyped(name, typed,
                        new com.legend.testexecutionplan.RootLoop.Report(com.legend.testexecutionplan.RootLoop.Family
                                .INSTANCE_OF, true, null, null, null, -1), side.on(env));
            }
            // TYPED (item 3's S4, the last families): the database's answer to the native (a value whose kind the
            // database decides), the value and the type's name riding its failure, as the decided form's record
            SideRows cs = planSide(cond, false, letPrefix, specs, env, hook);
            SideRows vs = type == null || cs.why() != null ? cs : planSide(args.get(0), false, letPrefix, specs, env, hook);
            BodyDriver.WrappedSide cw = cs.side();
            BodyDriver.WrappedSide vw = vs.side();
            if (type == null || cw == null || vw == null) {
                return unjudged(name, "instanceOf: " + (type == null ? "a type that is no literal reference"
                        : cs.why() != null ? cs.why() : String.valueOf(vs.why())));
            }
            var held = com.legend.lowering.VerdictSql.typedInstanceOfHeld(cw.plan(), vw.plan(), vk, type);
            return held == null ? unjudged(name, "instanceOf: a value with no typed reading") : runTyped(name, held,
                    new com.legend.testexecutionplan.RootLoop.Report(vw.plan().outputs().get(0).type()
                            instanceof com.legend.sql.SqlType.Tagged
                            ? com.legend.testexecutionplan.RootLoop.Family.INSTANCE_OF_TAGGED
                            : com.legend.testexecutionplan.RootLoop.Family.INSTANCE_OF, true, null, null, null, -1),
                    cs.on(env));
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
            if (collectionKind(args.get(0)) || collectionKind(args.get(1))) {
                // a Map's or a List's eq is its identity, which no carrier holds: refused by name, the host's words
                // (PureAsserts.eqHolds; the audit of 2026-11-22, M6: a Map's eq passed here by its values)
                throw new com.legend.error.NotImplementedException("assertEq over non-primitive values: eq is INSTANCE"
                        + " identity, which is not observable on a value wire (assertEquals is the structural compare)");
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
                .stringToken(AssertVerdicts.chaseLets(
                        com.legend.compiler.spec.VerdictQueries.throughJsonPrettyPrint(
                                AssertVerdicts.chaseLets(args.get(0), letPrefix)), letPrefix));
        if (goldenText == null) {
            return unjudged(name, "json golden is not one literal (a join of literals is the database's: not planned)");
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
        // TYPED (item 3's S4, the last families): the document built with its keys sorted against the golden's canonical
        // text (the user's ruling, 2026-11-03: no [x] for x -- a document serialized otherwise than the engine
        // serializes it is the serializer's to fix); a root the query does not order is a MULTISET of root objects, the
        // golden's root elements against the document's, under the host's own test (HostJudgments.jsonOf: the actual's
        // OrderView INCIDENTAL), the document as its plan writes it riding the failure
        // a literal actual is read and canonicalized at planning as the golden is (a constant folded; the audit of
        // 946d16f2: its keys were compared in their written order); a computed one the database builds keys sorted
        TypedSpec writtenActual = actualJson;
        String actualText = com.legend.compiler.spec.VerdictQueries.stringToken(actualJson);
        if (actualText != null) {
            // (an actual that does not parse raises at planning, as a golden that does not parse does -- before the
            // body's earlier asserts, where the host raises at the assert's own turn: the audit of 3f2fe452, recorded)
            actualJson = com.legend.compiler.spec.VerdictQueries.canonicalJsonGolden(JudgeJson.STRICT.read(actualText));
        }
        SideRows ja = planSide(actualJson, false, false, true, letPrefix, specs, env, hook);
        SideRows jw = planSide(writtenActual, false, false, false, letPrefix, specs, env, hook);
        SideRows je = planSide(com.legend.compiler.spec.VerdictQueries.canonicalJsonGolden(goldenRead), true, false,
                false, letPrefix, specs, env, hook);
        boolean unordered = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                == com.legend.compiler.spec.OrderView.INCIDENTAL;
        TypedSpec elements = unordered ? com.legend.compiler.spec.VerdictQueries.jsonRootElements(goldenRead) : null;
        SideRows jr = elements == null ? null : planSide(elements, true, false, false, letPrefix, specs, env, hook);
        BodyDriver.WrappedSide a = ja.side();
        BodyDriver.WrappedSide w = jw.side();
        BodyDriver.WrappedSide e = je.side();
        if (a == null || w == null || e == null) {
            return unjudged(name, "json side: " + (je.why() != null ? je.why() : ja.why() != null ? ja.why() : jw.why()));
        }
        BodyDriver.WrappedSide r = jr == null ? null : jr.side();
        com.legend.lowering.VerdictSql.TypedRow typed = r == null ? null
                : com.legend.lowering.VerdictSql.typedJson(e.plan(), a.plan(), w.plan(), goldenText, r.plan());
        if (r == null) {
            // an ordered root, or a golden that is no array: the canonical documents compared whole
            typed = com.legend.lowering.VerdictSql.typedJson(e.plan(), a.plan(), w.plan(), goldenText, null);
        }
        // (a dialect that cannot place the root's element rows refuses the body by name where it renders the row: no
        // whole-document comparison stands in for the multiset, the audit of 946d16f2)
        if (typed == null) {
            return unjudged(name, "json: a side with no typed value column");
        }
        com.legend.diagnostics.VerdictCensus.sqlRoute(name, "json-typed");
        return runTyped(name, typed, new com.legend.testexecutionplan.RootLoop.Report(unordered
                ? com.legend.testexecutionplan.RootLoop.Family.JSON_UNORDERED_ROOT
                : com.legend.testexecutionplan.RootLoop.Family.JSON, true, null, null, null, -1), ja.on(env));
    }
}
