// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.judge.TypedSide;
import com.legend.judge.PureAsserts;
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

final class DatabaseJudge {
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
                com.legend.judge.CanonicalDivergence.sqlJudgedInDatabase(name);
                return wantEqual ? AssertVerdicts.fail(name + ": kinds differ (" + AssertVerdicts.typeName(eSpec)
                        + " vs " + AssertVerdicts.typeName(aSpec) + ")") : AssertVerdicts.ok();
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
                ? env.withSession(pe.side().session())
                : pa.side() != null && !pa.side().storeFree() ? env.withSession(pa.side().session())
                : env;
        BodyDriver.WrappedSide we = pe.side() != null ? pe.side()
                : constantSide(pe.answered(), eSpec, re, false, env, on);
        BodyDriver.WrappedSide wa = pa.side() != null ? pa.side()
                : constantSide(pa.answered(), aSpec, ra, false, env, on);
        String why = we == null ? "host-value AssertVerdicts.side (expected): " + describe(pe.answered())
                : wa == null ? "host-value AssertVerdicts.side (actual): " + describe(pa.answered())
                : re.declined() != null ? "side-e: " + re.declined()
                : ra.declined() != null ? "side-a: " + ra.declined()
                : we.session() != wa.session() && !we.storeFree() && !wa.storeFree()
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
                            canonicalOrder);
                } else {
                    pq = com.legend.lowering.VerdictSql.gridPair(we.plan(), wa.plan(), canonicalOrder);
                }
                if (gridPairArm && AssertVerdicts.gridSchemasDiffer(eSpec, aSpec)) {
                    // names or widths differ: false by the schemas (decided at planning), the rows read for the message
                    pq = com.legend.lowering.VerdictSql.decidedFalse(pq);
                }
                return runVerdict(name, wantEqual, message.apply(pq),
                        env.withSession(we.storeFree() ? wa.session() : we.session()));
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
                            ? com.legend.lowering.VerdictSql.gridCells(grid, peer, gridE)
                            : com.legend.lowering.VerdictSql.gridRows(grid, peer, gridE,
                                    canonicalOrder);
                    return runVerdict(name, wantEqual, message.apply(gq),
                            env.withSession(gw.storeFree() ? pw.session() : gw.session()));
                }
            }
        }
        // a store-free side rides the store-reading side's database
        BodyDriver.ExecEnv runOn = we != null && wa != null
                ? env.withSession(we.storeFree() ? wa.session() : we.session()) : env;
        int ie = -1;
        int ia = -1;
        if (why == null) {
            boolean literal = !anyNil && (anyAny || re.literalOnly() || ra.literalOnly());
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
        com.legend.sql.SqlQuery vq = com.legend.lowering.VerdictSql.equality(
                new com.legend.lowering.VerdictSql.Side(
                        java.util.Objects.requireNonNull(we).plan(), "__canon" + ie,
                        re.many(), canonicalOrder,
                        re.kinds().get(ie) == com.legend.compiler.element.type.Type.Primitive.FLOAT),
                new com.legend.lowering.VerdictSql.Side(
                        java.util.Objects.requireNonNull(wa).plan(), "__canon" + ia,
                        ra.many(), canonicalOrder,
                        ra.kinds().get(ia) == com.legend.compiler.element.type.Type.Primitive.FLOAT));
        // two single Strings compared by their raw text (no literal channel, whose canon is quoted already): the
        // message prints them as upstream's %r prints a String, quoted and escaped (2026-11-03, round 18: a TDS's
        // toString against its golden read unquoted under the database judge alone on Postgres, MariaDB, MySQL and
        // ClickHouse -- the host quotes it)
        boolean rawStrings = ie != re.literalIndex() && ia != ra.literalIndex() && !re.many() && !ra.many()
                && re.kinds().get(ie) == com.legend.compiler.element.type.Type.Primitive.STRING
                && ra.kinds().get(ia) == com.legend.compiler.element.type.Type.Primitive.STRING;
        String expectedClass = rawStrings || !wantEqual ? null : instancesMessage(eSpec, aSpec);
        if (expectedClass != null) {
            // two class sides the database serializes as their objects: the failure message spells them as the host
            // judge prints instances, <values instanceOf Type> (2026-11-04, the core program)
            com.legend.sql.SqlQuery reprE = com.legend.lowering.VerdictSql.instancesRepr(we.plan(), expectedClass);
            com.legend.sql.SqlQuery reprA = com.legend.lowering.VerdictSql.instancesRepr(wa.plan(),
                    java.util.Objects.requireNonNull(classOf(aSpec)));
            if (reprE != null && reprA != null) {
                vq = com.legend.lowering.VerdictSql.messageAsInstances(vq,
                        com.legend.lowering.VerdictSql.firstValueRow(reprE),
                        com.legend.lowering.VerdictSql.firstValueRow(reprA));
            }
        }
        return runVerdict(name, wantEqual, rawStrings ? com.legend.lowering.VerdictSql.messageAsStrings(vq)
                : message.apply(vq), runOn);
    }

    /** The declared class of {@code e} when both {@code e} and {@code a} are class-typed (the expected side's, as the host
     *  judge names each side by its own declared type): the instance message's case; null otherwise. */
    private static @com.legend.base.Nullable String instancesMessage(TypedSpec e, TypedSpec a) {
        String ce = classOf(e);
        return ce != null && classOf(a) != null ? ce : null;
    }

    private static @com.legend.base.Nullable String classOf(TypedSpec s) {
        return s.info().type() instanceof com.legend.compiler.element.type.Type.ClassType ct
                && !com.legend.compiler.element.type.PlatformTypes.isAny(ct) ? ct.fqn() : null;
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
    /** {@code message}, a caller's failure text, as the verdict row carries it: a constant the compiler folds (a
     *  literal, a join of literals), else the one-row relation the database computes ({@link #textRow}). */
    static com.legend.lowering.VerdictSql.FailureMessage failureMessage(TypedSpec message, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        Object folded = com.legend.compiler.spec.typed.Literals.fold(message, letPrefix);
        if (folded instanceof String s) {
            return new com.legend.lowering.VerdictSql.FailureMessage(s, null);
        }
        com.legend.sql.SqlQuery row = textRow(message, letPrefix, specs, env, hook);
        if (row == null) {
            throw new IllegalStateException("an assert's message planned no side and folds to no String: "
                    + message.getClass().getSimpleName());
        }
        return new com.legend.lowering.VerdictSql.FailureMessage(null, row);
    }

    static com.legend.sql.@com.legend.base.Nullable SqlQuery textRow(TypedSpec spec, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        BodyDriver.PlannedValue pv = BodyDriver.planHostSide(spec, letPrefix, specs, env, false, false,
                hook == null ? null : hook::apply);
        BodyDriver.WrappedSide side = pv.side();
        return side == null ? null : com.legend.lowering.VerdictSql.firstValueRow(side.plan());
    }

    /** Whether {@code vq}, a verdict statement of the equality family, HOLDS: run now on {@code runOn}'s session (no
     *  batch open), its row read for the polarity it computed -- an unjudged row holds nothing. A generic comparison
     *  whose failure a ruled fallback decides ({@code AssertVerdicts.textFirst}, 2026-11-04, the core program). */
    static boolean holdsNow(String name, com.legend.sql.SqlQuery vq, BodyDriver.ExecEnv runOn) {
        com.legend.judge.VerdictBatch batch = runOn.verdictBatch();
        com.legend.sql.SqlQuery row = com.legend.lowering.VerdictSql.selfContained(vq, name, true);
        List<Object> read = com.legend.judge.VerdictBatch.executeOne(
                name, batch == null ? row : com.legend.sql.FrameCtes.attach(row, batch.frames()),
                runOn.session(), JudgePlans.runner(runOn));
        return read.get(3) == null && Boolean.TRUE.equals(read.get(5));
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
        com.legend.judge.VerdictBatch batch = runOn.verdictBatch();
        if (batch != null && batch.active()) {
            batch.defer(name, wantEqual, vq, runOn.session());   // leg 3.4: judged at the flush
            return AssertVerdicts.ok();
        }
        // the verdict statement is a plan whose one row is the verdict (JudgePlans, step 6), the row self-contained
        // (M8b): its polarity and its message in the statement
        com.legend.sql.SqlQuery row = com.legend.lowering.VerdictSql.selfContained(vq, name, wantEqual);
        return verdictOf(name, com.legend.judge.VerdictBatch.executeOne(
                name, batch == null ? row : com.legend.sql.FrameCtes.attach(row, batch.frames()),
                runOn.session(), JudgePlans.runner(runOn)));
    }

    /** The verdict of one row ({@code verdict, expected, actual, unjudged, lenient, holds, message}): unjudged
     * (counted, the assert fails with the reason), or the row's own answer -- holds, or its message raised (the
     * test-is-the-product plan's M8b: the polarity and the text are the statement's; this reads two columns). */
    static TypedSide verdictOf(String name, List<Object> row) {
        if (com.legend.diagnostics.Diagnostics.dumpSql()) {
            // the SQL dump's companion: the verdict row the statement returned
            System.err.println("[verdict] " + name + " verdict=" + row.get(0)
                    + " expected=" + row.get(1) + " actual=" + row.get(2)
                    + " unjudged=" + row.get(3) + " holds=" + row.get(5));
        }
        Object unjudged = row.get(3);
        if (unjudged != null) {
            // the evidence columns ride the message (bounded): an accepted
            // divergence matches its witness here, and a row is diagnosable
            // without a re-run
            return unjudged(name, String.valueOf(unjudged),
                    "\nexpected: " + AssertVerdicts.excerpt(row.get(1))
                    + "\nactual:   " + AssertVerdicts.excerpt(row.get(2)));
        }
        if (!(row.get(5) instanceof Boolean holds)) {
            throw new IllegalStateException(name + ": the holds column is not a boolean: " + row.get(5));
        }
        com.legend.judge.CanonicalDivergence.sqlJudgedInDatabase(name);
        if (Boolean.TRUE.equals(row.get(4))) {
            // the declared 2-ULP Float leniency decided it — counted as
            // host mode counts its own firings
            com.legend.judge.CanonicalDivergence.sqlUlpPolicy("database " + name);
        }
        if (holds) {
            return AssertVerdicts.ok();
        }
        return AssertVerdicts.fail(String.valueOf(row.get(6)));
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
            env.dialect().render(w.plan());
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
        com.legend.judge.CanonicalDivergence.sqlUnjudged(name, why);
        throw com.legend.error.AssertFailed.unjudged(why,
                name + ": UNJUDGED in database mode — " + why + detail);
    }

    static TypedSide databaseSize(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
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
        com.legend.sql.SqlQuery vq = cw.shape() == com.legend.plan.ResultShape.GRAPH
                ? com.legend.lowering.VerdictSql.sizeOfGraph(cw.plan(), n.scalarRow(false))
                : com.legend.lowering.VerdictSql.size(coll.countRows(), n.scalarRow(false), envelope);
        return runVerdict(name, true, vq, coll.on(env));
    }

    static TypedSide databaseEmpty(String name, TypedSpec arg, boolean wantEmpty,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        SideRows side = planSide(arg, false, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
        }
        var sw = java.util.Objects.requireNonNull(side.side());
        com.legend.sql.SqlQuery vq = sw.shape() == com.legend.plan.ResultShape.GRAPH
                ? com.legend.lowering.VerdictSql.emptyOfGraph(sw.plan(), wantEmpty)
                : com.legend.lowering.VerdictSql.empty(side.countRows(), wantEmpty);
        return runVerdict(name, true, vq, side.on(env));
    }

    static TypedSide databaseContains(String name, TypedSpec coll, TypedSpec val,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
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
        return runVerdict(name, true, com.legend.lowering.VerdictSql.contains(c.rows(literal), v.scalarRow(literal)),
                c.on(env));
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
            com.legend.judge.CanonicalDivergence.sqlJudgedInDatabase(name);
            return AssertVerdicts.fail(name + ": columns differ " + n1 + " vs " + n2);   // a static verdict
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
                        delta.rows(false), timeDelta.rows(false), n1),
                env.withSession(w1.storeFree() ? w2.session() : w1.session()));
    }

    static TypedSide databaseCondition(String name, TypedSpec cond, boolean wantTrue,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return databaseCondition(name, cond, wantTrue, letPrefix, specs, env, hook, null);
    }

    /** {@code message}: the failure's text where the assert has its own (an {@code assert}'s), the host's words;
     *  null, the condition's canons framed (an assert judged by a condition of its own making). */
    static TypedSide databaseCondition(String name, TypedSpec cond, boolean wantTrue,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook, @com.legend.base.Nullable String message) {
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
            return runVerdict(name, true, com.legend.lowering.VerdictSql.subset(need.rows(literal), have.rows(literal), wantTrue),
                    need.on(env));
        }
        SideRows side = planSide(cond, false, letPrefix, specs, env, hook);
        if (side.why() != null) {
            return unjudged(name, side.why());
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
        return runVerdict(name, true, com.legend.lowering.VerdictSql.tolerance(e.scalarRow(false), a.scalarRow(false),
                t.scalarRow(false), toleranceArithmetic(args)), a.on(env));
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
        List<Object> values = AssertVerdicts.decodeSideValues(answered);
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
                    com.legend.plan.ResultShape.COLLECTION, on.session(), true);
        }
        rider.wrap(w.kinds(), w.many(), w.literalIndex());
        return new BodyDriver.WrappedSide(w.plan(), spec.info(),
                com.legend.plan.ResultShape.COLLECTION, on.session(), true);
    }

    static String describe(@com.legend.base.Nullable TypedSide r) {
        return r == null ? "no plan" : r.getClass().getSimpleName();
    }


    // ── leg 3.1c: the one-line families in database mode. Each side is
    // planned like an equality side (planSide); the predicate statement
    // returns the same verdict row; unjudged fails by name.

    /** A planned side for the predicate forms, or a reason it could not be. */
    record SideRows(BodyDriver.@com.legend.base.Nullable WrappedSide side,
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
            return side != null && !side.storeFree() ? env.withSession(side.session()) : env;
        }
    }

    /** A statically identified {@code is} pair: decided by the compiler (the same count as
     * the static kind gate). */
    static void staticallyDecided(String name) {
        com.legend.judge.CanonicalDivergence.sqlJudgedInDatabase(name);
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
        @Override public void staticallyDecided(String name) {
            DatabaseJudge.staticallyDecided(name);
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
        @Override public TypedSide quantified(String fqn, TypedSpec predMap, boolean wantTrue, String message, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            // task #14 leg 2 (2026-09-21): the predicate vector is planned and the
            // database returns the one verdict row — no element true/false is read
            // in Java
            SideRows vector = planSide(predMap, false, letPrefix, specs, env, hook);
            if (vector.why() != null) {
                return unjudged(fqn, vector.why());
            }
            return runVerdict(fqn, true, com.legend.lowering.VerdictSql.allOf(vector.rows(false),
                    wantTrue, message), vector.on(env));
        }
        @Override public @com.legend.base.Nullable TypedSide quantifiedVector(String fqn, TypedSpec predMap, TypedSpec source,
                com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, List<TypedSpec> letPrefix,
                SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            com.legend.builtin.NativeFn.Verdict family = com.legend.builtin.NativeFn.Verdict
                    .of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
            if ((family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS
                    || family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQ_WITHIN_TOLERANCE)
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
            // the vector contract's assert/assertFalse: the host's words (HostJudge.quantifiedRows), "Assert failed";
            // another family's rows keep the count's frame
            boolean assertFamily = family == com.legend.builtin.NativeFn.Verdict.ASSERT
                    || family == com.legend.builtin.NativeFn.Verdict.ASSERT_FALSE;
            return runVerdict(fqn, true, com.legend.lowering.VerdictSql.allOf(vector.rows(false), true,
                    assertFamily ? "Assert failed" : null), vector.on(env));
        }
        @Override public TypedSide sameElements(String name, List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
            return databaseVerdict(name, true, args.get(0), args.get(1),
                    letPrefix, specs, env, hook, true, true);
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
                return databaseCondition(name, cond, true, letPrefix, specs, env, hook);
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
                        p == com.legend.compiler.element.type.Type.Primitive.STRING), side.on(env));
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
            return HostJudge.eq(name, args, letPrefix, specs, env, hook);
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
            // the host's words (HostJudge.conditionOf): "Assert failed"
            return databaseCondition(name, cond, wantTrue, letPrefix, specs, env, hook, "Assert failed");
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
        TypedSpec actualJson = com.legend.compiler.spec.VerdictQueries
                .throughJsonPrettyPrint(AssertVerdicts.chaseLets(args.get(1), letPrefix));
        SideRows ja = planSide(actualJson, false, false, true, letPrefix, specs, env, hook);
        // bucket 9: a root the query does not order is a MULTISET of root objects -- the golden's root elements against
        // the document's -- under the host's own test (HostJudge.jsonOf: the actual's OrderView INCIDENTAL), so the two
        // judges decide the same documents the same way (2026-11-03: the deleted rootMany sniff had gated it, and its
        // deletion left these unjudged here while the host judged them). A dialect that cannot place the element rows
        // falls to the byte compare below, which equal sequences pass and others fail
        if (ja.why() == null && com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                == com.legend.compiler.spec.OrderView.INCIDENTAL) {
            TypedSpec elements = com.legend.compiler.spec.VerdictQueries.jsonRootElements(goldenText);
            if (elements != null) {
                SideRows jr = planSide(elements, true, false, false, letPrefix, specs, env, hook);
                if (jr.why() == null) {
                    com.legend.sql.SqlQuery mq = com.legend.lowering.VerdictSql.jsonRootMultiset(
                            jr.textRowsMany(), ja.textRows());
                    boolean placeable;
                    try {
                        env.dialect().render(mq);
                        placeable = true;
                    } catch (com.legend.sql.dialect.DialectCapability wall) {
                        placeable = false;
                    }
                    if (placeable) {
                        com.legend.judge.CanonicalDivergence.sqlRoute(name, "json-bytes");
                        return runVerdict(name, true, mq, ja.on(env));
                    }
                }
            }
        }
        // the golden as written otherwise (the user's ruling, 2026-11-03: no [x] for x -- a document serialized
        // otherwise than the engine serializes it is the serializer's to fix)
        var golden = com.legend.compiler.spec.VerdictQueries.canonicalJsonGolden(goldenText);
        if (golden == null) {
            return unjudged(name, "json golden does not parse");
        }
        SideRows je = planSide(golden, true, false, false, letPrefix, specs, env, hook);
        if (je.why() != null || ja.why() != null) {
            return unjudged(name, "json side: " + (je.why() != null ? je.why() : ja.why()));
        }
        com.legend.judge.CanonicalDivergence.sqlRoute(name, "json-bytes");
        return runVerdict(name, true,
                com.legend.lowering.VerdictSql.jsonText(je.textRows(), ja.textRows()),
                ja.on(env));
    }
}
