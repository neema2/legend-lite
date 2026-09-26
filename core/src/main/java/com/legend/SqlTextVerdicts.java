// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;
import com.legend.exec.SqlReplayOracle;

import java.util.List;

/**
 * THE SQL-TEXT VERDICT ARM (SQLTEXT_ROW_VERDICT_CHARTER §3-§4, slice
 * 3a): a statement-root {@code assertEquals(golden, toSQLString({|q},
 * mapping, dialect, ext))} judges on ROWS, never on text — golden text
 * is H2-flavored, we execute DuckDB, so identical text proves the
 * emitter's spelling, not the answer (§0). Detection is typed-node +
 * exact callee FQN navigation of the assert's argument trees (§3.4;
 * never text sniffing); the producer node's CHILDREN are the
 * structured inputs. Four artifacts (§3.5): OUR TEXT and GOLDEN TEXT
 * by ordinary evaluation of the two sides; OUR ROWS by executing the
 * producer's own query lambda through the ONE router
 * ({@code evalValue} — the lambda wraps in {@code from(mapping)} with
 * the env's runtime, Appendix A); GOLDEN ROWS via the
 * {@link SqlReplayOracle} SPI on ExecEnv. Text match/diff is a CENSUS
 * number ({@link SqlTextEmission}); a row divergence FAILS whatever
 * the text said; an oracle decline (foreign dialects — no reference
 * database) keeps TEXT as the contract, hard pass/fail, counted (§4).
 * Production registers no oracle and this arm WALLS loudly (§2).
 *
 * <p>Eval-ledger tenet argument: NO evaluation here — both texts, our
 * rows, and the golden replay all compute in the database through
 * existing routes; this class navigates typed trees, sequences the
 * four derivations, and judges outcomes (Clause 2c judgment, the
 * AssertVerdicts discipline). Shapes outside the exact cohort return
 * null and keep their current path — never a guess.
 */
final class SqlTextVerdicts {

    private SqlTextVerdicts() {
    }

    /** Null = not this arm's shape (the caller's generic path
     * continues); otherwise the verdict. */
    static @com.legend.base.Nullable ExecutionResult tryArm(String name,
            boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (!wantEqual || args.size() < 2) {
            return null;
        }
        com.legend.compiler.spec.NativeDispatch.RoutineCall p0 = findProducer(args.get(0), letPrefix);
        com.legend.compiler.spec.NativeDispatch.RoutineCall p1 = findProducer(args.get(1), letPrefix);
        if ((p0 == null) == (p1 == null)) {
            if (p0 == null) {
                // no toSQLString producer anywhere: the exec-sql-read
                // spelling (§8.3c) is the remaining owned shape
                return tryArmExecRead(name, args, letPrefix, specs, env,
                        hook);
            }
            // two producers: not the golden-vs-render shape
            return null;
        }
        TypedSpec producerSide = p0 != null ? args.get(0) : args.get(1);
        TypedSpec goldenSide = p0 != null ? args.get(1) : args.get(0);
        com.legend.compiler.spec.NativeDispatch.RoutineCall producer = p0 != null ? p0 : p1;
        // the producer's structured inputs (§3.4, SqlTextInputs across
        // the overloads): query lambda, mapping ref, dialect, runtime.
        // Anything else is a shape this arm does not own yet.
        // the query lambda may be LET-BOUND (`let func = {|...};
        // toSQLString($func, mapping, DatabaseType.H2, ...)`) — chased
        // like the plan-text arm chases its lambda
        SqlTextInputs in = SqlTextInputs.of(producer, letPrefix);
        TypedSpec lamArg = in == null ? null
                : com.legend.compiler.spec.typed.Lets.bound(
                        in.query(), letPrefix);
        if (in == null
                || !(lamArg instanceof TypedLambda lam)
                || lam.body().isEmpty()
                || !(in.mapping() instanceof TypedPackageableRef mapping)) {
            return null;
        }
        // the dialect: the DatabaseType overload names it; the RUNTIME
        // overload (toSQLStringPretty(lambda, mapping, runtime, ext) —
        // the post-processor tests' spelling) and the toSQL-handle form
        // carry it on the connection, read through the let chase and a
        // helper inline
        String dbType;
        if (in.dialect() instanceof TypedEnumValue db) {
            dbType = db.value();
        } else {
            TypedSpec rt = com.legend.compiler.spec.typed.Lets.bound(in.runtime(), letPrefix);
            rt = new com.legend.compiler.spec.UserCallInliner(specs)
                    .inlineBody(List.of(rt)).get(0);
            com.legend.compiler.spec.typed.ExecutionContext frameCtx =
                    com.legend.compiler.spec.typed.ExecutionContext.reader()
                            .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                            .read(java.util.Optional.empty(), rt);
            String boundDb = frameCtx.databaseType();
            if (boundDb == null) {
                // a DRIVER that is neither an enum literal nor a runtime
                // (the per-driver pair loop's `$p.first`): its dialect is
                // not statically known — never ASSUMED H2 (batch 67: a
                // Postgres golden replayed on H2 read DATE_TRUNC week as
                // Sunday, a referee-dialect skew, not ours). The
                // foreign-dialect residue owns it: text is the contract.
                dbType = "unresolved";
            } else {
                dbType = boundDb;
            }
            if (dbType == null) {
                return null;
            }
            // the runtime's post-processors (replaceTables) apply to OUR
            // rows exactly as the frame path applies them — the same
            // recogniser, the env's tableReplace channel (the golden's
            // text already names the replaced tables)
            env = env.withFrame(frameCtx);
            java.util.Map<String, String> tr = env.postProcessors().tableReplace();
            if (!tr.isEmpty()) {
                env = env.withTableReplace(tr);
            }
        }
        // OUR TEXT + GOLDEN TEXT: ordinary evaluation, the one router
        String golden;
        String ours;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
            golden = scalarString(StatementExecutor.evalValue(
                    goldenSide, letPrefix, specs, env, null, false, hook));
            ours = scalarString(StatementExecutor.evalValue(
                    producerSide, letPrefix, specs, env, null, false, hook));
        }
        if (golden == null || ours == null) {
            return null;
        }
        golden = stripChainedPlanWarning(golden);
        // from here every path is THIS arm's verdict — the marker lets
        // the dual-channel probe bucket walk-vs-arm outcomes as the
        // DESIGNED text-vs-rows divergence, never pinned disagreement
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            // §2: no oracle registered = no goldens exist here — WALL
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        if (!"H2".equals(dbType)) {
            // §4 FOREIGN-DIALECT residue: no oracle database for this
            // dialect — text stays the contract, counted forever
            declined(env, name, "foreign-dialect:" + dbType);
            final String g = golden;
            return textVerdict(env, name, golden, ours, textEqual,
                    () -> name + " (sql-text, " + dbType
                            + " — text is the contract): expected " + g + ", got " + ours);
        }
        // a MULTI-STATEMENT lambda (leading lets, then the query — the
        // datePeriods `$fn`): the engine's plan is one statement per
        // store-backed let, and its index-less toSQLString prints
        // statement 0 (+ the chained-plan warning, stripped above) — so
        // the golden is let 0's OWN rows when the lambda has statement
        // lets (batch 69c); the lets scope the rows leg either way
        List<TypedSpec> lamPrefix = new java.util.ArrayList<>(letPrefix);
        lamPrefix.addAll(lam.body().subList(0, lam.body().size() - 1));
        List<com.legend.compiler.spec.typed.TypedLet> stmtLets = statementLets(lam);
        final String goldenF = golden;
        final StatementExecutor.ExecEnv envF = env;
        // the leg's from carries the producer's bound context (post-processors,
        // time zone); a toNonExecutableSQLString producer's query runs under
        // the nonExecutable pass (its golden reads zero rows by construction,
        // and so must ours — batch 81; on the from since batch 120)
        com.legend.compiler.spec.typed.ExecutionContext legCtx = legContext(producer, envF);
        if (!stmtLets.isEmpty() && !isPopulationGolden(golden)) {
            com.legend.compiler.spec.typed.TypedLet let0 = stmtLets.get(0);
            String letCls = let0.value().info().type()
                    instanceof com.legend.compiler.element.type.Type.ClassType ct
                    ? ct.fqn() : null;
            return rowsLegAndVerdict(
                    name, goldenF, ours, textEqual, oracle,
                    com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            let0.value(), mapping, legCtx),
                    null, mapping.fullPath(), letCls,
                    AssertVerdicts.replayFacts(let0.value(), lamPrefix),
                    AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            let0.value(), mapping, legCtx)),
                    lamPrefix, specs, envF, hook, lam);
        }
        TypedSpec query = lam.body().get(lam.body().size() - 1);
        // OUR ROWS (§3.5c): the referee executes the producer's own
        // query — mapping from the producer, runtime from the env
        return rowsLegAndVerdict(
                name, goldenF, ours, textEqual, oracle,
                com.legend.compiler.spec.VerdictQueries.fromWrapped(
                        query, mapping, legCtx),
                null, mapping.fullPath(), rootClassFqn(lam),
                AssertVerdicts.replayFacts(query, lamPrefix),
                AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries.fromWrapped(
                        query, mapping, legCtx)), lamPrefix,
                specs, envF, hook, lam);
    }

    /** The bound context a producer's rows leg runs under: the frame the
     * arm read off the producer's runtime (none when the producer names a
     * DatabaseType, which carries no post-processors), with the
     * nonExecutable pass installed for a toNonExecutableSQLString producer. */
    private static com.legend.compiler.spec.typed.ExecutionContext legContext(
            com.legend.compiler.spec.NativeDispatch.RoutineCall producer, StatementExecutor.ExecEnv env) {
        com.legend.compiler.spec.typed.ExecutionContext base = env.frame() == null
                ? com.legend.compiler.spec.typed.ExecutionContext.NONE : env.frame();
        boolean nonExec = com.legend.compiler.element.type.PlatformTypes
                .TO_NON_EXECUTABLE_SQL_STRING.equals(producer.fqn());
        return nonExec
                ? base.withPostProcessors(base.postProcessors().withNonExecutable(true))
                : base;
    }

    /** SQLTEXT charter §8.3b — the ROOT arm for
     * {@code assertSameSQL(golden, $result)} (the ~750-test
     * assert-form cohort): the statement root reaches the verdict
     * layer PRE-inline, so the arm owns the whole shape. OUR TEXT =
     * the minted {@code sqlRemoveFormatting($result)} (the envelope
     * splice folds it to the frame's EXECUTED SQL); OUR ROWS = the
     * minted {@code $result.values} (the splice swaps in the frame's
     * typed chain); golden rows + §6/§7 compare via the oracle SPI.
     * Same verdict policy as the toSQLString arm. Null = not the
     * simple shape (the String-overload spelling, extra args) — the
     * current path keeps it. */
    static @com.legend.base.Nullable ExecutionResult tryArmSameSql(
            com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (root.args().size() != 2) {
            return null;
        }
        TypedSpec goldenSide = root.args().get(0);
        TypedSpec resultArg = root.args().get(1);
        if (resultArg.info().type()
                == com.legend.compiler.element.type.Type.Primitive.STRING) {
            // the String overload (assertSameSQL(sqlString, result:String)
            // — `$result->sqlRemoveFormatting()`, a plan text, a TDG
            // fetch text) takes the SAME exec-read arm assertEquals takes:
            // an sql() read is a rows verdict, never a text comparison; a
            // plain String pair stays the ordinary string overload
            // (the general arm: a toSQLString producer takes the
            // dialect-aware verdict — rows on H2, the counted text
            // contract for a foreign dialect; otherwise the exec-read
            // arms)
            return tryArm("assertSameSQL", true, root.args(), letPrefix,
                    specs, env, hook);
        }
        TypedSpec strip = com.legend.compiler.spec.VerdictQueries
                .sqlStripRead(resultArg, env.ctx());
        // §8.3e hardening: from here the SQL shape is RECOGNIZED (a
        // Result-typed actual on the sql-assert form) — an underivable
        // leg WALLS counted instead of falling through to the generic
        // path, which would judge a sql assert by TEXT (the charter's
        // one forbidden outcome). Same for the golden/ours evaluation.
        if (strip == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assertSameSQL: Result actual is not a"
                            + " mintable frame read");
        }
        String golden = scalarString(StatementExecutor.evalValue(
                goldenSide, letPrefix, specs, env, null, false, hook));
        String ours = scalarString(StatementExecutor.evalValue(
                strip, letPrefix, specs, env, null, false, hook));
        if (golden == null || ours == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assertSameSQL: " + (golden == null
                            ? "golden side" : "actual side")
                            + " did not evaluate to a string");
        }
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        FrameFacts fm = frameMappingAndClass(resultArg, letPrefix, hook, specs);
        return rowsLegAndVerdict("assertSameSQL", golden, ours, textEqual,
                oracle, com.legend.compiler.spec.VerdictQueries
                        .valuesRead(resultArg),
                null, fm.mapping(), fm.cls(), fm.facts(), fm.populationRead(), letPrefix,
                specs, env, hook, fm.query());
    }

    /** SQLTEXT charter §8.3d — the DUAL-GOLDEN arm:
     * {@code assertEqualsH2Compatible(legacy, upgraded, $result)}
     * (h2Extension.pure:29 — the engine's own body picks ONE golden by
     * H2 version: legacy on 1.4.200, upgraded otherwise). Our oracle
     * session IS the upgraded H2 (the W10 4.138.2 pin), so the arm
     * replays the UPGRADED golden — the same choice the engine's own
     * dispatch makes on this oracle; the legacy golden is engine
     * H2-1.4.200 residue with no reference database on our stack
     * (inventory row 17). Rows judge; text vs the upgraded golden is
     * the census. Null = not the simple shape. */
    static @com.legend.base.Nullable ExecutionResult tryArmH2Compat(
            com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (root.args().size() != 3) {
            return null;
        }
        TypedSpec goldenSide = root.args().get(1);
        TypedSpec actualSide = root.args().get(2);
        // Two corpus spellings: a bare Result (the minted strip reads
        // it) or the test's OWN sqlRemoveFormatting($result) String —
        // the exec-sql-read chase finds the frame behind the read.
        TypedSpec oursExpr;
        TypedSpec resultArg;
        int readK = 0;
        if (actualSide.info().type()
                == com.legend.compiler.element.type.Type.Primitive.STRING) {
            com.legend.compiler.spec.typed.TypedUserCall read =
                    findSqlRead(actualSide, letPrefix);
            readK = read == null ? 0 : readIndex(read);
            if (read != null && readK > 0) {
                // the engine's n-th statement against our ONE statement
                // (statement-per-let plan, batch 69a): our text is our
                // index-0 read; golden(n) routes below by statementRoute
                oursExpr = com.legend.compiler.spec.VerdictQueries
                        .firstStatementRead(read);
                resultArg = read.args().get(0);
                String goldenN = scalarString(StatementExecutor.evalValue(
                        goldenSide, letPrefix, specs, env, null, false, hook));
                String oursN = scalarString(StatementExecutor.evalValue(
                        oursExpr, letPrefix, specs, env, null, false, hook));
                if (goldenN == null || oursN == null) {
                    return null;
                }
                return h2CompatVerdict(goldenN, oursN, resultArg, readK,
                        letPrefix, specs, env, hook);
            }
            if (read == null) {
                // §5: the plan-text spelling (a plan-node .sqlQuery
                // navigation) — the upgraded golden replays (same
                // reasoning as the frame arm below)
                ExecutionResult pv = tryArmPlanText(
                        "assertEqualsH2Compatible", goldenSide,
                        actualSide, letPrefix, specs, env, hook);
                if (pv != null) {
                    return pv;
                }
                // TDG flip: the generator fetch-text spelling
                // ($tdg.sqls->at(n)->sqlRemoveFormatting()) — the
                // fetch-text verdict, upgraded golden
                return tryArmTdgSql("assertEqualsH2Compatible",
                        goldenSide, actualSide, letPrefix, specs, env,
                        hook);
            }
            oursExpr = actualSide;
            resultArg = read.args().get(0);
        } else {
            TypedSpec strip = com.legend.compiler.spec.VerdictQueries
                    .sqlStripRead(actualSide, env.ctx());
            // §8.3e hardening: Result-typed actual = the SQL shape is
            // recognized — underivable legs WALL counted (a null
            // fallthrough would inline getH2Versions' store read and
            // wall anyway, but with an unattributable reason)
            if (strip == null) {
                throw new com.legend.error.NotImplementedException(
                        "sql-text assertEqualsH2Compatible: Result"
                                + " actual is not a mintable frame read");
            }
            oursExpr = strip;
            resultArg = actualSide;
        }
        String golden = scalarString(StatementExecutor.evalValue(
                goldenSide, letPrefix, specs, env, null, false, hook));
        String ours = scalarString(StatementExecutor.evalValue(
                oursExpr, letPrefix, specs, env, null, false, hook));
        if (golden == null || ours == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assertEqualsH2Compatible: " + (golden == null
                            ? "golden side" : "actual side")
                            + " did not evaluate to a string");
        }
        return h2CompatVerdict(golden, ours, resultArg, readK, letPrefix,
                specs, env, hook);
    }

    /** The H2Compatible verdict tail: rows leg + verdict for golden(k)
     * over the frame behind {@code resultArg}. */
    private static @com.legend.base.Nullable ExecutionResult h2CompatVerdict(
            String golden, String ours, TypedSpec resultArg, int readK,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        golden = stripChainedPlanWarning(golden);
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        FrameFacts fm = frameMappingAndClass(resultArg, letPrefix, hook, specs);
        // the engine's statement-per-let plan (batch 69a): golden(k) may
        // be a statement LET's own rows — same routing as the exec-read arm
        StatementRoute route = statementRoute(readK, golden, fm);
        if (route == null) {
            return null;
        }
        if (route.let() != null && fm.mappingRef() != null) {
            String letCls = route.let().value().info().type()
                    instanceof com.legend.compiler.element.type.Type.ClassType ct
                    ? ct.fqn() : null;
            return rowsLegAndVerdict("assertEqualsH2Compatible", golden, ours,
                    textEqual, oracle,
                    com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            route.let().value(), fm.mappingRef(), fm.context()),
                    null, fm.mapping(), letCls,
                    AssertVerdicts.replayFacts(route.let().value(), letPrefix),
                    AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            route.let().value(), fm.mappingRef(), fm.context())),
                    letPrefix, specs, env, hook, fm.query());
        }
        return rowsLegAndVerdict("assertEqualsH2Compatible", golden, ours,
                textEqual, oracle, com.legend.compiler.spec.VerdictQueries
                        .valuesRead(resultArg),
                null, fm.mapping(), fm.cls(), fm.facts(), fm.populationRead(), letPrefix,
                specs, env, hook, fm.query());
    }

    /** SQLTEXT charter §8.3c — the EXEC-SQL-READ arm (the ~700-test
     * cohort): {@code assertEquals(golden, sqlRemoveFormatting($res))}
     * where the test's OWN code reads the SQL out of an executed
     * Result. Detection is the same typed-node + exact-FQN discipline
     * (§3.4): a let-aware walk finds the {@code sql}/{@code
     * sqlRemoveFormatting} USER call whose first argument is
     * Result-typed (the String overload is ordinary string code and
     * never matches). ONLY the first-statement forms are owned —
     * {@code sql($res, n)} with n&gt;0 names the n-th activity's SQL,
     * and pairing that golden against the frame's RESULT rows would
     * judge the wrong statement, so those shapes return null and stay
     * counted on their current path. OUR TEXT = the whole actual side
     * evaluated as written (any wrapping string code runs in the DB);
     * OUR ROWS = the frame's values (the splice's typed chain); golden
     * rows + verdict via the SHARED tail. */
    private static @com.legend.base.Nullable ExecutionResult tryArmExecRead(
            String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        // TDG FIRST: a generator fetch text on exactly one side is the
        // fetch-text verdict whatever wraps it — the corpus's
        // assertSqlEquals inlines to assertEquals over
        // sqlRemoveFormatting on BOTH sides, and that string read used
        // to claim the side as an exec-read (a Result frame it never
        // was), so the TDG route below was unreachable for the whole
        // let-bound cohort.
        boolean tdg0 = hasTdgProducer(args.get(0), letPrefix);
        boolean tdg1 = hasTdgProducer(args.get(1), letPrefix);
        if (tdg0 != tdg1) {
            return tryArmTdgSql(name,
                    tdg0 ? args.get(1) : args.get(0),
                    tdg0 ? args.get(0) : args.get(1),
                    letPrefix, specs, env, hook);
        }
        com.legend.compiler.spec.typed.TypedUserCall r0 =
                findSqlRead(args.get(0), letPrefix);
        com.legend.compiler.spec.typed.TypedUserCall r1 =
                findSqlRead(args.get(1), letPrefix);
        if ((r0 == null) == (r1 == null)) {
            if (r0 == null) {
                // §5: plain assertEquals over a plan-node SQL read
                TypedNativeCall q0 = findPlanProducer(args.get(0),
                        letPrefix);
                TypedNativeCall q1 = findPlanProducer(args.get(1),
                        letPrefix);
                if (q0 == null && q1 == null && "assertEquals".equals(name)) {
                    // the producer behind a helper call (tryArmPlanText) —
                    // the plain assert's arm; an H2-compatible assert keeps
                    // its own normalised text compare when the arm declines
                    q0 = lookThroughPlanProducer(args.get(0), letPrefix, specs);
                    q1 = lookThroughPlanProducer(args.get(1), letPrefix, specs);
                }
                if ((q0 == null) != (q1 == null)) {
                    return tryArmPlanText(name,
                            q0 != null ? args.get(1) : args.get(0),
                            q0 != null ? args.get(0) : args.get(1),
                            letPrefix, specs, env, hook);
                }
                // (the plain-assertEquals TDG spelling routes ABOVE,
                // before the exec-read claim — one TDG door)
            }
            return null;
        }
        TypedSpec producerSide = r0 != null ? args.get(0) : args.get(1);
        TypedSpec goldenSide = r0 != null ? args.get(1) : args.get(0);
        com.legend.compiler.spec.typed.TypedUserCall read =
                r0 != null ? r0 : r1;
        TypedSpec resultArg = read.args().get(0);
        String golden = scalarString(StatementExecutor.evalValue(
                goldenSide, letPrefix, specs, env, null, false, hook));
        // THE ENGINE'S PLAN IS ONE STATEMENT PER STORE-BACKED LET of the
        // query lambda, in order, then the main statement (batch 69a,
        // 2026-09-05 — the datePeriods shape `let reportEndDate =
        // FiscalCalendarDate.all()->filter(..)->toOne()` asserts
        // sqlRemoveFormatting($res, 0) = the calendar instance select and
        // sqlRemoveFormatting($res, 1) = the group-by): golden(k) for
        // k < #lets is let k's OWN rows (its expression through the one
        // router, wrapped like the frame); golden(#lets) is the main
        // statement — ours is our ONE statement, read at index 0. The
        // in-list plan (batch 67) keeps its named route: golden n>0
        // reading tempTableForIn_<let> is the main statement.
        FrameFacts fm = frameMappingAndClass(resultArg, letPrefix, hook, specs);
        int k = readIndex(read);
        StatementRoute route = statementRoute(k, golden, fm);
        if (route == null) {
            return null;
        }
        if (k > 0) {
            producerSide = com.legend.compiler.spec.VerdictQueries
                    .firstStatementRead(read);
        }
        com.legend.compiler.spec.typed.TypedLet letStatement = route.let();
        String ours = scalarString(StatementExecutor.evalValue(
                producerSide, letPrefix, specs, env, null, false, hook));
        if (golden == null || ours == null) {
            return null;
        }
        golden = stripChainedPlanWarning(golden);
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        if (letStatement != null && fm.mappingRef() != null) {
            String letCls = letStatement.value().info().type()
                    instanceof com.legend.compiler.element.type.Type.ClassType ct
                    ? ct.fqn() : null;
            return rowsLegAndVerdict(name, golden, ours, textEqual, oracle,
                    com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            letStatement.value(), fm.mappingRef(), fm.context()),
                    null, fm.mapping(), letCls,
                    AssertVerdicts.replayFacts(letStatement.value(), letPrefix),
                    AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            letStatement.value(), fm.mappingRef(), fm.context())),
                    letPrefix, specs, env, hook, fm.query());
        }
        // the engine's two-statement in-list plan (batch 67): golden(0)
        // is the POPULATION statement of `let v = <to-many expr>` inside
        // the query lambda — its rows ARE that let's value, so the rows
        // leg evaluates the let's expression; golden(1) reads
        // tempTableForIn_<v>, which the oracle materializes from the
        // attempt's remembered population golden (TempTable "population")
        PopulationShape pop = populationShape(golden, fm.query());
        if (pop != null && pop.rowsRead() != null && fm.mappingRef() != null) {
            // the let's expression runs through the one router, wrapped
            // in the frame's own mapping like every rows leg
            return rowsLegAndVerdict(name, golden, ours, textEqual, oracle,
                    com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            pop.rowsRead(), fm.mappingRef(), fm.context()),
                    null, fm.mapping(), null,
                    AssertVerdicts.replayFacts(pop.rowsRead(), letPrefix),
                    AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries.fromWrapped(
                            pop.rowsRead(), fm.mappingRef(), fm.context())),
                    letPrefix, specs, env, hook, fm.query());
        }
        return rowsLegAndVerdict(name, golden, ours, textEqual, oracle,
                com.legend.compiler.spec.VerdictQueries
                        .valuesRead(resultArg),
                null, fm.mapping(), fm.cls(), fm.facts(), fm.populationRead(), letPrefix,
                specs, env, hook, fm.query(), null, java.util.Map.of(),
                pop == null ? List.of() : pop.temps());
    }

    private record PopulationShape(@com.legend.base.Nullable TypedSpec rowsRead,
            List<SqlReplayOracle.TempTable> temps) {
    }

    /** The engine's index-less {@code sqlRemoveFormatting($res)} over a
     * CHAINED plan prints statement 0 and appends this warning line
     * (relationalMappingExecution.pure): the referee reads the SQL and
     * drops the message before replay — a spec-text shape, like the
     * population statement's. */
    private static final String CHAINED_PLAN_WARNING =
            "\nWarning: Results only shown for first relational query.";

    private static String stripChainedPlanWarning(String golden) {
        int at = golden.indexOf(CHAINED_PLAN_WARNING);
        return at < 0 ? golden : golden.substring(0, at);
    }

    /** The statement index of an exec-sql read: {@code sql($res, n)} /
     * {@code sqlRemoveFormatting($res, n)} → n; the one-argument forms
     * → 0. */
    private static int readIndex(com.legend.compiler.spec.typed.TypedUserCall read) {
        return read.args().size() == 2
                && read.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCInteger ki
                ? ki.value().intValue() : 0;
    }

    /** Which engine statement golden(k) is: {@code let} = the k-th
     * statement let (its rows are the let's own); a null let = the MAIN
     * statement (k == #lets; k == 0 with no lets; the in-list plan's
     * named temp read; the `select distinct` population golden the
     * batch-67 route owns). */
    private record StatementRoute(
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedLet let) {
    }

    private static @com.legend.base.Nullable StatementRoute statementRoute(int k,
            @com.legend.base.Nullable String golden, FrameFacts fm) {
        List<com.legend.compiler.spec.typed.TypedLet> lets = statementLets(fm.query());
        if (golden != null && NAMED_IN_TEMP.matcher(golden).find()) {
            return new StatementRoute(null);
        }
        if (k < lets.size()) {
            if (k == 0 && golden != null && isPopulationGolden(golden)) {
                return new StatementRoute(null);
            }
            return new StatementRoute(lets.get(k));
        }
        return k == lets.size() ? new StatementRoute(null) : null;
    }

    /** The engine's population statement shape (batch 67): the ONE
     * recognizer of that golden text, shared by both routes. */
    private static boolean isPopulationGolden(String golden) {
        return golden.toLowerCase(java.util.Locale.ROOT)
                .startsWith("select distinct");
    }

    /** The query lambda's leading lets that are STATEMENTS of the
     * engine's plan — a class instance, instances, or a relation, each
     * executed as its own SQL in declaration order; value-only lets
     * (dates, strings from helpers) are not statements. */
    private static List<com.legend.compiler.spec.typed.TypedLet> statementLets(
            @com.legend.base.Nullable TypedSpec query) {
        List<com.legend.compiler.spec.typed.TypedLet> out = new java.util.ArrayList<>();
        if (!(query instanceof TypedLambda lam) || lam.body().size() < 2) {
            return out;
        }
        for (TypedSpec s : lam.body().subList(0, lam.body().size() - 1)) {
            if (s instanceof com.legend.compiler.spec.typed.TypedLet let) {
                com.legend.compiler.element.type.Type t = let.value().info().type();
                if (t instanceof com.legend.compiler.element.type.Type.ClassType
                        || t instanceof com.legend.compiler.element.type.Type.RelationType) {
                    out.add(let);
                }
            }
        }
        return out;
    }

    private static final java.util.regex.Pattern NAMED_IN_TEMP =
            java.util.regex.Pattern.compile("tempTableForIn_([A-Za-z_][A-Za-z0-9_]*)");

    /** The population shapes of a golden against the frame's query
     * lambda: a `select distinct` golden whose leading let feeds a temp
     * (rows leg = the let's expression, wrapped like the frame), or a
     * golden reading {@code tempTableForIn_<let>} (a "population" temp
     * spec the oracle fills from the remembered population golden). */
    private static @com.legend.base.Nullable PopulationShape populationShape(
            String golden, @com.legend.base.Nullable TypedSpec query) {
        if (!(query instanceof TypedLambda lam) || lam.body().size() < 2) {
            return null;
        }
        java.util.regex.Matcher m = NAMED_IN_TEMP.matcher(golden);
        String var = m.find() && !m.group(1).matches("\\d+") ? m.group(1) : null;
        for (TypedSpec s : lam.body().subList(0, lam.body().size() - 1)) {
            if (!(s instanceof com.legend.compiler.spec.typed.TypedLet let)
                    || !let.value().info().multiplicity().isMany()) {
                continue;
            }
            if (var != null && let.name().equals(var)) {
                String kind = let.value().info().type()
                        == com.legend.compiler.element.type.Type.Primitive.INTEGER
                        ? "population:integer" : "population:string";
                return new PopulationShape(null, List.of(
                        new SqlReplayOracle.TempTable("tempTableForIn_" + var,
                                kind, List.of())));
            }
            if (var == null && isPopulationGolden(golden)) {
                return new PopulationShape(let.value(), List.of());
            }
        }
        return null;
    }

    /** The exec-sql-read producer node: a {@code sql($res)} /
     * {@code sqlRemoveFormatting($res)} USER call (exact splice FQNs)
     * over a Result-typed receiver, first-statement form only
     * (1 argument, or 2 with a literal 0). LET-AWARE like
     * {@link #findProducer}. */
    private static @com.legend.base.Nullable
            com.legend.compiler.spec.typed.TypedUserCall findSqlRead(
            TypedSpec t, List<TypedSpec> letPrefix) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        java.util.Set<String> seenVars = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedUserCall uc) {
                String fqn = uc.callee().qualifiedName();
                if ((fqn.equals(com.legend.compiler.spec
                                .ResultEnvelopeSplice.SQL_FQN)
                        || fqn.equals(com.legend.compiler.spec
                                .ResultEnvelopeSplice
                                .SQL_REMOVE_FORMATTING_FQN))
                        && !uc.args().isEmpty()
                        && uc.args().get(0).info().type()
                                != com.legend.compiler.element.type.Type
                                        .Primitive.STRING
                        && (uc.args().size() == 1
                                || uc.args().size() == 2
                                        && uc.args().get(1) instanceof
                                        com.legend.compiler.spec.typed
                                        .TypedCInteger k
                                        && k.value().longValue() >= 0)) {
                    // n > 0 names the n-th ENGINE statement; the arm owns
                    // it only for the two-statement in-list plan (batch
                    // 67) and refuses the rest below
                    return uc;
                }
            }
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedVariable tv && seenVars.add(tv.name())) {
                com.legend.compiler.spec.typed.TypedLet tl =
                        com.legend.compiler.spec.typed.Lets.binding(letPrefix, tv.name());
                if (tl != null) {
                    work.add(tl.value());
                }
            }
            work.addAll(cur.children());
        }
        return null;
    }

    /** SQLTEXT charter §5 (slice 4) — the PLAN-TEXT arm: the assert's
     * actual side navigates a generated PLAN to a SQL text with
     * <code>${'$'}{param}</code> template holes (executionPlanTest.pure
     * spelling: {@code $plan.rootExecutionNode...sqlQuery}). The plan
     * itself is PLATFORM text (the K-native channel renders it — the
     * eval-ledger's engine-parity class); this arm judges it on ROWS:
     * REFEREE BINDINGS (VerdictQueries.refereeBindings — fixed scalar
     * values per parameter type, the charter's referee-chosen policy)
     * bind the plan lambda's parameters as minted lets; OUR ROWS =
     * the lambda body wrapped in from(mapping), evaluated with those
     * lets; GOLDEN ROWS = the golden text with every hole filled with
     * the SAME value's SQL spelling, replayed via the oracle. The
     * TEXT census compares the RAW golden (holes intact) as always.
     * Unbindable parameters and residual (freemarker-operation) holes
     * WALL counted — the measure-first residue. */
    private static @com.legend.base.Nullable ExecutionResult tryArmPlanText(
            String name, TypedSpec goldenSide, TypedSpec actualSide,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        TypedNativeCall producer = findPlanProducer(actualSide, letPrefix);
        // THE PRODUCER BEHIND A HELPER (2026-09-17): a test that gets its
        // plan string from a user function (executionPlanForQueryWith
        // DateTimeConstant...(dt, zone) builds the query, the runtime and
        // the plan inside its body) carries no producer in its own
        // statements. The platform's own inliner splices the helper into
        // the test's lets — parameters replaced by the call's arguments,
        // the helper's lets substituted forward — and the producer is
        // then in the test's scope like any other; the rows leg binds the
        // same reduced scope. The helper is never hijacked or emptied.
        List<TypedSpec> scope = letPrefix;
        if (producer == null && "assertEquals".equals(name)) {
            LookThrough lt = lookThrough(actualSide, letPrefix, specs);
            if (lt != null) {
                producer = lt.producer();
                scope = lt.scope();
            }
        }
        if (producer == null || producer.args().isEmpty()) {
            return null;
        }
        TypedSpec lamArg = producer.args().get(0);
        if (lamArg instanceof com.legend.compiler.spec.typed
                .TypedVariable lv) {
            com.legend.compiler.spec.typed.TypedLet tl =
                    com.legend.compiler.spec.typed.Lets.binding(scope, lv.name());
            if (tl != null) {
                lamArg = tl.value();
            }
        }
        if (!(lamArg instanceof TypedLambda lam) || lam.body().isEmpty()) {
            return null;
        }
        // the MAPPING-LESS executionPlan(lambda, extensions) (corpus-zero
        // cluster B, 2026-09-12): the query binds its mappings itself
        // through from() — testTwoMappingsOneRuntime joins two from()
        // sides over two mappings — so the rows read is the statement
        // as written, and the referee decodes no enum by mapping
        TypedPackageableRef mapping = producer.args().size() >= 2
                && producer.args().get(1) instanceof TypedPackageableRef m ? m : null;
        String golden;
        String ours;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
            golden = scalarString(StatementExecutor.evalValue(
                    goldenSide, letPrefix, specs, env, null, false, hook));
            ours = scalarString(StatementExecutor.evalValue(
                    actualSide, letPrefix, specs, env, null, false, hook));
        }
        if (golden == null || ours == null) {
            return null;
        }
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        com.legend.compiler.spec.VerdictQueries.PlanBindings bindings =
                com.legend.compiler.spec.VerdictQueries
                        .refereeBindings(lam);
        if (bindings == null) {
            declined(env, name, "plan-params-unbindable");
            final String g = golden;
            return textVerdict(env, name, golden, ours, textEqual,
                    () -> name + " (plan-text, params unbindable —"
                            + " text is the contract): expected " + g + ", got " + ours);
        }
        String filled = golden;
        for (var e : bindings.spellings().entrySet()) {
            filled = filled.replace("${" + e.getKey() + "}", e.getValue());
        }
        List<TypedSpec> bound = new java.util.ArrayList<>(scope);
        bound.addAll(bindings.lets());
        // the plan producer's bound context rides the verdict's from
        com.legend.compiler.spec.typed.ExecutionContext planCtx = producer.args().size() >= 3
                ? StatementExecutor.boundContext(producer.args().get(2), scope, specs)
                : com.legend.compiler.spec.typed.ExecutionContext.NONE;
        // a multi-statement plan lambda ({|let a = 10; Firm.all()->...})
        // scopes its leading lets over the last statement — the rows leg
        // evaluates the last statement under them (batch 66)
        bound.addAll(lam.body().subList(0, lam.body().size() - 1));
        // a PLAN-TEXT golden replays its SQL, never the plan text itself
        // (the sqltext homework's 12 "Syntax error in SQL statement
        // Relational(" misroutes). ONE sql node with simple holes replays
        // directly; a multi-node plan (Allocation values feeding the
        // later holes) or a template OPERATION hole (collectionSize,
        // renderCollection, GMTtoTZ, ...) is the oracle's plan replay
        // (batch 66): the referee runs the plan's nodes in order.
        String replay = filled.contains("${") ? null : planReplaySql(filled);
        TypedSpec last = lam.body().get(lam.body().size() - 1);
        TypedSpec read = mapping == null ? last
                : com.legend.compiler.spec.VerdictQueries.fromWrapped(last, mapping, planCtx);
        return rowsLegAndVerdict(name, golden, ours, textEqual, oracle, read,
                replay, mapping == null ? null : mapping.fullPath(), rootClassFqn(lam),
                AssertVerdicts.replayFacts(last, bound),
                AssertVerdicts.unpagedRead(read), bound,
                specs, env, hook, lam,
                replay == null ? golden : null, bindings.lists());
    }

    /** The SQL a golden replays: a bare SQL golden is itself; a plan
     * text ({@code Relational( ... sql = <sql> connection = ...)}, with or
     * without formatting) yields its ONE sql node; several nodes or none
     * = null. */
    private static @com.legend.base.Nullable String planReplaySql(String golden) {
        String g = golden.strip();
        String lower = g.toLowerCase(java.util.Locale.ROOT);
        if (lower.startsWith("select") || lower.startsWith("with")
                || lower.startsWith("(select")) {
            return g;
        }
        java.util.regex.Matcher m = PLAN_SQL_NODE.matcher(g);
        String found = null;
        int n = 0;
        while (m.find()) {
            n++;
            found = m.group(1).strip();
        }
        return n == 1 ? found : null;
    }

    /** {@code sql = <sql>} up to the node's {@code connection =} (the
     * formatted form ends the line; the unformatted form runs on). */
    private static final java.util.regex.Pattern PLAN_SQL_NODE =
            java.util.regex.Pattern.compile(
                    "\\bsql\\s*=\\s*(.*?)(?=\\s*(?:\\n\\s*)?connection\\s*=)",
                    java.util.regex.Pattern.DOTALL);

    /** The executionPlan producer node in an argument tree — exact
     * platform FQN, LET-AWARE like the other finders. */
    /** A plan producer found by INLINING the side's helper calls (the
     *  platform's own inliner: parameters replaced by the call's
     *  arguments, the helper's lets substituted forward), with the reduced
     *  let scope the rows leg then binds. */
    private record LookThrough(TypedNativeCall producer, List<TypedSpec> scope) {
    }

    private static @com.legend.base.Nullable LookThrough lookThrough(TypedSpec side,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        try {
            List<TypedSpec> seq = new java.util.ArrayList<>(letPrefix);
            seq.add(side);
            List<TypedSpec> reduced = new com.legend.compiler.spec.UserCallInliner(specs)
                    .inlineBody(seq);
            List<TypedSpec> prefix = new java.util.ArrayList<>(
                    reduced.subList(0, reduced.size() - 1));
            TypedNativeCall producer = findPlanProducer(reduced.get(reduced.size() - 1), prefix);
            return producer == null ? null : new LookThrough(producer, prefix);
        } catch (RuntimeException e) {
            return null;   // an un-inlinable helper: no producer, as before
        }
    }

    private static @com.legend.base.Nullable TypedNativeCall lookThroughPlanProducer(TypedSpec side,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        LookThrough lt = lookThrough(side, letPrefix, specs);
        return lt == null ? null : lt.producer();
    }

    private static @com.legend.base.Nullable TypedNativeCall findPlanProducer(
            TypedSpec t, List<TypedSpec> letPrefix) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        java.util.Set<String> seenVars = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof TypedNativeCall nc
                    && nc.callee().qualifiedName().equals(
                            com.legend.compiler.element.type.PlatformTypes
                                    .EXECUTION_PLAN)) {
                return nc;
            }
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedVariable tv && seenVars.add(tv.name())) {
                com.legend.compiler.spec.typed.TypedLet tl =
                        com.legend.compiler.spec.typed.Lets.binding(letPrefix, tv.name());
                if (tl != null) {
                    work.add(tl.value());
                }
            }
            work.addAll(cur.children());
        }
        return null;
    }

    /** The assertSqlEquals ROOT entry (the TDG family's own assert
     * function — a USER function reaching the verdict layer
     * PRE-inline, the assertSameSQL discipline): 2 args, the TDG side
     * identifies by its producer. Null = not the TDG shape. */
    static @com.legend.base.Nullable ExecutionResult tryArmTdgRoot(
            com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (root.args().size() != 2) {
            return null;
        }
        boolean t0 = hasTdgProducer(root.args().get(0), letPrefix);
        boolean t1 = hasTdgProducer(root.args().get(1), letPrefix);
        if (t0 == t1) {
            return null;
        }
        return tryArmTdgSql("assertSqlEquals",
                t0 ? root.args().get(1) : root.args().get(0),
                t0 ? root.args().get(0) : root.args().get(1),
                letPrefix, specs, env, hook);
    }

    /** TDG SCORING FLIP (charter burn map): the fetch-text verdict
     * for {@code assertSqlEquals(golden, $tdg.sqls->at(n))} and the
     * plain-assertEquals spelling of the same compare. BOTH sides are
     * generator fetch texts; the SPI executes ours on this session,
     * replays the golden on the oracle, and multiset-compares — the
     * walk's tdgSqlReplay semantics behind the oracle interface. A
     * decline (ordered fetch, chained temp tables) keeps TEXT as the
     * contract, counted. Null = not the TDG shape. */
    private static @com.legend.base.Nullable ExecutionResult tryArmTdgSql(
            String name, TypedSpec goldenSide, TypedSpec actualSide,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (!hasTdgProducer(actualSide, letPrefix)) {
            return null;
        }
        String golden = scalarString(StatementExecutor.evalValue(
                goldenSide, letPrefix, specs, env, null, false, hook));
        String ours = scalarString(StatementExecutor.evalValue(
                actualSide, letPrefix, specs, env, null, false, hook));
        if (golden == null || ours == null) {
            return null;
        }
        boolean textEqual = golden.equals(ours);
        SqlReplayOracle oracle = env.replayOracle();
        if (oracle == null) {
            throw new com.legend.error.NotImplementedException(
                    "sql-text assert verdict needs a replay oracle and"
                            + " none is registered on this env (correct"
                            + " outside tests: there are no goldens)");
        }
        com.legend.exec.VerdictBatch batch = env.verdictBatch();
        if (batch != null && batch.active()) {
            // TASK #14 LEG 2 (2026-09-21): the TDG fetch-text assert IS a verdict row of
            // the body's statement — the golden fetch text against ours, string
            // equality — and the referee (the fetch replayed on the oracle, rows
            // compared) is its APPEAL, run at the flush only when the row failed.
            batch.defer(name, true, com.legend.lowering.VerdictSql.textEquals(golden, ours),
                    env.connection(), () -> tdgRowsNow(name, golden, ours, textEqual, oracle,
                            actualSide, letPrefix, specs, env));
            return ok();
        }
        if (textEqual) {
            // the host judge, the same rule (option 1): a byte-equal text IS the verdict
            return ok();
        }
        return tdgRowsNow(name, golden, ours, textEqual, oracle, actualSide, letPrefix, specs, env);
    }

    /** The TDG rows leg itself — the referee's fetch replay, judged now. */
    private static ExecutionResult tdgRowsNow(String name, String golden, String ours,
            boolean textEqual, SqlReplayOracle oracle, TypedSpec actualSide,
            List<TypedSpec> letPrefix, SpecCompiler specs, StatementExecutor.ExecEnv env) {
        // batch 64: a hop addressed as $testData.sqls->at(i) carries its
        // hop index and the carrier's generator node — the oracle's
        // chained arm replays ancestor temps from the earlier hops'
        // goldens and compares the hop's transcript rows
        TdgHop hop = tdgHop(actualSide, letPrefix);
        SqlReplayOracle.RowVerdict rv;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_GOLDEN)) {
            rv = hop == null
                ? oracle.verifyFetchTexts(env.connection(), golden, ours)
                : oracle.verifyFetchChain(env.connection(), hop.index(),
                        golden, ours, () -> fetchTranscript(
                                com.legend.testdatagen.TestDataGenerationNatives
                                        .transcript(hop.source(), env.ctx(),
                                                env.connection(),
                                                StatementExecutor.viewSqlRenderer(specs, env))));
        }
        refereed(env, name, rv);
        return switch (rv.outcome()) {
            case MATCH -> {
                yield ok();
            }
            case DIVERGED -> fail(name + " (tdg fetch-text ROW verdict"
                    + " — golden rows vs ours diverged, whatever the"
                    + " text said): " + rv.detail());
            case FAULT -> fail(name + " (tdg fetch-text: referee FAULT —"
                    + " the referee's own machinery failed, the text cannot"
                    + " stand in): " + rv.detail());
            case DECLINED -> {
                declined(env, name, "oracle-declined");
                yield textEqual ? ok()
                        : fail(name + " (tdg fetch-text, declined: "
                                + rv.detail() + "): expected " + golden
                                + ", got " + ours);
            }
        };
    }

    /** The folded (S2) form of the generator carrier — what a let holds
     * once the orchestrator ran the extraction
     * ({@code CsvCensusChecker.literalTestData}). */
    private static final String TDG_RESULT_FQN =
            "meta::relational::testDataGeneration::TestDataGenResult";

    /** A generator fetch hop: its index in {@code sqls} and the carrier's
     * kept generator node. */
    private record TdgHop(int index,
            com.legend.compiler.spec.typed.TypedTestDataGen source) {
    }

    /** The hop address of a {@code $testData.sqls->at(i)} side: after the
     * fold the at() receiver IS the carrier's sqls collection (postFold
     * reads the property over the instance literal), the carrier is the
     * letPrefix binding whose sqls equals that collection, and its
     * {@code source} is the generator node the fold kept. Null = not
     * the shape (the plain fetch-text verdict owns it). */
    private static @com.legend.base.Nullable TdgHop tdgHop(TypedSpec actualSide,
            List<TypedSpec> letPrefix) {
        // the H2Compatible spelling flattens the hop's text through the
        // String overload: $testData.sqls->at(i)->sqlRemoveFormatting()
        if (actualSide instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && uc.callee().qualifiedName().equals(
                        com.legend.compiler.spec.ResultEnvelopeSplice
                                .SQL_REMOVE_FORMATTING_FQN)
                && uc.args().size() == 1) {
            actualSide = uc.args().get(0);
        }
        if (!(actualSide instanceof TypedNativeCall at
                && at.callee().qualifiedName().equals(
                        com.legend.builtin.Pure.AT__T_MANY__INTEGER_1
                                .qualifiedName())
                && at.args().size() == 2
                && at.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCInteger idx)) {
            return null;
        }
        TypedSpec receiver = at.args().get(0);
        // the let-bound spelling: $testData.sqls, the let still holding
        // the generator call (the fold runs at the let's own execution)
        String var = receiver instanceof
                com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("sqls")
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v
                ? v.name() : null;
        for (TypedSpec l : letPrefix) {
            if (!(l instanceof com.legend.compiler.spec.typed.TypedLet let)) {
                continue;
            }
            if (let.value() instanceof
                            com.legend.compiler.spec.typed.TypedNewInstance ni
                    && TDG_RESULT_FQN.equals(ni.classFqn())
                    && ni.properties().get("source") instanceof
                            com.legend.compiler.spec.typed.TypedTestDataGen g
                    && (receiver.equals(ni.properties().get("sqls"))
                            || let.name().equals(var))) {
                return new TdgHop(idx.value().intValue(), g);
            }
            if (let.name().equals(var)) {
                com.legend.compiler.spec.typed.TypedTestDataGen g =
                        generatorIn(let.value());
                if (g != null) {
                    return new TdgHop(idx.value().intValue(), g);
                }
            }
        }
        return null;
    }

    private static final java.util.regex.Pattern NUMBERED_IN_TEMP =
            java.util.regex.Pattern.compile("tempTableForIn_(\\d+)");

    /** The engine-session temp table a golden reads for an INLINE in-list
     * (batch 65): the engine numbers {@code tempTableForIn_N} by plan
     * node and fills it with the query's literal collection — so the
     * table's rows ARE the query's {@code in([...])} literal, read off
     * the typed query (exactly one inline in-collection, one temp name).
     * Empty when the golden reads no numbered temp or the shape is not
     * this one (the oracle's decline stays counted). */
    /** The connection time zone the read's own frame executes under: the
     * outermost from of the read — the ONE carrier of frame facts. A read
     * over a let-bound Result is SPLICED first (the frame's envelope chain,
     * a from, stands where the variable stood); a read without a from
     * executes under no connection zone. */
    private static @com.legend.base.Nullable String frameZone(TypedSpec read) {
        var froms = com.legend.compiler.spec.typed.ExecutionContext.froms(read);
        return froms.isEmpty() ? null : froms.get(0).context().timeZone();
    }

    private static List<SqlReplayOracle.TempTable> inListTemps(String golden,
            TypedSpec query, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable String zone) {
        var m = NUMBERED_IN_TEMP.matcher(golden);
        java.util.Set<String> names = new java.util.LinkedHashSet<>();
        while (m.find()) {
            names.add(m.group());
        }
        if (names.size() != 1) {
            return List.of();
        }
        List<com.legend.compiler.spec.typed.TypedCollection> lists =
                new java.util.ArrayList<>();
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        // the frame's own query (the exec-read arms resolve it through
        // the splice hook) or the wrapped rows leg; the lets are in scope
        work.add(query);
        work.addAll(letPrefix);
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof TypedNativeCall nc
                    && nc.callee().qualifiedName().equals(
                            com.legend.builtin.Pure.IN__ANY_1__ANY_MANY
                                    .qualifiedName())
                    && nc.args().size() == 2
                    && nc.args().get(1) instanceof
                            com.legend.compiler.spec.typed.TypedCollection c) {
                lists.add(c);
            }
            work.addAll(cur.children());
        }
        if (lists.size() != 1) {
            return List.of();
        }
        String kind = null;
        List<String> values = new java.util.ArrayList<>();
        for (TypedSpec e : lists.get(0).elements()) {
            String k;
            String v;
            if (e instanceof com.legend.compiler.spec.typed.TypedCDate d) {
                k = d.value() instanceof com.legend.values.PureDateLiteral
                        .StrictDate ? "date" : "datetime";
                // the engine seeds the temp in the connection's zone (batch 86)
                String iso = com.legend.lowering.LiteralSpelling.isoTimestamp(d.value());
                v = iso != null && zone != null
                        ? com.legend.lowering.LiteralSpelling.inZone(iso, zone)
                        : d.value().toEngineString();
            } else if (e instanceof com.legend.compiler.spec.typed.TypedCString s) {
                k = "string";
                v = s.value();
            } else if (e instanceof com.legend.compiler.spec.typed.TypedCInteger i) {
                k = "integer";
                v = String.valueOf(i.value());
            } else {
                return List.of();   // an unwitnessed literal kind
            }
            if (kind != null && !kind.equals(k)) {
                return List.of();
            }
            kind = k;
            values.add(v);
        }
        return kind == null ? List.of()
                : List.of(new SqlReplayOracle.TempTable(
                        names.iterator().next(), kind, values));
    }

    /** The generator's run in the SPI's own terms (exec never depends on
     * the generator package). */
    private static SqlReplayOracle.FetchTranscript fetchTranscript(
            com.legend.testdatagen.TestDataGenerator.Result r) {
        List<SqlReplayOracle.FetchHop> hops = new java.util.ArrayList<>();
        if (r.fetches() != null) {
            for (com.legend.testdatagen.TestDataGenerator.Fetch f : r.fetches()) {
                hops.add(new SqlReplayOracle.FetchHop(f.parentIndex(),
                        f.table(), f.columns(), f.rows()));
            }
        }
        return new SqlReplayOracle.FetchTranscript(r.sqls(), hops);
    }

    /** The generator node inside a let's value (through toOne and other
     * wrappers); null when absent or a plan flavor. */
    private static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedTestDataGen
            generatorIn(TypedSpec t) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof com.legend.compiler.spec.typed.TypedTestDataGen g
                    && !"plan".equals(g.flavor())) {
                return g;
            }
            work.addAll(cur.children());
        }
        return null;
    }

    /** A generateTestData producer under {@code t} — the typed carrier
     * node, the exact platform FQN, OR the carrier's FOLDED instance
     * literal (the orchestrator replaces the carrier with its executed
     * result per statement, so by verdict time a let-bound
     * {@code $testData} reads as a TestDataGenResult literal — the
     * 29-test TDG cohort had walled on that, its asserts falling through
     * to the scalar lowerer as "no scalar lowering for assertEquals"),
     * LET-AWARE. */
    private static boolean hasTdgProducer(TypedSpec t,
            List<TypedSpec> letPrefix) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        java.util.Set<String> seenVars = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedTestDataGen tg && !"plan".equals(tg.flavor())) {
                // a TDG PLAN is plan text, not a fetch-text producer
                return true;
            }
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedNewInstance ni
                    && TDG_RESULT_FQN.equals(ni.classFqn())) {
                return true;
            }
            if (cur instanceof TypedNativeCall nc
                    && nc.callee().qualifiedName().equals(
                            com.legend.compiler.element.type.PlatformTypes
                                    .GENERATE_TEST_DATA)) {
                return true;
            }
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedVariable tv && seenVars.add(tv.name())) {
                com.legend.compiler.spec.typed.TypedLet tl =
                        com.legend.compiler.spec.typed.Lets.binding(letPrefix, tv.name());
                if (tl != null) {
                    work.add(tl.value());
                }
            }
            work.addAll(cur.children());
        }
        return false;
    }

    /** The SHARED rows leg + verdict policy (§3.5c-§3.7): evaluate
     * {@code rowsRead} through the one router (REFEREE-CLASS
     * execution — wire-census suspended, save/restore), replay the
     * golden via the oracle, judge. Rows match → pass (text →
     * emission census); rows diverge → FAIL whatever the text said;
     * rows leg underivable or oracle declined → TEXT is the contract,
     * counted. */
    private static ExecutionResult rowsLegAndVerdict(String name,
            String golden, String ours, boolean textEqual,
            SqlReplayOracle oracle, TypedSpec rowsRead,
            @com.legend.base.Nullable String replaySqlOrNull,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String classFqn, SqlReplayOracle.ReplayFacts facts,
            @com.legend.base.Nullable TypedSpec populationRead,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook,
            @com.legend.base.Nullable TypedSpec query) {
        return rowsLegAndVerdict(name, golden, ours, textEqual, oracle,
                rowsRead, replaySqlOrNull, mappingFqn, classFqn, facts, populationRead,
                letPrefix, specs, env, hook, query, null, java.util.Map.of());
    }

    /** {@link #rowsLegAndVerdict} with a golden PLAN for the oracle's
     * plan replay ({@code goldenPlan} non-null: the nodes run in order,
     * {@code planBindings} = the referee's parameter values). */
    private static ExecutionResult rowsLegAndVerdict(String name,
            String golden, String ours, boolean textEqual,
            SqlReplayOracle oracle, TypedSpec rowsRead,
            @com.legend.base.Nullable String replaySqlOrNull,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String classFqn, SqlReplayOracle.ReplayFacts facts,
            @com.legend.base.Nullable TypedSpec populationRead,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook,
            @com.legend.base.Nullable TypedSpec query,
            @com.legend.base.Nullable String goldenPlan,
            java.util.Map<String, List<String>> planBindings) {
        return rowsLegAndVerdict(name, golden, ours, textEqual, oracle,
                rowsRead, replaySqlOrNull, mappingFqn, classFqn, facts, populationRead,
                letPrefix, specs, env, hook, query, goldenPlan, planBindings,
                List.of());
    }

    /** {@link #rowsLegAndVerdict} with extra temp tables the golden reads
     * ({@code temps} — beside the inline in-list temp the arm derives). */
    private static ExecutionResult rowsLegAndVerdict(String name,
            String golden, String ours, boolean textEqual,
            SqlReplayOracle oracle, TypedSpec rowsRead,
            @com.legend.base.Nullable String replaySqlOrNull,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String classFqn, SqlReplayOracle.ReplayFacts facts,
            @com.legend.base.Nullable TypedSpec populationRead,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook,
            @com.legend.base.Nullable TypedSpec query,
            @com.legend.base.Nullable String goldenPlan,
            java.util.Map<String, List<String>> planBindings,
            List<SqlReplayOracle.TempTable> temps) {
        com.legend.exec.VerdictBatch batch = env.verdictBatch();
        if (batch != null && batch.active()) {
            // BLOCK-COMPILER RUNG 2a (2026-09-21): the text assert IS a verdict row of
            // the body's statement — the golden text against our rendered text, the
            // same string equality every assertEquals compiles to — and the referee is
            // its APPEAL, run at the flush only when the row failed (a byte-equal text
            // needs none). No assert shape is routed around the batch any more.
            batch.defer(name, true, com.legend.lowering.VerdictSql.textEquals(golden, ours),
                    env.connection(), () -> rowsLegNow(name, golden, ours, textEqual, oracle,
                            rowsRead, replaySqlOrNull, mappingFqn, classFqn, facts, populationRead,
                            letPrefix, specs, env, hook, query, goldenPlan, planBindings, temps));
            return ok();
        }
        if (textEqual) {
            // THE HOST JUDGE, THE SAME RULE (user ruling 2026-09-21, option 1): a text
            // byte-equal to the golden IS the assert's verdict — the engine's own —
            // and rows are the APPEAL on a failed text only. Both judges agree; the
            // DuckDB-vs-H2 function divergences a held text used to expose belong
            // to the tests that assert VALUES.
            return ok();
        }
        return rowsLegNow(name, golden, ours, textEqual, oracle, rowsRead, replaySqlOrNull,
                mappingFqn, classFqn, facts, populationRead, letPrefix, specs, env, hook, query,
                goldenPlan, planBindings, temps);
    }

    /** The rows leg itself: OUR rows through the one router, the golden replayed
     * via the oracle, judged — now, on this thread. */
    private static ExecutionResult rowsLegNow(String name,
            String golden, String ours, boolean textEqual,
            SqlReplayOracle oracle, TypedSpec rowsRead,
            @com.legend.base.Nullable String replaySqlOrNull,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String classFqn, SqlReplayOracle.ReplayFacts facts,
            @com.legend.base.Nullable TypedSpec populationRead,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook,
            @com.legend.base.Nullable TypedSpec query,
            @com.legend.base.Nullable String goldenPlan,
            java.util.Map<String, List<String>> planBindings,
            List<SqlReplayOracle.TempTable> temps) {
        ExecutionResult rows;
        boolean priorSuspend = com.legend.exec.SqlTypeCensus
                .probeSuspended();
        provideStores(env, mappingFqn);
        try {
            com.legend.exec.SqlTypeCensus.probeSuspend(true);
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
            rows = StatementExecutor.evalValue(rowsRead,
                    letPrefix, specs, env, null, false, hook);
            }
        } catch (RuntimeException e) {
            // the rows leg is underivable — counted, text stays the
            // contract (§3.7: a counted decline, visible, never silent;
            // DataError joined RuntimeException at the seam)
            declined(env, name, "rows-underivable");
            return textEqual ? ok()
                    : fail(name + " (sql-text, rows underivable — " + e.getClass().getSimpleName()
                            + ": " + e.getMessage() + "): expected " + golden + ", got " + ours);
        } finally {
            com.legend.exec.SqlTypeCensus.probeSuspend(priorSuspend);
        }
        if (populationRead != null && rows != null) {
            // the PAGE-MEMBERSHIP verdict's population (batch 0.5b): our rows
            // for the same chain without its tail page. No catch: the paged
            // read just executed, so a failure here is a FAULT of ours and
            // fails the test loudly — never a decline that rescues the text
            com.legend.exec.SqlTypeCensus.probeSuspend(true);
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
                ExecutionResult population = StatementExecutor.evalValue(populationRead,
                        letPrefix, specs, env, null, false, hook);
                if (population != null) {
                    facts = facts.withPopulation(population);
                }
            } finally {
                com.legend.exec.SqlTypeCensus.probeSuspend(priorSuspend);
            }
        }
        if (rows == null) {
            declined(env, name, "rows-underivable");
            return textEqual ? ok()
                    : fail(name + " (sql-text, rows underivable):"
                            + " expected " + golden + ", got " + ours);
        }
        SqlReplayOracle.RowVerdict rv;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_GOLDEN)) {
            rv = goldenPlan != null
                ? oracle.verifyPlan(env.connection(), goldenPlan, planBindings,
                        rows, mappingFqn, classFqn, facts, env.ctx())
                : oracle.verify(env.connection(),
                        replaySqlOrNull != null ? replaySqlOrNull : golden,
                        rows, mappingFqn, classFqn, facts, env.ctx(),
                        temps.isEmpty()
                                ? inListTemps(golden, query != null ? query : rowsRead,
                                        letPrefix, frameZone(hook == null ? rowsRead
                                                : hook.apply(rowsRead, java.util.Set.of())))
                                : temps);
        }
        refereed(env, name, rv);
        return switch (rv.outcome()) {
            case MATCH -> {
                // rows are the verdict (§0); text is a census number
                yield ok();
            }
            case DIVERGED -> fail(name + " (sql-text ROW verdict —"
                    + " golden rows vs ours diverged, whatever the text"
                    + " said): " + rv.detail());
            case FAULT -> fail(name + " (sql-text: referee FAULT — the"
                    + " referee's own machinery failed, the text cannot"
                    + " stand in): " + rv.detail());
            case DECLINED -> {
                // oracle could not answer: text is the contract,
                // decline counted (§3.7)
                declined(env, name, "oracle-declined");
                yield textEqual ? ok()
                        : fail(name + " (sql-text, oracle declined: "
                                + rv.detail() + "): expected " + golden
                                + ", got " + ours);
            }
        };
    }

    /** The producer node in an argument tree, by EXACT callee FQN
     * (§3.4 — never names, never text). LET-AWARE (§3.2): a variable
     * reference chases its {@code letPrefix} binding — the platform
     * keeps lets as lets, so {@code let sql = toSQLString(...);
     * assertEquals(golden, $sql)} carries the producer BEHIND the
     * variable. Null when absent. */
    private static com.legend.compiler.spec.NativeDispatch.@com.legend.base.Nullable RoutineCall findProducer(
            TypedSpec t, List<TypedSpec> letPrefix) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        java.util.Set<String> seenVars = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            if (cur instanceof TypedNativeCall nc) {
                String fqn = nc.callee().qualifiedName();
                if (fqn.equals(com.legend.compiler.element.type
                                .PlatformTypes.TO_SQL_STRING)
                        || fqn.equals(com.legend.compiler.element.type
                                .PlatformTypes.TO_SQL_STRING_PRETTY)
                        || fqn.equals(com.legend.compiler.element.type
                                .PlatformTypes.TO_NON_EXECUTABLE_SQL_STRING)) {
                    return com.legend.compiler.spec.NativeDispatch.RoutineCall.of(nc);
                }
            }
            // the RECEIVER form: SQLResult's qualified property, implemented
            // by the toSQLString routine (NativeFn.JavaRoutine.implementedDerived)
            if (cur instanceof TypedNativeCall dc
                    && com.legend.builtin.NativeFn.JavaRoutine.ofDerived(dc.callee().qualifiedName()).isPresent()) {
                return com.legend.compiler.spec.NativeDispatch.RoutineCall.of(dc);
            }
            if (cur instanceof com.legend.compiler.spec.typed
                    .TypedVariable tv && seenVars.add(tv.name())) {
                com.legend.compiler.spec.typed.TypedLet tl =
                        com.legend.compiler.spec.typed.Lets.binding(letPrefix, tv.name());
                if (tl != null) {
                    work.add(tl.value());
                }
            }
            work.addAll(cur.children());
        }
        return null;
    }

    /** The query's root class when it returns instances (drives the
     * oracle's per-property enum decode); null for relation-shaped
     * results. */
    /** The exec-read frame's mapping FQN and root class (the oracle's
     * enum-decode inputs): the frame variable chased to its execute()
     * call — {@code execute(lambda, mapping, runtime, ...)}. Nulls when
     * the frame is not an execute() (a validate()/other frame keeps the
     * counted enum decline). */
    /** The executed frame's mapping, root class, and STATIC extent-subset
     * fact (a class extent through subset-preserving ops — the graph
     * compare's pk-collapse licence). */
    private record FrameFacts(@com.legend.base.Nullable String mapping,
            @com.legend.base.Nullable String cls, SqlReplayOracle.ReplayFacts facts,
            @com.legend.base.Nullable TypedSpec populationRead,
            @com.legend.base.Nullable TypedSpec query,
            @com.legend.base.Nullable TypedPackageableRef mappingRef,
            com.legend.compiler.spec.typed.ExecutionContext context) {
    }

    private static FrameFacts frameMappingAndClass(TypedSpec resultArg,
            List<TypedSpec> letPrefix,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook, SpecCompiler specs) {
        TypedSpec src = com.legend.compiler.spec.typed.Lets.bound(resultArg, letPrefix);
        while (src instanceof com.legend.compiler.spec.typed.TypedFrom sf) {
            src = sf.source();
        }
        if (!(src instanceof TypedNativeCall) && hook != null) {
            // an EXECUTED frame is no longer a let value: the splice hook
            // resolves the frame variable's reads to the frame's own
            // execute() call (the activities read is the frame's
            // sourceExec — ResultEnvelopeSplice)
            TypedSpec spliced = hook.apply(
                    com.legend.compiler.spec.VerdictQueries
                            .activitiesRead(resultArg),
                    java.util.Set.of());
            if (spliced instanceof com.legend.compiler.spec.typed
                    .TypedPropertyAccess pa) {
                src = pa.source();
                while (src instanceof com.legend.compiler.spec.typed.TypedFrom sf) {
                    src = sf.source();
                }
            }
        }
        if (src instanceof TypedNativeCall ec
                && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().id())
                && ec.args().size() >= 2) {
            String mapping = ec.args().get(1) instanceof TypedPackageableRef m
                    ? m.fullPath() : null;
            TypedSpec lamArg = com.legend.compiler.spec.typed.Lets.bound(ec.args().get(0), letPrefix);
            String cls = lamArg instanceof TypedLambda lam
                    && !lam.body().isEmpty() ? rootClassFqn(lam) : null;
            TypedPackageableRef mappingRef =
                    ec.args().get(1) instanceof TypedPackageableRef mr ? mr : null;
            com.legend.compiler.spec.typed.ExecutionContext context = ec.args().size() >= 3
                    ? StatementExecutor.boundContext(ec.args().get(2), letPrefix, specs)
                    : com.legend.compiler.spec.typed.ExecutionContext.NONE;
            TypedSpec chain = lamArg instanceof TypedLambda lam2 && !lam2.body().isEmpty()
                    ? lam2.body().get(lam2.body().size() - 1) : null;
            SqlReplayOracle.ReplayFacts facts = chain != null
                    ? AssertVerdicts.replayFacts(chain, letPrefix)
                    : SqlReplayOracle.ReplayFacts.NONE;
            // a PAGED frame chain: our unpaged population is the same chain
            // minus its tail page, wrapped in the frame's own context
            TypedSpec populationRead = chain != null && mappingRef != null
                    ? AssertVerdicts.unpagedRead(com.legend.compiler.spec.VerdictQueries
                            .fromWrapped(chain, mappingRef, context))
                    : null;
            return new FrameFacts(mapping, cls, facts, populationRead, lamArg,
                    mappingRef, context);
        }
        return new FrameFacts(null, null, SqlReplayOracle.ReplayFacts.NONE, null, null, null,
                com.legend.compiler.spec.typed.ExecutionContext.NONE);
    }

    private static @com.legend.base.Nullable String rootClassFqn(
            TypedLambda lam) {
        return lam.body().get(lam.body().size() - 1).info().type()
                instanceof com.legend.compiler.element.type.Type
                        .ClassType ct
                ? ct.fqn() : null;
    }

    private static @com.legend.base.Nullable String scalarString(
            @com.legend.base.Nullable ExecutionResult r) {
        return r instanceof ExecutionResult.Scalar s
                && s.value() instanceof String str ? str : null;
    }

    /** A text-decided arm reports its NAMED decline to the runner before
     * the text decides (Phase 0.6, audit §10 item 7 — no path returns
     * {@code textEqual ? ok() : fail(...)} uncounted). */
    private static void declined(StatementExecutor.ExecEnv env, String name,
            String reason) {
        com.legend.exec.AssertListener l = env.assertListener();
        if (l != null) {
            l.declined(name, reason);
        }
    }

    /** Fixture on demand (corpus-zero program, 2026-09-12): before the rows
     * leg reads, every store the golden's mapping reads (the compiled
     * mapping model's class-binding sources, includes followed) is offered
     * to the runner ({@link com.legend.exec.AssertListener#provideStore}),
     * which seeds it when exactly one fixture does. Model navigation only:
     * no SQL text, no driver message, no judgment. */
    private static void provideStores(StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable String mappingFqn) {
        com.legend.exec.AssertListener l = env.assertListener();
        if (l == null || mappingFqn == null) {
            return;
        }
        for (String store : storesOf(env.ctx(), mappingFqn, new java.util.LinkedHashSet<>())) {
            l.provideStore(store);
        }
    }

    private static java.util.Set<String> storesOf(com.legend.compiler.element.ModelContext ctx,
            String mappingFqn, java.util.Set<String> visited) {
        java.util.Set<String> stores = new java.util.LinkedHashSet<>();
        if (!visited.add(mappingFqn)) {
            return stores;
        }
        ctx.findMapping(mappingFqn).ifPresent(m -> {
            for (var b : m.classBindings()) {
                if (b instanceof com.legend.model.MappingDefinition.ClassBinding.Relational r
                        && r.source() instanceof com.legend.model.MappingDefinition
                                .RelationalSource.Table t) {
                    stores.add(t.database());
                }
            }
            for (var inc : m.includes()) {
                stores.addAll(storesOf(ctx, inc.mappingPath(), visited));
            }
        });
        return stores;
    }

    /** The referee's row verdict for this assert, reported to the runner
     * (Phase 0.7): the strength census reads a MATCH as the differential
     * witness. */
    private static void refereed(StatementExecutor.ExecEnv env, String name,
            SqlReplayOracle.RowVerdict rv) {
        com.legend.exec.AssertListener l = env.assertListener();
        if (l != null) {
            l.refereed(name, rv.outcome().name());
        }
    }

    private static ExecutionResult ok() {
        return new ExecutionResult.Scalar(Boolean.TRUE,
                com.legend.compiler.element.type.Type.Primitive.BOOLEAN);
    }

    /** A TEXT-DECIDED verdict (a counted decline — no referee can appeal it): under
     * a batch the text equality IS a verdict row of the body's statement and the
     * arm's own message is raised when the row fails (task #14 leg 2, 2026-09-21);
     * the host judge decides on the text now. */
    private static ExecutionResult textVerdict(StatementExecutor.ExecEnv env, String name,
            String golden, String ours, boolean textEqual,
            java.util.function.Supplier<String> message) {
        com.legend.exec.VerdictBatch batch = env.verdictBatch();
        if (batch != null && batch.active()) {
            batch.defer(name, true, com.legend.lowering.VerdictSql.textEquals(golden, ours),
                    env.connection(), () -> fail(message.get()));
            return ok();
        }
        return textEqual ? ok() : fail(message.get());
    }

    private static ExecutionResult fail(String message) {
        // the seam: verdicts speak the platform vocabulary
        throw new com.legend.error.AssertFailed(message);
    }
}
