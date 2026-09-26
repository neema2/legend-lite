// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.builtin.NativeFn;

import com.legend.compiler.element.ModelContext;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.exec.ExecutionResult;
import com.legend.exec.PureAsserts;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ASSERT-FAMILY VERDICT ARM (Charter Clause 2c): a statement-root
 * call to the assert family is a VERDICT — its result terminates in the
 * runner. Each ARGUMENT executes through the full pipeline in the
 * database; the judgment over the two sides is {@link PureAsserts}
 * (host mode) or one VerdictSql statement (database mode). Assert
 * bodies are never inlined into SQL to produce a verdict. Map-wrapped
 * asserts ({@code values->map(f|assert(...))}) are QUANTIFIED verdicts
 * served by the quantified arm; family members without a verdict arm
 * decline loudly with their shape — never a silent skip.
 */
final class AssertVerdicts {
    private AssertVerdicts() {
    }

    private static final String PKG = com.legend.compiler.element.type.PlatformTypes.ASSERTS_PACKAGE;

    /** V7 batch 2: the statement loop's result-envelope splice hook,
     * threaded into every side evaluation so an assert argument reading
     * an execute() frame compiles the SPLICED chain — identical to the
     * ordinary-statement path (audit 19d B2; splice pin:
     * AssertVerdictSpliceTest). Null = no frames in scope. */
    interface SpliceHook extends java.util.function.BiFunction<TypedSpec,
            java.util.Set<String>, TypedSpec> {
    }

    /** Null = not a statement-root assert this arm owns (generic path
     * continues); otherwise the verdict (TRUE, or the spec's failure
     * raised as the runner's failure). */
    static @com.legend.base.Nullable ExecutionResult tryAdjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.exec.AssertListener l = env.assertListener();
        com.legend.exec.VerdictBatch batch = env.verdictBatch();
        if (batch != null) {
            return batched(batch, bare, letPrefix, specs, env, rawHook);
        }
        if (l == null) {
            return adjudicate(bare, letPrefix, specs, env, rawHook);
        }
        // the listener observes the arm's OWN outcomes only: a non-null
        // verdict = pass; a raise out of an owned adjudication = fail
        // (side-evaluation errors fail the same test either way — the
        // detail says which). Judgment itself is untouched.
        boolean settled = false;
        com.legend.exec.CanonicalDivergence.sqlEnter();   // census: a new assert, no family yet
        try {
            ExecutionResult v = adjudicate(bare, letPrefix, specs, env,
                    rawHook);
            settled = true;
            if (v != null) {
                l.verdict(listenerName(bare), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed
                | com.legend.error.DataError e) {
            // the seam: a FALSE verdict or a side-evaluation data error
            // fails the same test either way — the detail says which
            settled = true;
            com.legend.exec.CanonicalDivergence.sqlRaised();
            if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                l.unjudged(listenerName(bare), af.unjudgedReason());
            }
            l.verdict(listenerName(bare), false, e.getMessage());
            throw e;
        } finally {
            if (!settled) {
                // any other exit (a wall, a not-implemented shape) leaves
                // without a verdict: counted for the census, never caught
                com.legend.exec.CanonicalDivergence.sqlRaised();
            }
        }
    }

    /** Leg 3.4: the assert's verdict statements DEFER into the body's batch
     * (VerdictBatch); a decided outcome or a raise is a step in order,
     * reported at the flush. Any other exit (a wall) flushes what came
     * before — those verdicts were already the body's — then surfaces. */
    private static @com.legend.base.Nullable ExecutionResult batched(com.legend.exec.VerdictBatch batch,
            TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.exec.CanonicalDivergence.sqlEnter();
        batch.open(listenerName(bare));
        ExecutionResult v = null;
        RuntimeException deferred = null;
        boolean settled = false;
        try {
            v = adjudicate(bare, letPrefix, specs, env, rawHook);
            settled = true;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            settled = true;
            deferred = e;
        } finally {
            if (!settled) {
                com.legend.exec.CanonicalDivergence.sqlRaised();
                batch.discard();
                flush(batch, env);
            }
        }
        if (deferred != null) {
            batch.resolve(deferred);
            batch.close();
            return ok();
        }
        if (v == null) {
            batch.discard();
            return null;
        }
        batch.close();
        return v;
    }

    static void flush(com.legend.exec.VerdictBatch batch, StatementExecutor.ExecEnv env) {
        batch.flush(env.dialect(), env.trace(), env.assertListener());
    }

    /** The one-row shape a verdict statement returns. */
    static final com.legend.compiler.element.type.ExprType ONE_ROW =
            new com.legend.compiler.element.type.ExprType(
                    com.legend.lowering.VerdictSql.schema(),
                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE);

    private static String listenerName(TypedSpec bare) {
        String fqn = calleeFqn(bare);
        return fqn != null ? fqn
                : bare instanceof com.legend.compiler.spec.typed.TypedMap
                        ? "quantified-assert" : bare.getClass().getSimpleName();
    }

    static boolean classKind(TypedSpec s) {
        return s.info().type() instanceof com.legend.compiler.element.type.Type.ClassType;
    }

    /** An if branch's statement: the lambda's last statement, or the
     * expression itself. */
    private static TypedSpec branchStatement(TypedSpec branch) {
        return branch instanceof com.legend.compiler.spec.typed.TypedLambda l
                && !l.body().isEmpty()
                ? l.body().get(l.body().size() - 1) : branch;
    }

    private static @com.legend.base.Nullable ExecutionResult adjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        // task #14 leg 1 (2026-09-21): a lineage tree print is judged as LINES —
        // the golden brought to its canon rows here, ours the prelude's own
        // rows; the ordinary collection verdict decides
        TypedSpec lines = com.legend.compiler.spec.LineageTreeLines.asLines(bare, letPrefix, env.ctx());
        if (lines != null) {
            bare = lines;
        }
        TypedSpec unwrapped = com.legend.compiler.spec.VerdictQueries.distinctTrueWrapper(bare);
        if (unwrapped instanceof com.legend.compiler.spec.typed.TypedMap qm2) {
            ExecutionResult u = unrolled(qm2, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
        }
        if (bare instanceof com.legend.compiler.spec.typed.TypedMap qm) {
            ExecutionResult u = unrolled(qm, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
            return quantified(qm, letPrefix, specs, env, hook);
        }
        // forAll(coll, x | <assert>) — the engine's per-element assert
        // idiom (stringToFloat testProject: `[123.456, 100.001]->zip(
        // $tds.rows.values)->forAll(pair | assertEqWithinTolerance(...))`)
        // IS the quantified assert: every element's assert holds (an
        // assert never yields false — it raises), so the forAll unrolls
        // exactly as the map form does
        com.legend.compiler.spec.typed.TypedMap forAllForm =
                com.legend.compiler.spec.VerdictQueries.forAllAsQuantified(bare);
        if (forAllForm != null) {
            ExecutionResult u = unrolled(forAllForm, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
        }
        // an if whose BRANCHES are asserts (assertEqualsH2Compatible's body
        // once the H2 version probe answers): the condition is a value
        // query the database evaluates; the taken branch IS the verdict
        if (bare instanceof com.legend.compiler.spec.typed.TypedIf ti
                && ti.elseBranch().isPresent()
                && calleeFqn(branchStatement(ti.thenBranch())) != null
                && calleeFqn(branchStatement(ti.elseBranch().get())) != null) {
            ExecutionResult c = StatementExecutor.evalValue(
                    ti.condition(), letPrefix, specs, env);
            if (c instanceof ExecutionResult.Scalar sc
                    && sc.value() instanceof Boolean taken) {
                return adjudicate(branchStatement(taken
                        ? ti.thenBranch() : ti.elseBranch().get()),
                        letPrefix, specs, env, rawHook);
            }
            return null;
        }
        String fqn = calleeFqn(bare);
        if (fqn == null) {
            return null;
        }
        // cleanup move 2b (2026-09-21): the run's judge mode names the arm ONCE; every
        // family below classifies, then hands the sides to it
        VerdictArm arm = arm(env);
        // SQLTEXT charter §8.3b — the assertSameSQL ROOT arm: the
        // statement root arrives PRE-inline, so SqlTextVerdicts owns
        // the whole golden-vs-executed-frame shape (rows judge, text
        // is the emission census). Null = not the simple shape — the
        // generic path (inline + fold) keeps it exactly as today.
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_SAME_SQL.equals(fqn)
                && bare instanceof TypedUserCall sroot) {
            ExecutionResult sv = SqlTextVerdicts.tryArmSameSql(sroot,
                    letPrefix, specs, env, hook);
            if (sv != null) {
                return sv;
            }
        }
        // TDG scoring flip — the assertSqlEquals root (same discipline)
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_SQL_EQUALS_TDG.equals(fqn)
                && bare instanceof TypedUserCall troot) {
            ExecutionResult tv = SqlTextVerdicts.tryArmTdgRoot(troot,
                    letPrefix, specs, env, hook);
            if (tv != null) {
                return tv;
            }
        }
        // §8.3d — the dual-golden sibling (same root-arm discipline)
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS_H2_COMPATIBLE.equals(fqn)
                && bare instanceof TypedUserCall hroot) {
            ExecutionResult hv = SqlTextVerdicts.tryArmH2Compat(hroot,
                    letPrefix, specs, env, hook);
            if (hv != null) {
                return hv;
            }
            // the arm is TOTAL: where no text arm applies, the function's own
            // meaning on our H2 (h2Extension.pure:29 picks the upgraded
            // golden on every version but 1.4.200) IS assertEquals(upgraded,
            // actual) — adjudicated as that verdict, never as a standing
            // call for some other route to catch
            if (hroot.args().size() == 3) {
                TypedSpec asEquals = com.legend.compiler.spec.VerdictQueries
                        .assertEqualsOf(hroot.args().get(1), hroot.args().get(2), specs);
                if (asEquals != null) {
                    return adjudicate(asEquals, letPrefix, specs, env, rawHook);
                }
            }
        }
        // THE GRID VERDICT (Clause 2c — TdsCompare's chartered route;
        // witness: the relation suite's 79 assertTdsEquivalent rows):
        // both relations execute IN THE DATABASE, the cell-zip
        // adjudicates host-side (tdsEquivalent.pure's numeric delta +
        // temporal seconds policies, already the one owner).
        // The family is the closed type NativeFn.Verdict: a member not in it
        // (assertError with its own arm, fail) is a normal fall-through —
        // and the switch below is an EXPRESSION with no default, so a new
        // member does not compile until it is placed (batch 3).
        NativeFn.Verdict fn = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn == null) {
            return null;
        }
        String name = fn.bareName();
        com.legend.exec.CanonicalDivergence.sqlFamily(name);   // leg 3.0 census
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(
                (bare instanceof TypedUserCall u) ? u.args() : ((TypedNativeCall) bare).args(),
                letPrefix, specs);
        ExecutionResult adjudicated = switch (fn) {
            case ASSERT_TDS_EQUIVALENT -> {
            List<TypedSpec> targs = ((bare instanceof TypedUserCall u2)
                    ? u2.args() : ((TypedNativeCall) bare).args());
            if (targs.size() < 3 || targs.size() > 4) {
                yield null;
            }
            yield arm.tdsEquivalent(name, targs, letPrefix, specs, env, hook);
            }
            // toCSV is an OPERAND form (a rendered grid text the verdict
            // compares), never an assert of its own
            case TO_CSV -> {
                yield null;
            }
            case ASSERT_EQUALS, ASSERT_NOT_EQUALS -> {
                if (args.size() < 2) {
                    yield null;
                }
                boolean wantEqual = fn == NativeFn.Verdict.ASSERT_EQUALS;
                // SQLTEXT charter slice 3a — the SQL-TEXT arm: a
                // toSQLString producer in an argument tree judges on
                // ROWS (SqlTextVerdicts; text is a census number).
                // Shapes outside the exact cohort yield null here.
                ExecutionResult sv = SqlTextVerdicts.tryArm(name,
                        wantEqual, args, letPrefix, specs, env, hook);
                if (sv != null) {
                    com.legend.exec.CanonicalDivergence.sqlRoute(name, "sql-text");
                    yield sv;
                }
                // D3 — the RENDERED-TEXT arm: exactly one side is a
                // DB-rendered grid text (toCSV/toString/replace/join
                // spellings), the peer a string value. The DATABASE
                // computed the render; TdsCompare.renderedText (the
                // one policy owner, R1b-probed) judges the texts.
                ExecutionResult ra = renderedArm(name, wantEqual, args,
                        letPrefix, specs, env, hook, true);
                if (ra != null) {
                    com.legend.exec.CanonicalDivergence.sqlRoute(name, "rendered-text");
                    yield ra;
                }
                // a bare no-key sort() over a FLAT-CELLS side
                // (`$result.values.rows.values->sort()` — the strictdate
                // testProject idiom, a mixed Integer/StrictDate pool):
                // the assert compares the pools under ONE total order,
                // i.e. cell-multiset equality — the flat-cells multiset
                // verdict (both channels judge order-insensitively);
                // sorting a mixed-type cell pool is never a SQL column
                TypedSpec cellsE = bareSortOverCells(args.get(0));
                TypedSpec cellsA = bareSortOverCells(args.get(1));
                if (wantEqual && (cellsE != null || cellsA != null)) {
                    // the sorted flat-cells idiom IS the cell-pool multiset
                    ExecutionResult pool = arm.cellPool(name, cellsE != null ? cellsE : args.get(0),
                            cellsA != null ? cellsA : args.get(1), letPrefix, specs, env, hook);
                    if (pool != null) {
                        yield pool;
                    }
                }
                // D3 — the GRID-PAIR arm: both sides statically
                // relation-stamped execute as grids; the grid owner
                // (TdsCompare.grids: columns ordered, rows under the
                // order policy) judges — never a byte decline.
                if (com.legend.compiler.element.type.Type.isRelation(
                        args.get(0).info().type())
                        && com.legend.compiler.element.type.Type.isRelation(
                                args.get(1).info().type())) {
                    ExecutionResult.Tabular te = tabular(args.get(0),
                            letPrefix, specs, env, hook);
                    ExecutionResult.Tabular ta = tabular(args.get(1),
                            letPrefix, specs, env, hook);
                    if (te == null || ta == null) {
                        throw new com.legend.error.NotImplementedException(
                                "relation-stamped assert side executed"
                                        + " to a non-tabular result");
                    }
                    boolean held = com.legend.exec.TdsCompare.grids(te,
                            ta, com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                                    == com.legend.compiler.spec.OrderView.SORTED);
                    com.legend.exec.CanonicalDivergence.sqlRoute(name, "grid-pair");
                    if (held != wantEqual) {
                        yield fail(name + ":\n" + HostJudge.summarize(te)
                                + "\n does not match:\n" + HostJudge.summarize(ta));
                    }
                    yield ok();
                }
                // D3 — the ORDER VIEW: an INCIDENTAL-order actual side
                // (unsorted store read / frame read) has SQL arrival
                // order — engine goldens encode H2's, ours is DuckDB's
                // — so both sides fetch with the CANONICAL-order riders
                // and the judgment is order-insensitive (exactly the
                // assertSameElements shape). SORTED/DEFINED sides stay
                // strictly ordered.
                boolean incidental = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL;
                // §8 leg 1 — grid-ness is STATIC (the declared result
                // shape, decided before execution — the ratified
                // no-runtime-sniffing rule): a grid pair fetches in
                // DEFINITION order (the peer's row chunking depends on
                // it; the canonical-order rider would destroy it) and
                // any multiset view sorts DB-computed canon texts
                // host-side instead — semantics-free string sorting.
                boolean gridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                yield arm.equals(name, wantEqual, args, incidental, gridPair, letPrefix, specs, env, hook);
            }
            case ASSERT_SAME_ELEMENTS -> {
                if (args.size() < 2) {
                    yield null;
                }
                // D3 — rendered-text sides (a sep-joined grid string
                // vs its golden): the token/line multiset judges
                ExecutionResult rse = renderedArm(name, true, args,
                        letPrefix, specs, env, hook, false);
                if (rse != null) {
                    com.legend.exec.CanonicalDivergence.sqlRoute(name, "rendered-text");
                    yield rse;
                }
                boolean seGridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                yield arm.sameElements(name, args, seGridPair, letPrefix, specs, env, hook);
            }
            case ASSERT_SIZE -> {
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.size(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_JSON_STRINGS_EQUAL -> {
                // D4 — the JSON verdict: engine semantics (object keys
                // order-INSENSITIVE, arrays order-SENSITIVE) over
                // PARSED structures; JsonCompare is the one tree owner
                // (V3 register). Sides are DB-computed strings.
                if (args.size() != 2) {
                    yield null;
                }
                yield arm.jsonStringsEqual(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_CONTAINS -> {
                // real pure membership (assertContains.pure): both
                // sides DB-computed, the lattice judges element
                // equality; message args are failure-position only
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.contains(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EQ -> {
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.eq(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EQ_WITHIN_TOLERANCE -> {
                if (args.size() < 3) {
                    yield null;
                }
                yield arm.tolerance(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT, ASSERT_FALSE -> {
                if (args.isEmpty()) {
                    yield null;
                }
                yield arm.condition(name, args.get(0), fn == NativeFn.Verdict.ASSERT,
                        letPrefix, specs, env, hook);
            }
            case ASSERT_INSTANCE_OF -> {
                if (args.size() < 2) {
                    yield null;
                }
                // the /3 message overload has no witness — fall through
                if (args.size() != 2) {
                    yield null;
                }
                yield arm.instanceOf(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_IS -> {
                // is() = IDENTITY (real pure is.pure:23, PCT.platformOnly).
                // World-1 adjudication for statically-identified operands
                // only; message overloads have no witness — fall through.
                if (args.size() != 2) {
                    yield null;
                }
                ExecutionResult isv = isVerdict(args.get(0), args.get(1));
                if (isv != null) {
                    arm.staticallyDecided(name);
                    yield isv;
                }
                yield arm.is(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EMPTY, ASSERT_NOT_EMPTY -> {
                if (args.isEmpty()) {
                    yield null;
                }
                yield arm.empty(name, args.get(0), fn == NativeFn.Verdict.ASSERT_EMPTY,
                        letPrefix, specs, env, hook);
            }
        };
        if (adjudicated == null) {
            com.legend.exec.CanonicalDivergence.sqlFellThrough();
        }
        return adjudicated;
    }

    /** A side typed as a SEEDED metaclass (its extent is in the system
     *  database — a Database, a Mapping, a Class …): its identity is a row key. */
    static boolean elementTyped(TypedSpec s, SpecCompiler specs) {
        if (!(peel(s).info().type() instanceof com.legend.compiler.element.type.Type.ClassType ct)) {
            return false;
        }
        // the seeded metaclass itself, or a SUPERTYPE a system function
        // answers with (resolveStore returns Store; the rows are Databases):
        // some system-mapped, seeded metaclass conforms to the side's type
        var ctx = specs.ctx();
        if (ctx.tracksClassifier(ct.fqn())) {
            return true;
        }
        var sys = ctx.findMapping(com.legend.builtin.SystemMetamodel.MAPPING_FQN).orElse(null);
        return sys != null && sys.classBindings().stream().anyMatch(cb ->
                ctx.tracksClassifier(cb.classFqn())
                        && ctx.isSubtype(cb.classFqn(), ct.fqn()));
    }

    /** The IDENTITY verdict ({@code assertIs} → {@code is()}, real pure
     * is.pure:23 "pointer equality"): adjudicable in World 1 ONLY when
     * both operands are STATICALLY identified — a type reference (bare
     * element, {@code type(x)->toOne()}, {@code genericType(x).rawType})
     * or the same let-bound instance by construction provenance. Any
     * other shape returns null: the legacy path then walls loudly on
     * {@code is}'s missing SQL rule — a wire carries values, never
     * reference identity (the eq/equalNonPrimitive irreducible ruling). */
    static @com.legend.base.Nullable ExecutionResult isVerdict(
            TypedSpec left, TypedSpec right) {
        String lt = typeIdentityOf(left);
        String rt = typeIdentityOf(right);
        if (lt != null && rt != null) {
            return lt.equals(rt) ? ok()
                    : fail("\nexpected: " + lt + "\nactual:   " + rt);
        }
        TypedSpec l = instanceOrigin(left);
        TypedSpec r = instanceOrigin(right);
        if (l instanceof com.legend.compiler.spec.typed.TypedVariable lv
                && r instanceof com.legend.compiler.spec.typed.TypedVariable rv
                && lv.name().equals(rv.name())) {
            // the same let-bound variable in one frame IS the same object
            return ok();
        }
        return null;
    }

    /** The statically-known TYPE a value expression identifies, or null.
     * {@code type()}/{@code genericType().rawType} resolve to the STATIC
     * type of their argument — sound exactly when that type is concrete
     * (a literal or constructed instance), which is what the witnesses
     * pass ({@code type(+1)}, {@code genericType(^LA_Person(...))}). */
    private static @com.legend.base.Nullable String typeIdentityOf(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr) {
            return canonicalTypeFqn(pr.fullPath());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedTypeRef tr) {
            return canonicalTypeFqn(tr.target().typeName());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::meta::type")
                && !c.args().isEmpty()) {
            return staticTypeName(c.args().get(0));
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("rawType")
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNativeCall gt
                && com.legend.builtin.NativeFn.SubtypeForm.of(gt.callee().id()).orElse(null) == com.legend.builtin.NativeFn.SubtypeForm.GENERIC_TYPE
                && !gt.args().isEmpty()) {
            return staticTypeName(gt.args().get(0));
        }
        return null;
    }

    private static @com.legend.base.Nullable String staticTypeName(TypedSpec arg) {
        // concrete static identification only: a literal's primitive or a
        // constructed/class-typed value — never an Any/generic stamp
        var ty = peel(arg).info().type();
        if (ty instanceof com.legend.compiler.element.type.Type.ClassType ct) {
            return ct.fqn();
        }
        String n = ty.typeName();
        return switch (n) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime" ->
                    canonicalTypeFqn(n);
            default -> null;
        };
    }

    /** ONE spelling for a type identity: PRIMITIVES canonicalize to their
     * M3 FQN so all three resolution arms agree (bare {@code Integer},
     * {@code @Integer}, and {@code type(1)} name the same element).
     * Anything else — including packageless user test classes — keeps
     * its resolved spelling untouched. */
    private static String canonicalTypeFqn(String name) {
        return switch (name) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime", "Number" ->
                    "meta::pure::metamodel::type::" + name;
            default -> name;
        };
    }

    /** Peel value-preserving wrappers ({@code toOne}) and fold a property
     * read over a constructed instance to its constructor argument — the
     * provenance chain the OneToOne witness rides. */
    private static TypedSpec peel(TypedSpec t) {
        TypedSpec s = t;
        while (true) {
            if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                    && !c.args().isEmpty()) {
                s = c.args().get(0);
                continue;
            }
            return s;
        }
    }

    private static TypedSpec instanceOrigin(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNewInstance ni
                && ni.properties().get(pa.property()) != null) {
            return instanceOrigin(ni.properties().get(pa.property()));
        }
        return s;
    }

    /** The QUANTIFIED verdict: {@code xs->map(f|assert(pred[, 'msg']))}
     * at a statement root. The predicate VECTORIZES in the database
     * ({@code xs->map(f|pred)} — pure data computation); the boolean
     * vector is judged here, first failure raising the assert's message
     * — the interpreter's per-element semantics without an interpreter.
     * Null = not a quantified assert (generic path continues); shapes
     * beyond assert/assertFalse with a literal-or-absent message decline
     * LOUDLY. */
    /** A quantified assert over a LITERAL collection whose lambda carries
     * lets or a non-boolean assert ({@code [pair(H2, sql), ...]->map(p|
     * let driver = $p.first; ...; assertEquals($expectedSql, $result,
     * fmt, args))}): UNROLLED — each element binds the parameter as a let
     * ahead of the lambda's own lets, the inliner reduces the lets (the
     * one substitution engine), and the final assert statement
     * adjudicates as a statement-root verdict. All elements must hold.
     * Null when not this shape (a runtime collection, a one-statement
     * predicate lambda — the vector form). */
    private static @com.legend.base.Nullable ExecutionResult unrolled(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        var lam = qm.mapper();
        // the collection through the caller's lets (let expected = [...])
        TypedSpec source = com.legend.compiler.spec.typed.Lets.bound(qm.source(), letPrefix);
        // a property read over a LET-BOUND instance literal is that field
        // ($_s2_hoisted.columnValuePairs — the hoisted constructor's zip):
        // the one rule Pipelines.instanceLiteralProp spells, through the lets
        if (source instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && com.legend.compiler.spec.typed.Lets.bound(pa.source(), letPrefix)
                        instanceof com.legend.compiler.spec.typed.TypedNewInstance inst
                && inst.properties().get(pa.property()) != null) {
            source = java.util.Objects.requireNonNull(inst.properties().get(pa.property()));
        }
        // a COMPUTED-but-spelled collection (DatabaseType->enumValues()
        // ->filter(e | $e->in([...]))) reduces through the one substitution
        // engine to its literal elements before the shape is judged
        if (!(source instanceof com.legend.compiler.spec.typed.TypedCollection
                || source instanceof com.legend.compiler.spec.typed.TypedNewInstance
                || source instanceof TypedNativeCall)) {
            TypedSpec last = com.legend.compiler.spec.UserCallInliner
                    .forVerdictSource(specs, rawHook).reduceVerdictSource(source, letPrefix);
            if (last instanceof com.legend.compiler.spec.typed.TypedCollection) {
                source = last;
            }
        }
        if (lam.parameters().size() != 1 || lam.body().isEmpty()) {
            return null;
        }
        TypedSpec root = lam.body().get(lam.body().size() - 1);
        String fqn = calleeFqn(root);
        // a NESTED quantification (ids->map(i | pairs->map(cv |
        // assert(...)))) — the engine's guard idiom in
        // createTableRowIdentifiers: each outer element's inner map is
        // itself a quantified assert, adjudicated recursively
        boolean nestedQuantified = root instanceof com.legend.compiler.spec.typed.TypedMap
                || com.legend.compiler.spec.VerdictQueries.forAllAsQuantified(root) != null;
        if (!nestedQuantified && (fqn == null || !fqn.startsWith(PKG))) {
            return null;
        }
        // the VECTOR form (quantified) raises the message host-side, so it
        // needs a literal one; a COMPUTED message ('Table : ' + $table->
        // getQualifiedTableName() + ...) rides each element through the
        // unroll instead, where the statement-root assert arm judges the
        // condition (the message is diagnostic, never the verdict)
        boolean literalMessage = true;
        if (root instanceof TypedUserCall ru && ru.args().size() >= 2) {
            literalMessage = ru.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString;
        } else if (root instanceof TypedNativeCall rn && rn.args().size() >= 2) {
            literalMessage = rn.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString;
        }
        // THE VECTOR CONTRACT (VerdictQueries.vectorContract, 2026-09-22): a per-element assert
        // is the predicate it means; over a row source, row-local, with a literal message, the
        // vector is planned and judged in the fused statement (zip = a join on the row number,
        // forAll = no failing row). The unroll below — which FETCHES a zip arm's cells into
        // Java to spell them as literals — stays the road for everything else.
        if (!nestedQuantified && lam.body().size() == 1 && fqn != null) {
            TypedSpec pred = com.legend.compiler.spec.VerdictQueries.assertAsPredicate(root, specs);
            if (pred != null && com.legend.compiler.spec.VerdictQueries.vectorContract(source, lam, root, pred)) {
                TypedSpec predMap = com.legend.compiler.spec.VerdictQueries.predicateVectorOver(source, qm, lam, pred);
                ExecutionResult planned = arm(env).quantifiedVector(
                        NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).map(NativeFn.Verdict::bareName).orElse(fqn), predMap, letPrefix, specs, env,
                        rawHook == null ? null : rawHook::apply);
                if (planned != null) {
                    return planned;
                }
            }
        }
        boolean simplePredicate = !nestedQuantified && fqn != null && lam.body().size() == 1
                && (fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT)
                        || fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT_FALSE))
                && literalMessage;
        if (simplePredicate) {
            return null;
        }
        // the elements are compiler-owned SYNTHESIS (VerdictQueries,
        // Invariant 7); a zip arm's values come from the database
        SpliceHook fetchHook = rawHook == null ? null : rawHook::apply;
        List<TypedSpec> elements = com.legend.compiler.spec.VerdictQueries
                .unrollElements(source, letPrefix, env.ctx(),
                        arm -> HostJudge.sideCells(arm, letPrefix, specs, env, fetchHook));
        if (elements == null) {
            return null;
        }
        ExecutionResult last = null;
        for (TypedSpec element : elements) {
            List<TypedSpec> reduced = com.legend.compiler.spec.VerdictQueries
                    .unrolledElement(specs, letPrefix, lam, element, rawHook);
            List<TypedSpec> lets = new java.util.ArrayList<>(
                    reduced.subList(0, reduced.size() - 1));
            TypedSpec bareStmt = reduced.get(reduced.size() - 1);
            ExecutionResult v = adjudicate(bareStmt, lets, specs, env, rawHook);
            if (v == null) {
                throw new com.legend.error.NotImplementedException(
                        "unrolled quantified assert: element verdict not"
                        + " adjudicable for " + calleeFqn(bareStmt));
            }
            last = v;
        }
        return last == null ? ok() : last;
    }

    private static @com.legend.base.Nullable ExecutionResult quantified(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook) {
        var lam = qm.mapper();
        if (lam.body().size() != 1) {
            return null;
        }
        TypedSpec root = lam.body().get(0);
        String fqn = calleeFqn(root);
        if (fqn == null || !fqn.startsWith(PKG)) {
            return null;
        }
        NativeFn.Verdict qfn = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
        List<TypedSpec> aargs = root instanceof TypedUserCall u ? u.args()
                : ((TypedNativeCall) root).args();
        if (!(qfn == NativeFn.Verdict.ASSERT || qfn == NativeFn.Verdict.ASSERT_FALSE)
                || aargs.isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: only map(f|assert/"
                    + "assertFalse(pred[, message])) is modeled — got '"
                    + fqn + "'/" + aargs.size());
        }
        String msg = aargs.size() >= 2
                && aargs.get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString cs
                ? cs.value() : "Assert failed";
        if (aargs.size() >= 2 && !(aargs.get(1) instanceof
                com.legend.compiler.spec.typed.TypedCString)) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: non-literal message"
                    + " expressions are not modeled");
        }
        // the predicate vector, computed in the database — SYNTHESIS is
        // compiler-owned (VerdictQueries, Invariant 7); the judgment
        // below stays host-side (Clause 2c)
        TypedSpec predMap = com.legend.compiler.spec.VerdictQueries
                .predicateVector(qm, lam, aargs.get(0));
        return arm(env).quantified(fqn, predMap, qfn == NativeFn.Verdict.ASSERT, msg, letPrefix, specs, env, hook);
    }

    /** D1 (V7_ARCH_AUDIT 2026-08-28) — THE ONE dual-verdict finisher:
     * the census probe, the verdict of record, and the failure
     * narrative all derive from a single judgment. {@code byteHeld}
     * null = the byte channel declined (already counted); the host
     * lattice judges. {@code hostMessage} is consulted ONLY when the
     * host lattice failed, and must speak then — a silent host failure
     * is a verdict/message divergence and THROWS (the reverted
     * flat-cells attempt's 28-row phantom, made structurally
     * impossible: no arm can print the byte-divergence text for a
     * judgment the byte channel never made, because the probe and the
     * message read the same two booleans). */
    /** The run's judge mode rides its options (ExecuteOptions.JudgeMode):
     * one mode per run, read from the execution environment — never a JVM
     * global. HOST is the verdict of record; DATABASE plans both sides and
     * lets the database return the verdict row. */
    static boolean databaseMode(StatementExecutor.ExecEnv env) {
        return env.options().judgeMode() == ExecuteOptions.JudgeMode.DATABASE;
    }

    /** THE ONE DISPATCH (cleanup move 2b, 2026-09-21): the run's judge mode names the
     * arm; the router classifies and hands the sides over, nowhere else forks. */
    static VerdictArm arm(StatementExecutor.ExecEnv env) {
        return databaseMode(env) ? DatabaseJudge.ARM : HostJudge.ARM;
    }

    /** DATABASE mode (leg 3.1, docs/DATABASE_MODE_HOMEWORK §4b): both
     * sides are PLANNED (never executed on their own), composed by
     * {@link com.legend.lowering.VerdictSql} into ONE statement, and the
     * database returns the verdict row: {@code __verdict} (never NULL),
     * the two framed canons as the evidence, and {@code __unjudged} when
     * the statement could not decide. A side the wrap declines, a
     * non-SQL side, a grid side (3.1b), a multi-candidate Number side or
     * an enum on the literal channel is UNJUDGED — the assert FAILS with
     * the reason and the census counts it; no host judgment is consulted. */
    /** A grid side's statement facts from its schema: the declared-Float
     * columns (the 2-ULP leniency) and, under toCSV's grammar, the String
     * columns whose empty cell and NULL print alike. */
    /** A grid side's schema by EFFECTIVE kind (a Number over a DOUBLE wire is a Float). */
    static com.legend.compiler.element.type.Type.@com.legend.base.Nullable RelationType effectiveSchema(
            StatementExecutor.WrappedSide side) {
        var d = com.legend.compiler.element.type.Type.schemaView(side.shapeInfo().type());
        return d == null || d.columns().size() > side.plan().outputs().size() ? d : com.legend.compiler
                .spec.VerdictQueries.wireDecidedKinds(d, side.plan().outputs().subList(0, d.columns().size()));
    }

    /** A verdict row's evidence column for a message: at most 600
     * characters (a grid text drowns the diagnosis past that). */
    static String excerpt(@com.legend.base.Nullable Object evidence) {
        String s = String.valueOf(evidence);
        return s.length() <= 600 ? s : s.substring(0, 600) + "…(" + s.length() + " chars)";
    }

    /** {@code sort(<flat cells>)} — a one-argument collection sort over
     * a statically table-shaped side; the cells, or null. */
    static @com.legend.base.Nullable TypedSpec bareSortOverCells(TypedSpec s) {
        if (s instanceof TypedNativeCall c
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::sort")
                && c.args().size() == 1
                && tabularShaped(c.args().get(0))) {
            return c.args().get(0);
        }
        return null;
    }

    /** A side's per-ROW canon texts via the grid policy owner: a
     * grid side reads its harvested row canons; a value peer frames
     * rows from its literal-channel element canons ({@link
     * com.legend.exec.TdsCompare} owns every rule and decline). */
    static @com.legend.base.Nullable List<String> sideRowCanons(
            HostJudge.SideFetch side, int width, boolean isExpected) {
        return side.grid() != null
                ? com.legend.exec.TdsCompare.tdsRowCanons(side.rider())
                : com.legend.exec.TdsCompare.peerRowCanons(side.rider(),
                        side.values().size(), width, isExpected);
    }

    /** A side's per-CELL canon texts (the sameElements view), via the
     * grid policy owner. */
    static @com.legend.base.Nullable List<String> sideCellCanons(
            HostJudge.SideFetch side, boolean isExpected) {
        return side.grid() != null
                ? com.legend.exec.TdsCompare.tdsCellCanons(side.rider())
                : com.legend.exec.TdsCompare.peerElementCanons(
                        side.rider(), side.values().size(), isExpected);
    }

    /** Whether a side is STATICALLY table-shaped (its declared result
     * shape — the same fact the executor's canon routing reads). */
    static boolean tabularShaped(TypedSpec s) {
        return com.legend.exec.ResultShape.of(s)
                == com.legend.exec.ResultShape.TABULAR;
    }

    /** A bare {@code .rows} view stamp (row collection — bare struct,
     * many multiplicity), through let bindings. */
    static boolean bareRowStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        return s.info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType
                && s.info().multiplicity().isMany();
    }

    /** A wrapped table stamp ({@code Relation<schema>}), through let
     * bindings. */
    static boolean wrappedRelationStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        return com.legend.compiler.element.type.Type.isRelation(
                chaseLets(s0, lets).info().type());
    }

    // ── D3 (batch-2 slice 2): the GOLDEN GRID/ORDER conventions move
    // into verdict construction — the ORDER VIEW of a side, the
    // rendered-text forms, and the grid-pair route. The comparison
    // POLICIES stay with their one production owner (TdsCompare).



    /** Is the serialized query's ROOT many-valued? Read from the typed
     * chain (through lets and the envelope splice) at its serialize node —
     * the engine prints a one-element result of a many-valued root as a
     * bare object, so a golden written bare stands for one element. */
    static boolean serializedRootMany(TypedSpec s, List<TypedSpec> lets,
            @com.legend.base.Nullable SpliceHook hook) {
        TypedSpec chain = chaseLets(s, lets);
        if (chain instanceof TypedNativeCall lq
                && com.legend.builtin.NativeFn.Handle.of(lq.callee().id()).orElse(null)
                        == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY) {
            // executeLegendQuery's result IS the envelope ({"builder":…,
            // "values":…}) — always one object; the root's many-ness lives
            // inside "values", the bare-object rule never applies
            return false;
        }
        if (chain instanceof com.legend.compiler.spec.typed.TypedVariable v && hook != null) {
            TypedSpec read = com.legend.compiler.spec.VerdictQueries.valuesRead(v);
            TypedSpec spliced = hook.apply(read, java.util.Set.of());
            if (spliced != read) {
                chain = spliced;
            }
        } else if (hook != null) {
            chain = hook.apply(chain, java.util.Set.of());
        }
        com.legend.compiler.spec.typed.TypedSerialize ser = findSerialize(chain);
        return ser != null && ser.source().info().multiplicity().isMany();
    }

    /** Is the planned side a RESULT ENVELOPE — its root projection a JSON
     * object carrying the engine's {@code builder} key? A plan fact (the
     * envelope is built by JsonEmission.result), read off the IR. */
    static boolean planIsEnvelope(com.legend.sql.SqlQuery plan) {
        // the planned side is the canon WRAP over the value select over the
        // document's own select: descend while the root projection is a
        // plain column read over a subselect
        com.legend.sql.SqlQuery q = plan;
        for (int depth = 0; depth < 4 && q instanceof com.legend.sql.SqlSelect s
                && !s.projections().isEmpty(); depth++) {
            com.legend.sql.SqlExpr root = s.projections().get(0).expr();
            while (root instanceof com.legend.sql.SqlExpr.Cast c) {
                root = c.value();
            }
            if (root instanceof com.legend.sql.SqlExpr.JsonObject j) {
                for (int i = 0; i + 1 < j.kv().size(); i += 2) {
                    if (j.kv().get(i) instanceof com.legend.sql.SqlExpr.StringLit k
                            && k.value().equals("builder")) {
                        return true;
                    }
                }
                return false;
            }
            // executeLegendQuery's envelope is spelled as a CONCAT whose first
            // piece is the literal '{"builder":…' (JsonEmission.result)
            if (root instanceof com.legend.sql.SqlExpr.Call call
                    && (call.fn() == com.legend.sql.SqlFn.CONCAT
                            || call.fn() == com.legend.sql.SqlFn.CONCAT_JOIN)
                    && !call.args().isEmpty()
                    && call.args().get(0) instanceof com.legend.sql.SqlExpr.StringLit first) {
                return first.value().startsWith("{\"builder\":");
            }
            if (root instanceof com.legend.sql.SqlExpr.Column
                    && s.from() instanceof com.legend.sql.SqlSource.Subselect sub) {
                q = sub.inner();
                continue;
            }
            return false;
        }
        return false;
    }

    private static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSerialize findSerialize(
            TypedSpec s) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSerialize ts) {
            return ts;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedJsonResult) {
            // a RESULT ENVELOPE ({"builder":…,"values":…}) is always one
            // object — the root's many-ness lives inside "values"; the
            // bare-object rule is the serialize document's alone
            return null;
        }
        if (s instanceof TypedNativeCall c && !c.args().isEmpty()
                && c.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam
                && !lam.body().isEmpty()) {
            var found = findSerialize(lam.body().get(lam.body().size() - 1));
            if (found != null) {
                return found;
            }
        }
        for (TypedSpec ch : s.children()) {
            var found = findSerialize(ch);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** The referee's gates for a verified chain, derived ONCE from the
     * typed chain by the arm that owns the verdict (Phase 0.5): the
     * extent-subset fact, the ORDER VIEW (ends in a sort = order is
     * contract), and the tail-most sort's keys. Typed-tree navigation
     * only — nothing evaluated. */
    static com.legend.exec.SqlReplayOracle.ReplayFacts replayFacts(
            TypedSpec chain, List<TypedSpec> letPrefix) {
        boolean ordered = com.legend.compiler.spec.OrderView.of(chain, letPrefix) == com.legend.compiler.spec.OrderView.SORTED;
        return new com.legend.exec.SqlReplayOracle.ReplayFacts(
                com.legend.compiler.spec.VerdictQueries.extentSubset(chain),
                ordered,
                ordered ? sortKeys(chain, letPrefix, new java.util.HashSet<>()) : null);
    }

    /** The rows read WITHOUT its tail page (batch 0.5b): the same read with
     * the first page node reached through order-preserving tails
     * ({@code TypedLimit} / {@code TypedDrop} / {@code TypedSlice}, the
     * typed forms of limit / take / drop / slice) replaced by its source —
     * OUR unpaged population for the referee's page-membership verdict.
     * Null when the chain carries no page at its tail. Typed-tree
     * navigation and rebuild ({@code withChildren}), nothing evaluated. */
    static @com.legend.base.Nullable TypedSpec unpagedRead(TypedSpec read) {
        if (read instanceof com.legend.compiler.spec.typed.TypedLimit l) {
            return l.source();
        }
        if (read instanceof com.legend.compiler.spec.typed.TypedDrop d) {
            return d.source();
        }
        if (read instanceof com.legend.compiler.spec.typed.TypedSlice sl) {
            return sl.source();
        }
        boolean wrapper = read instanceof com.legend.compiler.spec.typed.TypedFrom
                || read instanceof com.legend.compiler.spec.typed.TypedFilter
                || read instanceof com.legend.compiler.spec.typed.TypedProject
                || read instanceof com.legend.compiler.spec.typed.TypedSelect
                || read instanceof com.legend.compiler.spec.typed.TypedRename
                || read instanceof com.legend.compiler.spec.typed.TypedDistinct
                || read instanceof com.legend.compiler.spec.typed.TypedSort
                || read instanceof com.legend.compiler.spec.typed.TypedSortBy
                || read instanceof com.legend.compiler.spec.typed.TypedMap
                || read instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                || read instanceof com.legend.compiler.spec.typed.TypedCast
                || read instanceof com.legend.compiler.spec.typed.TypedNavigate;
        if (read instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            wrapper = com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn) && !c.args().isEmpty();
        }
        if (!wrapper || read.children().isEmpty()) {
            return null;
        }
        TypedSpec inner = unpagedRead(read.children().get(0));
        if (inner == null) {
            return null;
        }
        List<TypedSpec> kids = new java.util.ArrayList<>(read.children());
        kids.set(0, inner);
        return read.withChildren(kids);
    }

    /** The key names of the sort NEAREST THE TAIL (the engine's own
     * last-sort-wins semantics), through the same order-preserving tails
     * {@link #orderView} descends; null = underivable (a computed key,
     * a native sort spelling, a value the compared output cannot carry). */
    private static @com.legend.base.Nullable List<String> sortKeys(TypedSpec s,
            List<TypedSpec> lets, java.util.Set<String> seen) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSort so) {
            List<String> keys = new java.util.ArrayList<>();
            for (var k : so.keys()) {
                keys.add(k.column());
            }
            return keys.isEmpty() ? null : keys;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedSortBy sb) {
            if (sb.keyAlias() != null) {
                return List.of(sb.keyAlias());
            }
            List<TypedSpec> body = sb.key().body();
            return body.size() == 1 && body.get(0)
                    instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    ? List.of(pa.property()) : null;
        }
        if (s instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            if (com.legend.compiler.spec.OrderView.EXECUTE_FRAMES.contains(fqn) && !c.args().isEmpty()
                    && c.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam
                    && !lam.body().isEmpty()) {
                return sortKeys(lam.body().get(lam.body().size() - 1), lets, seen);
            }
            return com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn)
                    && !c.args().isEmpty() ? sortKeys(c.args().get(0), lets, seen) : null;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!seen.add(v.name())) {
                return null;
            }
            com.legend.compiler.spec.typed.TypedLet l =
                    com.legend.compiler.spec.typed.Lets.binding(lets, v.name());
            return l == null ? null : sortKeys(l.value(), lets, seen);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedFilter
                || s instanceof com.legend.compiler.spec.typed.TypedProject
                || s instanceof com.legend.compiler.spec.typed.TypedSelect
                || s instanceof com.legend.compiler.spec.typed.TypedRename
                || s instanceof com.legend.compiler.spec.typed.TypedDistinct
                || s instanceof com.legend.compiler.spec.typed.TypedLimit
                || s instanceof com.legend.compiler.spec.typed.TypedDrop
                || s instanceof com.legend.compiler.spec.typed.TypedSlice
                || s instanceof com.legend.compiler.spec.typed.TypedMap
                || s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                || s instanceof com.legend.compiler.spec.typed.TypedCast
                || s instanceof com.legend.compiler.spec.typed.TypedFrom
                || s instanceof com.legend.compiler.spec.typed.TypedNavigate
                || s instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess) {
            List<TypedSpec> ch = s.children();
            return ch.isEmpty() ? null : sortKeys(ch.get(0), lets, seen);
        }
        return null;
    }

    private static final String FQ_TO_STRING =
            com.legend.compiler.element.type.PlatformTypes.TO_STRING;

    private static final String FQ_REPLACE =
            com.legend.compiler.element.type.PlatformTypes.STRING_REPLACE;

    private static final String FQ_MAKE_STRING =
            com.legend.compiler.element.type.PlatformTypes.STRING_MAKE_STRING;

    private static final String FQ_JOIN_STRINGS =
            com.legend.compiler.element.type.PlatformTypes.STRING_JOIN_STRINGS;

    static TypedSpec chaseLets(TypedSpec s0, List<TypedSpec> lets) {
        TypedSpec s = s0;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (s instanceof com.legend.compiler.spec.typed.TypedVariable v
                && seen.add(v.name())) {
            TypedSpec bound = null;
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof
                        com.legend.compiler.spec.typed.TypedLet l
                        && l.name().equals(v.name())) {
                    bound = l.value();
                    break;
                }
            }
            if (bound == null) {
                return s;
            }
            s = bound;
        }
        return s;
    }

    /** The D3 RENDERED-TEXT verdict, or null when the pair is not the
     * shape (exactly one side a render form, both sides one string).
     * {@code orderedForm} false = the sameElements view (token/line
     * multiset regardless of the chain's sort). */
    static @com.legend.base.Nullable ExecutionResult renderedArm(
            String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook, boolean orderedForm) {
        String eForm = renderForm(args.get(0), letPrefix);
        String aForm = renderForm(args.get(1), letPrefix);
        // BOTH-RENDERED same-form pairs qualify too (two renders of one
        // unsorted query — pure guarantees the row MULTISET; each
        // render freezes its own execution's incident order, so a byte
        // compare was a coin flip: union testProjectThroughAsso's
        // toCSV-vs-toCSV flicker). Mismatched forms fall through.
        if ((eForm == null && aForm == null)
                || (eForm != null && aForm != null
                        && !eForm.equals(aForm))) {
            return null;
        }
        String form = aForm != null ? aForm
                : java.util.Objects.requireNonNull(eForm);
        TypedSpec rendered = aForm != null ? args.get(1) : args.get(0);
        return arm(env).rendered(name, wantEqual, args, form, rendered, eForm, aForm, orderedForm,
                        letPrefix, specs, env, hook);
    }

    /** One-line first-difference sketch of two rendered texts (failure
     * message position — the full texts drown the diagnosis). */
    static String firstTextDiff(String e, String a) {
        String[] el = e.split("\n", -1);
        String[] al = a.split("\n", -1);
        if (el.length != al.length) {
            return "line-count " + el.length + " != " + al.length
                    + " (expected first line: " + el[0] + ")";
        }
        for (int i = 0; i < el.length; i++) {
            if (!el[i].equals(al[i])) {
                return "line " + i + ": expected <" + el[i] + "> got <"
                        + al[i] + ">";
            }
        }
        return "texts differ only in leniency-adjudicated cells";
    }

    /** Database mode's rendered-text road (bucket 8): both sides rendered →
     * the two values as a multiset (two executions of one query); one side
     * rendered → its value against the golden brought to rows, ordered only
     * when the chain ends in a sort and the assert is ordered. */
    static ExecutionResult renderedValueVerdict(String name, boolean wantEqual,
            List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, @com.legend.base.Nullable SpliceHook hook,
            boolean orderedForm) {
        java.util.function.UnaryOperator<TypedSpec> chase = s -> chaseLets(s, letPrefix);
        var re = com.legend.compiler.spec.VerdictQueries.renderedSide(args.get(0), chase);
        var ra = com.legend.compiler.spec.VerdictQueries.renderedSide(args.get(1), chase);
        if (re != null && ra != null) {
            return DatabaseJudge.databaseVerdict(name, wantEqual, re.value(), ra.value(), letPrefix, specs, env,
                    hook, true, false);
        }
        boolean goldenIsExpected = ra != null;
        var r = goldenIsExpected ? ra : re;
        if (r == null) {
            return DatabaseJudge.unjudged(name, "rendered-text: no side is a render the grammar names");
        }
        String text = com.legend.compiler.spec.VerdictQueries.goldenText(
                goldenIsExpected ? args.get(0) : args.get(1), chase);
        if (text == null) {
            return DatabaseJudge.unjudged(name, "rendered-text: the golden is not a string constant");
        }
        com.legend.compiler.element.type.Type.RelationType schema = null;
        if (r.grid() && r.restrictTo() != null) {
            // columnValues: the relation restricted to the one column, minted
            // from the PLANNED schema (the static type may not carry it)
            DatabaseJudge.SideRows whole = DatabaseJudge.planSide(r.value(), false, false, letPrefix, specs, env, hook);
            if (whole.why() != null) {
                return DatabaseJudge.unjudged(name, "rendered-text side: " + whole.why());
            }
            TypedSpec one = com.legend.compiler.spec.VerdictQueries.restrictedTo(r.value(),
                    com.legend.compiler.element.type.Type.schemaView(
                            java.util.Objects.requireNonNull(whole.side()).shapeInfo().type()),
                    r.restrictTo());
            if (one == null) {
                return DatabaseJudge.unjudged(name, "rendered-text: column '" + r.restrictTo() + "' is not declared");
            }
            r = new com.legend.compiler.spec.VerdictQueries.RenderedSide(one, r.grammar(), true);
        }
        if (r.grid()) {
            // the PLANNED side's schema (its shape info) is the grid verdict's
            // own width and kinds — the static type can lag it (validate's
            // late-bound ID column)
            DatabaseJudge.SideRows planned = DatabaseJudge.planSide(r.value(), false, false, letPrefix, specs, env, hook);
            if (planned.why() != null) {
                return DatabaseJudge.unjudged(name, "rendered-text side: " + planned.why());
            }
            var side = java.util.Objects.requireNonNull(planned.side());
            var pr = java.util.Objects.requireNonNull(planned.rider());   // width recorded at wrap
            List<com.legend.sql.OutputCol> data = pr.dataPrefix(side.plan().outputs());
            schema = com.legend.compiler.element.type.Type.schemaView(side.shapeInfo().type());
            if (schema == null || schema.columns().isEmpty() || !schema.dynamicColumns().isEmpty()) {
                // late-bound (a raw grid framed by the database's columns): the slots type it
                schema = com.legend.compiler.spec.VerdictQueries.wireSchema(data);
            } else {
                // a wire-decided declaration's cell is what the wire printed
                schema = com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(schema, data);
            }
        }
        var parsed = com.legend.compiler.spec.VerdictQueries.parseRendered(text, r.grammar(),
                schema, r.grid() ? null : r.value().info().type());
        if (parsed.headerMismatch()) {
            com.legend.exec.CanonicalDivergence.sqlJudgedInDatabase(name);
            return fail(name + ": " + parsed.reason());   // a static verdict
        }
        if (parsed.literal() == null) {
            return DatabaseJudge.unjudged(name, java.util.Objects.requireNonNull(parsed.reason()));
        }
        boolean multiset = !orderedForm
                || com.legend.compiler.spec.OrderView.of(r.value(), letPrefix) != com.legend.compiler.spec.OrderView.SORTED;
        boolean csv = r.grammar() instanceof com.legend.compiler.spec.VerdictQueries.RenderGrammar.Csv;
        return goldenIsExpected
                ? DatabaseJudge.databaseVerdict(name, wantEqual, parsed.literal(), r.value(), letPrefix, specs,
                        env, hook, multiset, false, csv)
                : DatabaseJudge.databaseVerdict(name, wantEqual, r.value(), parsed.literal(), letPrefix, specs,
                        env, hook, multiset, false, csv);
    }

    /** The RENDERED-TEXT form of a side, or null: toCSV → CSVTEXT,
     * toString over a relation → TDSTEXT, toCSV->replace('\n', sep) →
     * CSVJOIN:sep (the calendar family's one-line spelling), and a
     * makeString/joinStrings join over an INCIDENTAL-order chain →
     * CSVJOIN:sep (token multiset — sep-joined DB arrival order).
     * The comparison policy is {@link com.legend.exec.TdsCompare
     * #renderedText} — the one owner, probed by its own R1b census. */
    static @com.legend.base.Nullable String renderForm(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        if (s instanceof TypedNativeCall rep
                && FQ_REPLACE.equals(rep.callee().qualifiedName())
                && rep.args().size() == 3
                && chaseLets(rep.args().get(0), lets)
                        instanceof TypedNativeCall csv
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv.callee().qualifiedName())
                && rep.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString from
                && "\n".equals(from.value())
                && rep.args().get(2) instanceof
                        com.legend.compiler.spec.typed.TypedCString to) {
            return "CSVJOIN:" + to.value();
        }
        if (s instanceof TypedNativeCall csv2
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv2.callee().qualifiedName())
                && csv2.args().size() == 1) {
            return "CSVTEXT";
        }
        if (s instanceof TypedNativeCall ts
                && FQ_TO_STRING.equals(ts.callee().qualifiedName())
                && ts.args().size() == 1
                && com.legend.compiler.element.type.Type.isRelation(
                        ts.args().get(0).info().type())) {
            return "TDSTEXT";
        }
        if (s instanceof TypedNativeCall j
                && (FQ_MAKE_STRING.equals(j.callee().qualifiedName())
                        || FQ_JOIN_STRINGS.equals(j.callee().qualifiedName()))
                && j.args().size() == 2
                && j.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString sep
                && com.legend.compiler.spec.OrderView.of(j.args().get(0), lets) == com.legend.compiler.spec.OrderView.INCIDENTAL) {
            return "CSVJOIN:" + sep.value();
        }
        return null;
    }

    /** The cluster-34 envelope READ SHAPE: {@code $r.values} (through
     * optional toOne/first/at(0) peels) over a binding OUTSIDE the let
     * prefix — an execution frame; the TDS envelope is ONE carrier. */
    static boolean envelopeValuesRead(TypedSpec arg,
            List<TypedSpec> letPrefix) {
        TypedSpec s = arg;
        while (s instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String fqn = c.callee().qualifiedName();
            if (fqn.equals(com.legend.builtin.Pure.TO_ONE__T_MANY.qualifiedName())
                    || fqn.equals(com.legend.compiler.element.type.PlatformTypes.FIRST)
                    || (fqn.equals(com.legend.builtin.Pure.AT__T_MANY__INTEGER_1.qualifiedName())
                            && c.args().size() == 2
                            && c.args().get(1) instanceof
                                    com.legend.compiler.spec.typed
                                            .TypedCInteger ci
                            && ci.value().longValue() == 0)) {
                s = c.args().get(0);
                continue;
            }
            break;
        }
        if (!(s instanceof com.legend.compiler.spec.typed
                .TypedPropertyAccess pa
                && pa.property().equals("values")
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v)) {
            return false;
        }
        // an ordinary let, not a frame
        return !com.legend.compiler.spec.typed.Lets.binds(letPrefix, v.name());
    }

    /** The {@code $exp->forAll(e|$act->contains($e))} SUBSET shape:
     * {expected, actual} sources, or null when not this idiom (the
     * predicate must be a contains of the forAll binder itself). */
    static TypedSpec @com.legend.base.Nullable [] forAllContains(
            TypedSpec a0) {
        if (a0 instanceof TypedNativeCall fa
                && "meta::pure::functions::collection::forAll"
                        .equals(fa.callee().qualifiedName())
                && fa.args().size() == 2
                && fa.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedLambda lam
                && lam.parameters().size() == 1
                && lam.body().size() == 1
                && lam.body().get(0) instanceof TypedNativeCall cont
                && "meta::pure::functions::collection::contains"
                        .equals(cont.callee().qualifiedName())
                && cont.args().size() == 2
                && cont.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedVariable ev
                && ev.name().equals(lam.parameters().get(0))) {
            return new TypedSpec[] {fa.args().get(0), cont.args().get(0)};
        }
        return null;
    }

    /** The root's callee — except {@code assertError}, whose arm is the host's own
     * (a non-callee here keeps it off the verdict routes). */
    private static @com.legend.base.Nullable String calleeFqn(TypedSpec bare) {
        String fqn = com.legend.compiler.spec.typed.Calls.calleeOf(bare);
        return com.legend.compiler.element.type.PlatformTypes.ASSERT_ERROR.equals(fqn) ? null : fqn;
    }

    /** The engine golden's null spelling anywhere in the EXPECTED wire
     * values (scalar cells, instance properties, nested lists). */
    static boolean containsTdsNullSentinel(
            @com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> false;
            case String s -> "TDSNull".equals(s);
            case List<?> l -> l.stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            case java.util.Map<?, ?> m -> m.values().stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            default -> false;
        };
    }

    static boolean isAnyStamped(TypedSpec s) {
        return s.info().type() instanceof
                com.legend.compiler.element.type.Type.ClassType ct
                && com.legend.compiler.element.type.PlatformTypes.isAny(ct);
    }

    static boolean containsTreeMarker(
            @com.legend.base.Nullable String text) {
        return text != null && text.contains(
                com.legend.lowering.CanonicalRenderSql.TREE_MARKER);
    }

    static List<Object> concat(List<Object> a, List<Object> b) {
        List<Object> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    /** F13 v1 exclusion scan: any KEYLESS model-class constructor (or
     * copy) under a lambda anywhere in the verdict's scope — the site
     * id cannot distinguish per-element evaluations. A plain
     * containment walk over {@code children()} (no shadow concerns —
     * this detects presence, it never resolves variables). */
    static boolean keylessCtorUnderLambda(List<TypedSpec> roots,
            StatementExecutor.ExecEnv env) {
        for (TypedSpec r : roots) {
            if (scanKeylessCtor(r, false, env)) {
                return true;
            }
        }
        return false;
    }

    private static boolean scanKeylessCtor(TypedSpec n, boolean inLambda,
            StatementExecutor.ExecEnv env) {
        if (inLambda) {
            String fqn = n instanceof
                    com.legend.compiler.spec.typed.TypedNewInstance ni
                    ? ni.classFqn()
                    : n instanceof
                            com.legend.compiler.spec.typed.TypedCopyInstance cp
                            ? cp.classFqn() : null;
            if (fqn != null && env.ctx().findClass(fqn).isPresent()
                    && com.legend.compiler.element.EqualityKeys
                            .resolve(env.ctx(), fqn) == null) {
                return true;
            }
        }
        boolean in = inLambda
                || n instanceof com.legend.compiler.spec.typed.TypedLambda;
        for (TypedSpec k : n.children()) {
            if (scanKeylessCtor(k, in, env)) {
                return true;
            }
        }
        return false;
    }

    /** True iff both sides are same-length all-finite-Double vectors
     * whose pairs each hold under the lattice's declared 2-ULP arm —
     * PureAsserts OWNS the tolerance, this only vectorizes it. */
    static boolean withinDeclaredUlp(List<Object> eVals,
            List<Object> aVals) {
        return com.legend.exec.Equality.differByLeniencyOnly(
                com.legend.exec.Equality.Typed.all(eVals, null),
                com.legend.exec.Equality.Typed.all(aVals, null));
    }

    static String typeName(TypedSpec spec) {
        var t = spec.info().type();
        return t.getClass().getSimpleName() + ":" + t;
    }

    /** The kind class of a SIDE, node-aware: a type reference written as a
     * value ({@code String}, {@code [Car, Bicycle]}) is stamped as its
     * prototype by the typer (Typer.typeRef) but IS a type value; a
     * metamodel type classifier or a tracked element class is the
     * name-valued kind. Everything else by its type. */
    static @com.legend.base.Nullable KindClass kindKey(TypedSpec spec,
            List<TypedSpec> letPrefix, StatementExecutor.ExecEnv env) {
        TypedSpec s = chaseLets(spec, letPrefix);
        if (isTypeValueNode(s)) {
            return KindClass.Primitive.TYPE;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedCollection c
                && !c.elements().isEmpty()
                && c.elements().stream().allMatch(AssertVerdicts::isTypeValueNode)) {
            return KindClass.Primitive.TYPE;
        }
        String fqn = com.legend.compiler.element.EqualityKeys.fqnOf(spec.info().type());
        if (fqn != null && com.legend.compiler.element.type.PlatformTypes.isTypeClassifier(fqn)) {
            return KindClass.Primitive.TYPE;
        }
        if (fqn != null && env.ctx().tracksClassifier(fqn)) {
            return new KindClass.Element(fqn);
        }
        return KindClass.of(spec.info().type());
    }

    /** A type written as a VALUE: a primitive type reference ({@code
     * String} — TypedTypeRef, stamped as its prototype) or a class
     * reference ({@code Car} — a packageable ref typed {@code Class<Car>}). */
    private static boolean isTypeValueNode(TypedSpec s) {
        return s instanceof com.legend.compiler.spec.typed.TypedTypeRef
                || (s instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                        && pr.info().type() instanceof com.legend.compiler.element.type.Type.GenericType g
                        && g.rawFqn().equals(
                                com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS));
    }

    /** X5 — the pair's shared key tree: non-null iff BOTH stamps are
     * the SAME keyed class (the engine's classifier-match precondition
     * plus resolvable {@code <<equality.Key>>} identity). */
    static com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
            instanceKeys(TypedSpec eSpec, TypedSpec aSpec,
                    StatementExecutor.ExecEnv env, List<Object> eVals, List<Object> aVals) {
        String ef = com.legend.compiler.element.EqualityKeys.fqnOf(
                eSpec.info().type());
        String af = com.legend.compiler.element.EqualityKeys.fqnOf(
                aSpec.info().type());
        // a side DECLARED wider (a program's Node[1] return) whose wire
        // values all carry the other side's class as their __type IS that
        // class at runtime — the engine's classifier match holds on the
        // evidence, not on the declaration (batch 54: assertConversion's
        // `let actual = convertElement(…)` judged against ^StringLiteral(…))
        if (ef != null && af != null && !ef.equals(af)) {
            if (env.ctx().isSubtype(ef, af) && HostJudge.allWireType(aVals, ef)) {
                af = ef;
                aSpec = eSpec;
            } else if (env.ctx().isSubtype(af, ef) && HostJudge.allWireType(eVals, af)) {
                ef = af;
                eSpec = aSpec;
            }
        }
        if (ef != null && ef.equals(af)) {
            // substitution-aware: the E-side stamp's instantiation
            // (key NAMES are instantiation-independent; nesting follows
            // the arguments — Pair-of-Pairs)
            return com.legend.compiler.element.EqualityKeys.resolve(
                    env.ctx(), eSpec.info().type());
        }
        return null;
    }

    /** A class-kind side's values with JSON object text decoded to structures. */
    static List<Object> structuredSide(TypedSpec spec, List<Object> vals) {
        if (com.legend.compiler.element.EqualityKeys.fqnOf(spec.info().type()) == null) {
            return vals;
        }
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(com.legend.exec.Executor.structured(v));
        }
        return out;
    }

    /** Declared classes only (no wire evidence in hand). */
    static com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
            instanceKeys(TypedSpec eSpec, TypedSpec aSpec, StatementExecutor.ExecEnv env) {
        return instanceKeys(eSpec, aSpec, env, List.of(), List.of());
    }



    static List<Object> side(TypedSpec arg, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook) {
        return decodeSide(StatementExecutor.evalValue(arg, letPrefix,
                specs, env, null, false, hook));
    }

    /** A JSON assert side's ONE document text: a GRAPH result's JSON
     * IS the {@code String[1]} (a serialize execute's {@code .values}
     * — leg 2 made its stamp String[1], and the DB-built envelope is
     * the value); any other result must decode to one string. Null =
     * not this shape (generic path, loud downstream). */
    static @com.legend.base.Nullable String jsonSideText(TypedSpec arg,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook) {
        ExecutionResult r = StatementExecutor.evalValue(arg, letPrefix,
                specs, env, null, false, hook);
        if (r instanceof ExecutionResult.Graph g) {
            return g.json();
        }
        List<Object> v = decodeSide(r);
        return v.size() == 1 && v.get(0) instanceof String s ? s : null;
    }

    static List<Object> decodeSide(
            @com.legend.base.Nullable ExecutionResult r) {
        List<Object> side = decodeSideValues(r);
        // V7 §5-1 instrument: the side-size histogram (VALUES-literal
        // cost bracket for V12's fused-verdict design) — measurement
        // only, fed from the one place every side already flows through
        com.legend.exec.CanonicalDivergence.v7SideRows(side.size());
        return side;
    }

    static List<Object> decodeSideValues(
            @com.legend.base.Nullable ExecutionResult r) {
        return switch (r) {
            case null -> new ArrayList<>();
            case ExecutionResult.Scalar s -> {
                // (a list wire arriving as one JDBC array cell is decoded to a
                // Collection at the exec seam — no JDBC carrier reaches the router)
                List<Object> out = new ArrayList<>(1);
                if (s.value() != null) {
                    out.add(s.value());
                }
                yield out;
            }
            case ExecutionResult.Collection c -> c.values();
            // D4: a GRAPH side's values are the DATABASE-built JSON
            // array's elements (the harness Eval convention moved to
            // the owner) — parsed structures; the lattice/JsonCompare
            // judge them, never raw json text
            case ExecutionResult.Graph g -> {
                Object p = com.legend.sql.Json.parse(g.json());
                yield p instanceof List<?> l ? new ArrayList<Object>(l)
                        : new ArrayList<>(List.of(p));
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "assert verdict over a " + r.getClass().getSimpleName()
                    + " side — grid asserts stay with their own"
                    + " compare owners");
        };
    }

    static Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value,"
                    + " got " + side.size());
        }
        return side.get(0);
    }

    /** The literal type argument's name: @Type annotation
     * ({@code TypedTypeRef}) or a bare reference in value position
     * ({@code TypedPackageableRef}); null = not literal (fall through,
     * the body inlines and walls on its own terms). */
    static @com.legend.base.Nullable String typeRefName(TypedSpec t) {
        return switch (t) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr ->
                    tr.target().typeName();
            case com.legend.compiler.spec.typed.TypedPackageableRef pr ->
                    pr.fullPath();
            default -> null;
        };
    }

    /** The relation arg executed in the database, as its TABULAR frame;
     * null = the value did not execute to a relation (fall through). */
    static ExecutionResult.@com.legend.base.Nullable Tabular tabular(
            TypedSpec arg, List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, @com.legend.base.Nullable SpliceHook hook) {
        ExecutionResult r = StatementExecutor.evalValue(arg,
                letPrefix, specs, env, null, false, hook);
        return r instanceof ExecutionResult.Tabular t ? t : null;
    }

    /** Row-major cell stream of a tabular HostJudge.frame (the cell-zip input). */
    /** Raw cell flatten — cells arrive ALREADY DECODED (the engine's
     * value-read conventions ride the FETCH: wrapTdsCanon conforms the
     * plan in SQL and the executor's label-driven unwrap delivers; the
     * former host-twin decode, valueRead, is deleted — Java-eviction
     * close of the disagree-9 burn). */
    static List<Object> cells(ExecutionResult.Tabular t) {
        List<Object> out = new java.util.ArrayList<>();
        for (com.legend.exec.Row r : t.rows()) {
            out.addAll(r.values());
        }
        return out;
    }

    static ExecutionResult ok() {
        return new ExecutionResult.Scalar(Boolean.TRUE,
                com.legend.compiler.element.type.Type.Primitive.BOOLEAN);
    }

    static ExecutionResult fail(String message) {
        // the seam: verdicts speak the platform vocabulary
        throw new com.legend.error.AssertFailed(message);
    }
}
