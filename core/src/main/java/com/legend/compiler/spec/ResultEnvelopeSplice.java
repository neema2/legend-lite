// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The Result-envelope SPLICE — the typed-tree rewrite rules that turn
 * reads over an executed {@code execute(...)} binding into ordinary
 * typed queries: {@code $r.values} becomes the frame's query chain;
 * {@code ->at(0)}/{@code ->toOne()} over it collapse for a relation
 * root (real selections for a class or scalar root); {@code $r->size()}
 * over a relation-rooted frame is the envelope's ONE; an inline
 * {@code execute(...).values} splices in place.
 *
 * <p><strong>Ownership (Invariant 7):</strong> these rules mint and
 * rewrite typed-HIR nodes — COMPILER work. They lived in
 * {@code StatementExecutor} ("moved VERBATIM from the harness", audit
 * 19d B2) until the splice-ownership leg moved them here. The executor
 * keeps what is genuinely execution-bound — building and eagerly
 * running frames over JDBC — and supplies it through {@link Frames}:
 * the compiler owns WHAT a splice means, the executor owns WHEN a
 * frame's value exists.
 */
public final class ResultEnvelopeSplice {

    private ResultEnvelopeSplice() {
    }

    // ---- Envelope-read recognizers — generic natives identified by
    // EXACT FQN (never suffix matching). Public: the executor's
    // alias-frame walk shares them.
    public static final String AT_FQN = "meta::pure::functions::collection::at";
    public static final String FIRST_FQN = "meta::pure::functions::collection::first";
    public static final String TO_ONE_FQN =
            "meta::pure::functions::multiplicity::toOne";
    public static final Set<String> SIZE_FQNS = Set.of(
            "meta::pure::functions::relation::size",
            "meta::pure::functions::collection::size");

    /** The compiler's view of one executed {@code execute()} binding:
     * the from-wrapped typed query chain (unresolved — downstream reads
     * compose over it and resolve as a whole) and whether the query
     * ROOT is relation-shaped (the engine's {@code Result.values} for a
     * TDS query holds ONE TDS; for a class or scalar root, values IS
     * the collection). */
    public record View(TypedSpec chain, boolean relationRooted,
            @com.legend.base.Nullable TypedNativeCall sourceExec,
            @com.legend.base.Nullable String cteName,
            com.legend.sql.@com.legend.base.Nullable SqlQuery plan,
            @com.legend.base.Nullable ExprType plannedInfo) {
        public View(TypedSpec chain, boolean relationRooted,
                @com.legend.base.Nullable TypedNativeCall sourceExec) {
            this(chain, relationRooted, sourceExec, null, null, null);
        }

        /** Whether the executor PLANNED this frame (leg 3.4 step 2): a
         * value read then splices to a reference to its CTE. */
        public boolean planned() {
            return cteName != null && plan != null && plannedInfo != null;
        }

        /** What a VALUE read of the frame splices to: a reference to the
         * planned frame's CTE (minted here — Invariant 7: typed nodes are
         * the compiler's to mint), typed as the PLANNED root (the resolver's
         * view of the frame's schema), else the chain. The structural
         * readers (activities, the rendered SQL, the documentation walk)
         * keep {@link #chain()}. */
        public TypedSpec spliced() {
            // a CLASS frame's plan is its extent's rows (executedExtent below),
            // never a relation reference
            if (cteName == null || plan == null || plannedInfo == null || !relationRooted) {
                return chain;
            }
            TypedSpec ref = new com.legend.compiler.spec.typed.TypedFrameRef(cteName, plannedInfo, plan);
            // the reference stands where the frame's QUERY stood: inside the
            // frame's own from-envelope (its mapping / runtime context — the
            // connection zone the side's literals spell in, the table
            // renames, the facts every reader of a from carries)
            return chain instanceof TypedFrom fr
                    ? fr.withSource(ref, fr.info())
                    : ref;
        }
    }

    /**
     * The executor-supplied half: frame lookup and the two operations
     * that genuinely need the execution boundary. The SPLICE RULES
     * never touch JDBC — they ask.
     */
    public interface Frames {

        /** The frame bound to a let name, or null when the name is not
         * an exec binding. */
        @com.legend.base.Nullable View frame(String name);

        /** Build the frame for an INLINE {@code execute(...)} call —
         * eager when nothing downstream consumes the chain (Pure is
         * strict: the query must run), lazy when the value is observed
         * where it stands (a separate eager run would execute twice). */
        View inlineExecute(TypedNativeCall ec, boolean eager);


        /** The activity log's {@code RelationalActivity[n].sql} — the
         * engine-style rendered SQL of the frame's OWN query (the
         * compiler's rendered text; helperFunctions.pure:38-60).
         * Null when the frame cannot answer (no retained execute call,
         * or an activity index this platform's single-statement
         * execution does not produce). */
        @com.legend.base.Nullable String relationalActivitySql(
                String frameName, long activityNumber);


        /** The same render for an INLINE execute call (a user-call
         * frame's query). */
        @com.legend.base.Nullable String relationalActivitySql(TypedNativeCall ec);

        /** A {@code toSQLString(...)} / {@code toSQLStringPretty(...)}
         * call's rendered text — the K-native evaluated WHEREVER the
         * call appears (the old statement-root-only dispatch was
         * position-dependent: nested under sqlRemoveFormatting the
         * lambda leaked to the resolver). Null when the call cannot
         * render (non-literal lambda etc.) — the caller's walls stand. */
    }

    /**
     * The per-node hook for {@link UserCallInliner}: rewrites envelope
     * reads against the supplied frames. The second argument is the
     * lambda-bound variable names in scope at the node — a lambda-bound
     * variable spelled like an exec-let is NOT a frame read (corpus:
     * {@code let r = execute(...)} + {@code ->map(r|$r.values...)} — the
     * map binder's {@code $r.values} is the ROW's cells, never the
     * Result envelope); shadowed names drop out of the frame map.
     */
    public static BiFunction<TypedSpec, Set<String>, TypedSpec> hook(
            Frames frames) {
        return (n, boundVars) -> rewrite(n, shadowed(frames, boundVars));
    }

    private static Frames shadowed(Frames frames, Set<String> boundVars) {
        if (boundVars.isEmpty()) {
            return frames;
        }
        return new Frames() {
            @Override
            public @com.legend.base.Nullable View frame(String name) {
                return boundVars.contains(name) ? null : frames.frame(name);
            }

            @Override
            public View inlineExecute(TypedNativeCall ec, boolean eager) {
                return frames.inlineExecute(ec, eager);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(TypedNativeCall ec) {
                return frames.relationalActivitySql(ec);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(
                    String frameName, long activityNumber) {
                return boundVars.contains(frameName) ? null
                        : frames.relationalActivitySql(frameName,
                                activityNumber);
            }

        };
    }

    private static TypedSpec rewrite(TypedSpec n, Frames frames) {
        // an INLINE string-entry call (assertEquals(expected,
        // executeLegendQuery(...))) IS its result JSON string: the frame's
        // chain (the envelope) stands where the call stood — observed
        // where it stands, no separate eager run
        if (n instanceof TypedNativeCall lq
                && (com.legend.builtin.NativeFn.Handle.of(lq.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
            // the hook fires BEFORE and AFTER the inliner's env
            // substitution: a still-variable query argument (a helper's
            // parameter — runLegendTest($f, …)) waits for the substituted
            // pass; the frame builds once the lambda is in view
            if (lq.args().get(0) instanceof TypedVariable) {
                return n;
            }
            return frames.inlineExecute(lq, false).chain();
        }
        // $result.rows->size(): POST-EXECUTE row count. The engine
        // counts the MATERIALIZED rows in memory; the in-query
        // single-column count(col) rule (processRowCount, null-
        // skipping) must not apply to this splice — a nullable
        // projected column would under-count (inline-embedded
        // golden: 5 rows, 3 TDSNull). A constant-1 projection makes
        // the size lowering emit COUNT(1) = the bare row count.
        if (n instanceof TypedNativeCall szr
                && SIZE_FQNS.contains(szr.callee().qualifiedName())
                && szr.args().size() == 1
                && szr.args().get(0) instanceof TypedPropertyAccess rp0
                && rp0.property().equals("rows")
                && Type.relationSchema(rp0.source().info().type())
                        instanceof Type.RelationType rrt) {
            var one1 = Multiplicity.Bounded.ONE;
            var intT = Type.Primitive.INTEGER;
            var lam = new TypedLambda(
                    List.of("_cntRow"),
                    List.of(new TypedCInteger(1L, new ExprType(intT, one1))),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(rrt, one1)),
                            new Type.Param(intT, one1)), one1));
            var cntRow = new Type.RelationType(
                    List.of(new Type.Column("cnt", intT, one1)),
                    List.of());
            TypedSpec proj = new TypedProject(
                    rp0.source(),
                    List.of(new TypedFuncCol("cnt", lam)),
                    new ExprType(Type.relation(cntRow), one1));
            return new TypedNativeCall(szr.callee(), List.of(proj), szr.info(), szr.pos());
        }
        // cast(@TabularDataSet) over a SPLICED relation is the identity
        // (CastChecker's rule, which typing could not apply: the envelope
        // read is a relation only after the splice)
        // (the hook is offered each node BEFORE its children: the source
        // splices here first — cast(@TabularDataSet) over ->at(0) over
        // the envelope read, testDataGeneration loadAndTestExecution)
        if (n instanceof TypedCast tc
                && PlatformTypes.isTdsType(tc.target())) {
            TypedSpec src = rewrite(tc.source(), frames);
            if (Type.isRelation(src.info().type())) {
                return src;
            }
        }
        // the Typer's `.rows` MARKER (identity over a relation value):
        // it exists so the arms below can tell a REAL row index
        // ($r.values.rows->at(k)) from the Result envelope
        // ($r.values->at(k)) — once seen, it erases to its source.
        if (n instanceof TypedPropertyAccess rp
                && rp.property().equals("rows")) {
            TypedSpec src = rewrite(rp.source(), frames);
            if (Type.isRelation(src.info().type())) {
                return src;
            }
        }
        // the Typer's `.columns.documentation` MARKER: the receiver is
        // spliced by the time this hook sees the node — walk to the
        // PROJECT and fold col()'s doc metadata (String[0..1] per
        // column: undocumented columns flatten away)
        if (n instanceof TypedPropertyAccess dm
                && dm.property().equals("columns.documentation")) {
            TypedSpec un = dm.source();
            boolean walked = true;
            while (walked) {
                walked = false;
                if (un instanceof TypedFrom f2) {
                    un = f2.source();
                    walked = true;
                } else if (un instanceof TypedNativeCall w2
                        && !w2.args().isEmpty()
                        && Type.isRelation(w2.args().get(0).info().type())) {
                    un = w2.args().get(0);
                    walked = true;
                } else if (un instanceof TypedPropertyAccess pv2) {
                    // an UNSPLICED envelope read ($result.values):
                    // resolve through the exec frame ourselves — the CHAIN
                    // (this walk reaches for the project, never a reference)
                    TypedSpec spl = valuesRead(pv2, chainsOnly(frames));
                    if (spl != null) {
                        un = spl;
                        walked = true;
                    }
                }
            }
            if (un instanceof TypedProject tp2) {
                return tp2.docsFold();
            }
            throw new IllegalStateException("columns.documentation read"
                    + " did not reach a project after the splice (source="
                    + un.getClass().getSimpleName() + ")");
        }
        // $r->size() / $tds->size(): ONE TDS value, never the row count
        if (n instanceof TypedNativeCall sz
                && SIZE_FQNS.contains(sz.callee().qualifiedName())
                && sz.args().size() == 1
                && sz.args().get(0) instanceof TypedVariable sv
                && frames.frame(sv.name()) instanceof View fv
                && fv.relationRooted()) {
            return new TypedCInteger(1L, sz.info());
        }
        // size(execute(...)) over an INLINE execute call (ledger
        // cluster 52): the Result envelope is Result<T|m>[1] — size
        // is 1, never the row count. eager MUST be true: nothing
        // downstream consumes the chain, so a lazy frame would
        // silently skip the query (Pure is strict). NOT gated on
        // relationRooted() — the query may be class-rooted.
        if (n instanceof TypedNativeCall szi
                && SIZE_FQNS.contains(szi.callee().qualifiedName())
                && szi.args().size() == 1) {
            TypedSpec earg = szi.args().get(0);
            while (earg instanceof TypedFrom ef) {
                earg = ef.source();
            }
            if (earg instanceof TypedNativeCall ec2
                    && com.legend.builtin.NativeFn.Handle.isExecute(ec2.callee().qualifiedName())) {
                frames.inlineExecute(ec2, true);
                return new TypedCInteger(1L, szi.info());
            }
        }
        // $r.values->at(k) / ->toOne(): collapse (relation root) or a
        // REAL selection over the spliced chain (class/scalar root)
        if (n instanceof TypedNativeCall w
                && (AT_FQN.equals(w.callee().qualifiedName())
                        || TO_ONE_FQN.equals(w.callee().qualifiedName())
                        || FIRST_FQN.equals(w.callee().qualifiedName()))
                && !w.args().isEmpty()) {
            // a type-level TDS cast between the values read and the
            // collapse (`.values->cast(@TabularDataSet)->toOne()`, the
            // m2m2r plan helper) is transparent — the same no-op the cast
            // arm below applies once the source has spliced
            TypedSpec spliced = valuesRead(peelEnvelopeCasts(w.args().get(0)), frames);
            if (spliced != null) {
                // relation-rootedness IS the spliced chain's root type
                boolean relation = Type.relationValued(spliced.info());
                if (relation) {
                    if (AT_FQN.equals(w.callee().qualifiedName())
                            && !(w.args().size() == 2 && w.args().get(1)
                                    instanceof TypedCInteger k
                                    && k.value().longValue() == 0)) {
                        throw new IllegalStateException(
                                "Result.values->at(k>0) on a relation-rooted"
                                + " query — the values envelope holds one TDS");
                    }
                    return spliced;
                }
                List<TypedSpec> args = new java.util.ArrayList<>(w.args());
                args.set(0, spliced);
                return new TypedNativeCall(w.callee(), args, w.info(), w.pos());
            }
        }
        // (rewrittenQuery is navigation over the activity ROWS: the router's
        // choice is recorded when the frame is built — batch 25)
        // sql(result[, n]) family (helperFunctions.pure:38-60, INLINED):
        // the activity log's RelationalActivity .sql — the frame's own
        // rendered SQL, same derived-read doctrine as rewrittenQuery
        TypedSpec sqlRead = relationalSqlRead(n, frames);
        if (sqlRead != null) {
            return sqlRead;
        }
        // The SAME functions matched AT THE CALL, by exact FQN, before
        // inlining: the hook rewrites a bare frame-variable ARGUMENT into
        // its query chain during inlining (the bare-frame arm below), so
        // by the time the verbatim bodies are spliced in, the frame
        // identity the activities read needs is gone. The corpus bodies
        // stay the SPEC; this fold mirrors them exactly (sql = render;
        // sqlRemoveFormatting = render with \n and \t stripped —
        // helperFunctions.pure:58).
        TypedSpec sqlCall = sqlProducerCall(n, frames);
        if (sqlCall != null) {
            return sqlCall;
        }
        // $r.activities — the execution-activity trail as ROWS under the
        // execute call's scope (registered when the frame was built): the
        // read re-roots at the call, the resolver serves the rows. A
        // frame that lost its call (an alias) keeps the loud wall — no
        // activity is invented. (The sql()/sqlRemoveFormatting arms above
        // stay: they are the SQL-TEXT REFEREE's classification — a
        // text-divergent render still row-verifies through the oracle.)
        TypedSpec rows = activitiesRowsRead(n, frames);
        if (rows != null) {
            return rows;
        }
        if ((n instanceof TypedFilter tf && activitiesRead(tf.source(), frames))
                || activitiesRead(n, frames)) {
            throw new com.legend.error.NotImplementedException(
                    "execution activities are not recorded");
        }
        // $r.values / execute(...).values → the spliced chain
        TypedSpec direct = valuesRead(n, frames);
        if (direct != null) {
            return direct;
        }
        // a BARE frame variable reads as the chain (harness parity)
        if (n instanceof TypedVariable bv
                && frames.frame(bv.name()) instanceof View bf) {
            return bf.spliced();
        }
        return n;
    }

    /** {@code $frame.activities} re-rooted at the frame's execute call
     * (its rows' scope); an INLINE {@code execute(...).activities} builds
     * the frame (registering the rows) and stands AS WRITTEN — the same
     * instance, the inliner's fixpoint. Null when not an activities read
     * or the frame has no call. */
    private static @com.legend.base.Nullable TypedSpec activitiesRowsRead(TypedSpec n,
            Frames frames) {
        if (n instanceof TypedFilter tf) {
            TypedSpec src = activitiesRowsRead(tf.source(), frames);
            return src == null ? null : src == tf.source() ? n
                    : new TypedFilter(src, tf.predicate(), tf.info());
        }
        if (!(n instanceof TypedPropertyAccess ap)
                || !ap.property().equals("activities")) {
            return null;
        }
        if (ap.source() instanceof TypedVariable av
                && frames.frame(av.name()) instanceof View f
                && f.sourceExec() != null) {
            return new TypedPropertyAccess(f.sourceExec(), "activities", ap.info());
        }
        TypedSpec src = ap.source();
        while (src instanceof TypedFrom sf) {
            src = sf.source();
        }
        if (src instanceof TypedNativeCall ec
                && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().qualifiedName())) {
            frames.inlineExecute(ec, false);
            return n;
        }
        return null;
    }

    /** A {@code <frameVar>.activities} read (the Result envelope's
     * execution-activity trail). */
    private static boolean activitiesRead(TypedSpec n, Frames frames) {
        return n instanceof TypedPropertyAccess ap
                && ap.property().equals("activities")
                && ap.source() instanceof TypedVariable av
                && frames.frame(av.name()) != null;
    }

    /** The m3 class of the relational activity record (registered
     * verbatim in {@code Pure.RELATIONAL_ACTIVITY}). */
    private static final String RELATIONAL_ACTIVITY_FQN =
            "meta::relational::mapping::RelationalActivity";
    private static final String INSTANCE_OF_FQN = com.legend.builtin.NativeFn.SubtypeForm.INSTANCE_OF.fqn();

    /** The INLINED {@code sql($result[, n])} chain
     * (helperFunctions.pure:38-60):
     * {@code $r.activities->filter(a|$a->instanceOf(RelationalActivity))
     * ->at(n)->cast(@RelationalActivity).sql} — folded to the frame's own
     * rendered SQL (a compile-time fact: the SQL is the compiler's
     * output, retained, not re-derived). Null when not this shape or the
     * frame cannot answer. */
    private static @com.legend.base.Nullable TypedSpec relationalSqlRead(
            TypedSpec n, Frames frames) {
        if (!(n instanceof TypedPropertyAccess pa)
                || !pa.property().equals("sql")) {
            return null;
        }
        TypedSpec src = pa.source();
        if (src instanceof TypedCast tc) {
            src = tc.source();
        }
        long k;
        TypedSpec coll;
        if (src instanceof TypedNativeCall w && !w.args().isEmpty()) {
            String fq = w.callee().qualifiedName();
            if (AT_FQN.equals(fq) && w.args().size() == 2
                    && w.args().get(1) instanceof TypedCInteger ki) {
                k = ki.value().longValue();
                coll = w.args().get(0);
            } else if (TO_ONE_FQN.equals(fq) || FIRST_FQN.equals(fq)) {
                k = 0;
                coll = w.args().get(0);
            } else {
                return null;
            }
        } else {
            return null;
        }
        if (coll instanceof TypedCast c2) {
            coll = c2.source();
        }
        if (!(coll instanceof TypedFilter tf
                && tf.source() instanceof TypedPropertyAccess ap
                && ap.property().equals("activities")
                && filterKeepsExactly(tf.predicate(), RELATIONAL_ACTIVITY_FQN))) {
            return null;
        }
        String sql;
        if (ap.source() instanceof TypedVariable av && frames.frame(av.name()) != null) {
            sql = frames.relationalActivitySql(av.name(), k);
        } else {
            // the corpus sql() body inlined over a USER-CALL frame
            // (let r = executeInternal(...)): the execute call stands
            // inline — the referee's text still classifies
            TypedSpec execSrc = ap.source();
            while (execSrc instanceof TypedFrom sf) {
                execSrc = sf.source();
            }
            sql = execSrc instanceof TypedNativeCall ec
                    && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().qualifiedName())
                    && k == 0 ? frames.relationalActivitySql(ec) : null;
        }
        return sql == null ? null : new TypedCString(sql, n.info());
    }

    /** The sql-producer FUNCTIONS of the corpus (helperFunctions.pure:
     * 38-60), by exact FQN — the classification register the sql-text
     * partition reads (never a name suffix). */
    public static final String SQL_FQN = "meta::relational::mapping::sql";
    public static final String SQL_REMOVE_FORMATTING_FQN =
            "meta::relational::mapping::sqlRemoveFormatting";

    /** A {@code sql($frame[, n])} / {@code sqlRemoveFormatting($frame[, n])}
     * USER CALL over a frame variable — folded to the frame's rendered
     * SQL (stripped of {@code \n}/{@code \t} for the RemoveFormatting
     * forms, mirroring the verbatim body). The String-typed
     * {@code sqlRemoveFormatting(String)} overload is NOT matched here —
     * it is ordinary string code and evaluates as written. */
    private static @com.legend.base.Nullable TypedSpec sqlProducerCall(
            TypedSpec n, Frames frames) {
        if (!(n instanceof com.legend.compiler.spec.typed.TypedUserCall uc)) {
            return null;
        }
        String fqn = uc.callee().qualifiedName();
        boolean strip = SQL_REMOVE_FORMATTING_FQN.equals(fqn);
        if (!strip && !SQL_FQN.equals(fqn)) {
            return null;
        }
        if (uc.args().isEmpty()
                || !(uc.args().get(0) instanceof TypedVariable av)
                || frames.frame(av.name()) == null) {
            return null;
        }
        long k;
        if (uc.args().size() == 1) {
            k = 0;
        } else if (uc.args().size() == 2
                && uc.args().get(1) instanceof TypedCInteger ki) {
            k = ki.value().longValue();
        } else {
            return null;
        }
        String sql = frames.relationalActivitySql(av.name(), k);
        if (sql == null) {
            return null;
        }
        return new TypedCString(
                strip ? sql.replace("\n", "").replace("\t", "") : sql,
                n.info());
    }

    /** Whether the filter predicate is the single-statement
     * {@code x|$x->instanceOf(<classFqn>)} shape — identified by EXACT
     * FQN of both the native and the class argument, never by name. */
    private static boolean filterKeepsExactly(TypedLambda pred, String classFqn) {
        if (pred.parameters().size() != 1 || pred.body().size() != 1) {
            return false;
        }
        return pred.body().get(0) instanceof TypedNativeCall io
                && com.legend.builtin.NativeFn.SubtypeForm.of(io.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.SubtypeForm.INSTANCE_OF
                && io.args().size() == 2
                && io.args().get(0) instanceof TypedVariable v
                && v.name().equals(pred.parameters().get(0))
                && io.args().get(1) instanceof com.legend.compiler.spec.typed
                        .TypedPackageableRef cr
                && classFqn.equals(cr.fullPath());
    }

    /** A TDS-shaped cast over an ENVELOPE read is an assertion over the
     * value the splice is about to reveal (`Result<Any>.values` types Any
     * before the splice — the plan-execute route): the TARGET alone
     * decides, exactly as the cast arm in {@link #rewrite} does once the
     * source has spliced. */
    private static TypedSpec peelEnvelopeCasts(TypedSpec n) {
        while (n instanceof TypedCast tc && PlatformTypes.isTdsShaped(tc.target())) {
            n = tc.source();
        }
        return n;
    }

    /** A cast to a TDS-shaped type over a TDS-shaped value is a type-level
     * no-op ({@code $result.values->cast(@TDS<Any>)}): the value beneath
     * (PlatformTypes.isTdsShaped owns the shape). ONE owner — the resolver's
     * Anchors delegates here. */
    public static TypedSpec peelTdsCasts(TypedSpec n) {
        while (n instanceof TypedCast tc
                && PlatformTypes.isTdsShaped(tc.target())
                && PlatformTypes.isTdsShaped(tc.source().info().type())) {
            n = tc.source();
        }
        return n;
    }

    /** Splice a {@code .values} read (over a frame variable or an INLINE
     * execute call) into the underlying typed query chain; null when the
     * node is not a values read the frames can answer. */
    /** The frames with every reference dropped: the chain view only. */
    private static Frames chainsOnly(Frames frames) {
        return new Frames() {
            @Override
            public @com.legend.base.Nullable View frame(String name) {
                View v = frames.frame(name);
                return v == null || !v.planned() ? v
                        : new View(v.chain(), v.relationRooted(), v.sourceExec());
            }

            @Override
            public View inlineExecute(TypedNativeCall ec, boolean eager) {
                return frames.inlineExecute(ec, eager);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(String frameName,
                    long activityNumber) {
                return frames.relationalActivitySql(frameName, activityNumber);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(TypedNativeCall ec) {
                return frames.relationalActivitySql(ec);
            }
        };
    }

    private static @com.legend.base.Nullable TypedSpec valuesRead(TypedSpec n,
            Frames frames) {
        if (n instanceof TypedPropertyAccess pa
                && pa.property().equals("values")) {
            if (pa.source() instanceof TypedVariable v
                    && frames.frame(v.name()) instanceof View f) {
                return executedExtent(f);
            }
            TypedSpec src = pa.source();
            while (src instanceof TypedFrom sf) {
                src = sf.source();
            }
            if (src instanceof TypedNativeCall ec
                    && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().qualifiedName())) {
                // inline read: the value is observed where it stands —
                // no separate eager run (it would execute twice)
                return executedExtent(frames.inlineExecute(ec, false));
            }
        }
        return null;
    }

    /** The frame's chain as an EXECUTED EXTENT (TypedFrom.executedExtent,
     * batch 78): a read over the values of a CLASS-rooted frame ranges
     * over the instances the execute materialized. A relation-rooted
     * frame (a TDS query inside the execute) IS its relation — no extent
     * notion applies, its chain stands; so does a chain without a from()
     * envelope. */
    private static TypedSpec executedExtent(View f) {
        if (!f.relationRooted() && f.chain() instanceof TypedFrom fr) {
            // rung 12: a PLANNED class frame — its rows are one CTE; the read
            // ranges over the class's BARE extent whose root table is that CTE
            // (the chain's own filters and caps already shaped the rows)
            if (f.planned()) {
                TypedSpec root = plainExtentRoot(fr.source());
                if (root != null) {
                    return new TypedFrom(root, fr.context(), true, f.cteName(), fr.info());
                }
            }
            return fr.withExecutedExtent();
        }
        return f.spliced();
    }

    /** The class extent a PLAIN chain ranges over (rung 12): {@code Class.all()}
     * under filters only — no map, project, graphFetch, sort, cap or milestoning
     * argument — so the chain's values ARE the root class's instances and a frame
     * of its root rows, planned once, stands for every reader. Null otherwise. */
    public static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedGetAll
            plainExtentRoot(TypedSpec n) {
        while (n instanceof TypedFilter f) {
            n = f.source();
        }
        return n instanceof com.legend.compiler.spec.typed.TypedGetAll g
                && g.milestoning().isEmpty() && !g.versionSweep() && !g.forEachDate()
                ? g : null;
    }
}
