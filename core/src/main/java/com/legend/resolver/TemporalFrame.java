// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.MilestoningStrategy;
import com.legend.compiler.element.Temporal;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCLatestDate;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.values.PureDateLiteral;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
/**
 * THE per-resolution temporal machinery: the root fetch's
 * {@link TemporalContext}, the chain-keyed property-function specs, the
 * ONE propagation rule ({@link #contextAt}) and the stamp emitters. One
 * construction site (the op-chain phase), immutable after
 * {@link #withSpecs} — the mutable-field smear this replaces was the
 * recurring audit bug seam.
 */
final class TemporalFrame {

    /** The temporal STAMP's lambda row var — the typed-HIR provenance
     * marker (like PK_ORDER_PREFIX): a TypedFilter whose predicate binds
     * this var was added by the temporal stamp and MAY relocate onto a
     * join condition (memory milestoning-onclause-seam); no other filter
     * producer uses it. */
    static final String STAMP_ROW_VAR = "ms_row";

    /** Generated milestone-date KEYS in the milestone-columns map — ONE
     * spelling for the producer (milestoneColumnsOf) and the consumers
     * (Substitution generated-date reads, GraphEmission generatedDateLeaf).
     * Audit 23 contract consolidation. */
    static final String GEN_BUSINESS_DATE = "genBusinessDate";
    static final String GEN_PROCESSING_DATE = "genProcessingDate";

    private final ModelContext ctx;
    private final ClassSources sources;
    private final TemporalContext root;

    /** The ROOT query context date for a strategy axis — the serialize
     * envelope spells it in milestoned qualified-property KEYS
     * ({@code synonyms(2023-10-15T00:00:00+0000)}). Null when the axis
     * carries no context date. */
    com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec rootContextDate(
            boolean business) {
        return business ? root.business() : root.processing();
    }

    /** The spec a PLAIN (context-propagated) access to a milestoned
     * target would carry — the date splitter reserves the BASE join
     * identity for it, so an EXPLICIT date equal to the context shares
     * the propagated join and every other date mints its own head
     * (engine merge-by-identity;
     * testMilestoneDatePropogationThru*IsIndenpendent...). Null for
     * bitemporal targets (the pair's ordering rides other machinery)
     * and when the root context lacks the strategy's date. */
    @com.legend.base.Nullable TemporalSpec contextSpec(
            @com.legend.base.Nullable String targetClassFqn) {
        MilestoningStrategy strat = targetClassFqn == null ? null
                : temporalStrategy(targetClassFqn);
        if (strat != MilestoningStrategy.BUSINESS
                && strat != MilestoningStrategy.PROCESSING) {
            return null;
        }
        com.legend.compiler.spec.typed.TypedSpec d = root.dateFor(strat);
        return d == null ? null : new TemporalSpec(List.of(d), false);
    }
    private final Map<String, TemporalSpec> specs;
    /** Query-body {@code let} bindings (name &rarr; bound value), shared
     * BY REFERENCE with the resolver — a variable-spelled milestoning date
     * ({@code let d = %2015-10-16; Product.all($d)}) resolves through it
     * (engine resolveMilestoningDateParams' inScopeVars arm, M:648-651). */
    private final Map<String, TypedSpec> letEnv;

    TemporalFrame(ModelContext ctx, ClassSources sources,
            TemporalContext root, Map<String, TemporalSpec> specs) {
        this(ctx, sources, root, specs, Map.of());
    }

    TemporalFrame(ModelContext ctx, ClassSources sources,
            TemporalContext root, Map<String, TemporalSpec> specs,
            Map<String, TypedSpec> letEnv) {
        this.ctx = ctx;
        this.sources = sources;
        this.root = root;
        this.specs = specs;
        this.letEnv = letEnv;
    }

    /** The frame with the demand scan's chain-keyed specs attached. */
    TemporalFrame withSpecs(Map<String, TemporalSpec> byChain) {
        return new TemporalFrame(ctx, sources, root, byChain, letEnv);
    }

    /**
     * ROOT context derivation per the {@code all(...)} arity — the engine's
     * {@code getMilestoningContextForAll} (M:830-844): 0 dates = none
     * (sweep = allVersions); 1 date + temporal strategy = POINT; 2 dates =
     * bitemporal both-slots, else RANGE (the {@code getAll(C, start, end)}
     * / {@code allVersionsInRange} spellings). Dates normalize through the
     * let env and context reads first.
     */
    static TemporalFrame rootFrame(ModelContext ctx, ClassSources sources,
            Map<String, TypedSpec> letEnv, List<TypedSpec> dates,
            boolean versionSweep, String classFqn) {
        TemporalFrame seed = new TemporalFrame(ctx, sources,
                TemporalContext.NONE, Map.of(), letEnv);
        List<TypedSpec> nd = seed.normalizeContextDates(dates);
        MilestoningStrategy strat = seed.temporalStrategy(classFqn);
        TemporalContext rc = TemporalContext.NONE;
        if (versionSweep) {
            rc = nd.size() == 2
                    ? TemporalContext.range(strat, nd.get(0), nd.get(1))
                    : TemporalContext.NONE;
        } else if (nd.size() == 2 && strat == MilestoningStrategy.BITEMPORAL) {
            rc = TemporalContext.bitemporal(nd.get(0), nd.get(1));
        } else if (nd.size() == 2) {
            rc = TemporalContext.range(strat, nd.get(0), nd.get(1));
        } else if (nd.size() == 1 && strat != null) {
            rc = TemporalContext.single(strat, nd.get(0));
        }
        return new TemporalFrame(ctx, sources, rc, Map.of(), letEnv);
    }

    /** The NESTED frame for an inner scope hanging off a DATED hop
     * (exists/filter cursor over {@code product(%d)}): the hop's date
     * becomes the nested ROOT context (the engine's one-context-per-
     * cursor rule — propagation to inner temporal targets flows from
     * it), and specs under the hop's chain prefix re-key LOCALLY
     * ({@code product.classification -> classification}) so the inner
     * scope's own lookups see them. Null when the hop has no usable
     * single-date spec — the caller keeps the outer frame. */
    @com.legend.base.Nullable TemporalFrame nestedFrame(String hopClassFqn, String chainPrefix) {
        TemporalSpec hopSpec = specs.get(chainPrefix);
        MilestoningStrategy strat = temporalStrategy(hopClassFqn);
        TemporalFrame nf;
        if (hopSpec != null && !hopSpec.sweep()
                && hopSpec.dates().size() == 1 && strat != null) {
            nf = new TemporalFrame(ctx, sources,
                    TemporalContext.single(strat, hopSpec.dates().get(0)),
                    Map.of(), letEnv);
        } else if (specs.keySet().stream()
                .anyMatch(k -> k.startsWith(chainPrefix + "."))) {
            // UNDATED (or non-temporal-target) hop whose INNER hops carry
            // dates (constraint 1c: $this.referenceSystem->toOne()->
            // project(r|$r.systemDescription($d)...)): the cursor's own
            // context is unchanged — re-key the composed specs locally so
            // the nested registrations find their dates
            nf = new TemporalFrame(ctx, sources, root, Map.of(), letEnv);
        } else {
            return null;
        }
        Map<String, TemporalSpec> local = new java.util.LinkedHashMap<>();
        for (var e : specs.entrySet()) {
            if (e.getKey().startsWith(chainPrefix + ".")) {
                // a generated-date read ($this.businessDate inside the
                // hop's qualifier) NORMALIZES against the NESTED root —
                // it means 'this cursor's date', which is now the hop's
                local.put(e.getKey().substring(chainPrefix.length() + 1),
                        new TemporalSpec(nf.normalizeContextDates(
                                e.getValue().dates()), e.getValue().sweep()));
            }
        }
        return nf.withSpecs(local);
    }

    TemporalContext root() {
        return root;
    }

    @com.legend.base.Nullable TemporalSpec spec(String chainKey) {
        return specs.get(chainKey);
    }

    boolean hasSpecs() {
        return !specs.isEmpty();
    }

    /**
     * Temporal fetch {@code Class.all(%date)}: filter the materialized
     * pipeline by the main table's milestoning columns for the CLASS's
     * temporal dimension (engine {@code milestoningCanSupportTemporalStrategy}
     * — a processing-temporal class on a bi-temporal table must filter the
     * PROCESSING columns, never whichever block happens to be declared
     * first). Range form {@code from <= d AND thru > d} flips to
     * {@code from < d AND thru >= d} under the block's inclusivity flag;
     * {@code %latest} selects {@code thru = INFINITY_DATE}, which the
     * engine REQUIRES the table to declare (milestoning.pure
     * getInfinityDate assert) — never a hardcoded constant.
     */
    /**
     * Stamp a pipe by its ROOT TABLE's OWN milestoning blocks against the
     * context — one filter per dimension the table supports AND the
     * context carries (engine applyMilestoningFilters: the table's own
     * milestoning meets the ambient date; cross-dimension = no filter).
     * The one rule for PHYSICAL join targets and chained-PM mid tables.
     */
    TypedSpec stampByOwnBlocks(TypedSpec pipe, TemporalContext c,
            String label) {
        if (c.isEmpty()) {
            return pipe;
        }
        TypedSpec out = pipe;
        for (MilestoningStrategy dim : List.of(MilestoningStrategy.PROCESSING,
                MilestoningStrategy.BUSINESS)) {
            if (!tableHasBlock(out, dim)) {
                continue;
            }
            TypedSpec dimDate = c.dateFor(dim);
            if (c.rangeAppliesTo(dim)) {
                out = rangeScanPipe(out,
                        java.util.Objects.requireNonNull(c.rangeStart(), "c.rangeStart()"),
                        java.util.Objects.requireNonNull(c.rangeEnd(), "c.rangeEnd()"), dim);
            } else if (dimDate != null) {
                // an OUTER-ROW context date ($o.orderDate) cannot stamp
                // in-pipe (the read is out of scope) — the DEFERRED
                // sub-window on the head's join ON covers these targets
                // (stampForClassOrDefer registered it); skipping here is
                // the same window, not a dropped filter
                // OUTER-ROW reads (own-column, nav-read, or wrapped —
                // adjust($o...)) cannot stamp in-pipe; the join-ON
                // composition owns their window (#32)
                if (singleVarChain(dimDate) != null) {
                    continue;
                }
                out = milestonedPipeByStrategy(out, dimDate, dim, label);
            }
        }
        return out;
    }

    /**
     * Stamp a hop TARGET pipeline by its CLASS's temporality against the
     * context: bi-temporal takes both dimensions (a partial pair is loud —
     * the engine compile-rejects), single-dimension takes its own date or
     * range, cross-dimension takes nothing.
     */
    TypedSpec stampForClass(TypedSpec pipe, TemporalContext c,
            String classFqn) {
        MilestoningStrategy strat = temporalStrategy(classFqn);
        if (c.isEmpty() || strat == null) {
            return pipe;
        }
        if (strat == MilestoningStrategy.BITEMPORAL) {
            if (c.processing() != null && c.business() != null) {
                return milestonedPipeByStrategy(
                        milestonedPipeByStrategy(pipe, c.processing(),
                                MilestoningStrategy.PROCESSING, classFqn),
                        c.business(), MilestoningStrategy.BUSINESS, classFqn);
            }
            if (c.processing() != null || c.business() != null) {
                throw new MappingResolutionException("navigation to"
                        + " bi-temporal class '" + classFqn + "' requires"
                        + " processing and business dates", classFqn);
            }
            return pipe;
        }
        if (c.rangeAppliesTo(strat)) {
            return rangeMilestonedPipe(pipe,
                    java.util.Objects.requireNonNull(c.rangeStart(), "c.rangeStart()"),
                    java.util.Objects.requireNonNull(c.rangeEnd(), "c.rangeEnd()"),
                    classFqn);
        }
        TypedSpec d = c.dateFor(strat);
        return d == null ? pipe
                : milestonedPipeByStrategy(pipe, d, strat, classFqn);
    }

    /** OUTER-dated PROPAGATED sub-hops: subChain -> [fromCol, thruCol,
     * inclusive] — {@link #stampForClassOrDefer} DEFERS (an in-pipe stamp
     * would read the outer date var out of scope) and the head's join-ON
     * composition consumes ({@link #withDeferredOuterSubWindows}, both
     * routes; idempotent). Per-resolution frame lifecycle. */
    private final Map<String, String[]> deferredOuterSubWindows =
            new java.util.LinkedHashMap<>();
    /** The deferred entry's raw DATE EXPRESSION (same keys) — the window
     * composition re-applies its wrappers (adjust etc.) over the outer
     * column read (#32 part 2). */
    private final Map<String, TypedSpec> deferredOuterSubDates =
            new java.util.LinkedHashMap<>();
    /** Heads whose outer-dated window RODE THE JOIN ON (the
     * applyJoinTemporalFilters composition) — the frame-WHERE channel
     * ({@link #applyOuterNavDateFilters}) must skip them: a WHERE copy
     * of the same window kills the LEFT semantics (W40: the id=1
     * NULL-classification row must survive). */
    private final java.util.Set<String> onWindowedHeads =
            new java.util.LinkedHashSet<>();

    /**
     * {@link #stampForClass} EXCEPT when the context's single-dimension
     * date is an OUTER-ROW read ({@code $o.orderDate} inherited through
     * the head's spec): stamping in-pipe would embed an out-of-scope
     * read — DEFER; the head's join-ON windows the sub's prefixed
     * columns against the outer date (engine: the depth-2 hop's ON reads
     * "root".orderDate too). Underivable shapes fall back to the stamp
     * (its loud wall beats a silent version fan).
     */
    TypedSpec stampForClassOrDefer(TypedSpec pipe, TemporalContext c,
            String classFqn, String chain) {
        MilestoningStrategy strat = temporalStrategy(classFqn);
        TypedSpec stratDate = strat == null ? null : c.dateFor(strat);
        if (strat != null && strat != MilestoningStrategy.BITEMPORAL && !c.isEmpty()
                && !c.rangeAppliesTo(strat) && chain != null && stratDate != null
                && outerRead(stratDate) != null
                && deferWindow(chain, strat, pipe, stratDate)) {
            return pipe;
        }
        // BITEMPORAL: per-dimension — the LITERAL dimension stamps
        // in-pipe, the OUTER-read dimension defers ITS window
        if (strat == MilestoningStrategy.BITEMPORAL && !c.isEmpty() && chain != null
                && c.processing() != null && c.business() != null) {
            boolean pOuter = outerRead(c.processing()) != null;
            boolean bOuter = outerRead(c.business()) != null;
            if (pOuter || bOuter) {
                TypedSpec out = pipe;
                if (pOuter) {
                    if (!deferWindow(chain, MilestoningStrategy.PROCESSING, pipe,
                            c.processing())) {
                        return stampForClass(pipe, c, classFqn);
                    }
                } else {
                    out = milestonedPipeByStrategy(out, c.processing(),
                            MilestoningStrategy.PROCESSING, classFqn);
                }
                if (bOuter) {
                    if (!deferWindow(chain, MilestoningStrategy.BUSINESS, pipe,
                            c.business())) {
                        return stampForClass(pipe, c, classFqn);
                    }
                } else {
                    out = milestonedPipeByStrategy(out, c.business(),
                            MilestoningStrategy.BUSINESS, classFqn);
                }
                return out;
            }
        }
        return stampForClass(pipe, c, classFqn);
    }

    /** The property name of an outer-row date read ({@code $o.orderDate},
     * toOne-wrapped or bare) — or the COMPOSED column candidate of a
     * NAV-READ date ({@code $o.orderDetails.settlementDate} &rarr;
     * {@code orderDetails_settlementDate}, #32: the sunk navigate step
     * exposes it on the head's left row; a wrong candidate dies loud at
     * the window's column lookup, never silently). Null otherwise. */
    private @com.legend.base.Nullable String outerRead(TypedSpec d) {
        List<String> ch = singleVarChain(d);
        return ch == null ? null : String.join("_", ch);
    }

    /** The SINGLE var-rooted property chain inside a date expression —
     * the expr may WRAP the read (adjust($o.orderDetails.settlementDate,
     * -1, DAYS)); exactly one chain of 1-2 hops = the outer read, else
     * null (a two-chain date has no single window column). */
    static @com.legend.base.Nullable List<String> singleVarChain(TypedSpec d) {
        if (d == null) {
            return null;
        }
        List<List<String>> chains = new ArrayList<>();
        collectVarChains(unwrapToOne(d), chains);
        return chains.size() == 1 && chains.get(0).size() <= 2
                ? chains.get(0) : null;
    }

    private static void collectVarChains(TypedSpec n,
            List<List<String>> out) {
        TypedSpec n0 = unwrapToOne(n);
        List<String> path = new ArrayList<>();
        TypedSpec cur = n0;
        while (cur instanceof TypedPropertyAccess pa) {
            path.add(0, pa.property());
            cur = unwrapToOne(pa.source());
        }
        if (!path.isEmpty()
                && cur instanceof com.legend.compiler.spec.typed.TypedVariable) {
            out.add(path);
            return;
        }
        for (TypedSpec c : n0.children()) {
            collectVarChains(c, out);
        }
    }

    /** The spec date EXPRESSION with its inner outer-row read replaced
     * by {@code colRead} — wrappers (adjust etc.) survive, so the window
     * compares against the TRANSFORMED date (engine: dateadd on the
     * join ON). Null when the date IS the bare read (no wrapper). */
    private static @com.legend.base.Nullable TypedSpec wrapOuterDate(TypedSpec specDate,
            TypedSpec colRead) {
        TypedSpec d0 = unwrapToOne(specDate);
        if (d0 instanceof TypedPropertyAccess) {
            return null;
        }
        if (d0 instanceof TypedNativeCall c) {
            List<TypedSpec> args = new ArrayList<>();
            boolean changed = false;
            for (TypedSpec a : c.args()) {
                TypedSpec a0 = unwrapToOne(a);
                if (a0 instanceof TypedPropertyAccess
                        && singleVarChain(a0) != null) {
                    args.add(colRead);
                    changed = true;
                } else {
                    TypedSpec inner = wrapOuterDate(a, colRead);
                    args.add(inner == null ? a : inner);
                    changed |= inner != null;
                }
            }
            return changed ? c.withChildren(args)
                    : null;
        }
        return null;
    }

    /** Register the deferred window for one dimension; false when the
     * sub table's block is underivable (caller keeps the loud stamp). */
    private boolean deferWindow(String chain, MilestoningStrategy strat, TypedSpec pipe,
            TypedSpec date) {
        var rt0 = rootTable(pipe);
        var ms0 = rt0 == null ? null
                : ctx.findTableMilestoning(rt0.store(), rt0.table())
                        .orElse(null);
        String f0 = null;
        String t0 = null;
        boolean inc0 = false;
        if (strat == MilestoningStrategy.BUSINESS && ms0 != null
                && ms0.business() != null
                && ms0.business().snapshotDate() == null) {
            f0 = ms0.business().from();
            t0 = ms0.business().thru();
            inc0 = ms0.business().thruIsInclusive();
        } else if (strat == MilestoningStrategy.PROCESSING && ms0 != null
                && ms0.processing() != null
                && ms0.processing().snapshotDate() == null) {
            f0 = ms0.processing().in();
            t0 = ms0.processing().out();
            inc0 = ms0.processing().outIsInclusive();
        }
        if (f0 == null || t0 == null) {
            return false;
        }
        deferredOuterSubWindows.put(chain + "#" + strat,
                new String[]{f0, t0, String.valueOf(inc0),
                        outerRead(date) == null ? "" : outerRead(date)});
        deferredOuterSubDates.put(chain + "#" + strat, date);
        return true;
    }

    /** AND the DEFERRED sub-hop windows for {@code chainHead} onto the
     * head's join condition: each deferred outer-dated sub-hop's prefixed
     * milestone columns window against the SAME outer date (idempotent —
     * entries stay for sibling head identities). */
    /** FLATTEN rung (#81): deferred outer-dated SUB-hops leave the head
     * composite and join as TOP-LEVEL siblings — the engine's flat shape
     * (testBusinessDateMilestoning.pure:598). Composing the sub window
     * into the HEAD's ON is row-wrong: a present-but-failing sub row must
     * null ONLY the sub columns (LEFT sibling does that), never kill the
     * head match. Returns null when a sub-join is not liftable off the
     * composite's spine (caller falls back to guarded composition). */
    private @com.legend.base.Nullable TypedSpec hoistDeferredOuterSubJoins(TypedJoin j,
            TypedSpec processedLeft, String chainHead, String outerCol,
            String navClass) {
        Type.RelationType rRow = Type.requireRelationSchema(j.right().info().type());
        Map<String, List<String[]>> byPfx = new LinkedHashMap<>();
        Map<String, List<String>> byPfxKeys = new LinkedHashMap<>();
        for (var de : deferredOuterSubWindows.entrySet()) {
            if (!de.getKey().startsWith(chainHead + ".")) {
                continue;
            }
            String subProp = de.getKey().substring(chainHead.length() + 1);
            if (subProp.indexOf('#') >= 0) {
                subProp = subProp.substring(0, subProp.indexOf('#'));
            }
            String[] w = de.getValue();
            String pfx = null;
            for (String cand : new String[]{subProp + "_", subProp + "_nav_"}) {
                final String probe = cand + w[0];
                if (rRow.columns().stream()
                        .anyMatch(x -> x.name().equalsIgnoreCase(probe))) {
                    pfx = cand;
                    break;
                }
            }
            if (pfx == null) {
                return null;
            }
            byPfx.computeIfAbsent(pfx, k -> new ArrayList<>()).add(w);
            byPfxKeys.computeIfAbsent(pfx, k -> new ArrayList<>())
                    .add(de.getKey());
        }
        if (byPfx.isEmpty()) {
            return null;
        }
        // detach each sub-join off the composite spine (joins above it
        // would need a column-subtraction rebuild — not liftable yet)
        TypedSpec stripped = j.right();
        List<TypedJoin> subJoins = new ArrayList<>();
        for (String pfx : byPfx.keySet()) {
            TypedJoin[] found = new TypedJoin[1];
            stripped = detachSpineJoin(stripped, pfx, found);
            if (stripped == null) {
                return null;
            }
            subJoins.add(found[0]);
        }
        // the head join's prefix renames its right side, so the composed
        // read names are <headPfx><subPfx><col> — the sibling reproduces
        // them with the composed prefix and re-points the sub condition's
        // head-side reads under the head prefix
        String headPfx = j.prefix().orElse("");
        List<String> pfxs = new ArrayList<>(byPfx.keySet());
        Type.RelationType origRow = Type.requireRelationSchema(j.info().type());
        List<Type.Column> headCols = new ArrayList<>();
        for (Type.Column c : origRow.columns()) {
            boolean subCol = pfxs.stream()
                    .anyMatch(p -> c.name().startsWith(headPfx + p));
            if (!subCol) {
                headCols.add(c);
            }
        }
        TemporalSpec headSpec = specs.get(chainHead);
        TypedSpec headDate = headSpec != null && headSpec.dates().size() == 1
                ? headSpec.dates().get(0) : null;
        TypedSpec out = new TypedJoin(processedLeft, stripped, j.kind(),
                outerDatedCond(j.condition(), j.left(), stripped, navClass,
                        outerCol, headDate),
                j.prefix(), j.frameName(),
                new ExprType(Type.relation(new Type.RelationType(headCols)),
                        Multiplicity.Bounded.ONE),
                j.userCondition() /* rebuild */);
        for (int i = 0; i < pfxs.size(); i++) {
            String pfx = pfxs.get(i);
            TypedJoin sj = subJoins.get(i);
            // head-side reads of the sub condition re-point under the head
            // prefix (they read composite-row names the head join renamed);
            // the target side is the raw sub pipe, names unchanged
            TypedLambda c = sj.condition();
            if (!headPfx.isEmpty()) {
                String sv = c.parameters().get(0);
                ExprType lInfo = new ExprType(
                        Type.requireRelationSchema(out.info().type()),
                        Multiplicity.Bounded.ONE);
                List<TypedSpec> body = new ArrayList<>();
                for (TypedSpec b : c.body()) {
                    body.add(Pipelines.prefixColumns(b, sv, headPfx,
                            v -> new TypedVariable(sv, lInfo)));
                }
                c = new TypedLambda(c.parameters(), body, c.info());
            }
            var ws = java.util.Objects.requireNonNull(
                    byPfx.get(pfx));
            for (int k = 0; k < ws.size(); k++) {
                String[] w = ws.get(k);
                String entryOuter = w.length > 3 && !w[3].isEmpty()
                        ? w[3] : outerCol;
                c = outerDatedWindowCond(c, out, sj.right(), w[0], w[1],
                        Boolean.parseBoolean(w[2]), entryOuter, navClass,
                        /*nullTolerant*/ false, deferredOuterSubDates.get(
                                java.util.Objects.requireNonNull(byPfxKeys.get(pfx)).get(k)));
            }
            Type.RelationType prev = Type.requireRelationSchema(out.info().type());
            List<Type.Column> cols = new ArrayList<>(prev.columns());
            for (Type.Column sc : (Type.requireRelationSchema(sj.right().info().type())).columns()) {
                String nm = headPfx + pfx + sc.name();
                Type.Column oc = origRow.columns().stream()
                        .filter(x -> x.name().equalsIgnoreCase(nm))
                        .findFirst().orElse(null);
                cols.add(oc != null ? oc
                        : new Type.Column(nm, sc.type(), sc.multiplicity()));
            }
            out = new TypedJoin(out, sj.right(), sj.kind(), c,
                    java.util.Optional.of(headPfx + pfx), sj.frameName(),
                    new ExprType(Type.relation(new Type.RelationType(cols)),
                            Multiplicity.Bounded.ONE),
                sj.userCondition() /* rebuild */);
        }
        return out;
    }

    /** The composite minus the spine join carrying {@code pfx} (returned
     * through {@code found}); filters above it rebuild, a JOIN above it
     * returns null (that shape needs column subtraction — fallback). */
    private static @com.legend.base.Nullable TypedSpec detachSpineJoin(TypedSpec pipe, String pfx,
            TypedJoin[] found) {
        if (pipe instanceof TypedJoin sj) {
            if (sj.prefix().isPresent() && sj.prefix().orElseThrow().equals(pfx)) {
                found[0] = sj;
                return sj.left();
            }
            return null;
        }
        if (pipe instanceof TypedFilter f) {
            TypedSpec src = detachSpineJoin(f.source(), pfx, found);
            return src == null ? null
                    : new TypedFilter(src, f.predicate(), src.info(),
                            f.stamp());
        }
        return null;
    }

    TypedLambda withDeferredOuterSubWindows(TypedLambda cond, TypedSpec left,
            TypedSpec right, String chainHead, String outerCol,
            String navClass) {
        for (var de : deferredOuterSubWindows.entrySet()) {
            if (!de.getKey().startsWith(chainHead + ".")) {
                continue;
            }
            String subProp = de.getKey().substring(chainHead.length() + 1);
            // per-dimension entries key '<chain>#<strategy>' (bitemp split)
            if (subProp.indexOf('#') >= 0) {
                subProp = subProp.substring(0, subProp.indexOf('#'));
            }
            String[] w = de.getValue();
            // the entry's own outer column (a bitemp dimension's date can
            // differ from the head's) wins over the head's odc
            String entryOuter = w.length > 3 && !w[3].isEmpty() ? w[3] : outerCol;
            Type.RelationType rRow = Type.requireRelationSchema(right.info().type());
            String pfx = null;
            for (String cand : new String[]{subProp + "_", subProp + "_nav_"}) {
                final String probe = cand + w[0];
                if (rRow.columns().stream()
                        .anyMatch(x -> x.name().equalsIgnoreCase(probe))) {
                    pfx = cand;
                    break;
                }
            }
            if (pfx == null) {
                throw new com.legend.error.NotImplementedException(
                        "outer-dated propagated sub-hop '" + de.getKey()
                        + "': its milestone columns are not on the head's"
                        + " join target row — the deferred window cannot"
                        + " compose (would fan versions silently)");
            }
            cond = outerDatedWindowCond(cond, left, right, pfx + w[0],
                    pfx + w[1], Boolean.parseBoolean(w[2]), entryOuter,
                    navClass, /*nullTolerant*/ true,
                    deferredOuterSubDates.get(de.getKey()));
        }
        return cond;
    }

    TypedSpec milestonedPipe(TypedSpec pipe, TypedSpec date, String classFqn) {
        MilestoningStrategy strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("milestoned fetch of '" + classFqn
                    + "': the class declares no temporal stereotype", classFqn);
        }
        return milestonedPipeByStrategy(pipe, date, strategy, classFqn);
    }

    /**
     * The temporal point filter for a pipeline whose ROOT TABLE carries a
     * milestoning block for {@code strategy} — shared by class fetches
     * (strategy from the class stereotype) and PHYSICAL join targets
     * (strategy from the QUERY's temporal context: the engine filters
     * EVERY milestoned table alias in the generated query).
     */
    /** The PARENT COLUMN a spec's single date reads ({@code
     * $o.orderDate->toOne()} &rarr; {@code orderDate}'s physical column
     * via the parent binding) — null when the spec is absent, multi-date,
     * sweep, or not an outer-row read. */
    private @com.legend.base.Nullable String outerColumnDate(
            @com.legend.base.Nullable TemporalSpec spec, ClassSource cs) {
        if (spec == null || spec.sweep() || spec.dates().size() != 1) {
            return null;
        }
        return outerReadColumn(spec.dates().get(0), cs);
    }

    /** Join-walk overload: the NAV-READ date arm probes the JOIN's actual
     * left row (materialized — the ClassSource row still holds raw
     * slots), so a date living on an already-joined row resolves to its
     * composed column and rides the ordinary outer-date calculus. */
    private @com.legend.base.Nullable String outerColumnDate(
            @com.legend.base.Nullable TemporalSpec spec, ClassSource cs,
            Type.RelationType leftRow) {
        String own = outerColumnDate(spec, cs);
        if (own != null || spec == null || spec.sweep()
                || spec.dates().size() != 1) {
            return own;
        }
        List<String> ch = singleVarChain(spec.dates().get(0));
        if (ch != null && ch.size() == 2) {
            for (String cand : new String[]{
                    ch.get(0) + "_" + ch.get(1),
                    ch.get(0) + "_nav_" + ch.get(1)}) {
                if (leftRow.columns().stream()
                        .anyMatch(c -> c.name().equalsIgnoreCase(cand))) {
                    return cand;
                }
            }
            if (System.getenv("LEGEND_LITE_NAVDATE_TRACE") != null) {
                System.err.println("[navdate] left row lacks composed column "
                        + String.join(".", ch) + ": "
                        + leftRow.columns().stream()
                                .map(Type.Column::name).toList());
            }
        }
        return null;
    }

    /** [processing, business] dates for a bi-temporal hop: an explicit
     * 2-date spec; else both root-context slots (through a temporal
     * parent); else the engine-generated 1-DATE form (the param is the
     * dimension the OWNER lacks, the owner's own fills from the context).
     * Null when underivable. */
    private @com.legend.base.Nullable List<TypedSpec> biTemporalDatesFor(
            @com.legend.base.Nullable TemporalSpec spec,
            ClassSource parent) {
        return biTemporalDatesFor(spec, parent, null);
    }

    private @com.legend.base.Nullable List<TypedSpec> biTemporalDatesFor(
            @com.legend.base.Nullable TemporalSpec spec,
            ClassSource parent,
            @com.legend.base.Nullable TemporalSpec parentSpec) {
        if (spec != null && !spec.sweep() && spec.dates().size() == 2) {
            return spec.dates();
        }
        // BITEMP UNDER BITEMP: the parent chain's own PAIR is the
        // inherited context (engine testBiTemporalDateInjection...: the
        // sub inherits both dimensions; a 1-date sub spec is the BUSINESS
        // dimension, processing rides the parent's — result2 golden)
        if (parentSpec != null && !parentSpec.sweep()
                && parentSpec.dates().size() == 2) {
            if (spec == null || spec.sweep() || spec.dates().isEmpty()) {
                return parentSpec.dates();
            }
            if (spec.dates().size() == 1) {
                return List.of(parentSpec.dates().get(0),
                        spec.dates().get(0));
            }
        }
        MilestoningStrategy parentStrat = temporalStrategy(parent.classFqn());
        if (root.processing() != null && root.business() != null
                && parentStrat != null) {
            return List.of(root.processing(), root.business());
        }
        if (spec != null && !spec.sweep() && spec.dates().size() == 1) {
            TypedSpec ownerDate = root.dateFor(parentStrat);
            if (ownerDate != null && parentStrat == MilestoningStrategy.BUSINESS) {
                return List.of(spec.dates().get(0), ownerDate);
            }
            if (ownerDate != null
                    && parentStrat == MilestoningStrategy.PROCESSING) {
                return List.of(ownerDate, spec.dates().get(0));
            }
        }
        return null;
    }

    /** BITEMPORAL outer-dated compose: BOTH dimension windows AND into the
     * join condition, each date an outer-col read or a literal (engine
     * testBiTemporalDateMilestoning:279; mixed variants :276-277). Null
     * when the head is not bitemporal-with-an-outer-date. */
    @com.legend.base.Nullable TypedLambda outerBiDatedJoinCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, ClassSource parent, ClassSource target,
            String head) {
        if (temporalStrategy(target.classFqn()) != MilestoningStrategy.BITEMPORAL) {
            return null;
        }
        List<TypedSpec> dates = biTemporalDatesFor(specs.get(head), parent);
        if (dates == null) {
            return null;
        }
        String pCol = outerReadColumn(dates.get(0), parent);
        String bCol = outerReadColumn(dates.get(1), parent);
        if (pCol == null && bCol == null) {
            return null;   // both literal: the stamped path serves it
        }
        TypedTableReference rt = rootTable(right);
        var ms = rt == null ? null
                : ctx.findTableMilestoning(rt.store(), rt.table()).orElse(null);
        if (ms == null || ms.processing() == null || ms.business() == null
                || ms.processing().snapshotDate() != null
                || ms.business().snapshotDate() != null) {
            throw new com.legend.error.NotImplementedException(
                    "outer-row bitemporal date: target table "
                    + (rt == null ? "?" : "'" + rt.table() + "'")
                    + " lacks plain processing+business blocks");
        }
        String sv = cond.parameters().get(0);
        String tv = cond.parameters().get(1);
        Type.RelationType lRow = Type.requireRelationSchema(left.info().type());
        Type.RelationType rRow = Type.requireRelationSchema(right.info().type());
        java.util.function.BiFunction<String, String, TypedSpec> col =
                (var vn, var name) -> {
                    Type.RelationType row = vn.equals(sv) ? lRow : rRow;
                    Type.Column c = row.columns().stream()
                            .filter(x -> x.name().equalsIgnoreCase(name))
                            .findFirst().orElseThrow(() ->
                                    new MappingResolutionException(
                                            "bitemporal window column '" + name
                                            + "' is not on the join row",
                                            target.classFqn()));
                    return new TypedPropertyAccess(new com.legend.compiler
                            .spec.typed.TypedVariable(vn, new ExprType(row,
                                    Multiplicity.Bounded.ONE)),
                            c.name(), new ExprType(c.type(), Multiplicity.Bounded.ONE));
                };
        java.util.function.BiFunction<String, TypedSpec, TypedSpec> dexpr =
                (var outerCol, var literal) -> outerCol != null
                        ? col.apply(sv, outerCol) : unwrapToOne(literal);
        ExprType boolT = new ExprType(Type.Primitive.BOOLEAN,
                Multiplicity.Bounded.ONE);
        TypedSpec pd = dexpr.apply(pCol, dates.get(0));
        TypedSpec bd = dexpr.apply(bCol, dates.get(1));
        TypedSpec win = cmpCall("meta::pure::functions::boolean::and",
                windowPair(col.apply(tv, ms.processing().in()),
                        col.apply(tv, ms.processing().out()), pd,
                        ms.processing().outIsInclusive(), boolT),
                windowPair(col.apply(tv, ms.business().from()),
                        col.apply(tv, ms.business().thru()), bd,
                        ms.business().thruIsInclusive(), boolT), boolT);
        TypedSpec merged = cmpCall("meta::pure::functions::boolean::and",
                cond.body().get(cond.body().size() - 1), win, boolT);
        return new TypedLambda(cond.parameters(), List.of(merged),
                cond.info());
    }

    /** Window dates print through the engine's LEGACY channel (uppercase
     * dateadd units — {@code Pure.Lite.ADJUST_TEMPORAL} javadoc): every
     * adjust call inside the date expression swaps to the channel-marked
     * twin (same shape, same execution; only the golden spelling moves). */
    private TypedSpec stampTemporalAdjust(TypedSpec d) {
        TypedSpec r = d.mapChildren(this::stampTemporalAdjust);
        if (r instanceof TypedNativeCall c && "meta::pure::functions::date::adjust"
                .equals(c.callee().qualifiedName())) {
            var fns = ctx.findFunction(
                    com.legend.builtin.Pure.Lite.ADJUST_TEMPORAL);
            if (fns.size() == 1) {
                return new TypedNativeCall(fns.get(0), c.args(), c.info());
            }
        }
        return r;
    }

    /** {@code from <= d AND thru > d} (inclusive flips the pair). */
    private TypedSpec windowPair(TypedSpec fromRead, TypedSpec thruRead,
            TypedSpec d, boolean inclusive, ExprType boolT) {
        return inclusive
                ? cmpCall("meta::pure::functions::boolean::and",
                        dateCmpCall("meta::pure::functions::boolean::lessThan",
                                fromRead, d, boolT),
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "greaterThanEqual", thruRead, d, boolT),
                        boolT)
                : cmpCall("meta::pure::functions::boolean::and",
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "lessThanEqual", fromRead, d, boolT),
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "greaterThan", thruRead, d, boolT), boolT);
    }

    /** ONE date expression's source-row physical column ({@code
     * $o.orderDate->toOne()} &rarr; the parent binding's column), or null
     * when the date is not a direct outer-row read. */
    private @com.legend.base.Nullable String outerReadColumn(TypedSpec d, ClassSource cs) {
        d = unwrapToOne(d);
        // NAV-READ date (#32): $o.<hop1>.<leaf> — the date lives on a
        // JOINED row; usable when the parent's materialized row already
        // carries the composed column (the hop1 join was demanded), same
        // outer-date calculus from there (engine: the frame exposes the
        // date column and every window reads it)
        if (d instanceof TypedPropertyAccess pa2
                && unwrapToOne(pa2.source()) instanceof TypedPropertyAccess pb2
                && pb2.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable) {
            for (String cand : new String[]{
                    pb2.property() + "_" + pa2.property(),
                    pb2.property() + "_nav_" + pa2.property()}) {
                if (cs.rowType().columns().stream()
                        .anyMatch(c -> c.name().equalsIgnoreCase(cand))) {
                    return cand;
                }
            }
            if (System.getenv("LEGEND_LITE_NAVDATE_TRACE") != null) {
                System.err.println("[navdate] no composed column for "
                        + pb2.property() + "." + pa2.property() + " on row: "
                        + cs.rowType().columns().stream()
                                .map(Type.Column::name).toList());
            }
            return null;
        }
        if (!(d instanceof TypedPropertyAccess pa)
                || !(pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable)) {
            return null;
        }
        TypedSpec b = cs.bindings().get(pa.property());
        b = b == null ? null : unwrapToOne(b);
        if (b instanceof TypedPropertyAccess pb
                && pb.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable) {
            return pb.property();
        }
        // FOR-EACH-DATE: a generated-date hop arg reads THE DATES
        // COLUMN on the source row (the per-row calendar date IS the
        // hop's context — temporalDateProjectionQuery's qualified
        // chains pick the version live AT that date)
        if ((pa.property().equals("businessDate")
                        || pa.property().equals("processingDate"))
                && b == null && forEachDateColumn != null) {
            return forEachDateColumn;
        }
        // VERSION SWEEP: a GENERATED-date hop arg ($this.businessDate
        // under allVersions/allVersionsInRange — the root context has no
        // point date, or normalizeContextDate would have replaced the
        // read) means each version row's OWN validity start — the
        // engine's golden joins on parent.from_z (testTemporalRangeQuery:
        // classificationTypeStr over allVersionsInRange).
        if ((pa.property().equals("businessDate")
                        || pa.property().equals("processingDate"))
                && b == null
                && root.dateFor(temporalStrategy(cs.classFqn())) == null) {
            TypedTableReference rt = rootTable(cs.pipeline());
            var ms = rt == null ? null
                    : ctx.findTableMilestoning(rt.store(), rt.table())
                            .orElse(null);
            if (pa.property().equals("businessDate") && ms != null
                    && ms.business() != null
                    && ms.business().snapshotDate() == null) {
                return ms.business().from();
            }
            if (pa.property().equals("processingDate") && ms != null
                    && ms.processing() != null
                    && ms.processing().snapshotDate() == null) {
                return ms.processing().in();
            }
        }
        return null;
    }

    /** The chain-spec date's SOURCE-ROW physical column for {@code head},
     * or null when the spec is absent / not an outer-row read — the
     * callers' switch between pipe-stamping and join-composition. */
    @com.legend.base.Nullable String outerDateColumn(String head, ClassSource parent) {
        return outerColumnDate(specs.get(head), parent);
    }

    /** FORM-2 outer date: {@code $o.<nav>.<leaf>->toOne()} — the date reads
     * a column of ANOTHER nav on the outer frame (engine golden
     * testBusinessDateMilestoning:569: window in the outer WHERE reading
     * orderdetailstable_0.settlementDate). Null when not that shape. */
    record OuterNavDate(String navHead, String leafColumn) {
    }

    @com.legend.base.Nullable OuterNavDate outerNavDate(String head, ClassSource cs) {
        TemporalSpec spec = specs.get(head);
        if (spec == null || spec.sweep() || spec.dates().size() != 1) {
            return null;
        }
        TypedSpec d = unwrapToOne(spec.dates().get(0));
        if (!(d instanceof TypedPropertyAccess leaf)
                || !(leaf.source() instanceof TypedPropertyAccess nav)
                || !(nav.source()
                        instanceof com.legend.compiler.spec.typed.TypedVariable)) {
            return null;
        }
        TypedSpec navB = cs.bindings().get(nav.property());
        if (navB == null) {
            return null;
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = InnerDemand.navSlotAlias(navB, cs.rowVar(),
                navSteps.keySet());
        if (alias == null
                || !(java.util.Objects.requireNonNull(navSteps.get(alias)).target()
                        instanceof com.legend.compiler.spec.typed.TypedGetAll g)) {
            return null;
        }
        TypedSpec lb = sources.navTarget(cs, g.classFqn(), navSteps.get(alias), alias)
                .bindings().get(leaf.property());
        lb = lb == null ? null : unwrapToOne(lb);
        return lb instanceof TypedPropertyAccess pb
                && pb.source()
                        instanceof com.legend.compiler.spec.typed.TypedVariable
                ? new OuterNavDate(nav.property(), pb.property()) : null;
    }

    private static TypedSpec unwrapToOne(TypedSpec d) {
        return d instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                ? c.args().get(0) : d;
    }

    /** The TARGET class of a nav-slot head on the outer pipeline. */
    private @com.legend.base.Nullable String hopNavClass(String head, ClassSource cs) {
        TypedSpec b = cs.bindings().get(head);
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = b == null ? null
                : InnerDemand.navSlotAlias(b, cs.rowVar(), navSteps.keySet());
        return alias != null && java.util.Objects.requireNonNull(navSteps.get(alias)).target()
                instanceof com.legend.compiler.spec.typed.TypedGetAll g
                ? g.classFqn() : null;
    }

    /** The materialized prefix of a nav-slot head on the outer frame. */
    private @com.legend.base.Nullable String prefixOf(String head, ClassSource cs,
            Map<String, String> slotPrefixes) {
        TypedSpec b = cs.bindings().get(head);
        String alias = b == null ? null
                : InnerDemand.navSlotAlias(b, cs.rowVar(),
                        Pipelines.navSteps(cs.pipeline()).keySet());
        return alias == null ? null : slotPrefixes.get(alias);
    }

    /** Apply every head's form-2 window onto the JOINED outer frame:
     * {@code <hopPfx>from <= <navPfx>leaf AND <hopPfx>thru > <navPfx>leaf}
     * (inclusive flag flips per the table block). Identity when no head
     * carries an outer-nav date; LOUD when the frame lacks a needed
     * column — never a silently dropped window. */
    Pipelines.Materialized applyOuterNavDateFilters(ClassSource cs,
            Pipelines.Materialized m,
            Map<String, AssociationJoins.AssocJoin> joinsByChain) {
        TypedSpec out = applyOuterNavDateFilters(cs, m.pipeline(),
                joinsByChain, m.slotPrefixes());
        return out == m.pipeline() ? m
                : new Pipelines.Materialized(out, m.slotPrefixes(),
                        m.stripped());
    }

    private TypedSpec applyOuterNavDateFilters(ClassSource cs, TypedSpec frame,
            Map<String, AssociationJoins.AssocJoin> joinsByChain,
            Map<String, String> slotPrefixes) {
        TypedSpec out = frame;
        // Every spec'd head, whatever route materialized it (assoc join OR
        // nav slot) — a route not covered here would SILENTLY drop the
        // window, the one unshippable failure mode (loud below instead).
        for (String head : specs.keySet()) {
            OuterNavDate odn = outerNavDate(head, cs);
            if (odn == null) {
                continue;
            }
            // the join-ON channel already windowed this head — a WHERE
            // copy over the joined frame would drop the head's LEFT NULL
            // rows (W40: engine keeps [1|<null>]); the two channels are
            // EXCLUSIVE per head
            if (onWindowedHeads.contains(head)) {
                continue;
            }
            // SYNTHETIC identities (product#d0, the lifted-head suffix) key
            // the SPEC and their OWN join; bindings speak the REAL
            // property. NEVER fall back to another identity's join — the
            // window would bind to the wrong-dated frame (twoDatesOneChain).
            String real = SyntheticHeads.realHead(head);
            AssociationJoins.AssocJoin aj = joinsByChain.get(head);
            String hopClass = aj != null ? aj.target().classFqn()
                    : hopNavClass(real, cs);
            String hopPfx = aj != null ? aj.prefix()
                    : prefixOf(real, cs, slotPrefixes);
            if (hopClass == null || hopPfx == null) {
                throw new com.legend.error.NotImplementedException(
                        "outer-nav milestoning date: dated hop '" + head
                        + "' has no materialized join on the outer frame");
            }
            TypedTableReference rt = rootTable(aj != null
                    ? aj.targetPipeline()
                    : sources.navTarget(cs, hopClass, ClassSources.stepOf(cs, real), real).pipeline());
            var ms = rt == null ? null
                    : ctx.findTableMilestoning(rt.store(), rt.table())
                            .orElse(null);
            MilestoningStrategy strat = temporalStrategy(hopClass);
            String fromCol;
            String thruCol;
            boolean inclusive;
            if (strat == MilestoningStrategy.BUSINESS && ms != null
                    && ms.business() != null
                    && ms.business().snapshotDate() == null) {
                fromCol = ms.business().from();
                thruCol = ms.business().thru();
                inclusive = ms.business().thruIsInclusive();
            } else if (strat == MilestoningStrategy.PROCESSING && ms != null
                    && ms.processing() != null
                    && ms.processing().snapshotDate() == null) {
                fromCol = ms.processing().in();
                thruCol = ms.processing().out();
                inclusive = ms.processing().outIsInclusive();
            } else {
                throw new com.legend.error.NotImplementedException(
                        "outer-nav milestoning date over a "
                        + (strat == null ? "non-temporal" : strat)
                        + " target ('" + hopClass
                        + "') is not supported yet");
            }
            // read through the SAME materialization the demand produced:
            // an assoc-route join for the nav wins over the slot prefix —
            // a second read-side join would fan the rows out.
            AssociationJoins.AssocJoin navAj = joinsByChain.get(odn.navHead());
            String navPfx = navAj != null ? navAj.prefix()
                    : prefixOf(odn.navHead(), cs, slotPrefixes);
            if (navPfx == null) {
                throw new com.legend.error.NotImplementedException(
                        "outer-nav milestoning date: nav '" + odn.navHead()
                        + "' is not materialized on the outer frame");
            }
            Type.RelationType row = Type.requireRelationSchema(out.info().type());
            String rv = "_odw";
            java.util.function.Function<String, TypedSpec> read = name -> {
                Type.Column c = row.columns().stream()
                        .filter(x -> x.name().equalsIgnoreCase(name))
                        .findFirst().orElseThrow(() ->
                                new com.legend.error.NotImplementedException(
                                        "outer-nav milestoning window column '"
                                        + name + "' is not on the outer frame"));
                return new TypedPropertyAccess(new com.legend.compiler.spec
                        .typed.TypedVariable(rv, new ExprType(row,
                                Multiplicity.Bounded.ONE)),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
            };
            TypedSpec dateRead = read.apply(navPfx + odn.leafColumn());
            ExprType boolT = new ExprType(Type.Primitive.BOOLEAN,
                    Multiplicity.Bounded.ONE);
            TypedSpec win = inclusive
                    ? cmpCall("meta::pure::functions::boolean::and",
                            dateCmpCall("meta::pure::functions::boolean::lessThan",
                                    read.apply(hopPfx + fromCol),
                                    dateRead, boolT),
                            dateCmpCall("meta::pure::functions::boolean::"
                                    + "greaterThanEqual",
                                    read.apply(hopPfx + thruCol),
                                    dateRead, boolT), boolT)
                    : cmpCall("meta::pure::functions::boolean::and",
                            dateCmpCall("meta::pure::functions::boolean::"
                                    + "lessThanEqual",
                                    read.apply(hopPfx + fromCol),
                                    dateRead, boolT),
                            dateCmpCall("meta::pure::functions::boolean::"
                                    + "greaterThan",
                                    read.apply(hopPfx + thruCol),
                                    dateRead, boolT), boolT);
            TypedLambda pred = new TypedLambda(List.of(rv), List.of(win),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(row,
                                    Multiplicity.Bounded.ONE)),
                            new Type.Param(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE)),
                            Multiplicity.Bounded.ONE));
            out = new com.legend.compiler.spec.typed.TypedFilter(out, pred,
                    out.info());
        }
        return out;
    }

    /** The join condition AND'd with the target's temporal window read
     * against the SOURCE row's date column (the outer-dated channel).
     * Plain single-date business/processing only — snapshot and
     * bi-temporal outer dates keep their walls. */
    TypedLambda outerDatedJoinCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, String navClass, String outerCol) {
        return outerDatedCond(cond, left, right, navClass, outerCol);
    }

    private TypedLambda outerDatedCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, String navClass, String outerCol) {
        return outerDatedCond(cond, left, right, navClass, outerCol, null);
    }

    private TypedLambda outerDatedCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, String navClass, String outerCol,
            @com.legend.base.Nullable TypedSpec specDate) {
        MilestoningStrategy strat = temporalStrategy(navClass);
        TypedTableReference rt = rootTable(right);
        var ms = rt == null ? null
                : ctx.findTableMilestoning(rt.store(), rt.table()).orElse(null);
        String fromCol;
        String thruCol;
        boolean inclusive;
        if (strat == MilestoningStrategy.BUSINESS && ms != null
                && ms.business() != null
                && ms.business().snapshotDate() == null) {
            fromCol = ms.business().from();
            thruCol = ms.business().thru();
            inclusive = ms.business().thruIsInclusive();
        } else if (strat == MilestoningStrategy.PROCESSING && ms != null
                && ms.processing() != null
                && ms.processing().snapshotDate() == null) {
            fromCol = ms.processing().in();
            thruCol = ms.processing().out();
            inclusive = ms.processing().outIsInclusive();
        } else {
            throw new com.legend.error.NotImplementedException(
                    "outer-row milestoning date over a "
                    + (strat == null ? "non-temporal" : strat)
                    + " target ('" + navClass + "') — only plain business/"
                    + "processing windows compose into the join yet");
        }
        if (fromCol == null || thruCol == null) {
            throw new com.legend.error.NotImplementedException(
                    "outer-row milestoning date: target table '"
                    + java.util.Objects.requireNonNull(rt).table() + "' has no FROM/THRU pair");
        }
        return outerDatedWindowCond(cond, left, right, fromCol, thruCol,
                inclusive, outerCol, navClass, false, specDate);
    }

    /** The window {@code r.<from> <= l.<outerCol> AND r.<thru> > l.<outerCol>}
     * ANDed onto {@code cond} — the column names are given VERBATIM (the
     * head passes raw table columns; a deferred SUB-hop passes its
     * composed prefixed spellings). */
    private TypedLambda outerDatedWindowCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, String fromCol, String thruCol,
            boolean inclusive, String outerCol, String navClass,
            boolean nullTolerant) {
        return outerDatedWindowCond(cond, left, right, fromCol, thruCol,
                inclusive, outerCol, navClass, nullTolerant, null);
    }

    private TypedLambda outerDatedWindowCond(TypedLambda cond, TypedSpec left,
            TypedSpec right, String fromCol, String thruCol,
            boolean inclusive, String outerCol, String navClass,
            boolean nullTolerant, @com.legend.base.Nullable TypedSpec specDate) {
        String sv = cond.parameters().get(0);
        String tv = cond.parameters().get(1);
        Type.RelationType lRow = Type.requireRelationSchema(left.info().type());
        Type.RelationType rRow = Type.requireRelationSchema(right.info().type());
        Function<String, TypedSpec> rcol = name -> {
            Type.Column c = rRow.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on"
                            + " the join target row", navClass));
            return new TypedPropertyAccess(new TypedVariable(tv,
                    new ExprType(rRow, Multiplicity.Bounded.ONE)),
                    c.name(), new ExprType(c.type(), c.multiplicity()));
        };
        Type.Column lc = lRow.columns().stream()
                .filter(x -> x.name().equalsIgnoreCase(outerCol)).findFirst()
                .orElseThrow(() -> new MappingResolutionException(
                        "outer milestoning date column '" + outerCol
                        + "' is not on the join source row", navClass));
        TypedSpec dExpr = new TypedPropertyAccess(new TypedVariable(sv,
                new ExprType(lRow, Multiplicity.Bounded.ONE)),
                lc.name(), new ExprType(lc.type(), lc.multiplicity()));
        if (specDate != null) {
            TypedSpec wrapped = wrapOuterDate(specDate, dExpr);
            if (wrapped != null) {
                dExpr = wrapped;
            }
        }
        ExprType boolT = new ExprType(Type.Primitive.BOOLEAN,
                Multiplicity.Bounded.ONE);
        TypedSpec win = inclusive
                ? cmpCall("meta::pure::functions::boolean::and",
                        dateCmpCall("meta::pure::functions::boolean::lessThan",
                                rcol.apply(java.util.Objects.requireNonNull(fromCol)), dExpr, boolT),
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "greaterThanEqual",
                                rcol.apply(java.util.Objects.requireNonNull(thruCol)), dExpr, boolT), boolT)
                : cmpCall("meta::pure::functions::boolean::and",
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "lessThanEqual",
                                rcol.apply(java.util.Objects.requireNonNull(fromCol)), dExpr, boolT),
                        dateCmpCall("meta::pure::functions::boolean::"
                                + "greaterThan",
                                rcol.apply(java.util.Objects.requireNonNull(thruCol)), dExpr, boolT), boolT);
        if (nullTolerant) {
            // a DEFERRED sub-hop window rides the HEAD's LEFT-join ON —
            // an ABSENT sub row (its milestone column NULL) must not kill
            // the whole match (engine: the window sits on the sub's OWN
            // join, only the sub columns NULL out). Residual divergence:
            // a PRESENT sub row failing its window drops the head match
            // where the engine keeps head + NULL sub — closing that needs
            // flat joins (the sub window on its own ON with the outer
            // date in scope), the flatten rung.
            var isEmptyFn = ctx.findFunction(
                    "meta::pure::functions::collection::isEmpty").stream()
                    .filter(f -> f.parameters().size() == 1)
                    .findFirst().orElseThrow(() -> new IllegalStateException(
                            "resolver bug: no 1-arg isEmpty"));
            TypedSpec absent = new TypedNativeCall(isEmptyFn,
                    List.of(rcol.apply(java.util.Objects.requireNonNull(fromCol))), boolT);
            win = cmpCall("meta::pure::functions::boolean::or", win, absent,
                    boolT);
        }
        if (nullTolerant) {
            // a DEFERRED sub-hop window rides the HEAD's LEFT-join ON —
            // an ABSENT sub row (its milestone column NULL) must not kill
            // the whole match (engine: the window sits on the sub's OWN
            // join, only the sub columns NULL out). Residual divergence:
            // a PRESENT sub row failing its window drops the head match
            // where the engine keeps head + NULL sub — closing that needs
            // flat joins (the sub window on its own ON with the outer
            // date in scope), the flatten rung.
            var isEmptyFn = ctx.findFunction(
                    "meta::pure::functions::collection::isEmpty").stream()
                    .filter(f -> f.parameters().size() == 1)
                    .findFirst().orElseThrow(() -> new IllegalStateException(
                            "resolver bug: no 1-arg isEmpty"));
            TypedSpec absent = new TypedNativeCall(isEmptyFn,
                    List.of(rcol.apply(java.util.Objects.requireNonNull(fromCol))), boolT);
            win = cmpCall("meta::pure::functions::boolean::or", win, absent,
                    boolT);
        }
        TypedSpec merged = cmpCall("meta::pure::functions::boolean::and",
                cond.body().get(cond.body().size() - 1), win, boolT);
        return new TypedLambda(cond.parameters(), List.of(merged),
                cond.info());
    }

    TypedSpec milestonedPipeByStrategy(TypedSpec pipe, TypedSpec date,
            MilestoningStrategy strategy, String classFqn) {
        if (System.getenv("LEGEND_LITE_STAMP_TRACE") != null) {
            TypedSpec d0 = unwrapToOne(date);
            if (d0 instanceof com.legend.compiler.spec.typed.TypedVariable bv) {
                System.err.println("[stamp] BYSTRAT BARE-VAR date $"
                        + bv.name() + " cls=" + classFqn + " letEnv="
                        + letEnv.keySet());
                Thread.dumpStack();
            }
            if (d0 instanceof TypedPropertyAccess p0
                    && p0.source() instanceof
                            com.legend.compiler.spec.typed.TypedVariable v0) {
                System.err.println("[stamp] BYSTRAT OUTER-READ date $"
                        + v0.name() + "." + p0.property() + " cls=" + classFqn);
                Thread.dumpStack();
            } else if (d0 instanceof TypedPropertyAccess p0
                    && unwrapToOne(p0.source())
                            instanceof TypedPropertyAccess p1
                    && p1.source() instanceof
                            com.legend.compiler.spec.typed.TypedVariable v1) {
                System.err.println("[stamp] BYSTRAT NAV-READ date $"
                        + v1.name() + "." + p1.property() + "."
                        + p0.property() + " cls=" + classFqn);
                Thread.dumpStack();
            }
        }
        // HYBRID union (across-tables milestoning): each member filters by
        // ITS OWN table's block for this dimension — deriving capability
        // from the first member's table silently unfiltered every OTHER
        // dimension's members (the hybrid trio's 18v12: processing arms
        // in_z/out_z and processing-snapshotDate never stamped)
        if (Pipelines.containsConcatenate(pipe)) {
            final TypedSpec fdate = date;
            return replaceScan(pipe, sc -> tableHasBlock(sc, strategy)
                    ? milestonedPipeByStrategy(sc, fdate, strategy, classFqn)
                    : sc);
        }
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        return stampWithBlock(pipe, date, strategy, classFqn, root, ms);
    }

    /** The window/snapshot stamp with the milestoning BLOCK supplied —
     *  the for-each-date pipe passes the BASE pipe's block (rootTable on
     *  the joined pipe would find the DATES side and silently skip). */
    private TypedSpec stampWithBlock(TypedSpec pipe, TypedSpec date,
            MilestoningStrategy strategy, String classFqn,
            @com.legend.base.Nullable TypedTableReference root,
            com.legend.model.DatabaseDefinition.TableDefinition
                    .@com.legend.base.Nullable Milestoning ms) {
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        String infinity;
        if (strategy == MilestoningStrategy.BUSINESS) {
            var b = ms == null ? null : ms.business();
            if (b == null) {
                // CAPABILITY TOLERANCE (engine relationalElementCanSupport-
                // Strategy + testLatestIgnoredForNonMilestonedMapped
                // goldens): a table that cannot support the strategy is
                // silently UNFILTERED, never an error
                return pipe;
            }
            fromCol = b.from();
            thruCol = b.thru();
            snapCol = b.snapshotDate();
            inclusive = b.thruIsInclusive();
            infinity = b.infinityDate();
        } else if (strategy == MilestoningStrategy.PROCESSING) {
            var p = ms == null ? null : ms.processing();
            if (p == null) {
                return pipe;   // capability tolerance — see above
            }
            fromCol = p.in();
            thruCol = p.out();
            snapCol = p.snapshotDate();
            inclusive = p.outIsInclusive();
            infinity = p.infinityDate();
        } else {
            throw new MappingResolutionException("bi-temporal class fetch of '"
                    + classFqn + "' is not supported yet", classFqn);
        }
        if (snapCol == null && fromCol == null && thruCol == null) {
            return pipe;   // capability tolerance (ABSENT block) — see above
        }
        if (snapCol == null && (fromCol == null || thruCol == null)) {
            // audit 23 #75: a HALF-declared milestoning block (FROM
            // without THRU or vice versa) is malformed, not absent — the
            // engine's capability tolerance covers absent blocks only;
            // returning unfiltered here served EVERY version silently
            throw new MappingResolutionException("milestoning block of '"
                    + classFqn + "' declares only one of FROM/THRU — a"
                    + " half-declared block cannot filter versions",
                    classFqn);
        }
        // non-literal dates (let-bound vars the inliner kept, adjust()
        // expressions) embed as scalar SQL expressions; an unresolvable
        // variable stays loud at the lowerer ("no row scope")
        // VIEW-backed pipes: the view row does not carry the milestone
        // columns — the engine filters every TABLE ALIAS, so the filter
        // pushes down to the internal scan (whose row has them)
        if (!pipeRowHasMilestoneCols(pipe, fromCol, thruCol, snapCol)
                && root != null
                && pipeRowHasMilestoneCols(root, fromCol, thruCol, snapCol)) {
            // TOLERANT per-scan wrap: a PARTIALLY milestoned union filters
            // only its milestoned members (engine: per-table-alias filters)
            final TypedSpec fdate = date;
            // tolerant: only members whose table declares THIS dimension's
            // block filter (a partially milestoned union keeps its other
            // members raw — audit 10 dropped the over-broad disjunct that
            // admitted other-dimension tables into a throwing path)
            return replaceScan(pipe, sc -> tableHasBlock(sc, strategy)
                    ? milestonedPipeByStrategy(sc, fdate, strategy, classFqn)
                    : sc);
        }
        Type.RelationType row = Type.requireRelationSchema(pipe.info().type());
        String v = STAMP_ROW_VAR;
        ExprType rowT =
                new ExprType(row,
                        Multiplicity.Bounded.ONE);
        Function<String, TypedSpec> col = name -> {
            Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            // MACHINE columns window UNGUARDED (h2New plan goldens) —
            // [1] keeps comparison-site null guards out of stamps
            return new TypedPropertyAccess(
                    new TypedVariable(v, rowT),
                    c.name(), new ExprType(
                            c.type(), Multiplicity.Bounded.ONE));
        };
        ExprType boolT =
                new ExprType(
                        Type.Primitive.BOOLEAN,
                        Multiplicity.Bounded.ONE);
        TypedSpec cond;
        if (snapCol != null) {
            // SNAPSHOT milestoning: the fetch date selects its snapshot rows.
            // A DATETIME param TRUNCATES to the date (engine golden:
            // `snapshotDate = cast(truncate(ts) as date)`).
            if (date instanceof TypedCLatestDate) {
                throw new MappingResolutionException("%latest over a SNAPSHOT-"
                        + "milestoned table is not supported yet", classFqn);
            }
            TypedSpec snapDate = date;
            boolean snapColIsDate = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(snapCol)).findFirst()
                    .map(x -> x.type() == Type
                            .Primitive.STRICT_DATE)
                    .orElse(true);
            if (snapColIsDate
                    && date instanceof TypedCDate cd
                    && !(cd.value()
                            instanceof PureDateLiteral.StrictDate)) {
                // STRUCTURAL day truncation — substring surgery on the
                // spelling mis-truncated non-4-digit years (T1.2)
                PureDateLiteral.StrictDate day = cd.value().strictDatePart();
                if (day != null) {
                    snapDate = new TypedCDate(day,
                            new ExprType(
                                    Type
                                            .Primitive.STRICT_DATE,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE));
                } else {
                    // audit 23 #75: a PARTIAL date (%2015, %2015-04) has
                    // no snapshot-day truncation — comparing it raw
                    // against the DATE column silently matches nothing
                    throw new MappingResolutionException("snapshot fetch"
                            + " date '" + cd.value().toEngineString()
                            + "' has no day component —"
                            + " a full date is required", classFqn);
                }
            } else if (snapColIsDate
                    && !(date instanceof TypedCDate)) {
                // NON-LITERAL datetime param against a DATE column: wrap in
                // datePart (engine golden: snapshotDate = cast(truncate(ts)
                // as date)) — a raw timestamp equality silently matches
                // nothing
                // the strict [1] overload (audit slice 2 registered the
                // real [0..1] sibling; this synth site's operand is [1])
                var dpFns = ctx.findFunction("meta::pure::functions::date::datePart")
                        .stream()
                        .filter(f2 -> f2.parameters().size() == 1
                                && f2.parameters().get(0).multiplicity()
                                        .equals(com.legend.compiler.element
                                                .type.Multiplicity.Bounded.ONE))
                        .toList();
                if (dpFns.size() != 1) {
                    throw new IllegalStateException("resolver bug: datePart"
                            + " resolves to " + dpFns.size() + " overloads —"
                            + " the raw timestamp equality would silently"
                            + " match nothing");
                }
                {
                    snapDate = new TypedNativeCall(dpFns.get(0),
                            List.of(date),
                            new ExprType(
                                    Type
                                            .Primitive.STRICT_DATE,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE));
                }
            }
            cond = cmpCall("meta::pure::functions::boolean::equal",
                    col.apply(snapCol), snapDate, boolT);
        } else if (date instanceof TypedCLatestDate) {
            if (infinity == null) {
                // engine: getInfinityDate ASSERTS the declaration — a
                // defaulted constant would silently return zero rows for
                // any table milestoned with a different infinity date
                throw new MappingResolutionException("%latest usage for"
                        + " temporal fetch of '" + classFqn + "' requires"
                        + " table '" + java.util.Objects.requireNonNull(root).table() + "' to specify a"
                        + " milestoning 'INFINITY_DATE'", classFqn);
            }
            ExprType dt =
                    new ExprType(
                            Type.Primitive.DATE_TIME,
                            Multiplicity.Bounded.ONE);
            cond = cmpCall("meta::pure::functions::boolean::equal",
                    col.apply(java.util.Objects.requireNonNull(thruCol, "thruCol")),
                    new TypedCDate(
                            PureDateLiteral.parse(
                                    // INFINITY_DATE reaches here in both
                                    // corpus spellings (%-prefixed pure
                                    // literal and bare ISO); tolerated at
                                    // THE one consumption site (audit 23
                                    // #75 — parser normalization would
                                    // churn the model record for no
                                    // second consumer)
                                    infinity.startsWith("%")
                                            ? infinity.substring(1) : infinity),
                            dt),
                    boolT);
        } else if (inclusive) {
            // THRU/OUT_IS_INCLUSIVE=true: engine flips both boundary
            // operators — from < d AND thru >= d
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThan",
                            col.apply(java.util.Objects.requireNonNull(fromCol)), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(java.util.Objects.requireNonNull(thruCol)), date, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(java.util.Objects.requireNonNull(fromCol)), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(java.util.Objects.requireNonNull(thruCol)), date, boolT),
                    boolT);
        }
        TypedLambda pred = new TypedLambda(List.of(v),
                List.of(cond),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(row,
                                        Multiplicity.Bounded.ONE)),
                                new Type.Param(
                                        Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new TypedFilter(pipe, pred, pipe.info(), TypedFilter.Stamp.TEMPORAL);
    }

    /**
     * ENGINE RULE: every milestoned TABLE alias in the generated query gets
     * the temporal filter for the query's temporal context — including
     * PHYSICAL joinslot targets (join PMs like {@code @Product_Classification}
     * feeding a scalar/enum read) and demanded navigate targets, which are
     * not class pipelines and so never pass temporalTargetPipe. Applied to
     * the MATERIALIZED pipeline before the association joins append (those
     * targets are class-filtered separately — no double filter).
     */
    TypedSpec applyJoinTemporalFilters(TypedSpec n, ClassSource cs,
            Map<String, String> navPrefixToClass) {
        return applyJoinTemporalFilters(n, cs, navPrefixToClass, Map.of());
    }

    TypedSpec applyJoinTemporalFilters(TypedSpec n, ClassSource cs,
            Map<String, String> navPrefixToClass,
            Map<String, String> navPrefixToChain) {
        return applyJoinTemporalFilters(n, cs, navPrefixToClass,
                navPrefixToChain, Map.of(), Map.of());
    }

    TypedSpec applyJoinTemporalFilters(TypedSpec n, ClassSource cs,
            Map<String, String> navPrefixToClass,
            Map<String, String> navPrefixToChain,
            Map<String, String> midPrefixToChain,
            Map<String, MilestoningStrategy> midPrefixToDim) {
        // ROOT context absent: physical joinslot targets have nothing to
        // filter by, but CLASS-typed navigate targets may carry EXPLICIT
        // property-function dates (specs) — those still apply
        // (a non-temporal root navigating $p.firm(%d) filters firm's
        // versions; audit: the lifted union navigate joined unfiltered)
        if (root.isEmpty()
                && (navPrefixToClass.isEmpty() || specs.isEmpty())) {
            return n;
        }
        return switch (n) {
            case TypedJoin j -> {
                TypedSpec right = j.right();
                String navClass = j.prefix()
                        .map(navPrefixToClass::get).orElse(null);
                String midChain = j.prefix()
                        .map(midPrefixToChain::get).orElse(null);
                TypedSpec filtered;
                if (midChain != null) {
                    // MID table of a chained PM: its OWN milestoning
                    // filters by the chain's context — the chain SPEC
                    // (dimension = the spec's target-class strategy) wins;
                    // else the root context, exactly the physical-slot
                    // rule this replaces (audit 14 F1: target-class
                    // governance left spec-less mids unstamped).
                    TemporalSpec midSpec = specs.get(midChain);
                    MilestoningStrategy specDim = midPrefixToDim.get(j.prefix().orElseThrow());
                    // audit 23 #75: a chain spec that EXISTS but is not
                    // the single-date shape (range/pair or sweep) must
                    // not silently fall back to the ROOT context — the
                    // explicit spec's window would be ignored
                    if (midSpec != null && specDim != null
                            && (midSpec.dates().size() != 1
                                    || midSpec.sweep())) {
                        throw new com.legend.error.NotImplementedException(
                                "chained-PM mid table '" + midChain
                                + "' carries a "
                                + (midSpec.sweep() ? "version-sweep"
                                        : midSpec.dates().size() + "-date")
                                + " chain spec — only single-date mid"
                                + " stamping is built");
                    }
                    TemporalContext midCtx = midSpec != null
                            && midSpec.dates().size() == 1 && !midSpec.sweep()
                            && specDim != null
                            ? TemporalContext.single(specDim,
                                    midSpec.dates().get(0))
                            : root;
                    filtered = stampByOwnBlocks(right, midCtx, "join target");
                } else if (navClass != null) {
                    // CLASS-typed navigate target: governed by the TARGET
                    // CLASS's temporality (a non-temporal class mapped to a
                    // temporal table gets NO filter — corpus
                    // testMilestoningFiltersNotPropogated... golden)
                    String bare = j.prefix().orElseThrow().substring(0,
                            j.prefix().orElseThrow().length() - 1);
                    // the spec registry keys by the DOTTED chain (drilled
                    // embedded heads) — the alias is the fallback (audit
                    // 13 B1: alias-keyed lookup silently root-dated
                    // explicitly-dated drilled chains)
                    String chainHead = navPrefixToChain
                            .getOrDefault(j.prefix().orElseThrow(), bare);
                    // OUTER-ROW date ($o.product($o.orderDate)): the
                    // temporal predicate composes into the JOIN ON — both
                    // rows in scope (engine golden: on (fk=id and from_z
                    // <= root.orderDate and thru_z > root.orderDate));
                    // filtering the target pipe would read the outer var
                    // out of scope (task #32)
                    // BITEMPORAL nav head with an OUTER-ROW dimension:
                    // BOTH windows compose into the head's ON (the
                    // association channel's outerBiDatedJoinCond — same
                    // rule for nav heads; the single-date outerCol gate
                    // below returns null for 2-date specs, and the head's
                    // deferred windows previously DROPPED silently —
                    // wrong rows, engine testBiTemporalDateMilestoning
                    // :279 result1 carries all four windows on the ON,
                    // :291 result4 the mixed literal+outer pair).
                    ClassSource navTarget =
                            sources.navTarget(cs, navClass, ClassSources.stepOf(cs, bare), bare);
                    if (navTarget != null && temporalStrategy(navClass)
                            == MilestoningStrategy.BITEMPORAL) {
                        TypedSpec biRight = temporalTargetPipe(cs, navTarget,
                                chainHead, right);
                        TypedLambda bi = outerBiDatedJoinCond(j.condition(),
                                j.left(), biRight, cs, navTarget, chainHead);
                        if (bi != null) {
                            yield new TypedJoin(applyJoinTemporalFilters(
                                    j.left(), cs, navPrefixToClass,
                                    navPrefixToChain, midPrefixToChain,
                                    midPrefixToDim),
                                    biRight, j.kind(), bi, j.prefix(),
                                    j.frameName(), j.info(),
                j.userCondition() /* rebuild */);
                        }
                    }
                    String outerCol = outerColumnDate(specs.get(chainHead), cs,
                            Type.requireRelationSchema(j.left().info().type()));
                    if (outerCol != null) {
                        onWindowedHeads.add(chainHead);
                        TypedSpec processedLeft = applyJoinTemporalFilters(
                                j.left(), cs, navPrefixToClass,
                                navPrefixToChain, midPrefixToChain,
                                midPrefixToDim);
                        TypedSpec hoisted = hoistDeferredOuterSubJoins(j,
                                processedLeft, chainHead, outerCol, navClass);
                        if (hoisted != null) {
                            yield hoisted;
                        }
                        TemporalSpec chSpec = specs.get(chainHead);
                        yield new TypedJoin(processedLeft,
                                right, j.kind(),
                                withDeferredOuterSubWindows(
                                        outerDatedCond(j.condition(), j.left(),
                                                right, navClass, outerCol,
                                                chSpec != null && chSpec
                                                        .dates().size() == 1
                                                        ? chSpec.dates().get(0)
                                                        : null),
                                        j.left(), right, chainHead, outerCol,
                                        navClass),
                                j.prefix(), j.frameName(), j.info(),
                j.userCondition() /* rebuild */);
                    }
                    filtered = temporalTargetPipe(cs,
                            sources.navTarget(cs, navClass, ClassSources.stepOf(cs, bare), bare),
                            chainHead, right);
                } else {
                    // PHYSICAL joinslot target: every milestoned table alias
                    // in the query filters by the ambient context (per its
                    // OWN blocks — cross-dimension takes nothing)
                    filtered = stampByOwnBlocks(right, root,
                            "join target");
                }
                yield new TypedJoin(
                        applyJoinTemporalFilters(j.left(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                        filtered, j.kind(), j.condition(), j.prefix(),
                        j.frameName(), j.info(),
                j.userCondition() /* rebuild */);
            }
            case TypedFilter f -> new TypedFilter(
                    applyJoinTemporalFilters(f.source(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                    f.predicate(), f.info(), f.stamp());
            case TypedDistinct d ->
                    new TypedDistinct(
                            applyJoinTemporalFilters(d.source(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                            d.columns(), d.info());
            case TypedSelect sel -> new TypedSelect(
                    applyJoinTemporalFilters(sel.source(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                    sel.columns(), sel.info());
            case TypedProject pr -> new TypedProject(
                    applyJoinTemporalFilters(pr.source(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                    pr.columns(), pr.info());
            case TypedConcatenate cc ->
                    new TypedConcatenate(
                            applyJoinTemporalFilters(cc.left(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                            applyJoinTemporalFilters(cc.right(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                            cc.info());
            default -> {
                // LOUD on unrecognized shapes carrying joins (audit 10): a
                // silently skipped milestoned join target fans out versions
                if (containsJoinToMilestoned(n)) {
                    throw new MappingResolutionException("temporal join-target"
                            + " filtering through "
                            + n.getClass().getSimpleName()
                            + " is not supported yet", "");
                }
                yield n;
            }
        };
    }

    /** The pipe's root table declares a milestoning block for {@code strategy}. */
    boolean tableHasBlock(TypedSpec pipe, MilestoningStrategy strategy) {
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            return false;
        }
        return strategy == MilestoningStrategy.BUSINESS ? ms.business() != null
                : strategy == MilestoningStrategy.PROCESSING && ms.processing() != null;
    }

    /** The pipe's TOP row carries the milestone columns the block needs. */
    private static boolean pipeRowHasMilestoneCols(TypedSpec pipe,
            @com.legend.base.Nullable String fromCol,
            @com.legend.base.Nullable String thruCol, @com.legend.base.Nullable String snapCol) {
        Type.RelationType row = Type.relationSchema(pipe.info().type());
        if (row == null) {
            return false;
        }
        Predicate<String> has = name -> name != null
                && row.columns().stream()
                        .anyMatch(c -> c.name().equalsIgnoreCase(name));
        return snapCol != null ? has.test(snapCol)
                : has.test(fromCol) && has.test(thruCol);
    }

    /** Rebuild {@code pipe} with its deepest LEFT-spine scan wrapped. */
    static TypedSpec replaceScan(TypedSpec pipe,
            UnaryOperator<TypedSpec> wrap) {
        return switch (pipe) {
            case TypedTableReference t -> wrap.apply(t);
            case TypedFilter f -> new TypedFilter(replaceScan(f.source(), wrap),
                    f.predicate(), f.info(), f.stamp());
            case TypedSelect sel -> new TypedSelect(replaceScan(sel.source(), wrap),
                    sel.columns(), sel.info());
            case TypedDistinct d ->
                    new TypedDistinct(
                            replaceScan(d.source(), wrap), d.columns(), d.info());
            case TypedProject pr -> new TypedProject(replaceScan(pr.source(), wrap),
                    pr.columns(), pr.info());
            case TypedJoin j ->
                    new TypedJoin(
                            replaceScan(j.left(), wrap),
                            j.right() instanceof com.legend.compiler.spec.typed
                                    .TypedTableReference rt
                                    ? wrap.apply(rt) : j.right(),
                            j.kind(), j.condition(), j.prefix(),
                            j.frameName(), j.info(),
                j.userCondition() /* rebuild */);
            case TypedJoinSlot js ->
                    new TypedJoinSlot(
                            replaceScan(js.source(), wrap), js.alias(), js.target(),
                            js.condition(), js.frameName(), js.info());
            // a UNION pipeline: the temporal filter applies to EACH member
            // (every table alias filters — engine rule, per member scan)
            case TypedConcatenate c ->
                    new TypedConcatenate(
                            replaceScan(c.left(), wrap),
                            replaceScan(c.right(), wrap), c.info());
            default -> throw new MappingResolutionException(
                    "milestone filter pushdown through "
                            + pipe.getClass().getSimpleName()
                            + " is not supported yet", "");
        };
    }

    /**
     * {@code Class.allVersionsInRange(start, end)}: versions whose validity
     * window OVERLAPS the range — engine getTemporalMilestoneRangeFilter:
     * inclusive-thru blocks use {@code from < end AND thru >= start}, else
     * {@code from <= end AND thru > start}; snapshot milestoning selects
     * {@code start <= snap AND snap <= end}. %latest is not a valid range
     * bound (the engine asserts).
     */
    TypedSpec rangeMilestonedPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, String classFqn) {
        MilestoningStrategy strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("allVersionsInRange of '" + classFqn
                    + "': the class declares no temporal stereotype", classFqn);
        }
        return rangeScanPipe(pipe, start, end, strategy, classFqn);
    }

    /** The range filter over a pipe by an EXPLICIT strategy (raw slot-target
     * scans under a range context — audit 13 F3). */
    TypedSpec rangeScanPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, MilestoningStrategy strategy) {
        return rangeScanPipe(pipe, start, end, strategy, "join target");
    }

    TypedSpec rangeScanPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, MilestoningStrategy strategy, String classFqn) {
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        if (strategy == MilestoningStrategy.BUSINESS) {
            var b = ms == null ? null : ms.business();
            if (b == null) {
                return pipe;   // capability tolerance (engine gating)
            }
            fromCol = b.from();
            thruCol = b.thru();
            snapCol = b.snapshotDate();
            inclusive = b.thruIsInclusive();
        } else if (strategy == MilestoningStrategy.PROCESSING) {
            var pr = ms == null ? null : ms.processing();
            if (pr == null) {
                return pipe;   // capability tolerance (engine gating)
            }
            fromCol = pr.in();
            thruCol = pr.out();
            snapCol = pr.snapshotDate();
            inclusive = pr.outIsInclusive();
        } else {
            throw new MappingResolutionException("bi-temporal allVersionsInRange"
                    + " of '" + classFqn + "' is not supported yet", classFqn);
        }
        if (start instanceof TypedCLatestDate
                || end instanceof TypedCLatestDate) {
            // engine: '%latest not a valid parameter for allVersionsInRange'
            throw new MappingResolutionException("%latest is not a valid"
                    + " parameter for allVersionsInRange", classFqn);
        }
        // RELATION-kind (~func) pipes project class properties only — the
        // milestone columns live on the TABLE row, so the range filter
        // pushes down to the internal scan (same rule as the fetch-date
        // path: the engine filters every table alias)
        if (!pipeRowHasMilestoneCols(pipe, fromCol, thruCol, snapCol)
                && root != null
                && pipeRowHasMilestoneCols(root, fromCol, thruCol, snapCol)) {
            return replaceScan(pipe, sc -> tableHasBlock(sc, strategy)
                    ? rangeScanPipe(sc, start, end, strategy, classFqn)
                    : sc);
        }
        Type.RelationType row =
                Type.requireRelationSchema(pipe.info().type());
        String v = STAMP_ROW_VAR;
        ExprType rowT =
                new ExprType(row,
                        Multiplicity.Bounded.ONE);
        Function<String, TypedSpec> col = name -> {
            Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            // MACHINE columns window UNGUARDED (h2New plan goldens) —
            // [1] keeps comparison-site null guards out of stamps
            return new TypedPropertyAccess(
                    new TypedVariable(v, rowT),
                    c.name(), new ExprType(
                            c.type(), Multiplicity.Bounded.ONE));
        };
        ExprType boolT =
                new ExprType(
                        Type.Primitive.BOOLEAN,
                        Multiplicity.Bounded.ONE);
        TypedSpec cond;
        if (snapCol != null) {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(snapCol), start, boolT),
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(snapCol), end, boolT),
                    boolT);
        } else if (inclusive) {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThan",
                            col.apply(java.util.Objects.requireNonNull(fromCol)), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(java.util.Objects.requireNonNull(thruCol)), start, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(java.util.Objects.requireNonNull(fromCol)), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(java.util.Objects.requireNonNull(thruCol)), start, boolT),
                    boolT);
        }
        TypedLambda pred = new TypedLambda(List.of(v),
                List.of(cond),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new com.legend.compiler.element
                                        .type.Type.Param(row,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                new Type.Param(
                                        Type
                                                .Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new TypedFilter(pipe, pred, pipe.info(), TypedFilter.Stamp.TEMPORAL);
    }

    /**
     * The class's temporal stereotype ({@code <<businesstemporal>>}
     * etc., inherited through superclasses), or {@code null} for a
     * non-temporal class. Drives which milestoning block filters the fetch
     * — engine {@code milestoningCanSupportTemporalStrategy}.
     */
    @com.legend.base.Nullable MilestoningStrategy temporalStrategy(String classFqn) {
        return Temporal.strategyOf(ctx, classFqn);
    }

    /** The LEFTMOST physical table of a materialized pipeline. */
    private static @com.legend.base.Nullable TypedTableReference rootTable(TypedSpec n) {
        if (n instanceof TypedTableReference tr) {
            return tr;
        }
        for (TypedSpec c : n.children()) {
            TypedTableReference r = rootTable(c);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    void collectTemporalSpecs(TypedLambda lambda,
            Map<String, TemporalSpec> out) {
        collectTemporalSpecs(lambda.body(), lambda.parameters().get(0), out);
    }

    void collectTemporalSpecs(List<TypedSpec> body, String userVar,
            Map<String, TemporalSpec> out) {
        for (TypedSpec b : body) {
            collectTemporalNodes(b, userVar, out);
        }
    }

    void collectTemporalNodes(TypedSpec n, String userVar,
            Map<String, TemporalSpec> out) {
        collectTemporalNodes(n, userVar, out, "");
    }

    private void collectTemporalNodes(TypedSpec n, String userVar,
            Map<String, TemporalSpec> out, String prefix) {
        if (n instanceof TypedMilestonedAccess ma) {
            // specs key by the FULL CHAIN prefix (engine: one milestoning
            // context per cursor, an explicit property-function date builds
            // a NEW context for ITS hop — MIL:846-868). A 1-hop access keys
            // by the bare property (chain of one). An INNER collection
            // lambda's accesses key under the COMPOSED chain (the arm
            // below); a root the walk cannot tie stays loud.
            List<String> maPath = Substitution.pathOf(ma, userVar);
            if (maPath == null) {
                throw new NotImplementedException("milestoned property access '"
                        + ma.property() + "' on a NESTED navigation is not"
                        + " supported yet");
            }
            String chainKey = prefix + String.join(".", maPath);
            TemporalSpec spec = new TemporalSpec(
                    normalizeContextDates(ma.dates()), ma.sweep());
            TemporalSpec prior = out.putIfAbsent(chainKey, spec);
            if (prior != null && !prior.equals(spec)) {
                throw new NotImplementedException("navigation '" + chainKey
                        + "' with two different milestoning dates in one query"
                        + " is not supported yet");
            }
        }
        // INNER COLLECTION LAMBDA over a navigation (exists($f.employees,
        // e|$e.classification(%d)...) / ->filter / ->map): the lambda's
        // own dated accesses belong to the INNER cursor — key them under
        // the composed chain (employees.classification), exactly the
        // spelling the sub-materialization's spec lookups consume.
        if (n instanceof TypedNativeCall nc && !nc.args().isEmpty()) {
            List<String> hp = Substitution.pathOf(nc.args().get(0), userVar);
            if (hp != null && nc.args().size() > 1) {
                boolean tied = false;
                for (int i = 1; i < nc.args().size(); i++) {
                    if (nc.args().get(i) instanceof TypedLambda il
                            && il.parameters().size() == 1) {
                        for (TypedSpec bb : il.body()) {
                            collectTemporalNodes(bb, il.parameters().get(0),
                                    out, prefix
                                            + String.join(".", hp) + ".");
                        }
                        tied = true;
                    }
                }
                if (tied) {
                    collectTemporalNodes(nc.args().get(0), userVar, out,
                            prefix);
                    return;
                }
            }
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedFilter tf
                && tf.predicate().parameters().size() == 1) {
            List<String> fp = Substitution.pathOf(tf.source(), userVar);
            if (fp != null) {
                for (TypedSpec bb : tf.predicate().body()) {
                    collectTemporalNodes(bb, tf.predicate().parameters().get(0),
                            out, prefix + String.join(".", fp) + ".");
                }
                collectTemporalNodes(tf.source(), userVar, out, prefix);
                return;
            }
        }
        // PROJECT over a navigation (constraint bodies: $this
        // .referenceSystem->toOne()->project(r|$r.systemDescription($d)
        // ...)): each column lambda is an INNER cursor over the projected
        // nav — same composition as filter/map.
        if (n instanceof com.legend.compiler.spec.typed.TypedProject tp) {
            List<String> pp = Substitution.pathOf(
                    InnerDemand.instanceProjectSource(tp), userVar);
            if (pp != null) {
                for (var col : tp.columns()) {
                    TypedLambda fl = col.fn();
                    if (fl.parameters().size() == 1) {
                        for (TypedSpec bb : fl.body()) {
                            collectTemporalNodes(bb,
                                    fl.parameters().get(0), out,
                                    prefix + String.join(".", pp) + ".");
                        }
                    }
                }
                collectTemporalNodes(tp.source(), userVar, out, prefix);
                return;
            }
        }
        // qualifier AUTO-MAP spelling (map($o.product(...), v_qam|$v_qam
        // .classification(...))) — same cursor composition as filter
        if (n instanceof com.legend.compiler.spec.typed.TypedMap tm
                && tm.mapper().parameters().size() == 1) {
            List<String> mp = Substitution.pathOf(tm.source(), userVar);
            if (mp != null) {
                for (TypedSpec bb : tm.mapper().body()) {
                    collectTemporalNodes(bb, tm.mapper().parameters().get(0),
                            out, prefix + String.join(".", mp) + ".");
                }
                collectTemporalNodes(tm.source(), userVar, out, prefix);
                return;
            }
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectTemporalNodes(c, userVar, out, prefix);
        }
    }

    /** The temporal arguments a milestoned property function supplied for a
     * navigation head ({@code product(%d)} / sweep / range spellings). */
    record TemporalSpec(List<TypedSpec> dates, boolean sweep) {}

    /** Serialize-tree temporal specs: SWEEP nodes (productAllVersions{...})
     * register as dateless sweeps — the hop serves the RAW extent, exactly
     * like the query-position propAllVersions() spelling; DATE-ARG nodes
     * ({@code product(%2015-08-20){...}}) register their dates as the
     * hop's spec — the graph child's window (the engine serializes the
     * date into the KEY and filters the child query by it; unfiltered
     * children serialize every version row — the multi-level union
     * duplication). Non-date args (qualifier calls like
     * {@code nameWithTitle('Mr')}) never register. */
    void collectTreeSweeps(List<com.legend.compiler.spec.typed.TypedGraphTree> tree,
            Map<String, TemporalSpec> out) {
        for (com.legend.compiler.spec.typed.TypedGraphTree n : tree) {
            if (n.sweep()) {
                out.putIfAbsent(n.property(),
                        new TemporalSpec(List.of(), true));
            } else if (!n.args().isEmpty() && n.args().size() <= 2
                    && n.args().stream().allMatch(TemporalFrame::isDateTyped)) {
                out.putIfAbsent(n.property(),
                        new TemporalSpec(normalizeContextDates(n.args()), false));
            }
            collectTreeSweeps(n.children(), out);
        }
    }

    private static boolean isDateTyped(com.legend.compiler.spec.typed.TypedSpec a) {
        return a.info().type() instanceof Type.Primitive p
                && p.family() == Type.Primitive.Family.TEMPORAL;
    }

    /**
     * DATED EMBEDDED property ({@code $p.classification(constantDate())}
     * where classification is an embedded block of the set): the physical
     * joinslots the block reads — its OWN milestoned tables — and the MID
     * slots of nav steps chained beneath it (chain classification.system,
     * no spec of its own) stamp by the head's spec and the head class's
     * dimension, never the root context: one milestoning context per
     * cursor, an explicit property-function date building a NEW context
     * for its hop (engine MIL:846-868; golden
     * testDateFunctionInMilestonedPropertyWithMilestonedEntity dates
     * ProductClassificationSystemTable by constantDate()).
     */
    void datedEmbeddedMidSlots(ClassSource cs, Map<String, String> navHeadByAlias,
            Map<String, String> slotPrefixes, Set<String> slotAliases,
            Map<String, String> midPrefixToChain,
            Map<String, MilestoningStrategy> midPrefixToDim) {
        for (var be : cs.bindings().entrySet()) {
            TypedSpec eb = Pipelines.unwrapToOne(be.getValue());
            if (!(eb instanceof com.legend.compiler.spec.typed.TypedNewInstance ector)
                    || spec(be.getKey()) == null) {
                continue;
            }
            Set<String> reads = new java.util.LinkedHashSet<>();
            CorrelatedSubselects.collectAliasReads(ector, cs.rowVar(), slotAliases, reads);
            for (var navE : Pipelines.navSteps(cs.pipeline()).entrySet()) {
                String chain = navHeadByAlias.getOrDefault(navE.getKey(), navE.getKey());
                if (!chain.startsWith(be.getKey() + ".") || spec(chain) != null) {
                    continue;
                }
                for (TypedSpec b : navE.getValue().predicate().body()) {
                    for (String slot : slotAliases) {
                        if (Pipelines.referencesAliasOn(b,
                                navE.getValue().predicate().parameters().get(0),
                                Set.of(slot))) {
                            reads.add(slot);
                        }
                    }
                }
            }
            MilestoningStrategy dim = temporalStrategy(ector.classFqn());
            for (String slot : reads) {
                String pfx = slotPrefixes.getOrDefault(slot, slot + "_");
                midPrefixToChain.put(pfx, be.getKey());
                if (dim != null) {
                    midPrefixToDim.put(pfx, dim);
                }
            }
        }
    }

    /**
     * A temporal TARGET's pipeline filtered by its milestoning columns —
     * explicit spec (property-function dates) wins; else the ROOT context
     * propagates when the immediate parent is temporal and the strategies
     * match; else LOUD (the engine compiles this to an error too).
     */
    TypedSpec temporalTargetPipe(ClassSource parent, ClassSource target,
            String head, TypedSpec pipe) {
        MilestoningStrategy strat = temporalStrategy(target.classFqn());
        if (strat == null) {
            return pipe;
        }
        TemporalSpec spec = specs.get(head);
        if (spec != null && spec.sweep() && spec.dates().isEmpty()) {
            return pipe;   // propAllVersions(): the RAW extent, any dimension
        }
        if (strat == MilestoningStrategy.BITEMPORAL) {
            TemporalSpec parentSpec = head.contains(".")
                    ? specs.get(head.substring(0, head.lastIndexOf('.')))
                    : null;
            List<TypedSpec> dates = biTemporalDatesFor(spec, parent,
                    parentSpec);
            if (dates == null) {
                throw new MappingResolutionException("navigation '" + head
                        + "' to bi-temporal class '" + target.classFqn()
                        + "' requires processing and business dates",
                        target.classFqn());
            }
            if (singleVarChain(dates.get(0)) != null
                    || singleVarChain(dates.get(1)) != null) {
                // any OUTER-ROW dimension date (rooted at ANY outer var,
                // not just the immediate parent's bindings): per-dimension
                // split — outer dims DEFER (composed at the head's join
                // ON / hoisted sibling), literal dims stamp in-pipe
                // (engine testBiTemporalDateInjectionFromVarReference:
                // mixed literal+outer keeps the literal in the sub's
                // WHERE and the outer pair on the ON).
                return stampForClassOrDefer(pipe,
                        TemporalContext.bitemporal(dates.get(0),
                                dates.get(1)),
                        target.classFqn(), head);
            }
            return milestonedPipeByStrategy(
                    milestonedPipeByStrategy(pipe, dates.get(0),
                            MilestoningStrategy.PROCESSING, target.classFqn()),
                    dates.get(1), MilestoningStrategy.BUSINESS, target.classFqn());
        }
        if (spec != null) {
            if (spec.sweep() && spec.dates().isEmpty()) {
                return pipe;   // propAllVersions(): the raw extent
            }
            if (spec.sweep()) {
                return rangeMilestonedPipe(pipe, spec.dates().get(0),
                        spec.dates().get(1), target.classFqn());
            }
            if (outerColumnDate(spec, parent) != null
                    || outerNavDate(head, parent) != null) {
                // OUTER-ROW date ($o.product($o.orderDate)): stamping the
                // target pipe would read $o out of scope — the caller
                // composes the window into the JOIN condition (form 1,
                // outerDatedJoinCond) or the outer frame's WHERE (form 2,
                // applyOuterNavDateFilters; engine :568/:569).
                return pipe;
            }
            return milestonedPipe(pipe, spec.dates().get(0), target.classFqn());
        }
        // PROPAGATION: same-dimension context through temporal parents.
        // The PARENT HOP's explicit spec beats the root's (audit 13 B2:
        // $p.t(%d1).s must filter s by %d1, not the root date — engine
        // getMilestoningContextForQualifiedProperty builds a NEW context
        // at the dated hop that flows onward).
        if (head.contains(".") && temporalStrategy(parent.classFqn()) != null) {
            String parentChain = head.substring(0, head.lastIndexOf('.'));
            TemporalSpec ps = specs.get(parentChain);
            if (ps != null && !ps.sweep() && ps.dates().size() == 1
                    && strat.equals(temporalStrategy(parent.classFqn()))) {
                return milestonedPipe(pipe, ps.dates().get(0), target.classFqn());
            }
        }
        // ROOT propagation through a TEMPORAL parent: the target takes its
        // OWN dimension's date from the context (a bi-temporal root
        // supplies both; cross-dimension takes nothing; a RANGE root
        // range-filters same-dimension — every supporting alias, audit 13
        // F3).
        if (temporalStrategy(parent.classFqn()) != null) {
            TypedSpec d = root.dateFor(strat);
            if (d != null) {
                return milestonedPipeByStrategy(pipe, d, strat,
                        target.classFqn());
            }
            if (root.rangeAppliesTo(strat)) {
                return rangeMilestonedPipe(pipe,
                        java.util.Objects.requireNonNull(root.rangeStart(), "root.rangeStart()"),
                        java.util.Objects.requireNonNull(root.rangeEnd(), "root.rangeEnd()"),
                        target.classFqn());
            }
        }
        throw new MappingResolutionException("navigation '" + head
                + "' to temporal class '" + target.classFqn() + "' requires a"
                + " milestoning date (property function argument, or a"
                + " propagated temporal context through temporal parents)",
                target.classFqn());
    }

    /** The hop's effective single-dimension date context: its chain-keyed
     * spec (non-sweep), else the propagated root context. Null = none. */
    /**
     * THE context in force at a hop (engine: one milestoning context per
     * cursor). Resolution order: the hop's chain-keyed SPEC (an explicit
     * property-function date builds a NEW context for its hop — MIL:
     * 846-868); else DIMENSION-PROJECTED inheritance from the parent hop
     * (audit 13: propagation fills a single-date target only from the
     * SAME stereotype, and a non-temporal target — which cannot carry a
     * context — clears it: the NotPropogated goldens); else the ROOT
     * context for HEAD hops only (audit 13 F5: the root date leaked
     * through non-temporal intermediates).
     */
    TemporalContext contextAt(@com.legend.base.Nullable String chainPrefix,
            String targetClassFqn, TemporalContext inherited) {
        TemporalSpec spec = chainPrefix == null ? null
                : specs.get(chainPrefix);
        MilestoningStrategy targetStrat = temporalStrategy(targetClassFqn);
        if (spec != null && !spec.dates().isEmpty()) {
            if (spec.dates().size() == 2) {
                // 2 dates = bi-temporal PAIR for a bi-temporal target,
                // else the allVersionsInRange RANGE spelling
                return !spec.sweep() && targetStrat == MilestoningStrategy.BITEMPORAL
                        ? TemporalContext.bitemporal(spec.dates().get(0),
                                spec.dates().get(1))
                        : TemporalContext.range(targetStrat,
                                spec.dates().get(0), spec.dates().get(1));
            }
            if (!spec.sweep()) {
                return TemporalContext.single(targetStrat,
                        spec.dates().get(0));
            }
        }
        if (spec == null && targetStrat != null) {
            if (inherited != null && !inherited.isEmpty()) {
                if (targetStrat == MilestoningStrategy.BITEMPORAL) {
                    // bitemp targets inherit only a FULL pair
                    if (inherited.processing() != null
                            && inherited.business() != null) {
                        return inherited;
                    }
                } else if (inherited.dateFor(targetStrat) != null) {
                    return TemporalContext.single(targetStrat,
                            java.util.Objects.requireNonNull(
                                    inherited.dateFor(targetStrat)));
                } else if (inherited.rangeAppliesTo(targetStrat)) {
                    return inherited;
                }
            }
            if (chainPrefix != null && !chainPrefix.contains(".")) {
                if (targetStrat == MilestoningStrategy.BITEMPORAL
                        && root.processing() != null
                        && root.business() != null) {
                    return root;
                }
                if (root.dateFor(targetStrat) != null) {
                    return TemporalContext.single(targetStrat,
                            java.util.Objects.requireNonNull(
                                    root.dateFor(targetStrat)));
                }
                if (root.rangeAppliesTo(targetStrat)) {
                    return root;
                }
            }
        }
        return TemporalContext.NONE;
    }

    /**
     * Filter joins whose RIGHT is a RAW milestoned table scan supporting
     * the context's OWN dimension by the hop context. Audit 13: (a) only
     * the matching dimension filters — a business date must never stamp
     * processing columns (engine relationalElementCanSupportStrategy:
     * unsupporting tables stay UNFILTERED); (b) only raw scans — a
     * composite right (a sub-nav pipeline) already carries ITS hop's
     * filter, double-stamping voided explicitly-dated sub-hops; (c) a
     * 2-date single-dimension context RANGE-filters (root .all(d1,d2)).
     */
    TypedSpec filterMilestonedJoinTargets(TypedSpec n,
            TemporalContext c) {
        return filterMilestonedJoinTargets(n, c, null);
    }

    /** {@code chainPrefix} form (the nav-composite materialization): an
     * OUTER-READ context date cannot stamp a slot target in-pipe
     * ({@link #stampByOwnBlocks} skips it) — REGISTER the deferred
     * window under {@code <chainPrefix>.<alias>} so the head's join-ON
     * composes it over the slot's prefixed milestone columns (the
     * physical-slot form of {@link #stampForClassOrDefer}; the silent
     * skip fanned classification versions — W40). */
    TypedSpec filterMilestonedJoinTargets(TypedSpec n,
            TemporalContext c, @com.legend.base.Nullable String chainPrefix) {
        if (n instanceof TypedJoin j) {
            TypedSpec right = j.right();
            if (right instanceof TypedTableReference) {
                right = stampByOwnBlocks(right, c, "nested join target");
                if (chainPrefix != null && j.prefix().isPresent()) {
                    deferOuterSlotWindows(j.right(), c, chainPrefix,
                            j.prefix().orElseThrow());
                }
            }
            return new TypedJoin(
                    filterMilestonedJoinTargets(j.left(), c, chainPrefix),
                    right, j.kind(), j.condition(), j.prefix(),
                    j.frameName(), j.info(),
                j.userCondition() /* rebuild */);
        }
        // SLOT form of the same rule (the view-propagation golden): an
        // UNEXPANDED slot's raw table target stamps by its own blocks —
        // the STAMP filter rides the slot and relocates onto the join
        // ON at expansion (onFormRelocate), landing the engine's
        // per-table-alias condition inside the view frame
        if (n instanceof com.legend.compiler.spec.typed.TypedJoinSlot js
                && js.target() instanceof TypedTableReference) {
            TypedSpec st = stampByOwnBlocks(js.target(), c,
                    "slot join target");
            if (chainPrefix != null) {
                deferOuterSlotWindows(js.target(), c, chainPrefix,
                        js.alias() + "_");
            }
            return st == js.target() ? js
                    : new com.legend.compiler.spec.typed.TypedJoinSlot(
                            filterMilestonedJoinTargets(js.source(), c,
                                    chainPrefix),
                            js.alias(), st, js.condition(), js.frameName(),
                            js.info());
        }
        // generic descent: slots nest under filters/maps in the pipe
        java.util.List<TypedSpec> kids = n.children();
        java.util.List<TypedSpec> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec m = filterMilestonedJoinTargets(k, c, chainPrefix);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? n.withChildren(mapped) : n;
    }

    /** Register deferred windows for a converted SLOT's raw table target
     * whose own-dimension context date is an OUTER read — mirrors the
     * {@link #stampByOwnBlocks} skip exactly: every dimension it skips
     * for out-of-scope reads defers here instead of dropping. */
    private void deferOuterSlotWindows(TypedSpec target, TemporalContext c,
            String chainPrefix, String prefix) {
        if (c.isEmpty()) {
            return;
        }
        String alias = prefix.endsWith("_")
                ? prefix.substring(0, prefix.length() - 1) : prefix;
        for (MilestoningStrategy dim : List.of(MilestoningStrategy.PROCESSING,
                MilestoningStrategy.BUSINESS)) {
            if (!tableHasBlock(target, dim) || c.rangeAppliesTo(dim)) {
                continue;
            }
            TypedSpec dimDate = c.dateFor(dim);
            if (dimDate != null && singleVarChain(dimDate) != null) {
                deferWindow(chainPrefix + "." + alias, dim, target, dimDate);
            }
        }
    }

    /** Any table scan in the pipeline carrying a SNAPSHOT milestoning block. */
    boolean hasSnapshotScan(TypedSpec pipeline) {
        if (pipeline instanceof TypedTableReference tr) {
            var ms = ctx.findTableMilestoning(tr.store(), tr.table()).orElse(null);
            return ms != null
                    && ((ms.business() != null && ms.business().snapshotDate() != null)
                        || (ms.processing() != null
                                && ms.processing().snapshotDate() != null));
        }
        for (TypedSpec c : pipeline.children()) {
            if (hasSnapshotScan(c)) {
                return true;
            }
        }
        return false;
    }

    /** The ALIASES of Join-PM slots whose target table is milestoned —
     * the demand-aware form of {@link #hasMilestonedSlotTarget}: a
     * context-less sub-materialization is unsafe only when a DEMANDED
     * read actually crosses one of these. */
    java.util.Set<String> milestonedSlotAliases(TypedSpec pipeline) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        collectMilestonedSlotAliases(pipeline, out);
        return out;
    }

    private void collectMilestonedSlotAliases(TypedSpec pipeline,
            java.util.Set<String> out) {
        if (pipeline instanceof TypedJoinSlot js
                && js.target() instanceof TypedTableReference tr
                && ctx.findTableMilestoning(tr.store(), tr.table()).isPresent()) {
            out.add(js.alias());
        }
        for (TypedSpec c : pipeline.children()) {
            collectMilestonedSlotAliases(c, out);
        }
    }

    /** Any join-slot target table in the pipeline carrying a milestoning block. */
    boolean hasMilestonedSlotTarget(TypedSpec pipeline) {
        if (pipeline instanceof TypedJoinSlot js
                && js.target() instanceof
                        TypedTableReference tr
                && ctx.findTableMilestoning(tr.store(), tr.table()).isPresent()) {
            return true;
        }
        for (TypedSpec c : pipeline.children()) {
            if (hasMilestonedSlotTarget(c)) {
                return true;
            }
        }
        return false;
    }

    /** The for-each-date DATES column on the joined row — when set, the
     *  generated businessDate/processingDate reads THIS column (the
     *  engine projects the calendar date per row), overriding the
     *  table-derived sweep column. */
    private @com.legend.base.Nullable String forEachDateColumn;

    /** The for-each-date DATES column, or null outside that mode. */
    @com.legend.base.Nullable String forEachDateColumn() {
        return forEachDateColumn;
    }

    /** The generated milestone-struct leaf -> physical column map for the
     * pipe's root table, by the class's temporal dimension. */
    Map<String, String> milestoneColumnsOf(TypedSpec pipe, String classFqn) {
        MilestoningStrategy strat = temporalStrategy(classFqn);
        TypedTableReference root = rootTable(pipe);
        var ms = root == null || strat == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            if (forEachDateColumn != null && strat != null) {
                return Map.of(strat == MilestoningStrategy.PROCESSING
                        ? GEN_PROCESSING_DATE : GEN_BUSINESS_DATE,
                        forEachDateColumn);
            }
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        if (forEachDateColumn != null && strat != null) {
            out.put(strat == MilestoningStrategy.PROCESSING
                    ? GEN_PROCESSING_DATE : GEN_BUSINESS_DATE,
                    forEachDateColumn);
        }
        if (strat != MilestoningStrategy.PROCESSING && ms.business() != null) {
            var b = ms.business();
            if (b.from() != null) {
                out.put("from", b.from());
            }
            if (b.thru() != null) {
                out.put("thru", b.thru());
            }
            if (b.snapshotDate() != null) {
                out.put("snapshotDate", b.snapshotDate());
            }
            // the GENERATED businessDate under a version sweep: the
            // INCLUSIVE endpoint is the date at which .all(d) returns the
            // version (engine allVersions goldens: THRU_IS_INCLUSIVE
            // reads thru_z)
            String gen = b.snapshotDate() != null ? b.snapshotDate()
                    : b.thruIsInclusive() ? b.thru() : b.from();
            if (gen != null) {
                out.putIfAbsent(GEN_BUSINESS_DATE, gen);
            }
        }
        if (strat != MilestoningStrategy.BUSINESS && ms.processing() != null) {
            var pr = ms.processing();
            if (pr.in() != null) {
                out.putIfAbsent("in", pr.in());
                out.putIfAbsent("from", pr.in());
            }
            if (pr.out() != null) {
                out.putIfAbsent("out", pr.out());
                out.putIfAbsent("thru", pr.out());
            }
            if (pr.snapshotDate() != null) {
                out.putIfAbsent("snapshotDate", pr.snapshotDate());
            }
            String gen = pr.snapshotDate() != null ? pr.snapshotDate()
                    : pr.outIsInclusive() ? pr.out() : pr.in();
            if (gen != null) {
                out.put(GEN_PROCESSING_DATE, gen);
            }
        }
        return out;
    }

    /** A DATE argument spelled as a temporal-context read
     * ({@code $this.businessDate} in a milestoned qualified property, or
     * any instance's businessDate/processingDate) IS the context date —
     * normalize to it so the literal-only filters apply. */
    List<TypedSpec> normalizeContextDates(List<TypedSpec> dates) {
        List<TypedSpec> out = new ArrayList<>(dates.size());
        for (TypedSpec d : dates) {
            out.add(normalizeContextDate(d));
        }
        return out;
    }

    TypedSpec normalizeContextDate(TypedSpec d) {
        // let-bound variable date ($d after `let d = %2015-10-16`) —
        // resolve through the query-body env, transitively; a toOne wrap
        // around a resolvable variable drops with it. Unresolvable
        // variables pass through (downstream walls stay the honest
        // failure — never a silent guess).
        if (d instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            TypedSpec bound = letEnv.get(v.name());
            if (bound != null) {
                return normalizeContextDate(bound);
            }
        }
        if (d instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                && c.args().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedVariable iv
                && letEnv.containsKey(iv.name())) {
            return normalizeContextDate(letEnv.get(iv.name()));
        }
        if (d instanceof TypedPropertyAccess pa
                && (pa.property().equals("businessDate")
                        || pa.property().equals("processingDate"))
                // ONLY the GENERATED property on a temporal receiver — an
                // ordinary user property legally named businessDate must
                // not be rewritten (audit 10)
                && Type.asClassType(pa.source().info().type())
                        instanceof Type.ClassType rc
                && temporalStrategy(rc.fqn()) != null
                ) {
            TypedSpec ctxD = pa.property().equals("businessDate")
                    ? root.business() : root.processing();
            if (ctxD != null) {
                return ctxD;
            }
        }
        // COMPUTED date ($this.businessDate->adjust(1, DAYS)): the context
        // read normalizes INSIDE the computation and the computation itself
        // rides to SQL (engine golden: dateadd(DAY, 1, '<ctx-date>')).
        if (d instanceof com.legend.compiler.spec.typed.TypedNativeCall call) {
            List<TypedSpec> na = new ArrayList<>(call.args().size());
            boolean changed = false;
            for (TypedSpec a : call.args()) {
                TypedSpec r = normalizeContextDate(a);
                changed |= r != a;
                na.add(r);
            }
            if (changed) {
                return new com.legend.compiler.spec.typed.TypedNativeCall(
                        call.callee(), na, call.info());
            }
        }
        return d;
    }

    /** Explicit per-head property-function dates for the substitution. */
    Map<String, List<TypedSpec>> headTemporalDates() {
        Map<String, List<TypedSpec>> out =
                new LinkedHashMap<>();
        for (var e : specs.entrySet()) {
            if (!e.getValue().dates().isEmpty()) {
                out.put(e.getKey(), e.getValue().dates());
            }
        }
        return out;
    }

    private boolean containsJoinToMilestoned(TypedSpec n) {
        if (n instanceof TypedJoin j
                && (tableHasBlock(j.right(), MilestoningStrategy.BUSINESS)
                        || tableHasBlock(j.right(), MilestoningStrategy.PROCESSING))) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsJoinToMilestoned(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The SOLE 2-arg overload of {@code fqn} ({@code and}/{@code equal}) —
     * loud if the catalog ever gains a second one, so a reorder can never
     * silently stamp a different signature.
     */
    /** The FOR-EACH-DATE extent (engine getAllForEachDate,
     *  golden testGetAllForEachDate.pure:85): the resolved DATES relation
     *  cross-joins the class extent and the milestoning window stamps the
     *  JOINED row, its date operand reading the dates COLUMN off the same
     *  stamp row var — {@code from <= d.date and thru > d.date} per date.
     *  The block comes from the BASE pipe's root table (rootTable on the
     *  joined pipe would find the DATES side and silently skip); the
     *  generated businessDate registers as the dates column
     *  ({@link #milestoneColumnsOf}). */
    TypedSpec forEachDatePipe(TypedSpec base, TypedSpec datesRel,
            String classFqn) {
        MilestoningStrategy strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("getAllForEachDate of '"
                    + classFqn + "': the class declares no temporal"
                    + " stereotype", classFqn);
        }
        if (strategy == MilestoningStrategy.BITEMPORAL) {
            throw new MappingResolutionException("getAllForEachDate of the"
                    + " bi-temporal class '" + classFqn
                    + "' is not supported yet", classFqn);
        }
        if (!(Type.relationSchema(datesRel.info().type()) instanceof Type.RelationType dRow)
                || dRow.columns().size() != 1) {
            throw new MappingResolutionException("getAllForEachDate of '"
                    + classFqn + "': the dates argument must resolve to a"
                    + " ONE-column relation", classFqn);
        }
        Type.Column dCol = dRow.columns().get(0);
        Type.RelationType bRow = Type.requireRelationSchema(base.info().type());
        java.util.List<Type.Column> joinedCols =
                new java.util.ArrayList<>(dRow.columns());
        joinedCols.addAll(bRow.columns());
        Type.RelationType joinedRow = new Type.RelationType(joinedCols);
        var one = Multiplicity.Bounded.ONE;
        ExprType boolT = new ExprType(Type.Primitive.BOOLEAN, one);
        TypedLambda onTrue = new TypedLambda(
                java.util.List.of("_fed_d", "_fed_p"),
                java.util.List.of(
                        new com.legend.compiler.spec.typed.TypedCBoolean(
                                true, boolT)),
                new ExprType(new Type.FunctionType(
                        java.util.List.of(new Type.Param(dRow, one),
                                new Type.Param(bRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        var kind = new com.legend.compiler.spec.typed.TypedEnumValue(
                "meta::pure::functions::relation::JoinKind", "LEFT",
                new ExprType(new Type.EnumType(
                        "meta::pure::functions::relation::JoinKind"), one));
        TypedSpec joined = new com.legend.compiler.spec.typed.TypedJoin(
                datesRel, base, kind, onTrue, java.util.Optional.empty(),
                null, new ExprType(joinedRow,
                        Multiplicity.Bounded.ZERO_MANY),
                false /* stamp-join synth */);
        TypedSpec dateRead = new TypedPropertyAccess(
                new com.legend.compiler.spec.typed.TypedVariable(
                        STAMP_ROW_VAR, new ExprType(joinedRow, one)),
                dCol.name(), new ExprType(dCol.type(), one));
        TypedTableReference baseRoot = rootTable(base);
        var baseMs = baseRoot == null ? null
                : ctx.findTableMilestoning(baseRoot.store(), baseRoot.table())
                        .orElse(null);
        if (baseMs == null) {
            throw new MappingResolutionException("getAllForEachDate of '"
                    + classFqn + "': the mapped table declares no"
                    + " milestoning block", classFqn);
        }
        forEachDateColumn = dCol.name();
        return stampWithBlock(joined, dateRead, strategy, classFqn,
                baseRoot, baseMs);
    }

    private TypedSpec cmpCall(String fqn, TypedSpec a, TypedSpec b,
            ExprType out) {
        var fns = ctx.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == 2)
                .toList();
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected exactly"
                    + " one 2-arg overload of " + fqn + ", found " + fns.size());
        }
        return new TypedNativeCall(fns.get(0),
                List.of(a, b), out);
    }

    /**
     * The Date×Date comparison overload of {@code fqn} — pinned by
     * parameter TYPE, never catalog order (the comparison family carries
     * Date, Number, String and Boolean overloads).
     */
    private TypedSpec dateCmpCall(String fqn, TypedSpec a, TypedSpec b,
            ExprType out) {
        // every temporal window comparison passes here — the single
        // stamping choke for the date operand's print channel
        b = stampTemporalAdjust(b);
        var fn = ctx.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == 2
                        && f.parameters().stream().allMatch(p ->
                                Type.Primitive.DATE
                                        .equals(p.type())))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no Date,Date overload of " + fqn));
        return new TypedNativeCall(fn,
                List.of(a, b), out);
    }
}
