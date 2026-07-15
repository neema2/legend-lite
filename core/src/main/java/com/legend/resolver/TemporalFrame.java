// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
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

    private final ModelContext ctx;
    private final ClassSources sources;
    private final TemporalContext root;
    private final Map<String, TemporalSpec> specs;

    TemporalFrame(ModelContext ctx, ClassSources sources,
            TemporalContext root, Map<String, TemporalSpec> specs) {
        this.ctx = ctx;
        this.sources = sources;
        this.root = root;
        this.specs = specs;
    }

    /** The frame with the demand scan's chain-keyed specs attached. */
    TemporalFrame withSpecs(Map<String, TemporalSpec> byChain) {
        return new TemporalFrame(ctx, sources, root, byChain);
    }

    TemporalContext root() {
        return root;
    }

    TemporalSpec spec(String chainKey) {
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
        for (String dim : List.of("processingtemporal",
                "businesstemporal")) {
            if (!tableHasBlock(out, dim)) {
                continue;
            }
            if (c.rangeAppliesTo(dim)) {
                out = rangeScanPipe(out, c.rangeStart(), c.rangeEnd(), dim);
            } else if (c.dateFor(dim) != null) {
                out = milestonedPipeByStrategy(out, c.dateFor(dim), dim, label);
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
        String strat = temporalStrategy(classFqn);
        if (c.isEmpty() || strat == null) {
            return pipe;
        }
        if ("bitemporal".equals(strat)) {
            if (c.processing() != null && c.business() != null) {
                return milestonedPipeByStrategy(
                        milestonedPipeByStrategy(pipe, c.processing(),
                                "processingtemporal", classFqn),
                        c.business(), "businesstemporal", classFqn);
            }
            if (c.processing() != null || c.business() != null) {
                throw new MappingResolutionException("navigation to"
                        + " bi-temporal class '" + classFqn + "' requires"
                        + " processing and business dates", classFqn);
            }
            return pipe;
        }
        if (c.rangeAppliesTo(strat)) {
            return rangeMilestonedPipe(pipe, c.rangeStart(), c.rangeEnd(),
                    classFqn);
        }
        TypedSpec d = c.dateFor(strat);
        return d == null ? pipe
                : milestonedPipeByStrategy(pipe, d, strat, classFqn);
    }

    TypedSpec milestonedPipe(TypedSpec pipe, TypedSpec date, String classFqn) {
        String strategy = temporalStrategy(classFqn);
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
    TypedSpec milestonedPipeByStrategy(TypedSpec pipe, TypedSpec date,
            String strategy, String classFqn) {
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        String infinity;
        if (strategy.equals("businesstemporal")) {
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
        } else if (strategy.equals("processingtemporal")) {
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
        if (snapCol == null && (fromCol == null || thruCol == null)) {
            return pipe;   // capability tolerance — see above
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
        Type.RelationType row = (Type.RelationType) pipe.info().type();
        String v = "ms_row";
        ExprType rowT =
                new ExprType(row,
                        Multiplicity.Bounded.ONE);
        Function<String, TypedSpec> col = name -> {
            Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            return new TypedPropertyAccess(
                    new TypedVariable(v, rowT),
                    c.name(), new ExprType(
                            c.type(), c.multiplicity()));
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
                String iso = cd.value().toEngineString();
                if (iso.length() >= 10) {
                    snapDate = new TypedCDate(
                            PureDateLiteral.parse(
                                    iso.substring(0, 10)),
                            new ExprType(
                                    Type
                                            .Primitive.STRICT_DATE,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE));
                }
            } else if (snapColIsDate
                    && !(date instanceof TypedCDate)) {
                // NON-LITERAL datetime param against a DATE column: wrap in
                // datePart (engine golden: snapshotDate = cast(truncate(ts)
                // as date)) — a raw timestamp equality silently matches
                // nothing
                var dpFns = ctx.findFunction("meta::pure::functions::date::datePart");
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
                        + " table '" + root.table() + "' to specify a"
                        + " milestoning 'INFINITY_DATE'", classFqn);
            }
            ExprType dt =
                    new ExprType(
                            Type.Primitive.DATE_TIME,
                            Multiplicity.Bounded.ONE);
            cond = cmpCall("meta::pure::functions::boolean::equal",
                    col.apply(thruCol),
                    new TypedCDate(
                            PureDateLiteral.parse(
                                    infinity.startsWith("%")
                                            ? infinity.substring(1) : infinity),
                            dt),
                    boolT);
        } else if (inclusive) {
            // THRU/OUT_IS_INCLUSIVE=true: engine flips both boundary
            // operators — from < d AND thru >= d
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThan",
                            col.apply(fromCol), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(thruCol), date, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(fromCol), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(thruCol), date, boolT),
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
        return new TypedFilter(pipe, pred, pipe.info());
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
            Map<String, String> midPrefixToDim) {
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
                    String specDim = midPrefixToDim.get(j.prefix().get());
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
                    String bare = j.prefix().get().substring(0,
                            j.prefix().get().length() - 1);
                    // the spec registry keys by the DOTTED chain (drilled
                    // embedded heads) — the alias is the fallback (audit
                    // 13 B1: alias-keyed lookup silently root-dated
                    // explicitly-dated drilled chains)
                    String chainHead = navPrefixToChain
                            .getOrDefault(j.prefix().get(), bare);
                    filtered = temporalTargetPipe(cs,
                            sources.get(cs.mappingFqn(), navClass), chainHead, right);
                } else {
                    // PHYSICAL joinslot target: every milestoned table alias
                    // in the query filters by the ambient context (per its
                    // OWN blocks — cross-dimension takes nothing)
                    filtered = stampByOwnBlocks(right, root,
                            "join target");
                }
                yield new TypedJoin(
                        applyJoinTemporalFilters(j.left(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                        filtered, j.kind(), j.condition(), j.prefix(), j.info());
            }
            case TypedFilter f -> new TypedFilter(
                    applyJoinTemporalFilters(f.source(), cs, navPrefixToClass, navPrefixToChain, midPrefixToChain, midPrefixToDim),
                    f.predicate(), f.info());
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
    boolean tableHasBlock(TypedSpec pipe, String strategy) {
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            return false;
        }
        return strategy.equals("businesstemporal") ? ms.business() != null
                : strategy.equals("processingtemporal") && ms.processing() != null;
    }

    /** The pipe's TOP row carries the milestone columns the block needs. */
    private static boolean pipeRowHasMilestoneCols(TypedSpec pipe, String fromCol,
            String thruCol, String snapCol) {
        if (!(pipe.info().type()
                instanceof Type.RelationType row)) {
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
                    f.predicate(), f.info());
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
                            j.kind(), j.condition(), j.prefix(), j.info());
            case TypedJoinSlot js ->
                    new TypedJoinSlot(
                            replaceScan(js.source(), wrap), js.alias(), js.target(),
                            js.condition(), js.info());
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
        String strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("allVersionsInRange of '" + classFqn
                    + "': the class declares no temporal stereotype", classFqn);
        }
        return rangeScanPipe(pipe, start, end, strategy, classFqn);
    }

    /** The range filter over a pipe by an EXPLICIT strategy (raw slot-target
     * scans under a range context — audit 13 F3). */
    TypedSpec rangeScanPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, String strategy) {
        return rangeScanPipe(pipe, start, end, strategy, "join target");
    }

    TypedSpec rangeScanPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, String strategy, String classFqn) {
        TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        if (strategy.equals("businesstemporal")) {
            var b = ms == null ? null : ms.business();
            if (b == null) {
                return pipe;   // capability tolerance (engine gating)
            }
            fromCol = b.from();
            thruCol = b.thru();
            snapCol = b.snapshotDate();
            inclusive = b.thruIsInclusive();
        } else if (strategy.equals("processingtemporal")) {
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
        Type.RelationType row =
                (Type.RelationType) pipe.info().type();
        String v = "ms_row";
        ExprType rowT =
                new ExprType(row,
                        Multiplicity.Bounded.ONE);
        Function<String, TypedSpec> col = name -> {
            Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            return new TypedPropertyAccess(
                    new TypedVariable(v, rowT),
                    c.name(), new ExprType(
                            c.type(), c.multiplicity()));
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
                            col.apply(fromCol), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(thruCol), start, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(fromCol), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(thruCol), start, boolT),
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
        return new TypedFilter(pipe, pred, pipe.info());
    }

    /**
     * The class's temporal stereotype ({@code <<businesstemporal>>}
     * etc., inherited through superclasses), or {@code null} for a
     * non-temporal class. Drives which milestoning block filters the fetch
     * — engine {@code milestoningCanSupportTemporalStrategy}.
     */
    String temporalStrategy(String classFqn) {
        return Temporal.strategyOf(ctx, classFqn);
    }

    /** The LEFTMOST physical table of a materialized pipeline. */
    private static TypedTableReference rootTable(TypedSpec n) {
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
        if (n instanceof TypedMilestonedAccess ma) {
            // specs key by the FULL CHAIN prefix (engine: one milestoning
            // context per cursor, an explicit property-function date builds
            // a NEW context for ITS hop — MIL:846-868). A 1-hop access keys
            // by the bare property (chain of one). Non-var-rooted accesses
            // (inner lambdas) stay loud.
            List<String> maPath = Substitution.pathOf(ma, userVar);
            if (maPath == null) {
                throw new NotImplementedException("milestoned property access '"
                        + ma.property() + "' on a NESTED navigation is not"
                        + " supported yet");
            }
            String chainKey = String.join(".", maPath);
            TemporalSpec spec = new TemporalSpec(
                    normalizeContextDates(ma.dates()), ma.sweep());
            TemporalSpec prior = out.putIfAbsent(chainKey, spec);
            if (prior != null && !prior.equals(spec)) {
                throw new NotImplementedException("navigation '" + chainKey
                        + "' with two different milestoning dates in one query"
                        + " is not supported yet");
            }
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectTemporalNodes(c, userVar, out);
        }
    }

    /** The temporal arguments a milestoned property function supplied for a
     * navigation head ({@code product(%d)} / sweep / range spellings). */
    record TemporalSpec(List<TypedSpec> dates, boolean sweep) {}

    /**
     * A temporal TARGET's pipeline filtered by its milestoning columns —
     * explicit spec (property-function dates) wins; else the ROOT context
     * propagates when the immediate parent is temporal and the strategies
     * match; else LOUD (the engine compiles this to an error too).
     */
    TypedSpec temporalTargetPipe(ClassSource parent, ClassSource target,
            String head, TypedSpec pipe) {
        String strat = temporalStrategy(target.classFqn());
        if (strat == null) {
            return pipe;
        }
        TemporalSpec spec = specs.get(head);
        if (spec != null && spec.sweep() && spec.dates().isEmpty()) {
            return pipe;   // propAllVersions(): the RAW extent, any dimension
        }
        if (strat.equals("bitemporal")) {
            List<TypedSpec> dates =
                    spec != null && !spec.sweep() && spec.dates().size() == 2
                            ? spec.dates()
                            : root.processing() != null
                                    && root.business() != null
                                    && temporalStrategy(parent.classFqn()) != null
                                    ? List.of(root.processing(),
                                            root.business())
                                    : null;
            // the engine-generated 1-DATE bitemporal property: the param is
            // the dimension the OWNER lacks; the owner's own dimension
            // fills from $this.<date> = the propagated context
            if (dates == null && spec != null && !spec.sweep()
                    && spec.dates().size() == 1) {
                String parentStrat = temporalStrategy(parent.classFqn());
                TypedSpec ownerDate = root.dateFor(parentStrat);
                if (ownerDate != null && "businesstemporal".equals(parentStrat)) {
                    dates = List.of(spec.dates().get(0), ownerDate);
                } else if (ownerDate != null
                        && "processingtemporal".equals(parentStrat)) {
                    dates = List.of(ownerDate, spec.dates().get(0));
                }
            }
            if (dates == null) {
                throw new MappingResolutionException("navigation '" + head
                        + "' to bi-temporal class '" + target.classFqn()
                        + "' requires processing and business dates",
                        target.classFqn());
            }
            return milestonedPipeByStrategy(
                    milestonedPipeByStrategy(pipe, dates.get(0),
                            "processingtemporal", target.classFqn()),
                    dates.get(1), "businesstemporal", target.classFqn());
        }
        if (spec != null) {
            if (spec.sweep() && spec.dates().isEmpty()) {
                return pipe;   // propAllVersions(): the raw extent
            }
            if (spec.sweep()) {
                return rangeMilestonedPipe(pipe, spec.dates().get(0),
                        spec.dates().get(1), target.classFqn());
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
                return rangeMilestonedPipe(pipe, root.rangeStart(),
                        root.rangeEnd(), target.classFqn());
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
    TemporalContext contextAt(String chainPrefix,
            String targetClassFqn, TemporalContext inherited) {
        TemporalSpec spec = chainPrefix == null ? null
                : specs.get(chainPrefix);
        String targetStrat = temporalStrategy(targetClassFqn);
        if (spec != null && !spec.dates().isEmpty()) {
            if (spec.dates().size() == 2) {
                // 2 dates = bi-temporal PAIR for a bi-temporal target,
                // else the allVersionsInRange RANGE spelling
                return !spec.sweep() && "bitemporal".equals(targetStrat)
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
                if ("bitemporal".equals(targetStrat)) {
                    // bitemp targets inherit only a FULL pair
                    if (inherited.processing() != null
                            && inherited.business() != null) {
                        return inherited;
                    }
                } else if (inherited.dateFor(targetStrat) != null) {
                    return TemporalContext.single(targetStrat,
                            inherited.dateFor(targetStrat));
                } else if (inherited.rangeAppliesTo(targetStrat)) {
                    return inherited;
                }
            }
            if (chainPrefix != null && !chainPrefix.contains(".")) {
                if ("bitemporal".equals(targetStrat)
                        && root.processing() != null
                        && root.business() != null) {
                    return root;
                }
                if (root.dateFor(targetStrat) != null) {
                    return TemporalContext.single(targetStrat,
                            root.dateFor(targetStrat));
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
        if (n instanceof TypedJoin j) {
            TypedSpec right = j.right();
            if (right instanceof TypedTableReference) {
                right = stampByOwnBlocks(right, c, "nested join target");
            }
            return new TypedJoin(
                    filterMilestonedJoinTargets(j.left(), c), right,
                    j.kind(), j.condition(), j.prefix(), j.info());
        }
        return n;
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

    /** The generated milestone-struct leaf -> physical column map for the
     * pipe's root table, by the class's temporal dimension. */
    Map<String, String> milestoneColumnsOf(TypedSpec pipe, String classFqn) {
        String strat = temporalStrategy(classFqn);
        TypedTableReference root = rootTable(pipe);
        var ms = root == null || strat == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        if (!"processingtemporal".equals(strat) && ms.business() != null) {
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
                out.put("genBusinessDate", gen);
            }
        }
        if (!"businesstemporal".equals(strat) && ms.processing() != null) {
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
                out.put("genProcessingDate", gen);
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
        if (d instanceof TypedPropertyAccess pa
                && (pa.property().equals("businessDate")
                        || pa.property().equals("processingDate"))
                // ONLY the GENERATED property on a temporal receiver — an
                // ordinary user property legally named businessDate must
                // not be rewritten (audit 10)
                && pa.source().info().type()
                        instanceof Type.ClassType rc
                && temporalStrategy(rc.fqn()) != null
                ) {
            TypedSpec ctxD = pa.property().equals("businessDate")
                    ? root.business() : root.processing();
            if (ctxD != null) {
                return ctxD;
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
                && (tableHasBlock(j.right(), "businesstemporal")
                        || tableHasBlock(j.right(), "processingtemporal"))) {
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
