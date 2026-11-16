package com.legend.compiler.element;

/**
 * The temporal-stereotype lookup shared by the type checker (milestoned
 * property functions) and the store resolver (temporal fetch/join filters)
 * &mdash; engine {@code milestoningCanSupportTemporalStrategy}'s class half.
 */
public final class Temporal {

    /** One member real pure GENERATES on a temporal class (legend-engine's
     * Milestoning.generateMilestoningProperties, legend-pure's
     * MilestoningStereotypeEnum): its name, its type -- a primitive's simple
     * name or a class FQN -- and its multiplicity, [0..1] when
     * {@code zeroOne}, else [1]. */
    public record GeneratedMember(String name, String typeName, boolean zeroOne) {
    }

    /** The primitive every generated date member is ({@code businessDate: Date[1]}). */
    private static final String GENERATED_DATE_TYPE = "Date";

    /** THE ONE RULE from a class's milestoning strategy to the members real
     * pure generates on it (2026-11-14): businessDate (a business axis) and
     * processingDate (a processing axis), each Date[1] -- the instance's
     * context date -- and the milestoning struct, [0..1]. Read at LOOKUP time,
     * after the declared lookup misses, by the layers that ask "may this class
     * carry property P": {@link #generatedMember(ModelContext, String, String)}
     * for the typer, graph trees and constructor keys (NewChecker), and
     * {@code KnowledgeLayer.propertyType}/{@code propertyDef} for the
     * normalizer's mapping checks and to-one coercion. Never stored on
     * {@code TypedClass}, so a constructor never demands them
     * ({@code NewChecker.requireAllRequired}; the engine does not), and never
     * answered by {@code ModelContext.findProperty}: the store resolver reads
     * that lookup as the DECLARED surface (a declared {@code milestoning} wins
     * over the generated struct, StoreResolver's hop walk) -- making it total
     * is a resolver audit of its own. Null when {@code prop} is not generated
     * under {@code strat}. */
    public static @com.legend.base.Nullable GeneratedMember generatedMember(
            @com.legend.base.Nullable MilestoningStrategy strat, String prop) {
        if (strat == null) {
            return null;
        }
        Member m = Member.spelled(prop);
        if (m == null || m.axis != null && !strat.has(m.axis)) {
            return null;
        }
        // the struct's type is lite's spelling, kept as it was before the rule
        // moved here: a bitemporal class's struct is BusinessDateMilestoning
        // (the engine's MilestoningStereotypeEnum names BiTemporalMilestoning)
        String type = m.axis != null ? GENERATED_DATE_TYPE : "meta::pure::milestoning::"
                + (strat == MilestoningStrategy.PROCESSING ? "ProcessingDateMilestoning" : "BusinessDateMilestoning");
        return new GeneratedMember(m.spelling, type, m.axis == null);
    }

    /** The members the rule generates, as DATA: each one's spelling and the
     * temporal axis that generates it (none for the milestoning struct, which
     * every temporal class carries). The one place the three spellings live. */
    private enum Member {
        BUSINESS_DATE("businessDate", MilestoningStrategy.Dimension.BUSINESS),
        PROCESSING_DATE("processingDate", MilestoningStrategy.Dimension.PROCESSING),
        MILESTONING("milestoning", null);

        final String spelling;
        final MilestoningStrategy.@com.legend.base.Nullable Dimension axis;

        Member(String spelling, MilestoningStrategy.@com.legend.base.Nullable Dimension axis) {
            this.spelling = spelling;
            this.axis = axis;
        }

        /** The member spelled {@code prop}, or null. */
        static @com.legend.base.Nullable Member spelled(String prop) {
            for (Member m : values()) {
                if (m.spelling.equals(prop)) {
                    return m;
                }
            }
            return null;
        }
    }

    /** Whether {@code prop} is a name the rule can generate at all -- the
     * cheap test a lookup's miss path asks before walking the hierarchy for
     * a strategy. */
    public static boolean isGeneratedMemberName(String prop) {
        return Member.spelled(prop) != null;
    }

    /** A generated member's typed form: its type classified (a primitive, or
     * the milestoning struct's class) and its multiplicity. */
    public static com.legend.compiler.element.type.ExprType typed(GeneratedMember m) {
        com.legend.compiler.element.type.Type t = m.typeName().equals(GENERATED_DATE_TYPE)
                ? com.legend.compiler.element.type.Type.Primitive.DATE
                : new com.legend.compiler.element.type.Type.ClassType(m.typeName());
        return new com.legend.compiler.element.type.ExprType(t, m.zeroOne()
                ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE
                : com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
    }

    /** The GENERATED milestoning member surface real pure adds to
     * temporal classes — businessDate/processingDate (the instance's
     * context date), the milestoning struct ({@link #generatedMember(
     * MilestoningStrategy, String)}), and the struct's own members. Query-
     * position typing (Typer) and graph trees (GraphFetchChecker) read it;
     * null when {@code prop} is not generated for {@code classFqn}. */
    public static com.legend.compiler.element.type.@com.legend.base.Nullable ExprType generatedMember(
            ModelContext ctx, String classFqn, String prop) {
        GeneratedMember m = isGeneratedMemberName(prop)
                ? generatedMember(strategyOf(ctx, classFqn), prop) : null;
        if (m != null) {
            return typed(m);
        }
        if ((classFqn.equals(
                        "meta::pure::milestoning::BusinessDateMilestoning")
                || classFqn.equals(
                        "meta::pure::milestoning::ProcessingDateMilestoning"))
                && java.util.Set.of("from", "thru", "in", "out",
                        "snapshotDate").contains(prop)) {
            // DATE_TIME, not abstract Date: the wire keeps the physical
            // precision (engine milestone columns read back as timestamps)
            return new com.legend.compiler.element.type.ExprType(
                    com.legend.compiler.element.type.Type.Primitive.DATE_TIME,
                    com.legend.compiler.element.type.Multiplicity
                            .Bounded.ZERO_ONE);
        }
        return null;
    }

    private Temporal() {
    }

    /** Whether {@code prop} names a GENERATED temporal date property
     * under {@code strat} — the two spellings live HERE once (shared by
     * the graph envelope's implicit tree and the flat form's k_ carrier
     * rename). */
    /** The three generated milestoning member names (real pure generates
     *  them on temporal classes; no mapping exists for them) — the ONE owner
     *  of the spellings the lineage scan reads. */
    public static boolean isGeneratedDateName(String prop) {
        // the two date members (Member's spellings), and the struct's own snapshotDate
        Member m = Member.spelled(prop);
        return m != null && m.axis != null || "snapshotDate".equals(prop);
    }

    public static boolean isGeneratedDateProperty(String prop,
            MilestoningStrategy strat) {
        // Member's spelling and axis: the one rule generatedMember reads
        Member m = Member.spelled(prop);
        return m != null && m.axis != null && strat.has(m.axis);
    }

    /**
     * Whether ANY extent fetched by the (PRE-resolution) query body is
     * temporally stamped &mdash; the engine's exists-form gate
     * ({@code shouldBuildExistsPredicate}, pureToSQLQuery:6149): a
     * milestoned extent is select-wrapped at exists-build time, so the
     * engine keeps the correlated EXISTS predicate; the join-distinct
     * rewrite (ExistsJoinForm) applies only over plain-table extents.
     */
    public static boolean anyTemporalGetAll(
            java.util.List<? extends com.legend.compiler.spec.typed.TypedSpec> body,
            ModelContext ctx) {
        for (var b : body) {
            if (anyTemporalGetAll(b, ctx)) {
                return true;
            }
        }
        return false;
    }

    private static boolean anyTemporalGetAll(
            com.legend.compiler.spec.typed.TypedSpec n, ModelContext ctx) {
        if (n instanceof com.legend.compiler.spec.typed.TypedGetAll g
                && strategyOf(ctx, g.classFqn()) != null) {
            return true;
        }
        for (var c : n.children()) {
            if (anyTemporalGetAll(c, ctx)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The class's milestoning strategy ({@code <<temporal.businesstemporal>>}
     * etc., inherited through superclasses), or {@code null} for a
     * non-temporal class.
     */
    public static @com.legend.base.Nullable MilestoningStrategy strategyOf(ModelContext ctx,
            String classFqn) {
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        work.add(classFqn);
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            var def = ctx.findClassDefinition(fqn).orElse(null);
            if (def != null) {
                for (var st : def.stereotypes()) {
                    MilestoningStrategy s = MilestoningStrategy
                            .ofStereotypeOrNull(st.profileName(),
                                    st.stereotypeName());
                    if (s != null) {
                        return s;
                    }
                }
            }
            ctx.findClass(fqn).ifPresent(tc -> work.addAll(tc.superClassFqns()));
        }
        return null;
    }
}
