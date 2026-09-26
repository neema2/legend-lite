package com.legend.compiler.element;

/**
 * The temporal-stereotype lookup shared by the type checker (milestoned
 * property functions) and the store resolver (temporal fetch/join filters)
 * &mdash; engine {@code milestoningCanSupportTemporalStrategy}'s class half.
 */
public final class Temporal {

    /** The GENERATED milestoning member surface real pure adds to
     * temporal classes — businessDate/processingDate (the instance's
     * context date), the milestoning struct, and the struct's own
     * members. ONE registry for query-position typing (Typer) and graph
     * trees (GraphFetchChecker); null when {@code prop} is not generated
     * for {@code classFqn}. */
    public static com.legend.compiler.element.type.@com.legend.base.Nullable ExprType generatedMember(
            ModelContext ctx, String classFqn, String prop) {
        MilestoningStrategy strat = strategyOf(ctx, classFqn);
        boolean generated = strat != null
                && (prop.equals("businessDate")
                        && strat.has(MilestoningStrategy.Dimension.BUSINESS)
                || prop.equals("processingDate")
                        && strat.has(MilestoningStrategy.Dimension.PROCESSING));
        if (generated) {
            return new com.legend.compiler.element.type.ExprType(
                    com.legend.compiler.element.type.Type.Primitive.DATE,
                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        }
        if (prop.equals("milestoning") && strat != null) {
            return new com.legend.compiler.element.type.ExprType(
                    new com.legend.compiler.element.type.Type.ClassType(
                            "meta::pure::milestoning::"
                                    + (strat == MilestoningStrategy.PROCESSING
                                    ? "ProcessingDateMilestoning"
                                    : "BusinessDateMilestoning")),
                    com.legend.compiler.element.type.Multiplicity
                            .Bounded.ZERO_ONE);
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
        return "businessDate".equals(prop) || "processingDate".equals(prop)
                || "snapshotDate".equals(prop);
    }

    public static boolean isGeneratedDateProperty(String prop,
            MilestoningStrategy strat) {
        return "businessDate".equals(prop)
                        && strat != MilestoningStrategy.PROCESSING
                || "processingDate".equals(prop)
                        && strat != MilestoningStrategy.BUSINESS;
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
