package com.legend.compiler.element;

/**
 * The temporal-stereotype lookup shared by the type checker (milestoned
 * property functions) and the store resolver (temporal fetch/join filters)
 * &mdash; engine {@code milestoningCanSupportTemporalStrategy}'s class half.
 */
public final class Temporal {

    private Temporal() {
    }

    /**
     * The class's temporal stereotype ({@code <<temporal.businesstemporal>>}
     * etc., inherited through superclasses), or {@code null} for a
     * non-temporal class.
     */
    public static String strategyOf(ModelContext ctx, String classFqn) {
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
                    if (("temporal".equals(st.profileName())
                            || "meta::pure::profiles::temporal".equals(st.profileName()))
                            && java.util.Set.of("businesstemporal",
                                    "processingtemporal", "bitemporal")
                                    .contains(st.stereotypeName())) {
                        return st.stereotypeName();
                    }
                }
            }
            ctx.findClass(fqn).ifPresent(tc -> work.addAll(tc.superClassFqns()));
        }
        return null;
    }
}
