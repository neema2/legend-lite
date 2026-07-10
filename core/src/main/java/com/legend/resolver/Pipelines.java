package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Pipeline surgery shared by H2 (slot elision for main-table-only queries)
 * and H3's materialization (which re-adds DEMANDED slots as prefix joins).
 *
 * <p>H2 rule (plan audit catch 5): un-demanded {@code TypedJoinSlot}s are
 * STRIPPED &mdash; the query never reads through them, so the joins must
 * not fire (the 0-JOIN absence pins). A mapping ~filter that references a
 * slot alias makes the whole mapping loud "H3-pending" (the filter is
 * row-set-defining; dropping its join would change results). Bindings that
 * reference stripped slots go loud at substitution time
 * ({@link Substitution}).
 */
final class Pipelines {

    private Pipelines() {
    }

    /** A slot-stripped pipeline + the aliases that were dropped. */
    record Stripped(TypedSpec pipeline, Set<String> slotAliases) {}

    static Stripped stripSlots(TypedSpec pipeline, String classFqn) {
        Set<String> aliases = new LinkedHashSet<>();
        collectSlotAliases(pipeline, aliases);
        if (aliases.isEmpty()) {
            return new Stripped(pipeline, Set.of());
        }
        return new Stripped(strip(pipeline, aliases, classFqn), aliases);
    }

    private static void collectSlotAliases(TypedSpec n, Set<String> out) {
        if (n instanceof TypedJoinSlot js) {
            out.add(js.alias());
        }
        for (TypedSpec c : n.children()) {
            collectSlotAliases(c, out);
        }
    }

    private static TypedSpec strip(TypedSpec n, Set<String> aliases, String classFqn) {
        return switch (n) {
            case TypedJoinSlot js -> strip(js.source(), aliases, classFqn);
            case TypedFilter f -> {
                if (referencesAlias(f.predicate(), aliases)) {
                    throw new NotImplementedException("mapping ~filter for '"
                            + classFqn + "' reads through join slot(s) " + aliases
                            + "; join-mediated mapping filters are H3-pending");
                }
                TypedSpec src = strip(f.source(), aliases, classFqn);
                yield new TypedFilter(src, f.predicate(), src.info());
            }
            default -> {
                // Any OTHER op above a slot (groupBy/distinct/...) may carry
                // slot columns in its schema — conservative until H3.
                if (containsSlot(n)) {
                    throw new NotImplementedException("mapping pipeline for '"
                            + classFqn + "' has " + n.getClass().getSimpleName()
                            + " above join slot(s); H3-pending");
                }
                yield n;
            }
        };
    }

    private static boolean containsSlot(TypedSpec n) {
        if (n instanceof TypedJoinSlot) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsSlot(c)) {
                return true;
            }
        }
        return false;
    }

    /** Any {@code $var.alias} read where alias is a (stripped) slot. */
    static boolean referencesAlias(TypedSpec n, Set<String> aliases) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable
                && aliases.contains(pa.property())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (referencesAlias(c, aliases)) {
                return true;
            }
        }
        return false;
    }
}
