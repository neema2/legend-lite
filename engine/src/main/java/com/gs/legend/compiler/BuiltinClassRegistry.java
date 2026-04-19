package com.gs.legend.compiler;

import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.parser.PureParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Catalog of platform-defined <em>Pure classes</em> shipped by legend-lite — the declaration
 * layer counterpart to {@link BuiltinRegistry}'s function signatures and platform enums.
 *
 * <h2>Design</h2>
 *
 * <p>Each entry in {@link #BUILTIN_CLASS_SOURCES} is a self-contained fragment of Pure source
 * using <strong>fully-qualified names only</strong> — no {@code import} statements, no simple
 * names. The catalog is parsed exactly once at JVM init into {@link #BUILTIN_CLASS_DEFINITIONS}
 * via {@link PureParser#parseModel(String)}, bypassing the name-resolution pass entirely
 * (all references are already FQN-qualified, so there is nothing to canonicalize).
 *
 * <p>{@link com.gs.legend.model.PureModelBuilder} pulls {@link #BUILTIN_CLASS_DEFINITIONS} at
 * construction time and seeds every fresh builder with a {@code registerClassStub} pass
 * followed by a full {@code addClass} pass (same pattern that lets user source reference
 * classes declared later in the same file).
 *
 * <h2>Scope (phase 2.5e — L4)</h2>
 *
 * <p>Starter set: enough entries to prove the infrastructure end-to-end. The full 26-entry
 * catalog (covering Relation machinery, SortInfo, ColSpec, frame helpers, tuple types, etc.)
 * is scheduled for expansion after this commit.
 *
 * <h2>Bootstrap stubs vs. full bodies</h2>
 *
 * <p>m3 root classes ({@code Type}, {@code Function}, {@code Relation}) whose bodies cannot
 * be authored in Pure are declared {@code native Class}. Every other builtin ships with its
 * full property list — matching legend-pure fidelity.
 */
public final class BuiltinClassRegistry {

    private BuiltinClassRegistry() {}

    /**
     * Pure source strings — one or more {@code Class} / {@code native Class} declarations per
     * entry. FQNs only; no {@code import} statements. Stereotypes and tagged values must also
     * use FQN form so the catalog parses without a populated import scope.
     */
    public static final List<String> BUILTIN_CLASS_SOURCES = List.of(

            // ===== Bootstrap stubs =====
            // m3 root "Type" — every other class ultimately generalises to Type. Declared
            // native because its body (stereotypes, classifier property, etc.) is intrinsic
            // to the metamodel and not expressible in surface Pure.
            "native Class meta::pure::metamodel::type::Type {}",

            // ===== Collections =====
            // Pair<U,V> — the canonical two-element heterogeneous tuple. Both components
            // participate in structural equality via <<equality.Key>>, matching legend-pure
            // (platform/pure/essential/collection/_collection.pure).
            """
            Class meta::pure::functions::collection::Pair<U, V>
            {
                <<meta::pure::profiles::equality.Key>> first: U[1];
                <<meta::pure::profiles::equality.Key>> second: V[1];
            }
            """);

    /**
     * Parsed definitions — one {@link ClassDefinition} per class declared across all sources.
     * Populated once at class init; static-final so there is no synchronisation cost on
     * subsequent reads. A parse failure here aborts JVM startup with a clear diagnostic —
     * which is the desired behaviour: a malformed platform class catalog is a fatal bug.
     */
    public static final List<ClassDefinition> BUILTIN_CLASS_DEFINITIONS;

    static {
        var defs = new ArrayList<ClassDefinition>();
        for (String src : BUILTIN_CLASS_SOURCES) {
            List<PackageableElement> parsed;
            try {
                parsed = PureParser.parseModel(src);
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "BuiltinClassRegistry failed to parse source:\n" + src, e);
            }
            for (PackageableElement el : parsed) {
                if (el instanceof ClassDefinition cd) {
                    defs.add(cd);
                } else {
                    throw new IllegalStateException(
                            "BuiltinClassRegistry source produced non-ClassDefinition "
                                    + el.getClass().getSimpleName() + " from:\n" + src);
                }
            }
        }
        BUILTIN_CLASS_DEFINITIONS = List.copyOf(defs);
    }

    /**
     * FQNs of every builtin class — derived from {@link #BUILTIN_CLASS_DEFINITIONS} and
     * folded into {@link BuiltinRegistry#BUILTIN_IMPORTS} so user source can reference
     * them by simple name.
     */
    public static final List<String> BUILTIN_CLASS_FQNS =
            BUILTIN_CLASS_DEFINITIONS.stream().map(ClassDefinition::qualifiedName).toList();
}
