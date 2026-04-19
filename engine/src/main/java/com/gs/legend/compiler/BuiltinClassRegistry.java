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
     * entry. <strong>Fully-qualified names everywhere</strong>: class declarations, property
     * types, stereotype applications, superclass references, generic type arguments. No
     * {@code import} statements, no simple names. Type-parameter letters ({@code T}, {@code U},
     * {@code V}, ...) are the only unqualified tokens and are bound by the enclosing {@code <...>}.
     *
     * <p>This is a hard discipline: the catalog is parsed at JVM init with no populated import
     * scope, and seeded into {@link com.gs.legend.model.PureModelBuilder} via paths (superclass
     * resolution, stereotype canonicalization) that don't uniformly route through the import
     * scope. Requiring FQNs side-steps every "does this layer canonicalize?" question and makes
     * the catalog a self-contained, hermetically-sealed artifact.
     *
     * <p>FQNs and bodies are taken directly from legend-pure / legend-engine upstream sources
     * (see {@code M3Paths.java}, {@code anonymousCollections.pure}, {@code relation.pure},
     * {@code window/{sort,window,frame}.pure}, {@code olap/frame/range.pure},
     * {@code olap/frame/rows.pure}, {@code math/aggregator/mathUtility.pure}). The sole
     * legend-lite-local addition is {@code _Traversal}, a marker type for chainable FK-path
     * hops that does not exist upstream.
     */
    public static final List<String> BUILTIN_CLASS_SOURCES = List.of(

            // ===== M3 bootstrap stubs =====
            // Root of the type lattice; every other class ultimately generalises to Type.
            // Declared native — body is intrinsic to the metamodel, not expressible in Pure.
            "native Class meta::pure::metamodel::type::Type {}",
            // Function<T> / FunctionDefinition<T> — m3 function root and its direct subtype.
            // Used everywhere in native signatures as Function<{T[1]->R[1]}>.
            "native Class meta::pure::metamodel::function::Function<T> {}",
            "native Class meta::pure::metamodel::function::FunctionDefinition<T> {}",
            // Relation<T> — m3 root for the relational algebra types used by
            // filter/project/join/etc. native signatures.
            "native Class meta::pure::metamodel::relation::Relation<T> {}",
            // RootGraphFetchTree<T> — native stub; full body (from graph.pure) depends on
            // Class<T>, Referenceable, GraphFetchTree which we don't catalog yet. legend-lite
            // does not execute graph fetch, so the stub is sufficient for signature validation.
            "native Class meta::pure::graphFetch::RootGraphFetchTree<T> {}",

            // ===== Collections (platform/pure/anonymousCollections.pure) =====
            // Pair<U,V> — canonical two-element heterogeneous tuple; both components participate
            // in structural equality via <<equality.Key>>.
            """
            Class meta::pure::functions::collection::Pair<U, V>
            {
                <<meta::pure::profiles::equality.Key>> first: U[1];
                <<meta::pure::profiles::equality.Key>> second: V[1];
            }
            """,
            """
            Class meta::pure::functions::collection::List<T>
            {
                <<meta::pure::profiles::equality.Key>> values: T[*];
            }
            """,
            // Map<U,V> — ships empty in legend-pure (properties commented out to simplify
            // function refactoring). Matching that shape here.
            "Class meta::pure::functions::collection::Map<U, V> {}",
            """
            Class meta::pure::functions::collection::TreeNode
            {
                childrenData: meta::pure::functions::collection::TreeNode[*];
            }
            """,
            """
            Class meta::pure::functions::collection::ValueHolder<T>
            {
                value: T[1];
            }
            """,

            // ===== Relation column specs (platform/pure/relation.pure) =====
            // ColSpec<T> — named reference to a column in a relation type. The phantom T
            // binds the relation schema; T is never read by the body.
            """
            Class meta::pure::metamodel::relation::ColSpec<T>
            {
                name: meta::pure::metamodel::type::String[1];
            }
            """,
            """
            Class meta::pure::metamodel::relation::ColSpecArray<T>
            {
                names: meta::pure::metamodel::type::String[*];
            }
            """,
            """
            Class meta::pure::metamodel::relation::FuncColSpec<Z, T>
            {
                name: meta::pure::metamodel::type::String[1];
                function: meta::pure::metamodel::function::Function<Z>[1];
            }
            """,
            """
            Class meta::pure::metamodel::relation::FuncColSpecArray<Z, T>
            {
                funcSpecs: meta::pure::metamodel::relation::FuncColSpec<Z, meta::pure::metamodel::type::Any>[*];
            }
            """,
            """
            Class meta::pure::metamodel::relation::AggColSpec<Z, V, T>
            {
                name: meta::pure::metamodel::type::String[1];
                map: meta::pure::metamodel::function::Function<Z>[1];
                reduce: meta::pure::metamodel::function::Function<V>[1];
            }
            """,
            """
            Class meta::pure::metamodel::relation::AggColSpecArray<A, B, T>
            {
                aggSpecs: meta::pure::metamodel::relation::AggColSpec<A, B, meta::pure::metamodel::type::Any>[*];
            }
            """,

            // ===== Window / sort (platform/pure/grammar/functions/meta/type/relation/window) =====
            """
            Class meta::pure::functions::relation::SortInfo<T>
            {
                column: meta::pure::metamodel::relation::ColSpec<T>[1];
                direction: meta::pure::functions::relation::SortType[1];
            }
            """,
            """
            Class meta::pure::functions::relation::_Window<T>
            {
                partition: meta::pure::metamodel::type::String[*];
                sortInfo: meta::pure::functions::relation::SortInfo<meta::pure::metamodel::type::Any>[*];
                frame: meta::pure::functions::relation::Frame[0..1];
            }
            """,

            // ===== Frame hierarchy (platform/pure/.../window/frame.pure + legend-engine olap/frame) =====
            """
            Class meta::pure::functions::relation::Frame
            {
                offsetFrom: meta::pure::functions::relation::FrameValue[1];
                offsetTo: meta::pure::functions::relation::FrameValue[1];
            }
            """,
            "Class meta::pure::functions::relation::FrameValue {}",
            """
            Class meta::pure::functions::relation::FrameIntValue extends meta::pure::functions::relation::FrameValue
            {
                value: meta::pure::metamodel::type::Integer[1];
            }
            """,
            """
            Class meta::pure::functions::relation::FrameNumericValue extends meta::pure::functions::relation::FrameValue
            {
                value: meta::pure::metamodel::type::Number[1];
            }
            """,
            """
            Class meta::pure::functions::relation::FrameIntervalValue extends meta::pure::functions::relation::FrameValue
            {
                value: meta::pure::metamodel::type::Integer[1];
                durationUnit: meta::pure::functions::date::DurationUnit[1];
            }
            """,
            "Class meta::pure::functions::relation::UnboundedFrameValue extends meta::pure::functions::relation::FrameValue {}",
            // _Range and Rows are Frame subtypes declared in legend-engine (not legend-pure core).
            "Class meta::pure::functions::relation::_Range extends meta::pure::functions::relation::Frame {}",
            "Class meta::pure::functions::relation::Rows extends meta::pure::functions::relation::Frame {}",

            // ===== Math aggregator (legend-engine/math/aggregator/mathUtility.pure) =====
            """
            Class meta::pure::functions::math::mathUtility::RowMapper<T, U>
            {
                rowA: T[0..1];
                rowB: U[0..1];
            }
            """,

            // ===== Legend-lite only =====
            // _Traversal — marker type for chainable FK-path hops in extend(rel, traverse(), fn).
            // No upstream counterpart; invented here to replace the "signature-layer pseudo-type"
            // parser hack that was recognising _Traversal as an unconstrained string token.
            "Class meta::pure::functions::relation::_Traversal {}");

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
