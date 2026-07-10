package com.legend.compiler.element;

import com.legend.compiler.element.type.Type;

import java.util.List;
import java.util.Optional;

/**
 * The single lookup choke point over the compiled (Phase F) model &mdash;
 * engine parity with {@code com.gs.legend.model.ModelContext}.
 *
 * <p><strong>An interface, deliberately.</strong> Every subtype / inheritance /
 * member walk goes through {@code findClass(fqn)} rather than dereferencing a
 * live {@code superClass} pointer, so a future dependency-backed implementation
 * (lazy load on cache miss) drops in behind the same contract with no consumer
 * changes (doc §4; AGENTS.md invariant 5). The concrete F implementation wraps
 * the parsed index ({@code compiler/ModelBuilder}) + bootstrap {@code builtin/Pure}
 * elements and serves the compiled {@code Typed*} map.
 *
 * <p><strong>One member API.</strong> {@link #findProperty} returns the
 * polymorphic {@link Property} (stored / derived / association-injected) from a
 * single member-walk &mdash; there is no {@code findDerivedProperty} or
 * {@code findConstraint}. A derived property's body, like every body, is
 * resolved by the one {@link #findFunction}. Constraints are metadata on
 * {@link TypedClass}, not a lookup (doc §1.4, §4).
 */
public interface ModelContext {

    /** O(1)-ish. The {@link TypedClass} for {@code fqn}, if present. */
    Optional<TypedClass> findClass(String fqn);

    /** The {@link TypedEnum} for {@code fqn}, if present. */
    Optional<TypedEnum> findEnum(String fqn);

    /**
     * The overload set for {@code fqn} (empty if none). This is the
     * <strong>single</strong> function/body resolver &mdash; user, native, and
     * synthesized (derived-property, constraint, service-query) functions are
     * all found here. The call site disambiguates overloads by arity +
     * argument types; there is no per-overload key at this layer.
     */
    List<TypedFunction> findFunction(String fqn);

    /**
     * The canonical (post-normalization) mapping for {@code fqn}, if present
     * &mdash; the Phase-H resolver's dispatch surface: its
     * {@code ClassBinding}s name the realizing functions whose typed bodies
     * the resolver inlines.
     */
    Optional<com.legend.parser.element.MappingDefinition> findMapping(String fqn);

    /**
     * The association whose end {@code propName} injects onto
     * {@code ownerClassFqn}, and that end itself &mdash; the Phase-H
     * resolver's navigation dispatch (association FQN &rarr; the mapping's
     * AssociationBinding; the end carries target class + multiplicity).
     */
    Optional<com.legend.parser.element.AssociationDefinition> findAssociationOf(
            String ownerClassFqn, String propName);

    Optional<com.legend.parser.element.AssociationDefinition.AssociationEndDefinition>
            findAssociationEnd(String ownerClassFqn, String propName);

    /**
     * The runtime for {@code fqn}, if present &mdash; supplies the active
     * mapping when a class query names a runtime (explicitly via
     * {@code ->from(...)} or through the driver's execution context).
     */
    Optional<com.legend.parser.element.RuntimeDefinition> findRuntime(String fqn);

    /**
     * Classify an FQN into a kinded {@link Type}: <strong>primitive &rarr; class
     * &rarr; enum</strong> (engine {@code findType} parity), FQN-only. The single
     * place a name becomes a kind for nominal references.
     */
    Optional<Type> findType(String fqn);

    /**
     * Resolve a member named {@code name} on {@code classFqn}, walking
     * {@link TypedClass#superClassFqns()} via {@link #findClass}. Returns stored,
     * derived, and association-injected navigation properties uniformly as
     * {@link Property} (the latter resolved from the association index, not
     * stored on the class &mdash; doc §5 discipline 3). The most-derived
     * declaration wins.
     */
    Optional<Property> findProperty(String classFqn, String name);

    /**
     * Resolve a relational table {@code name} within database {@code dbFqn} to its
     * column schema as a bare {@link Type.RelationType} (engine
     * {@code ModelContext.findTable} &rarr; {@code Table}, here pre-projected to the
     * row-struct the Phase-G {@code tableReference} source consumes &mdash; doc
     * &sect;G-&alpha;). Columns appear in declaration order; each column's
     * multiplicity is {@code [1]} when {@code NOT NULL}/{@code PRIMARY KEY}, else
     * {@code [0..1]}. Searches the database's top-level tables, then its schemas'
     * tables. Empty if the database or table is absent.
     */
    Optional<Type.RelationType> findTable(String dbFqn, String name);

    /**
     * Whether {@code fqn} names an <em>execution-context</em> element &mdash; a
     * mapping, runtime, connection, or database. These are referenced as values by
     * {@code from}/{@code write} (typed {@code Any[1]}, exactly their signature
     * parameters); they are not classes and carry no member surface.
     */
    boolean isExecutionContextElement(String fqn);

    /**
     * {@code true} iff {@code childFqn} is {@code parentFqn} or a (transitive)
     * subtype, walking superclass FQNs through {@link #findClass} so lazy /
     * cross-project resolution stays transparent. Primitives participate too,
     * since the bootstrap primitive lattice (Integer &lt; Number &lt; Any) is
     * carried as {@link TypedClass} superclass chains.
     */
    default boolean isSubtype(String childFqn, String parentFqn) {
        if (childFqn.equals(parentFqn)) {
            return true;
        }
        // Nil is the BOTTOM type — a subtype of every type (real pure:
        // m3 navigation Type.subTypeOf's explicit isBottomType arm). It is
        // the type of the empty collection [], so LUB(X, Nil) = X and a
        // Nil value conforms to any expected type.
        if (childFqn.equals("meta::pure::metamodel::type::Nil")) {
            return true;
        }
        Optional<TypedClass> child = findClass(childFqn);
        if (child.isEmpty()) {
            return false;
        }
        for (String superFqn : child.get().superClassFqns()) {
            if (isSubtype(superFqn, parentFqn)) {
                return true;
            }
        }
        return false;
    }
}
