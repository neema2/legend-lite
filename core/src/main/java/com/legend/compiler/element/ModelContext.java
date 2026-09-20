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

    /**
     * A DERIVED FACT with the GRAPH's lifetime: {@code derive} runs once
     * per compiled graph (execution overlays are views of the same graph
     * and share it), keyed by the fact's class. For facts that are pure
     * functions of the graph — the system database holding its metamodel
     * rows — never for per-query state.
     */
    <T> T derived(Class<T> key, java.util.function.Function<ModelContext, T> derive);

    /**
     * Every model element FQN (classes, enums, associations, mappings,
     * databases, runtimes, functions) — the wildcard-import candidate
     * universe for SECTION-scoped query resolution.
     */
    default java.util.Set<String> elementFqns() {
        return java.util.Set.of();
    }

    /** {@link #elementFqns()} united with the platform's type universe —
     * the candidate set a query's name resolution reads; an implementation
     * memoizes it (the set is a per-context constant). */
    default java.util.Set<String> resolutionUniverse() {
        java.util.Set<String> u = new java.util.HashSet<>(elementFqns());
        u.addAll(com.legend.compiler.NameResolver.platformFqns());
        return java.util.Set.copyOf(u);
    }

    /**
     * The raw parsed class declaration (stereotypes, tagged values) —
     * {@link TypedClass} deliberately drops annotations, but the store
     * resolver needs the temporal stereotype to pick a milestoning
     * dimension.
     */
    default Optional<com.legend.model.ClassDefinition>
            findClassDefinition(String fqn) {
        return Optional.empty();
    }

    /** The RAW legacy mapping DSL element (pre-Door-1 rewrite) — the
     * static lineage analyzer walks its joins/PMs by name (#44). */
    default Optional<com.legend.model.LegacyMappingDefinition>
            findLegacyMapping(String fqn) {
        return Optional.empty();
    }

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
    Optional<com.legend.model.MappingDefinition> findMapping(String fqn);

    /** The Measure element at {@code fqn}, if the model declares one (a
     * measure named as a value is an instance of the Measure metaclass; its
     * units {@code M~u} are Unit values). */
    Optional<com.legend.model.MeasureDefinition> findMeasure(String fqn);

    /** Whether {@code fqn} names a PACKAGE — a proper prefix of some
     * element's qualified name (m3: a Package value; never a text scan). */
    boolean isPackage(String fqn);

    /** The profile at {@code fqn}, if the model declares one (a profile
     * named as a VALUE is an instance of the Profile metaclass). */
    Optional<com.legend.model.ProfileDefinition> findProfile(String fqn);


    /**
     * The association whose end {@code propName} injects onto
     * {@code ownerClassFqn}, and that end itself &mdash; the Phase-H
     * resolver's navigation dispatch (association FQN &rarr; the mapping's
     * AssociationBinding; the end carries target class + multiplicity).
     */
    Optional<com.legend.model.AssociationDefinition> findAssociationOf(
            String ownerClassFqn, String propName);

    Optional<com.legend.model.AssociationDefinition.AssociationEndDefinition>
            findAssociationEnd(String ownerClassFqn, String propName);

    /**
     * The runtime for {@code fqn}, if present &mdash; supplies the active
     * mapping when a class query names a runtime (explicitly via
     * {@code ->from(...)} or through the driver's execution context).
     */
    Optional<com.legend.model.RuntimeDefinition> findRuntime(
            @com.legend.Nullable String fqn);

    /** Normalization-failure reason for {@code mapping::class}, when its
     * class mapping (the ROOT binding) was poisoned. */
    default Optional<String> mappingPoison(String mappingFqn, String classFqn) {
        return Optional.empty();
    }

    /** Normalization-failure reason for ONE non-root set of {@code class}
     * in {@code mapping} — the per-set fault isolation arm's recorded
     * reason (audit 2026-09-15 P1-1: written and never readable before). */
    default Optional<String> mappingSetPoison(String mappingFqn, String classFqn, String setId) {
        return Optional.empty();
    }

    /** Normalization-failure reason for an association's join synthesis
     * in {@code mapping}. */
    default Optional<String> mappingAssociationPoison(String mappingFqn, String associationFqn) {
        return Optional.empty();
    }

    /** Member set ids of a MIXED-KIND Operation union (a Pure member —
     * resolver-side arm synthesis, route b); {@code null} otherwise. */
    /** The member CLASSES of a union Operation mapping of {@code classFqn}
     * in {@code mappingFqn} (declared member set ids resolved to their
     * classes); null when the class is not union-mapped there or a member
     * set is not declared in that mapping — the chain-position cast rule
     * (StoreResolver) reads the extent's real membership. */
    default java.util.@com.legend.Nullable List<String> unionMemberClasses(
            String mappingFqn, String classFqn) {
        return null;
    }

    /** The class of the set a class-typed property mapping ROUTES to
     * ({@code prop[setId]: @J} on {@code ownerClass}'s Relational set in
     * {@code mappingFqn}); null when unrouted or unknown. */
    default @com.legend.Nullable String routedTargetClass(String mappingFqn,
            String ownerClass, String prop) {
        return null;
    }

    default java.util.@com.legend.Nullable List<String> mixedUnionMembers(String mappingFqn,
            String classFqn) {
        return null;
    }

    /** The primary-key THREADS of the Operation union mapping {@code classFqn}
     * under {@code mappingFqn} (member order, {@code <col>_<ordinal>} with
     * the column's Pure kind) — the engine's importDataFlow columns; null
     * when the class is not a union there. */
    default java.util.@com.legend.Nullable List<com.legend.model.KeyThread> unionKeyThreads(
            String mappingFqn, String classFqn) {
        return null;
    }

    /**
     * The connection for {@code fqn}, if present &mdash; carries the declared
     * {@code DatabaseType} that selects the SQL dialect a runtime's queries
     * render in.
     */
    Optional<com.legend.model.ConnectionDefinition> findConnection(String fqn);

    /** Whether {@code fqn} names a MODEL-store connection (Json/Xml/
     *  ModelChain) — defined, but carrying no database type, so dialect
     *  selection skips rather than refuses it. */
    default boolean isModelConnection(String fqn) {
        return false;
    }

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
     * Whether {@code name} is a TabularFunction rather than a table.
     *
     * Both resolve through {@link #findTable}, because both are named
     * relations with declared columns and the TYPE is identical. Only
     * the rendered source differs -- {@code NAME()} against {@code
     * NAME} -- so the distinction is carried separately rather than
     * splitting the type lookup in two.
     */
    default boolean isTabularFunction(String dbFqn, String name) {
        return false;
    }

    /** Every USER function FQN in the model (natives excluded) — the
     * eager-G compileAll mode enumerates over this. */
    default java.util.Set<String> functionFqns() {
        return java.util.Set.of();
    }

    /** THE VIRTUAL METAMODEL GRAPH source: the compile context's
     * registered instances of {@code classifierFqn}, as element FQNs —
     * the engine's instancesByClassifier index transposed onto our
     * element registry. No metaclass list rides any DISPATCH: coverage
     * is a fact about what the registry stores, and new element kinds
     * join by joining the registry. {@code null} = the registry does not
     * TRACK this classifier (a user class — the store lane owns it); an
     * empty list is an honest empty extent. Deterministic: sorted by FQN. */
    default java.util.@com.legend.Nullable List<String> classifierInstances(
            String classifierFqn) {
        return null;
    }

    /** Whether the registry TRACKS {@code classifierFqn} — the yes/no the
     * resolver asks per element reference; never build the extent to
     * answer it. Equal to {@code classifierInstances(fqn) != null}. */
    default boolean tracksClassifier(String classifierFqn) {
        return classifierInstances(classifierFqn) != null;
    }

    /** Is {@code fqn} a Database store element? (Typed as the store
     * metaclass in value position — see Typer.classReference.) */
    default boolean isDatabase(String fqn) {
        return false;
    }

    /**
     * The full store TABLE DEFINITION (columns with relational data types)
     * — the K-native {@code dropAndCreateTableInDb} renders DDL from it.
     * {@code name} may be schema-qualified ({@code hr.EMPLOYEES}).
     */
    default Optional<com.legend.model.DatabaseDefinition.TableDefinition>
            findTableDefinition(String dbFqn, String name) {
        return Optional.empty();
    }

    /** The full store definition — DDL derivation (the harness's model-
     * driven seeding) enumerates a module's databases through this. */
    default Optional<com.legend.model.DatabaseDefinition>
            findDatabase(@com.legend.Nullable String dbFqn) {
        return Optional.empty();
    }

    /** A join by name, INCLUDE-CLOSURE aware (real Legend: Database
     * MyDb ( include db ) resolves db's joins; own definitions win). */
    default Optional<com.legend.model.DatabaseDefinition.JoinDefinition>
            findJoinDefinition(@com.legend.Nullable String dbFqn, String joinName) {
        return Optional.empty();
    }

    /** The PARSED definition of a user function (module-resolved body) —
     * spec-walking consumers (PK inference #78, derived-property scans). */
    default Optional<com.legend.model.FunctionDefinition>
            findFunctionDefinition(String fqn) {
        return Optional.empty();
    }

    /** EVERY parsed definition under the FQN (the arity overloads), in
     * declaration order; callers select by arity. */
    default java.util.List<com.legend.model.FunctionDefinition>
            findFunctionDefinitions(String fqn) {
        return findFunctionDefinition(fqn).map(java.util.List::of).orElse(java.util.List.of());
    }

    /** The table's temporal columns, when it declares a milestoning block. */
    default Optional<com.legend.model.DatabaseDefinition.TableDefinition.Milestoning>
            findTableMilestoning(String dbFqn, String name) {
        return Optional.empty();
    }

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
        return isSubtype(childFqn, parentFqn, new java.util.HashSet<>());
    }

    /** {@code childFqn <: parentFqn} by the DECLARED generalizations alone
     * — no class is compiled (a scan over every model class must not
     * compile a poisoned one); a generalization cycle contributes nothing
     * new. The compiled {@link #isSubtype} stays the typing question. */
    default boolean isDeclaredSubtype(String childFqn, String parentFqn) {
        return isDeclaredSubtype(childFqn, parentFqn, new java.util.HashSet<>());
    }

    private boolean isDeclaredSubtype(String cls, String sup, java.util.Set<String> visited) {
        if (cls.equals(sup)) {
            return true;
        }
        if (!visited.add(cls)) {
            return false;
        }
        Optional<com.legend.model.ClassDefinition> cd = findClassDefinition(cls);
        if (cd.isEmpty()) {
            return false;
        }
        for (com.legend.protocol.TypeExpression s : cd.get().superClasses()) {
            String name = s instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name()
                    : s instanceof com.legend.protocol.TypeExpression.Generic g ? g.name() : null;
            if (name != null && isDeclaredSubtype(name, sup, visited)) {
                return true;
            }
        }
        return false;
    }

    /** The walk with a VISITED set (DEEP_AUDIT §5b: an inheritance
     * CYCLE — `A extends B` / `B extends A`, which the frontend still
     * accepts — was a StackOverflowError on the first miss; the guard
     * existed in InferenceKernel.ancestorsOf but not in the primitive
     * everything else calls). A revisited node simply contributes no
     * new ancestors. */
    private boolean isSubtype(String childFqn, String parentFqn,
            java.util.Set<String> visited) {
        if (childFqn.equals(parentFqn)) {
            return true;
        }
        // Nil is the BOTTOM type — a subtype of every type (real pure:
        // m3 navigation Type.subTypeOf's explicit isBottomType arm). It is
        // the type of the empty collection [], so LUB(X, Nil) = X and a
        // Nil value conforms to any expected type.
        if (childFqn.equals(com.legend.compiler.element.type.PlatformTypes.NIL)) {
            return true;
        }
        if (!visited.add(childFqn)) {
            return false;
        }
        Optional<TypedClass> child = findClass(childFqn);
        if (child.isEmpty()) {
            return false;
        }
        for (String superFqn : child.get().superClassFqns()) {
            if (isSubtype(superFqn, parentFqn, visited)) {
                return true;
            }
        }
        return false;
    }
}
