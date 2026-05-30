package com.legend.compiler;

import com.legend.parser.ImportScope;
import com.legend.parser.ParsedModel;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.ConnectionDefinition;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.DatabaseDefinition.FilterDefinition;
import com.legend.parser.element.DatabaseDefinition.JoinDefinition;
import com.legend.parser.element.DatabaseDefinition.ViewDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.Function;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JsonModelConnection;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.NativeFunctionDefinition;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.ProfileDefinition;
import com.legend.parser.element.RuntimeDefinition;
import com.legend.parser.element.ServiceDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Indexed view of a {@link ParsedModel}.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.PureModelBuilder} as
 * the symbol-table layer between parser and downstream phases (the
 * normalizer today; type checker, spec compiler, executor tomorrow).
 * Each phase consumes {@code ModelBuilder} instead of walking
 * {@link ParsedModel} and rebuilding lookups locally.
 *
 * <h2>Lifecycle</h2>
 * One-shot, immutable after construction. Build with
 * {@link #from(ParsedModel)}; after the call returns the instance is
 * read-only and safe to share across threads (all internal state is
 * populated before {@code from} returns; no lazy caches).
 *
 * <h2>Storage layout</h2>
 * Matches engine: every {@link PackageableElement} kind gets its own
 * {@link ArrayList} indexed by id from a single {@link SymbolTable}.
 * Lookups by FQN go through the symbol table to obtain an id, then a
 * direct {@code ArrayList.get(id)}. {@link Function}s (overload sets)
 * use {@code ArrayList<List<Function>>}; all other kinds are at most
 * one entry per FQN.
 *
 * <h2>What this layer does NOT do</h2>
 * <ul>
 *   <li><strong>No name resolution.</strong> {@code ModelBuilder}
 *       reads what is in {@link ParsedModel} verbatim. Imports &rarr;
 *       FQN expansion is the job of {@code ImportResolver} (runs
 *       before {@code ModelBuilder.from}).</li>
 *   <li><strong>No type checking.</strong> Bodies and references are
 *       not validated; that is the type checker's job.</li>
 *   <li><strong>No semantic computation</strong> beyond a tiny set of
 *       indexes the normalizer requires today (filters/joins per
 *       database, mapped-class set). New indexes are added as new
 *       consumers arrive, not speculated.</li>
 *   <li><strong>No mutation after build.</strong> Unlike engine's
 *       {@code PureModelBuilder} (which supports batch ingestion
 *       across multiple sources), this layer is one-shot. Batch
 *       ingestion may be added later when a real driver materializes;
 *       deferred until then.</li>
 * </ul>
 *
 * <h2>Validation performed at build time</h2>
 * <ul>
 *   <li>Within a single {@link MappingDefinition}, a class FQN may
 *       appear in at most one {@link ClassMapping}. Multiple
 *       {@code ClassMapping}s for the same class across different
 *       {@code MappingDefinition}s is allowed (each is its own setId
 *       namespace).</li>
 * </ul>
 * Cross-element validation (duplicate top-level FQNs, dangling
 * references, etc.) is the parser's or a later validator's job. This
 * class deliberately accepts last-write-wins for top-level kinds to
 * preserve current normalizer behavior.
 */
public final class ModelBuilder {

    // ====================================================================
    // Storage
    // ====================================================================

    private final SymbolTable symbols = new SymbolTable();

    // One slot per id; null where the kind doesn't apply to that id.
    private final ArrayList<ClassDefinition>       classes       = new ArrayList<>();
    private final ArrayList<AssociationDefinition> associations  = new ArrayList<>();
    private final ArrayList<EnumDefinition>        enums         = new ArrayList<>();
    private final ArrayList<ProfileDefinition>     profiles      = new ArrayList<>();
    private final ArrayList<DatabaseDefinition>    databases     = new ArrayList<>();
    private final ArrayList<MappingDefinition>     mappings      = new ArrayList<>();
    private final ArrayList<ServiceDefinition>     services      = new ArrayList<>();
    private final ArrayList<RuntimeDefinition>     runtimes      = new ArrayList<>();
    private final ArrayList<ConnectionDefinition>  connections   = new ArrayList<>();

    /**
     * Functions are overload sets keyed by FQN. Each slot holds the
     * ordered list of {@link Function} variants ({@link FunctionDefinition}
     * and/or {@link NativeFunctionDefinition}) for that FQN.
     */
    private final ArrayList<List<Function>>        functions     = new ArrayList<>();

    /**
     * Set of class FQNs that have at least one {@link ClassMapping}
     * anywhere in the model. Backs {@link #isMappedClass(String)}.
     */
    private final Set<Integer> mappedClassIds = new HashSet<>();

    /**
     * Per-database secondary lookup: {@code dbFqn} (interned id) &rarr;
     * {@code filterName} &rarr; {@link FilterDefinition}. Precomputed
     * during ingest to keep {@code ~filter [db]Name} resolution O(1).
     */
    private final Map<Integer, Map<String, FilterDefinition>> filtersByDb = new HashMap<>();

    /**
     * Per-database secondary lookup: {@code dbFqn} (interned id) &rarr;
     * {@code joinName} &rarr; {@link JoinDefinition}. Precomputed during
     * ingest to keep {@code @JoinName} resolution O(1).
     */
    private final Map<Integer, Map<String, JoinDefinition>> joinsByDb = new HashMap<>();

    /**
     * Per-database secondary lookup: {@code dbFqn} (interned id) &rarr;
     * {@code viewName} &rarr; {@link ViewDefinition}. Precomputed during
     * ingest to keep {@code ~mainTable [DB] V} view detection O(1). Keys
     * include both bare view names and {@code SCHEMA.VIEW} dotted forms
     * for per-schema views.
     */
    private final Map<Integer, Map<String, ViewDefinition>> viewsByDb = new HashMap<>();

    private final ImportScope imports;

    // ====================================================================
    // Construction
    // ====================================================================

    private ModelBuilder(ImportScope imports) {
        this.imports = imports;
    }

    /**
     * Builds an indexed view of {@code model}. Phased ingest, mirroring
     * engine's {@code PureModelBuilder.addSources} phasing so that
     * forward references resolve naturally regardless of element order:
     *
     * <ol>
     *   <li><b>Phase 1 — stubs.</b> Intern every element FQN. After this
     *       phase the {@link SymbolTable} has the full name universe;
     *       later phases can freely cross-reference by name.</li>
     *   <li><b>Phase 2 — data-model elements.</b> Classes, enums,
     *       profiles, associations, databases. These are the elements
     *       that mappings, services, runtimes may reference.</li>
     *   <li><b>Phase 3a — top-level definitions.</b> Mappings,
     *       services, functions (and natives), connections.</li>
     *   <li><b>Phase 3b — runtimes.</b> Runtimes come last so their
     *       {@code JsonModelConnection} bindings can cross-bake
     *       synthetic identity {@link ClassMapping.Relational}s (with
     *       {@code sourceUrl} set) into the {@link MappingDefinition}s
     *       they bind, with all bound classes guaranteed to be
     *       registered (engine parity: {@code PureModelBuilder.addRuntime}
     *       performs the same cross-bake during the engine's
     *       phase 5c).</li>
     * </ol>
     *
     * Runs in O(N + filters + joins + views + json-connections).
     *
     * @param model parsed model (non-null); {@link ParsedModel#imports()}
     *              is carried through to {@link #imports()}.
     * @return immutable {@code ModelBuilder} ready for queries
     * @throws IllegalStateException if a {@link MappingDefinition}
     *         contains two {@link ClassMapping}s with the same class FQN
     */
    public static ModelBuilder from(ParsedModel model) {
        Objects.requireNonNull(model, "model");
        ModelBuilder mb = new ModelBuilder(model.imports());

        // Phase 1: intern every FQN so cross-references (e.g. a
        // RuntimeDefinition naming a Class) can resolve in any order.
        for (PackageableElement el : model.elements()) {
            mb.intern(el.qualifiedName());
        }

        // Phase 2: data-model elements. Order within this phase is
        // arbitrary; each element only depends on the symbol table.
        for (PackageableElement el : model.elements()) {
            switch (el) {
                case ClassDefinition cd -> putAtId(mb.classes, mb.intern(cd.qualifiedName()), cd);
                case AssociationDefinition ad -> putAtId(mb.associations, mb.intern(ad.qualifiedName()), ad);
                case EnumDefinition ed -> putAtId(mb.enums, mb.intern(ed.qualifiedName()), ed);
                case ProfileDefinition pd -> putAtId(mb.profiles, mb.intern(pd.qualifiedName()), pd);
                case DatabaseDefinition db -> mb.ingestDatabase(db);
                default -> { /* phase 3 */ }
            }
        }

        // Phase 3a: top-level definitions that may reference phase-2
        // elements. Mappings must be registered before runtimes so the
        // JSON cross-bake (phase 3b) can mutate the bound mapping.
        for (PackageableElement el : model.elements()) {
            switch (el) {
                case MappingDefinition md -> mb.ingestMapping(md);
                case ServiceDefinition sd -> putAtId(mb.services, mb.intern(sd.qualifiedName()), sd);
                case ConnectionDefinition cd -> putAtId(mb.connections, mb.intern(cd.qualifiedName()), cd);
                case FunctionDefinition fd -> mb.appendFunction(fd);
                case NativeFunctionDefinition nfd -> mb.appendFunction(nfd);
                default -> { /* phase 2 or phase 3b */ }
            }
        }

        // Phase 3b: runtimes. Each runtime's JsonModelConnection
        // bindings synthesize an identity ClassMapping.Relational (with
        // sourceUrl set) and inject it into every MappingDefinition the
        // runtime binds, unless the user already declared a class
        // mapping for that class (user wins; engine parity).
        for (PackageableElement el : model.elements()) {
            if (el instanceof RuntimeDefinition rd) mb.ingestRuntime(rd);
        }

        return mb;
    }

    private void ingestDatabase(DatabaseDefinition db) {
        int id = intern(db.qualifiedName());
        putAtId(databases, id, db);
        // Precompute filter, join, and view secondary indexes.
        if (!db.filters().isEmpty() || !db.multiGrainFilters().isEmpty()) {
            Map<String, FilterDefinition> byName = new HashMap<>();
            for (FilterDefinition f : db.filters()) {
                byName.put(f.name(), f);
            }
            for (FilterDefinition f : db.multiGrainFilters()) {
                byName.put(f.name(), f);
            }
            filtersByDb.put(id, byName);
        }
        if (!db.joins().isEmpty()) {
            Map<String, JoinDefinition> byName = new HashMap<>();
            for (JoinDefinition j : db.joins()) {
                byName.put(j.name(), j);
            }
            joinsByDb.put(id, byName);
        }
        // Views: index both top-level and per-schema (schema-qualified)
        // forms. The same view is reachable as "V" (top-level) or as
        // "SCHEMA.V" (qualified) depending on how the user references
        // it from ~mainTable.
        if (!db.views().isEmpty() || !db.schemas().isEmpty()) {
            Map<String, ViewDefinition> byName = new HashMap<>();
            for (ViewDefinition v : db.views()) {
                byName.put(v.name(), v);
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (ViewDefinition v : s.views()) {
                    byName.put(s.name() + "." + v.name(), v);
                    byName.putIfAbsent(v.name(), v);
                }
            }
            if (!byName.isEmpty()) viewsByDb.put(id, byName);
        }
    }

    private void ingestMapping(MappingDefinition md) {
        putAtId(mappings, intern(md.qualifiedName()), md);
        // R2: enforce single ClassMapping per class FQN within one MappingDefinition.
        // Cross-mapping duplication is allowed (each is its own setId
        // namespace); set semantics collapse cross-mapping repeats.
        Set<String> seen = new LinkedHashSet<>();
        for (ClassMapping cm : md.classMappings()) {
            String classFqn = cm.className();
            if (!seen.add(classFqn)) {
                throw new IllegalStateException(
                        "MappingDefinition '" + md.qualifiedName() + "' contains multiple "
                              + "ClassMappings for class '" + classFqn + "'. Each "
                              + "MappingDefinition may map a given class at most once "
                              + "(use distinct MappingDefinitions for alternative "
                              + "ClassMappings of the same class).");
            }
            mappedClassIds.add(intern(classFqn));
        }
    }

    /**
     * Cross-bakes a {@link RuntimeDefinition}'s
     * {@link JsonModelConnection}s into every {@link MappingDefinition}
     * the runtime binds.
     *
     * <p>For each {@code JsonModelConnection(class, url)}:
     * <ul>
     *   <li>For each {@code mappingName} in {@code runtimeDef.mappings()}:
     *       locate the {@link MappingDefinition}. If the user already
     *       wrote a {@link ClassMapping} for the JSON-bound class, skip
     *       (user-authored wins — engine parity).</li>
     *   <li>Otherwise synthesize a {@link ClassMapping.Relational} with
     *       {@code mainTable=null}, {@code sourceUrl=url},
     *       {@code propertyMappings=List.of()}. The normalizer detects
     *       {@code sourceUrl != null} and emits a {@code sourceUrl(url)}
     *       pipeline source with property bindings derived from the
     *       class's declared properties (engine parity:
     *       {@code RelationalMapping.variantIdentity}).</li>
     *   <li>Rebuild the {@code MappingDefinition} with the synthesized
     *       class mapping appended and update {@link #mappedClassIds}.</li>
     * </ul>
     */
    private void ingestRuntime(RuntimeDefinition rd) {
        putAtId(runtimes, intern(rd.qualifiedName()), rd);
        if (rd.jsonConnections().isEmpty()) return;

        for (JsonModelConnection jmc : rd.jsonConnections()) {
            String classFqn = jmc.className();
            for (String mappingName : rd.mappings()) {
                int mappingId = intern(mappingName);
                MappingDefinition md = idGet(mappings, mappingId);
                if (md == null) continue;       // mapping declared in this batch?
                // User-authored class mapping wins.
                boolean alreadyMapped = md.classMappings().stream()
                        .anyMatch(cm -> classFqn.equals(cm.className()));
                if (alreadyMapped) continue;
                ClassMapping.Relational synthetic = new ClassMapping.Relational(
                        classFqn,
                        /* setId */ null,
                        /* extendsSetId */ null,
                        /* root */ true,
                        /* mainTable */ null,
                        /* filter */ null,
                        /* distinct */ false,
                        /* groupBy */ List.of(),
                        /* primaryKey */ List.of(),
                        /* propertyMappings */ List.of(),
                        /* sourceUrl */ jmc.url());
                List<ClassMapping> updated = new ArrayList<>(md.classMappings());
                updated.add(synthetic);
                MappingDefinition rebuilt = md.withClassMappings(updated);
                putAtId(mappings, mappingId, rebuilt);
                mappedClassIds.add(intern(classFqn));
            }
        }
    }

    private void appendFunction(Function fn) {
        int id = intern(fn.qualifiedName());
        ensureCapacity(functions, id);
        List<Function> overloads = functions.get(id);
        if (overloads == null) {
            overloads = new ArrayList<>(2);
            functions.set(id, overloads);
        }
        overloads.add(fn);
    }

    private int intern(String fqn) {
        return symbols.intern(fqn);
    }

    private static <T> void putAtId(ArrayList<T> list, int id, T value) {
        ensureCapacity(list, id);
        list.set(id, value);
    }

    private static <T> void ensureCapacity(ArrayList<T> list, int id) {
        while (list.size() <= id) {
            list.add(null);
        }
    }

    private static <T> T idGet(ArrayList<T> list, int id) {
        if (id < 0 || id >= list.size()) return null;
        return list.get(id);
    }

    // ====================================================================
    // Lookups by FQN
    // ====================================================================

    /** O(1). Returns {@link ClassDefinition} for {@code fqn}, if any. */
    public Optional<ClassDefinition> findClass(String fqn) {
        return Optional.ofNullable(idGet(classes, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link AssociationDefinition} for {@code fqn}, if any. */
    public Optional<AssociationDefinition> findAssociation(String fqn) {
        return Optional.ofNullable(idGet(associations, symbols.resolveId(fqn)));
    }

    /**
     * Resolve an association property on {@code ownerClassFqn}: returns the
     * target-class type of the association end named {@code propName} that is
     * <em>owned by</em> {@code ownerClassFqn}, if any. In an
     * {@code Association(p1: B, p2: A)}, property {@code p1} is declared on the
     * class {@code p2} points at (and vice versa).
     *
     * <p>Association properties are class properties semantically; the
     * {@code MappingNormalizer} consults this so injected per-end association
     * property mappings (Option A; see {@code docs/MAPPING_LEGACY_TO_FUNCTION.md}
     * §5.6.1b) resolve their terminus class. Linear over declared associations
     * (typically few); not indexed.
     */
    public Optional<TypeExpression> findAssociationProperty(String ownerClassFqn,
                                                            String propName) {
        for (AssociationDefinition ad : associations) {
            if (ad == null) continue;
            AssociationDefinition.AssociationEndDefinition p1 = ad.property1();
            AssociationDefinition.AssociationEndDefinition p2 = ad.property2();
            if (p1.propertyName().equals(propName) && isNameRef(p2.targetClass(), ownerClassFqn)) {
                return Optional.of(p1.targetClass());
            }
            if (p2.propertyName().equals(propName) && isNameRef(p1.targetClass(), ownerClassFqn)) {
                return Optional.of(p2.targetClass());
            }
        }
        return Optional.empty();
    }

    private static boolean isNameRef(TypeExpression t, String fqn) {
        return t instanceof TypeExpression.NameRef nr && nr.name().equals(fqn);
    }

    /** O(1). Returns {@link EnumDefinition} for {@code fqn}, if any. */
    public Optional<EnumDefinition> findEnum(String fqn) {
        return Optional.ofNullable(idGet(enums, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link ProfileDefinition} for {@code fqn}, if any. */
    public Optional<ProfileDefinition> findProfile(String fqn) {
        return Optional.ofNullable(idGet(profiles, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link DatabaseDefinition} for {@code fqn}, if any. */
    public Optional<DatabaseDefinition> findDatabase(String fqn) {
        return Optional.ofNullable(idGet(databases, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link MappingDefinition} for {@code fqn}, if any. */
    public Optional<MappingDefinition> findMapping(String fqn) {
        return Optional.ofNullable(idGet(mappings, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link ServiceDefinition} for {@code fqn}, if any. */
    public Optional<ServiceDefinition> findService(String fqn) {
        return Optional.ofNullable(idGet(services, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link RuntimeDefinition} for {@code fqn}, if any. */
    public Optional<RuntimeDefinition> findRuntime(String fqn) {
        return Optional.ofNullable(idGet(runtimes, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link ConnectionDefinition} for {@code fqn}, if any. */
    public Optional<ConnectionDefinition> findConnection(String fqn) {
        return Optional.ofNullable(idGet(connections, symbols.resolveId(fqn)));
    }

    /**
     * O(1). Returns all {@link Function} overloads for {@code fqn}.
     * Empty list if none are declared. The list is unmodifiable.
     */
    public List<Function> findFunction(String fqn) {
        List<Function> hit = idGet(functions, symbols.resolveId(fqn));
        return hit == null ? List.of() : Collections.unmodifiableList(hit);
    }

    // ====================================================================
    // Nested lookups
    // ====================================================================

    /**
     * O(1). Returns the {@link FilterDefinition} named {@code filterName}
     * inside database {@code dbFqn}, if any. Looks across both
     * {@code filters} and {@code multiGrainFilters} (the shapes are
     * structurally identical).
     */
    public Optional<FilterDefinition> findFilter(String dbFqn, String filterName) {
        int id = symbols.resolveId(dbFqn);
        if (id == SymbolTable.UNRESOLVED) return Optional.empty();
        Map<String, FilterDefinition> byName = filtersByDb.get(id);
        if (byName == null) return Optional.empty();
        return Optional.ofNullable(byName.get(filterName));
    }

    /**
     * O(1). Returns the {@link JoinDefinition} named {@code joinName}
     * inside database {@code dbFqn}, if any.
     */
    public Optional<JoinDefinition> findJoin(String dbFqn, String joinName) {
        int id = symbols.resolveId(dbFqn);
        if (id == SymbolTable.UNRESOLVED) return Optional.empty();
        Map<String, JoinDefinition> byName = joinsByDb.get(id);
        if (byName == null) return Optional.empty();
        return Optional.ofNullable(byName.get(joinName));
    }

    /**
     * O(1). Returns the {@link ViewDefinition} named {@code viewName}
     * inside database {@code dbFqn}, if any. Matches both bare names
     * (top-level views) and {@code SCHEMA.NAME} dotted forms
     * (per-schema views). Used by the normalizer to detect when a
     * {@code ~mainTable [DB] X} reference resolves to a view rather
     * than a table.
     */
    public Optional<ViewDefinition> findView(String dbFqn, String viewName) {
        int id = symbols.resolveId(dbFqn);
        if (id == SymbolTable.UNRESOLVED) return Optional.empty();
        Map<String, ViewDefinition> byName = viewsByDb.get(id);
        if (byName == null) return Optional.empty();
        return Optional.ofNullable(byName.get(viewName));
    }

    // ====================================================================
    // Set membership
    // ====================================================================

    /**
     * {@code true} iff some {@link MappingDefinition} contains a
     * {@link ClassMapping} for {@code classFqn}. Used by the normalizer's
     * Layer 3 emission to validate that class-typed {@code Join} PM
     * targets are mapped somewhere in the model.
     */
    public boolean isMappedClass(String classFqn) {
        int id = symbols.resolveId(classFqn);
        return id != SymbolTable.UNRESOLVED && mappedClassIds.contains(id);
    }

    // ====================================================================
    // Iteration accessors
    // ====================================================================

    /** All {@link ClassDefinition}s in ingest order. Sparse slots filtered out. */
    public Stream<ClassDefinition> classes() {
        return classes.stream().filter(Objects::nonNull);
    }

    /** All {@link AssociationDefinition}s in ingest order. */
    public Stream<AssociationDefinition> associations() {
        return associations.stream().filter(Objects::nonNull);
    }

    /** All {@link DatabaseDefinition}s in ingest order. */
    public Stream<DatabaseDefinition> databases() {
        return databases.stream().filter(Objects::nonNull);
    }

    /** All {@link MappingDefinition}s in ingest order. */
    public Stream<MappingDefinition> mappings() {
        return mappings.stream().filter(Objects::nonNull);
    }

    /** All {@link EnumDefinition}s in ingest order. */
    public Stream<EnumDefinition> enums() {
        return enums.stream().filter(Objects::nonNull);
    }

    /** All {@link RuntimeDefinition}s in ingest order. */
    public Stream<RuntimeDefinition> runtimes() {
        return runtimes.stream().filter(Objects::nonNull);
    }

    /** {@link ImportScope} carried through from {@link ParsedModel#imports()}. */
    public ImportScope imports() {
        return imports;
    }

    /**
     * Symbol table escape hatch for callers that want id-based APIs
     * (e.g. a future compiled-graph layer above this AST layer). The
     * normalizer and other AST-level consumers should not need this.
     */
    public SymbolTable symbols() {
        return symbols;
    }
}
