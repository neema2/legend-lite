package com.legend.compiler;

import com.legend.model.ImportScope;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassMapping;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;
import com.legend.model.ConnectionDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.DatabaseDefinition.FilterDefinition;
import com.legend.model.DatabaseDefinition.JoinDefinition;
import com.legend.model.DatabaseDefinition.ViewDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ProfileDefinition;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;

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
 * ONE index per graph (T4.1 step 2). Built with {@link #from(ParsedModel)}
 * from the name-resolved, knowledge-adopted elements BEFORE Phase E;
 * Phase E reads it and writes nothing into it (its products ride the
 * compiled mapping); at the E&rarr;F gate {@link #add(List)} indexes
 * those products and the boot layer's prepared elements. After the gate
 * the instance is read-only and safe to share across threads (the two
 * lazy indexes rebuild on first read after a batch).
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
 *       indexes the normalizer requires today (filters/joins/views per
 *       database). Mapping facts (which classes are mapped, poisons,
 *       unions) are Phase E's own products, never held here.</li>
 *   <li><strong>No mutation by a phase.</strong> Only the driver adds
 *       batches ({@link #add}); a phase that needs to record something
 *       records it on its own artifact.</li>
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
public final class ModelBuilder implements com.legend.compiler.StoreLookups {

    // ====================================================================
    // Storage
    // ====================================================================

    private final SymbolTable symbols = new SymbolTable();

    /** Element ids in REGISTRATION order across every {@link #add} batch
     * &mdash; the iteration order the accessors publish ("ingest order"),
     * independent of when a name was first interned as a reference. */
    private final java.util.LinkedHashSet<Integer> elementOrder = new java.util.LinkedHashSet<>();
    /** Ids taken in the shared packageable-element namespace (classes,
     * enums, associations, profiles, measures, databases) across batches
     * &mdash; a second registration is a duplicate (D6b). */
    private final java.util.Set<Integer> registeredElements = new java.util.HashSet<>();

    // One slot per id; null where the kind doesn't apply to that id.
    private final ArrayList<ClassDefinition>       classes       = new ArrayList<>();
    private final ArrayList<AssociationDefinition> associations  = new ArrayList<>();
    /**
     * Association ends indexed by {@code ownerFqn -> propName ->} every
     * (association, end) injecting that property onto that class, in
     * declaration order — the ONE index behind {@link #findAssociationEnd}
     * and {@link #findAssociationOf}. Association navigation is on the
     * type checker's and the resolver's HOT PATH, so both are lookups, not
     * scans. Built on first use after a batch (all associations are
     * interned by then) and dropped when a batch is added.
     */
    private @com.legend.base.Nullable Map<String, Map<String, List<InjectedEnd>>>
            associationEndsByOwner;

    /** One association end injected onto a class. */
    private record InjectedEnd(AssociationDefinition association,
            AssociationDefinition.AssociationEndDefinition end) {
    }
    private final ArrayList<EnumDefinition>        enums         = new ArrayList<>();
    private final ArrayList<ProfileDefinition>     profiles      = new ArrayList<>();
    private final ArrayList<com.legend.model.MeasureDefinition> measures = new ArrayList<>();
    private final ArrayList<DatabaseDefinition>    databases     = new ArrayList<>();
    // Legacy mapping surface trees — the cross-bake and MappingNormalizer read
    // these. Canonical binding tables — Phase F / dispatch read these. Both can
    // be populated in one build: a model may mix legacy-DSL mappings and
    // clean-sheet (Door 1) mappings, which parse to the two record types.
    private final ArrayList<LegacyMappingDefinition> legacyMappings = new ArrayList<>();
    private final ArrayList<MappingDefinition>     mappings      = new ArrayList<>();
    private final ArrayList<ServiceDefinition>     services      = new ArrayList<>();
    /** {@code ###Data} elements — the bodies test suites reference by name. */
    private final ArrayList<com.legend.model.DataDefinition> dataElements = new ArrayList<>();
    private final ArrayList<RuntimeDefinition>     runtimes      = new ArrayList<>();
    private final ArrayList<ConnectionDefinition>  connections   = new ArrayList<>();
    /** Named model-store connections (Json/Xml/ModelChain) — indexed so a
     *  runtime binding to one is distinguishable from an undefined name;
     *  execution semantics (cross-baking a POINTED-AT model connection)
     *  arrive with the Runtime migration. */
    private final Map<String, com.legend.model.ModelConnectionDefinition>
            modelConnections = new HashMap<>();
    private final Map<String, com.legend.model.ModelChainConnectionDefinition>
            modelChainConnections = new HashMap<>();
    /** Foreign-flavor connections (ServiceStore/Deephaven/Mongo) — defined,
     *  carrying no database type; dialect selection skips them. */
    private final java.util.Set<String> foreignConnections =
            new java.util.HashSet<>();

    /**
     * Functions are overload sets keyed by FQN. Each slot holds the
     * ordered list of {@link Function} variants ({@link FunctionDefinition}
     * and/or {@link NativeFunctionDefinition}) for that FQN.
     */
    private final ArrayList<List<Function>>        functions     = new ArrayList<>();
    /** Engine signature id -> the overloads declaring it, registered with the
     *  function (a function's element name upstream IS its id). */
    private final java.util.Map<String, List<Function>> functionsById = new java.util.HashMap<>();

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
    private final Map<Integer, Map<String, IndexedView>> viewsByDb = new HashMap<>();
    /** Per-database tables by spelling ({@link TableIndex}), built at ingest
     * beside the join and view indexes. */
    private final Map<Integer, TableIndex> tablesByDb = new HashMap<>();
    private final Map<Integer, List<IndexedView>> viewsInOrder = new HashMap<>();

    /**
     * Element FQNs registered MORE THAN ONCE in the packageable-element
     * namespace (classes, enums, associations, profiles, databases — one
     * shared namespace, engine parity: {@code PureModelBuilder} rejects
     * "Duplicated element"). Recorded here at ingest — last-wins slotting
     * has already discarded the loser by the time integrity runs — and
     * THROWN by {@code ModelIntegrity} so the poison-not-drop/wallSink
     * discipline applies uniformly (D6b frontend-leniency batch).
     * Insertion-ordered for deterministic first-error reporting.
     */
    private final java.util.LinkedHashMap<String, String> duplicateElements =
            new java.util.LinkedHashMap<>();

    private final ImportScope imports;
    private final java.util.Map<String, ImportScope> elementImports;

    // ====================================================================
    // Construction
    // ====================================================================

    private ModelBuilder(ImportScope imports) {
        this(imports, java.util.Map.of());
    }

    private ModelBuilder(ImportScope imports,
            java.util.Map<String, ImportScope> elementImports) {
        this.elementImports = elementImports;
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
        ModelBuilder mb = new ModelBuilder(model.imports(),
                model.elementImports());
        mb.add(model.elements());
        return mb;
    }

    /**
     * Index a BATCH of elements (T4.1 step 2: ONE index per graph — built
     * from the parsed elements before Phase E, then Phase E's products and
     * the boot layer's prepared elements are ADDED at the E&rarr;F gate;
     * nothing is re-indexed). An element already indexed under its FQN
     * (the same object) is skipped, so a normalized element list &mdash;
     * pass-through structural elements plus new compiled mappings and
     * lifted functions &mdash; adds exactly the new ones. Every batch runs
     * the same phases; the lazy indexes rebuild on the next read.
     */
    public void add(List<PackageableElement> elements) {
        List<PackageableElement> fresh = new ArrayList<>(elements.size());
        for (PackageableElement el : elements) {
            if (!indexed(el)) {
                fresh.add(el);
            }
        }
        if (fresh.isEmpty()) {
            return;
        }
        directSubclasses = null;
        associationEndsByOwner = null;
        knowledge = null;

        // Phase 1: intern every FQN so cross-references (e.g. a
        // RuntimeDefinition naming a Class) can resolve in any order;
        // registration order is the accessors' iteration order.
        for (PackageableElement el : fresh) {
            elementOrder.add(intern(el.qualifiedName()));
        }

        // Phase 2: data-model elements. Order within this phase is
        // arbitrary; each element only depends on the symbol table.
        // One shared element namespace across the kinds below: a second
        // registration of ANY kind under an already-taken FQN is a
        // duplicate (recorded, thrown by ModelIntegrity — D6b).
        for (PackageableElement el : fresh) {
            switch (el) {
                case ClassDefinition cd -> putAtId(classes,
                        internElement(cd.qualifiedName()), cd);
                case com.legend.model.PrimitiveExtensionDefinition pe ->
                        primitiveExtensions.put(pe.qualifiedName(), pe.baseTypeName());
                case AssociationDefinition ad -> putAtId(associations,
                        internElement(ad.qualifiedName()), ad);
                case EnumDefinition ed -> putAtId(enums,
                        internElement(ed.qualifiedName()), ed);
                case ProfileDefinition pd -> putAtId(profiles,
                        internElement(pd.qualifiedName()), pd);
                case com.legend.model.MeasureDefinition me -> putAtId(measures,
                        internElement(me.qualifiedName()), me);
                case DatabaseDefinition db -> ingestDatabase(db);
                default -> { /* phase 3 */ }
            }
        }

        // Phase 3a: top-level definitions that may reference phase-2
        // elements. Lifted behavior functions (Phase E output) need no
        // special pass: they are ordinary FunctionDefinition elements,
        // ingested by the function arm exactly like user-written functions
        // (docs/CLEAN_SHEET_INVERSION.md §2.2); their reserved '$' sigil
        // cannot collide with a user-writable name in findFunction.
        for (PackageableElement el : fresh) {
            switch (el) {
                case LegacyMappingDefinition md -> ingestLegacyMapping(md);
                case MappingDefinition md -> putAtId(mappings, intern(md.qualifiedName()), md);
                case ServiceDefinition sd -> putAtId(services, intern(sd.qualifiedName()), sd);
                case com.legend.model.DataDefinition dd ->
                        putAtId(dataElements, intern(dd.qualifiedName()), dd);
                case ConnectionDefinition cd -> putAtId(connections, intern(cd.qualifiedName()), cd);
                case com.legend.model.ModelConnectionDefinition mc ->
                        modelConnections.put(mc.qualifiedName(), mc);
                case com.legend.model.ModelChainConnectionDefinition mcc ->
                        modelChainConnections.put(mcc.qualifiedName(), mcc);
                case FunctionDefinition fd -> appendFunction(fd);
                case NativeFunctionDefinition nfd -> appendFunction(nfd);
                default -> { /* phase 2 or phase 3b */ }
            }
        }

        // Phase 3b: runtimes (their inline connections register here).
        // A runtime's JsonModelConnection identity sets are Phase E's
        // pre-pass product (MappingPrePass), not an index-time rewrite of
        // the bound mapping.
        for (PackageableElement el : fresh) {
            if (el instanceof RuntimeDefinition rd) ingestRuntime(rd);
        }
    }

    /** Whether {@code el} (this very object) already sits in its slot. */
    private boolean indexed(PackageableElement el) {
        int id = symbols.resolveId(el.qualifiedName());
        if (id == SymbolTable.UNRESOLVED) {
            return false;
        }
        return switch (el) {
            case ClassDefinition cd -> idGet(classes, id) == cd;
            case AssociationDefinition ad -> idGet(associations, id) == ad;
            case EnumDefinition ed -> idGet(enums, id) == ed;
            case ProfileDefinition pd -> idGet(profiles, id) == pd;
            case com.legend.model.MeasureDefinition me -> idGet(measures, id) == me;
            case DatabaseDefinition db -> idGet(databases, id) == db;
            case LegacyMappingDefinition md -> idGet(legacyMappings, id) == md;
            case MappingDefinition md -> idGet(mappings, id) == md;
            case ServiceDefinition sd -> idGet(services, id) == sd;
            case com.legend.model.DataDefinition dd -> idGet(dataElements, id) == dd;
            case ConnectionDefinition cd -> idGet(connections, id) == cd;
            case RuntimeDefinition rd -> idGet(runtimes, id) == rd;
            case Function fn -> {
                List<Function> overloads = idGet(functions, id);
                yield overloads != null && overloads.stream().anyMatch(f -> f == fn);
            }
            case com.legend.model.PrimitiveExtensionDefinition pe ->
                    primitiveExtensions.containsKey(pe.qualifiedName());
            case com.legend.model.ModelConnectionDefinition mc ->
                    modelConnections.get(mc.qualifiedName()) == mc;
            case com.legend.model.ModelChainConnectionDefinition mcc ->
                    modelChainConnections.get(mcc.qualifiedName()) == mcc;
            // the pre-E clean-sheet surface is Phase E's input only; the
            // index holds nothing for it
            case com.legend.model.CleanSheetMappingDefinition cs -> true;
            default -> false;
        };
    }

    /** Intern + record a duplicate when {@code fqn}'s slot is already
     * taken in this build's shared element namespace (engine parity:
     * "Duplicated element"). Registration still proceeds last-wins; the
     * throw is ModelIntegrity's (poison-not-drop). */
    private int internElement(String fqn) {
        int id = intern(fqn);
        if (!registeredElements.add(id)) {
            duplicateElements.putIfAbsent(fqn,
                    "Duplicated element '" + fqn + "'");
        }
        return id;
    }

    /** Element FQNs registered more than once (see field doc). */
    public java.util.Map<String, String> duplicateElements() {
        return java.util.Collections.unmodifiableMap(duplicateElements);
    }

    private void ingestDatabase(DatabaseDefinition db) {
        int id = internElement(db.qualifiedName());
        putAtId(databases, id, db);
        tablesByDb.put(id, TableIndex.of(db));
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
            // THE ONE place a view's spelling is decided: a top-level view is
            // its bare name, a schema view is SCHEMA.NAME (reachable bare too,
            // first declared wins) — the accessor, the lift and every lookup
            // read the spelling from here
            Map<String, IndexedView> byName = new HashMap<>();
            List<IndexedView> declared = new ArrayList<>();
            for (ViewDefinition v : db.views()) {
                IndexedView iv = new IndexedView(v, new ViewLift(db.qualifiedName(), v.name()));
                byName.put(v.name(), iv);
                declared.add(iv);
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (ViewDefinition v : s.views()) {
                    IndexedView iv = new IndexedView(v,
                            new ViewLift(db.qualifiedName(), s.name() + "." + v.name()));
                    byName.put(iv.lift().spelling(), iv);
                    byName.putIfAbsent(v.name(), iv);
                    declared.add(iv);
                }
            }
            if (!byName.isEmpty()) {
                viewsByDb.put(id, byName);
                viewsInOrder.put(id, List.copyOf(declared));
            }
        }
    }

    /** Precise primitives: extension FQN → declared base type name (chains allowed). */
    final java.util.Map<String, String> primitiveExtensions = new java.util.LinkedHashMap<>();

    /**
     * The BASE {@link Type.Primitive} behind a primitive-extension FQN,
     * chasing extension-of-extension chains. EXACT-FQN lookup only —
     * references resolve to FQNs in NameResolver (extensions are in
     * knownFqns), so fuzzy matching here would be the banned suffix-match
     * pattern (two same-simple-named extensions, or an extension whose FQN
     * suffix collides with a class, would silently mis-resolve).
     */
    /** The declared primitive-extension FQNs — {@code elementFqns()}
     * publishes them so NameResolver's {@code knownFqns} sees them and
     * the EXACT-FQN precondition above actually holds (leg 7b R0: a
     * simple {@code @ExtendedInteger} under a wildcard import never
     * qualified, so findPrimitiveExtension never matched). */
    public java.util.Set<String> primitiveExtensionFqns() {
        return primitiveExtensions.keySet();
    }

    public java.util.Optional<com.legend.compiler.element.type.Type.Primitive> findPrimitiveExtension(String name) {
        String cur = name;
        for (int hops = 0; hops < 16; hops++) {
            String base = primitiveExtensions.get(cur);
            if (base == null) {
                return java.util.Optional.empty();
            }
            var prim = com.legend.compiler.element.type.Type.Primitive.findByFqn(base);
            if (prim.isEmpty()) {
                prim = com.legend.compiler.element.type.Type.Primitive.findByFqn("meta::pure::metamodel::type::" + base);
            }
            if (prim.isPresent()) {
                return prim;
            }
            cur = base;
        }
        return java.util.Optional.empty();
    }

    private void ingestLegacyMapping(LegacyMappingDefinition md) {
        putAtId(legacyMappings, intern(md.qualifiedName()), md);
        // R2: within one MappingDefinition a class maps EITHER once, or
        // through multiple set-ID'd ClassMappings of which exactly ONE
        // carries the root marker (* picks the .all() set). Anything else —
        // duplicate set IDs, zero roots, two roots — is the R2 error.
        java.util.Map<String, java.util.List<ClassMapping>> byClass = new java.util.LinkedHashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            byClass.computeIfAbsent(cm.className(), k -> new java.util.ArrayList<>()).add(cm);
        }
        for (var e : byClass.entrySet()) {
            if (e.getValue().size() == 1) {
                continue;
            }
            long roots = e.getValue().stream().filter(ClassMapping::root).count();
            Set<String> setIds = new LinkedHashSet<>();
            // a UNION Operation set needs no id of its own (its members do).
            // Identity = EFFECTIVE id (explicit or the engine's implicit
            // classFqn-underscored default) — raw setId() spuriously
            // rejected one-explicit-one-implicit and missed effective
            // collisions (audit 11a-F3).
            boolean idsDistinct = e.getValue().stream()
                    .allMatch(cm -> cm instanceof ClassMapping.Union && cm.setId() == null
                            || setIds.add(com.legend.model.SetId.of(cm)));
            if (roots != 1 || !idsDistinct) {
                // ZERO roots with distinct set IDs is the UNION shape: the
                // root lives in an Operation class mapping (a roadmap
                // family the parser skips). The model must still LOAD —
                // other classes in it are queryable; fetching THIS class
                // stays loud at resolution (multi-set wall). Duplicate set
                // IDs / two roots remain build errors.
                if (roots == 0 && idsDistinct) {
                    continue;
                }
                throw new com.legend.error.ModelException(
                              com.legend.error.LegendCompileException.Phase.NORMALIZE,
                        "MappingDefinition '" + md.qualifiedName() + "' contains multiple "
                              + "ClassMappings for class '" + e.getKey() + "'. Each "
                              + "MappingDefinition may map a given class at most once, "
                              + "or through distinct set IDs with exactly one root "
                              + "marker (*) naming the .all() set.");
            }
        }
        // CROSS-CLASS effective-id collision (engine MappingValidator spans
        // all class mappings): silent bySetId overwrites downstream would
        // bind extends/union references to the wrong set (audit 11a-F3).
        java.util.Map<String, String> idOwner = new java.util.HashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Union && cm.setId() == null) {
                continue;
            }
            String effective = com.legend.model.SetId.of(cm);
            String prev = idOwner.putIfAbsent(effective, cm.className());
            if (prev != null && !prev.equals(cm.className())) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.NORMALIZE,
                        "MappingDefinition '" + md.qualifiedName() + "': set id '"
                                + effective + "' is used by class mappings of both '"
                                + prev + "' and '" + cm.className()
                                + "' — duplicated set ids are ambiguous");
            }
        }
    }

    /** A runtime and its anonymous inline connections. */
    private void ingestRuntime(RuntimeDefinition rd) {
        putAtId(runtimes, intern(rd.qualifiedName()), rd);
        // Anonymous embedded connections (names carry the reserved '$'
        // sigil) register like their standalone twins, so the runtime's
        // bindings to them resolve in findConnection / isModelConnection.
        for (PackageableElement inline : rd.inlineConnections()) {
            switch (inline) {
                case ConnectionDefinition cd ->
                        putAtId(connections, intern(cd.qualifiedName()), cd);
                case com.legend.model.ModelConnectionDefinition mc ->
                        modelConnections.put(mc.qualifiedName(), mc);
                case com.legend.model.ModelChainConnectionDefinition mcc ->
                        modelChainConnections.put(mcc.qualifiedName(), mcc);
                case com.legend.model.GenericSectionElementDefinition ge ->
                        foreignConnections.add(ge.qualifiedName());
                default -> throw new IllegalStateException(
                        "unexpected inline connection kind: "
                                + inline.getClass().getSimpleName());
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
        functionsById.computeIfAbsent(com.legend.model.SignatureMangle.mangle(fn),
                k -> new ArrayList<>(1)).add(fn);
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

    private static <T> @com.legend.base.Nullable T idGet(ArrayList<T> list, int id) {
        if (id < 0 || id >= list.size()) return null;
        return list.get(id);
    }

    // ====================================================================
    // Lookups by FQN
    // ====================================================================

    /** Lazily built DIRECT subclass index over the model's classes (super
     * FQN &rarr; declaring classes, ingest order); the model is fully
     * ingested before any consumer asks. */
    private @com.legend.base.Nullable Map<String, List<String>> directSubclasses;

    /** THE knowledge kernel over this index (F1): class lookup
     * native-first, the memoized subtype relation, the ancestor and
     * subtree walks — one implementation for Phase E and Phase F alike.
     * Derived from the index and rebuilt when a batch is added. */
    private @com.legend.base.Nullable KnowledgeLayer knowledge;

    public KnowledgeLayer knowledge() {
        KnowledgeLayer k = knowledge;
        if (k == null) {
            k = new KnowledgeLayer(this);
            knowledge = k;
        }
        return k;
    }

    /** The model classes that DIRECTLY extend {@code fqn} (ingest order;
     * empty when none) — "the subclasses of X" as a walk of X's subtree,
     * never a scan of every class (UnionSynthesis's inheritance members
     * scanned the whole universe per class per call). */
    public List<String> directSubclasses(String fqn) {
        Map<String, List<String>> index = directSubclasses;
        if (index == null) {
            index = new HashMap<>();
            for (ClassDefinition cd : classes) {
                if (cd == null) {
                    continue;
                }
                // bare and generic heads alike (extends Foo<T> IS a
                // superclass) — the relation isSubtype walks upward
                for (TypeExpression sup : cd.superClasses()) {
                    String head = TypeExpression.rawClassName(sup);
                    if (head != null) {
                        index.computeIfAbsent(head, k -> new ArrayList<>())
                                .add(cd.qualifiedName());
                    }
                }
            }
            directSubclasses = index;
        }
        List<String> subs = index.get(fqn);
        return subs == null ? List.of() : Collections.unmodifiableList(subs);
    }

    /** O(1). Returns {@link ClassDefinition} for {@code fqn}, if any. */
    public Optional<ClassDefinition> findClass(@com.legend.base.Nullable String fqn) {
        if (fqn == null) {
            return Optional.empty();
        }
        ClassDefinition exact = idGet(classes, symbols.resolveId(fqn));
        if (exact != null) {
            return Optional.of(exact);
        }
        // EXACT-FQN ONLY (NAME_RESOLUTION_BUG.md) — see findDatabase.
        return Optional.empty();
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
     * §5.6.1b) resolve their terminus class. O(1) via the lazily built
     * association-end index (the type checker's hot path).
     */
    public Optional<TypeExpression> findAssociationProperty(String ownerClassFqn,
                                                            String propName) {
        return findAssociationEnd(ownerClassFqn, propName)
                .map(AssociationDefinition.AssociationEndDefinition::targetClass);
    }

    /**
     * The full association END injecting {@code propName} onto
     * {@code ownerClassFqn} (name + target class + multiplicity) — Phase F's
     * lookup-time resolution of association navigation properties
     * (never stored on the class; Property doc §5 discipline 3).
     */
    /**
     * The ASSOCIATION whose end named {@code propName} injects onto
     * {@code ownerClassFqn} — the Phase-H resolver's dispatch key from a
     * navigation to the mapping's AssociationBinding. Linear scan
     * (associations are few; the hot per-end lookup is the indexed
     * {@link #findAssociationEnd}).
     */
    public Optional<AssociationDefinition> findAssociationOf(String ownerClassFqn,
                                                             String propName) {
        // INHERITANCE (real pure): an association end injects onto the
        // named class AND its subclasses (extends-family: AE's 'e' end on
        // A serves B extends A). Nearest class wins; ambiguity stays loud
        // PER LEVEL (the split-brain rule below).
        java.util.Set<String> visited = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<String> level = new java.util.ArrayDeque<>();
        level.add(ownerClassFqn);
        while (!level.isEmpty()) {
            String cls = level.poll();
            if (!visited.add(cls)) {
                continue;
            }
            Optional<AssociationDefinition> hit =
                    findAssociationAtClass(cls, propName);
            if (hit.isPresent()) {
                return hit;
            }
            findClass(cls).ifPresent(cd -> {
                for (var sup : cd.superClasses()) {
                    if (sup instanceof com.legend.protocol.TypeExpression.NameRef nr) {
                        level.add(nr.name());
                    }
                }
            });
        }
        return Optional.empty();
    }

    private Optional<AssociationDefinition> findAssociationAtClass(
            String ownerClassFqn, String propName) {
        java.util.List<AssociationDefinition> hits = new java.util.ArrayList<>();
        for (InjectedEnd e : injectedEnds(ownerClassFqn, propName)) {
            // a self-association naming both ends alike injects twice: one association
            if (hits.isEmpty() || hits.get(hits.size() - 1) != e.association()) {
                hits.add(e.association());
            }
        }
        if (hits.size() > 1) {
            // Two associations injecting the same property name onto one
            // class: first-wins here vs last-wins in the end index was a
            // SPLIT-BRAIN (one association's condition on another's table —
            // silent wrong SQL). Real pure rejects the model; loud.
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    "property '" + propName + "' is injected onto class '"
                            + ownerClassFqn + "' by " + hits.size() + " associations ("
                            + hits.stream().map(AssociationDefinition::qualifiedName)
                                    .collect(java.util.stream.Collectors.joining(", "))
                            + "); duplicate association-end names are ambiguous",
                    ownerClassFqn);
        }
        return hits.isEmpty() ? Optional.empty() : Optional.of(hits.get(0));
    }

    /** The end named {@code propName} injected onto {@code ownerClassFqn};
     * with several, the LAST declared (see {@link #findAssociationAtClass},
     * which refuses the ambiguity). */
    public Optional<AssociationDefinition.AssociationEndDefinition> findAssociationEnd(
            String ownerClassFqn, String propName) {
        List<InjectedEnd> ends = injectedEnds(ownerClassFqn, propName);
        return ends.isEmpty() ? Optional.empty() : Optional.of(ends.get(ends.size() - 1).end());
    }

    private List<InjectedEnd> injectedEnds(String ownerClassFqn, String propName) {
        Map<String, Map<String, List<InjectedEnd>>> idx = associationEndsByOwner;
        if (idx == null) {
            idx = new HashMap<>();
            for (AssociationDefinition ad : associations) {
                if (ad == null) continue;
                // each end injects onto the class the OPPOSITE end targets
                indexEnd(idx, ad, ad.property2().targetClass(), ad.property1());
                indexEnd(idx, ad, ad.property1().targetClass(), ad.property2());
            }
            associationEndsByOwner = idx;
        }
        return idx.getOrDefault(ownerClassFqn, Map.of()).getOrDefault(propName, List.of());
    }

    private static void indexEnd(Map<String, Map<String, List<InjectedEnd>>> idx,
            AssociationDefinition ad, TypeExpression ownerRef,
            AssociationDefinition.AssociationEndDefinition end) {
        if (ownerRef instanceof TypeExpression.NameRef n) {
            idx.computeIfAbsent(n.name(), k -> new HashMap<>())
                    .computeIfAbsent(end.propertyName(), k -> new ArrayList<>())
                    .add(new InjectedEnd(ad, end));
        }
    }

    /** O(1). Returns {@link EnumDefinition} for {@code fqn}, if any. */
    public Optional<EnumDefinition> findEnum(String fqn) {
        return Optional.ofNullable(idGet(enums, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link ProfileDefinition} for {@code fqn}, if any. */
    public Optional<ProfileDefinition> findProfile(String fqn) {
        return Optional.ofNullable(idGet(profiles, symbols.resolveId(fqn)));
    }

    /** O(1). Returns the {@code Measure} element at {@code fqn}, if any. */
    public Optional<com.legend.model.MeasureDefinition> findMeasure(String fqn) {
        return Optional.ofNullable(idGet(measures, symbols.resolveId(fqn)));
    }


    /** O(1). Returns {@link DatabaseDefinition} for {@code fqn}, if any. */
    @Override
    public Optional<DatabaseDefinition> findDatabase(@com.legend.base.Nullable String fqn) {
        if (fqn == null) {
            return Optional.empty();
        }
        DatabaseDefinition exact = idGet(databases, symbols.resolveId(fqn));
        if (exact != null) {
            return Optional.of(exact);
        }
        // EXACT-FQN ONLY (NAME_RESOLUTION_BUG.md): the global suffix scan
        // that resolved a bare name against the whole model bound elements
        // the referring file never imported — silent wrong SQL. A bare
        // name reaching this lookup is an upstream qualification failure
        // and stays a MISS; the caller's error names the reference.
        return Optional.empty();
    }

    /**
     * O(1). Returns the canonical {@link MappingDefinition} (binding table) for
     * {@code fqn}, if any. Populated at Phase F (from a {@code NormalizedModel}).
     */
    public Optional<MappingDefinition> findMapping(String fqn) {
        return Optional.ofNullable(idGet(mappings, symbols.resolveId(fqn)));
    }

    /**
     * O(1). Returns the legacy {@link LegacyMappingDefinition} surface tree for
     * {@code fqn}, if any. Populated only at resolution time (from a
     * {@code ParsedModel}); {@code MappingNormalizer} is the sole consumer.
     */
    public Optional<LegacyMappingDefinition> findLegacyMapping(String fqn) {
        return Optional.ofNullable(idGet(legacyMappings, symbols.resolveId(fqn)));
    }

    /** ARCHIVE a pre-Door-1 mapping surface into a builder rebuilt from a
     * NORMALIZED model (whose element list no longer carries legacy
     * records) — analysis consumers only (static lineage #44); the F+
     * compilation pipeline never reads it. */
    public void retainLegacySurface(LegacyMappingDefinition md) {
        int id = intern(md.qualifiedName());
        elementOrder.add(id);
        putAtId(legacyMappings, id, md);
    }

    /** O(1). Returns {@link ServiceDefinition} for {@code fqn}, if any. */
    public Optional<ServiceDefinition> findService(String fqn) {
        return Optional.ofNullable(idGet(services, symbols.resolveId(fqn)));
    }

    /** O(1). Returns the {@code ###Data} element for {@code fqn}, if any. */
    public Optional<com.legend.model.DataDefinition> findData(String fqn) {
        return Optional.ofNullable(idGet(dataElements, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link RuntimeDefinition} for {@code fqn}, if any. */
    public Optional<RuntimeDefinition> findRuntime(@com.legend.base.Nullable String fqn) {
        if (fqn == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(idGet(runtimes, symbols.resolveId(fqn)));
    }

    /** O(1). Returns {@link ConnectionDefinition} for {@code fqn}, if any. */
    public Optional<ConnectionDefinition> findConnection(String fqn) {
        return Optional.ofNullable(idGet(connections, symbols.resolveId(fqn)));
    }

    /** Whether {@code fqn} names a MODEL-store connection (Json/Xml/
     *  ModelChain) — a defined connection that carries no database type. */
    public boolean isModelConnection(String fqn) {
        return modelConnections.containsKey(fqn)
                || modelChainConnections.containsKey(fqn)
                || foreignConnections.contains(fqn);
    }

    /**
     * O(1). Returns all {@link Function} overloads for {@code fqn}.
     * Empty list if none are declared. The list is unmodifiable.
     */
    public List<Function> findFunction(String fqn) {
        List<Function> hit = idGet(functions, symbols.resolveId(fqn));
        return hit == null ? List.of() : Collections.unmodifiableList(hit);
    }

    /** The overloads whose engine signature id is exactly {@code qualifiedId}
     * ({@code meta::pure::functions::boolean::and_Boolean_1__Boolean_1__Boolean_1_}):
     * an exact key, never a name cut apart. Empty if none. */
    public List<Function> findFunctionById(String qualifiedId) {
        List<Function> hit = functionsById.get(qualifiedId);
        return hit == null ? List.of() : Collections.unmodifiableList(hit);
    }

    /** Whether some registered function's engine signature id is exactly {@code qualifiedId}. */
    public boolean hasFunctionId(String qualifiedId) {
        return functionsById.containsKey(qualifiedId);
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
    public Optional<FilterDefinition> findFilter(@com.legend.base.Nullable String dbFqn,
            String filterName) {
        return findFilter(dbFqn, filterName, new java.util.HashSet<>());
    }

    /** Include-closure aware, mirroring {@link #findJoin}: an including
     * database resolves the included database's filters. Own wins. */
    private Optional<FilterDefinition> findFilter(@com.legend.base.Nullable String dbFqn,
            String filterName, java.util.Set<String> seen) {
        if (dbFqn == null) {
            return Optional.empty();
        }
        if (!seen.add(dbFqn)) {
            return Optional.empty();
        }
        int id = symbols.resolveId(dbFqn);
        if (id == SymbolTable.UNRESOLVED) return Optional.empty();
        Map<String, FilterDefinition> byName = filtersByDb.get(id);
        FilterDefinition own = byName == null ? null : byName.get(filterName);
        if (own != null) {
            return Optional.of(own);
        }
        DatabaseDefinition db = findDatabase(dbFqn).orElse(null);
        if (db != null) {
            for (String inc : db.includes()) {
                Optional<FilterDefinition> hit = findFilter(inc, filterName, seen);
                if (hit.isPresent()) {
                    return hit;
                }
            }
        }
        return Optional.empty();
    }

    /**
     * O(1). Returns the {@link JoinDefinition} named {@code joinName}
     * inside database {@code dbFqn}, if any.
     */
    public Optional<JoinDefinition> findJoin(@com.legend.base.Nullable String dbFqn,
            String joinName) {
        return findJoin(dbFqn, joinName, new java.util.HashSet<>());
    }

    /** Include-closure aware (real Legend: Database MyDb ( include db )
     * resolves db's joins — the store-substitution corpus family,
     * testSubtypeMapping.pure:170-172). Own definitions win. */
    private Optional<JoinDefinition> findJoin(@com.legend.base.Nullable String dbFqn,
            String joinName,
            java.util.Set<String> seen) {
        if (dbFqn == null) {
            return Optional.empty();
        }
        if (!seen.add(dbFqn)) {
            return Optional.empty();
        }
        int id = symbols.resolveId(dbFqn);
        if (id != SymbolTable.UNRESOLVED) {
            Map<String, JoinDefinition> byName = joinsByDb.get(id);
            JoinDefinition own = byName == null ? null : byName.get(joinName);
            if (own != null) {
                return Optional.of(own);
            }
            DatabaseDefinition db = findDatabase(dbFqn).orElse(null);
            if (db != null) {
                for (String inc : db.includes()) {
                    Optional<JoinDefinition> hit =
                            findJoin(inc, joinName, seen);
                    if (hit.isPresent()) {
                        return hit;
                    }
                }
            }
        }
        // EXACT-FQN ONLY (NAME_RESOLUTION_BUG.md) — see findDatabase.
        return Optional.empty();
    }

    /**
     * O(1). Returns the {@link ViewDefinition} named {@code viewName}
     * inside database {@code dbFqn}, if any. Matches both bare names
     * (top-level views) and {@code SCHEMA.NAME} dotted forms
     * (per-schema views). Used by the normalizer to detect when a
     * {@code ~mainTable [DB] X} reference resolves to a view rather
     * than a table.
     */
    @Override
    public Optional<ViewDefinition> findView(String dbFqn, String viewName) {
        return indexedView(dbFqn, viewName, new java.util.HashSet<>()).map(IndexedView::definition);
    }

    /** The TABLE {@code name} reached from {@code dbFqn} through the include
     *  closure (own first), with its declared columns — the one lookup behind
     *  every store fact read off a column (a view's signature, a column's
     *  relational type). */
    @Override
    public Optional<DatabaseDefinition.TableDefinition> findTableDefinition(String dbFqn,
            String name) {
        return findTableDefinition(dbFqn, name, new java.util.HashSet<>());
    }

    private Optional<DatabaseDefinition.TableDefinition> findTableDefinition(String dbFqn,
            String name, java.util.Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return Optional.empty();
        }
        DatabaseDefinition db = findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return Optional.empty();
        }
        Optional<DatabaseDefinition.TableDefinition> own = findOwnTableDefinition(dbFqn, name);
        if (own.isPresent()) {
            return own;
        }
        for (String inc : db.includes()) {
            Optional<DatabaseDefinition.TableDefinition> hit = findTableDefinition(inc, name, seen);
            if (hit.isPresent()) {
                return hit;
            }
        }
        return Optional.empty();
    }

    /** The TABLE {@code name} declared in database {@code dbFqn} itself (its
     *  includes not consulted), by {@link TableIndex}'s spelling rules. */
    @Override
    public Optional<DatabaseDefinition.TableDefinition> findOwnTableDefinition(String dbFqn, String name) {
        TableIndex tables = tablesByDb.get(symbols.resolveId(dbFqn));
        return tables == null ? Optional.empty() : Optional.ofNullable(tables.find(name));
    }

    /** {@link #findView} by schema and name: the {@code default} schema (or
     *  none) is the bare spelling, any other schema the dotted one. */
    public Optional<ViewDefinition> findView(String dbFqn, @com.legend.base.Nullable String schema,
            String viewName) {
        return findView(dbFqn, schema == null || "default".equals(schema)
                ? viewName : schema + "." + viewName);
    }

    /** A view with the spelling the index gave it. */
    public record IndexedView(ViewDefinition definition, ViewLift lift) {
    }

    /** The views {@code dbFqn} itself declares (no include closure), in
     *  declaration order: top-level views, then each schema's. */
    public List<IndexedView> viewsOf(String dbFqn) {
        int id = symbols.resolveId(dbFqn);
        return id == SymbolTable.UNRESOLVED ? List.of()
                : viewsInOrder.getOrDefault(id, List.of());
    }

    /**
     * THE view's MAIN TABLE — the engine's {@code findMainTableForView}, a
     * store fact read off the view's column mappings and the database's
     * joins (ONE rule: the normalizer's relation body, the lineage's tree
     * seed and the test-data generator's root all read it here):
     * <ul>
     *   <li>the ONE table the view's non-join column expressions read
     *       (a column that navigates a join anywhere reads its terminal,
     *       not the root);</li>
     *   <li>a JOIN-ONLY view (every column {@code @J|T.COL}): the first
     *       chain's first join's condition tables minus the terminals the
     *       columns read — a single remainder is the root (a table or a
     *       view; PersonViewWithDistinct);</li>
     *   <li>no table, or several: loud — a view resolves to one root.</li>
     * </ul>
     * The spelling is the column reference's own ({@code T} or
     * {@code SCHEMA.T}), what the accessor and the lift name it by.
     */
    public String viewMainTable(String dbFqn, ViewDefinition view) {
        Set<String> tables = new LinkedHashSet<>();
        for (ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            if (!vc.expression().navigatesJoin()) {
                tables.addAll(vc.expression().tables());
            }
        }
        if (tables.isEmpty()) {
            String root = joinOnlyViewRoot(dbFqn, view);
            if (root != null) {
                return root;
            }
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    "View '" + view.name() + "' of '" + dbFqn + "': cannot infer its main"
                    + " table — no non-join column references found");
        }
        if (tables.size() > 1) {
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    "View '" + view.name() + "' of '" + dbFqn + "' references multiple root"
                    + " tables " + tables + "; a view must resolve to a single root table");
        }
        return tables.iterator().next();
    }

    private @com.legend.base.Nullable String joinOnlyViewRoot(String dbFqn, ViewDefinition view) {
        Set<String> terminals = new LinkedHashSet<>();
        com.legend.model.JoinChainElement first = null;
        for (ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            if (!(vc.expression() instanceof com.legend.model.RelationalOperation.JoinNavigation jn)
                    || jn.chain().isEmpty()) {
                continue;
            }
            if (first == null) {
                first = jn.chain().get(0);
            }
            if (jn.terminal() != null) {
                terminals.addAll(jn.terminal().tables());
            }
        }
        if (first == null) {
            return null;
        }
        String joinDb = first.databaseName() != null ? first.databaseName() : dbFqn;
        String joinName = first.joinName();
        JoinDefinition jd = findDatabase(joinDb)
                .flatMap(d -> d.joins().stream().filter(j -> j.name().equals(joinName)).findFirst())
                .orElseThrow(() -> new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "View '" + view.name() + "' of '" + dbFqn + "' navigates join '"
                        + joinName + "' which '" + joinDb + "' does not declare"));
        Set<String> condTables = new LinkedHashSet<>(jd.operation().tables());
        condTables.removeAll(terminals);
        return condTables.size() == 1 ? condTables.iterator().next() : null;
    }

    /** A view's lift identity: the OWNING database of the include closure
     *  and the lift's own spelling (a schema view {@code SCHEMA.NAME}, a
     *  top-level view bare) — what {@code #>{db.<spelling>}#} names it by and
     *  what its lifted function is named by (E.5, {@code <owner>$view$<spelling>}). */
    public record ViewLift(String ownerDb, String spelling) {
        public String fqn() {
            return SynthFqn.view(ownerDb, spelling);
        }
    }

    /** The view reached as {@code viewName} from {@code dbFqn} (include
     *  closure, like {@link #findTable}), whatever spelling reached it. */
    public Optional<ViewLift> viewLift(String dbFqn, String viewName) {
        return indexedView(dbFqn, viewName, new java.util.HashSet<>()).map(IndexedView::lift);
    }

    /** Include-closure aware, mirroring {@link #findJoin}: an including
     * database resolves the included database's views. Own wins. */
    private Optional<IndexedView> indexedView(String dbFqn, String viewName,
            java.util.Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return Optional.empty();
        }
        int id = symbols.resolveId(dbFqn);
        if (id == SymbolTable.UNRESOLVED) return Optional.empty();
        Map<String, IndexedView> byName = viewsByDb.get(id);
        IndexedView own = byName == null ? null : byName.get(viewName);
        if (own != null) {
            return Optional.of(own);
        }
        DatabaseDefinition db = findDatabase(dbFqn).orElse(null);
        if (db != null) {
            for (String inc : db.includes()) {
                Optional<IndexedView> hit = indexedView(inc, viewName, seen);
                if (hit.isPresent()) {
                    return hit;
                }
            }
        }
        return Optional.empty();
    }

    // ====================================================================
    // Iteration accessors — REGISTRATION order (element-list order per
    // batch, batches in add order), never the symbol table's id order
    // ====================================================================

    private <T> Stream<T> inOrder(ArrayList<T> slots) {
        return elementOrder.stream().map(id -> idGet(slots, id)).filter(Objects::nonNull);
    }

    /** Whether {@code fqn} names a REGISTERED element of this model — the
     * symbol table's id and the registration order, no scan (the name
     * resolver's existence question, asked per candidate name). */
    public boolean hasElement(String fqn) {
        int id = symbols.resolveId(fqn);
        return id >= 0 && elementOrder.contains(id);
    }

    /** All {@link ClassDefinition}s in ingest order. Sparse slots filtered out. */
    public Stream<ClassDefinition> classes() {
        return inOrder(classes);
    }

    /** All {@link AssociationDefinition}s in ingest order. */
    public Stream<AssociationDefinition> associations() {
        return inOrder(associations);
    }

    /** All function overloads (user + Phase-E lifted) in ingest order. */
    public Stream<Function> functions() {
        return inOrder(functions).flatMap(List::stream);
    }

    /** All {@link DatabaseDefinition}s in ingest order. */
    public Stream<DatabaseDefinition> databases() {
        return inOrder(databases);
    }

    /** All canonical {@link MappingDefinition}s in ingest order (Phase F). */
    public Stream<MappingDefinition> mappings() {
        return inOrder(mappings);
    }

    /** All legacy {@link LegacyMappingDefinition}s in ingest order (resolution time). */
    public Stream<LegacyMappingDefinition> legacyMappings() {
        return inOrder(legacyMappings);
    }

    /** All {@link EnumDefinition}s in ingest order. */
    public Stream<EnumDefinition> enums() {
        return inOrder(enums);
    }

    /** All Measure elements in ingest order. */
    public Stream<com.legend.model.MeasureDefinition> measures() {
        return inOrder(measures);
    }

    /** All {@link RuntimeDefinition}s in ingest order. */
    public Stream<RuntimeDefinition> runtimes() {
        return inOrder(runtimes);
    }

    /** The ELEMENT's own import scope, else the model-wide one — store
     * refs inside a mapping resolve through the section that declared it. */
    public ImportScope importsOf(String elementFqn) {
        return elementImports.getOrDefault(elementFqn, imports);
    }

    /** Registered database by EXACT FQN only (no simple-name leniency) —
     * the import-scope qualification's membership probe. */
    public boolean hasDatabaseExact(String fqn) {
        return idGet(databases, symbols.resolveId(fqn)) != null;
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
