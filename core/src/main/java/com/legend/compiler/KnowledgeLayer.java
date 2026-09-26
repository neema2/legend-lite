// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.TypeExpression;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * F1 &mdash; the KNOWLEDGE layer (docs/T4_1_KNOWLEDGE_BEFORE_NORMALIZATION_2026_09_13.md
 * &sect;7): what the compiled model knows about classes, their hierarchy,
 * their properties and associations, settled BEFORE Phase E normalizes
 * mappings against it and asked by E and F alike through ONE
 * implementation. Two faces:
 * <ul>
 *   <li>the static PASSES over a name-resolved model (step 1: association
 *       qualified-property adoption);</li>
 *   <li>the per-graph KERNEL over the one model index
 *       ({@link ModelBuilder#knowledge()}): class lookup native-first
 *       (the platform catalog, then the graph &mdash; the rule
 *       {@code TypeClassifier.classDef} and the normalizer's shadow
 *       walkers each carried), the subtype relation (memoized: asked
 *       millions of times per corpus run), the ancestor and subtree walks
 *       over the two direct-subclass indexes. Step 3 retires the
 *       normalizer's shadow copies family by family onto this kernel.</li>
 * </ul>
 * Read-only over the index; rebuilt when the index gains a batch.
 */
public final class KnowledgeLayer {

    private final ModelBuilder model;
    /** Each asked class's REACH: itself and every class its declared supers
     * lead to (an unknown class reaches only itself) — the hierarchy's
     * closure, walked once per class, so a subtype test is a membership
     * test. Was: a memo of (child, parent) answers, one string key per
     * pair asked, growing with the square of the classes. */
    private final Map<String, java.util.Set<String>> reach =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** Facts a phase DERIVES from the index and memoizes here for the
     * graph's lifetime (rebuilt with the kernel when a batch is added) —
     * the {@code ModelContext.derived} idiom, one level down. */
    private final Map<Class<?>, Object> derived = new java.util.HashMap<>();

    KnowledgeLayer(ModelBuilder model) {
        this.model = Objects.requireNonNull(model, "model");
    }

    /** The memoized derivation {@code key} over this index: computed on
     * first ask, the same object after. A pure function of the index. */
    public <T> T derived(Class<T> key, java.util.function.Function<ModelBuilder, T> derive) {
        Object v = derived.get(key);
        if (v == null) {
            v = derive.apply(model);
            derived.put(key, v);
        }
        return key.cast(v);
    }

    // ====================================================================
    // Classes and their hierarchy
    // ====================================================================

    /** The class behind an FQN, NATIVE-FIRST: a platform class the
     * catalog declares is read from the catalog even when the graph
     * carries a copy (a corpus re-declaration of a platform class is
     * parsed against parents the corpus doesn't carry, so its chain
     * dead-ends). User classes live outside the native FQN set. */
    public Optional<ClassDefinition> classDef(String fqn) {
        Optional<ClassDefinition> nat = com.legend.builtin.Pure.findNativeClass(fqn);
        return nat.isPresent() ? nat : model.findClass(fqn);
    }

    /** {@link #classDef} for the MAPPING CALCULUS and the hierarchy
     * walks: a PRIMITIVE is not a class there (scalar detection reads
     * "no class at this name") even though the catalog declares its
     * lattice node as a native Class; a null name is no class. */
    public Optional<ClassDefinition> hierarchyClass(@com.legend.base.Nullable String fqn) {
        if (fqn == null
                || com.legend.compiler.element.type.Type.Primitive.findByFqn(fqn).isPresent()) {
            return Optional.empty();
        }
        return classDef(fqn);
    }

    /** The declared superclass FQNs of {@code cd}: bare and generic
     * heads alike ({@code extends Foo<T>} IS a superclass). */
    public List<String> superClassFqns(ClassDefinition cd) {
        List<String> out = new ArrayList<>(cd.superClasses().size());
        for (TypeExpression sup : cd.superClasses()) {
            String fqn = TypeExpression.rawClassName(sup);
            if (fqn != null) {
                out.add(fqn);
            }
        }
        return out;
    }

    /** Whether {@code child} is {@code parent} or a (transitive) declared
     * subclass of it. An unknown class is nobody's subtype (the miss is
     * the answer: metamodel and protocol class names are not user
     * classes); a cycle contributes nothing new. */
    public boolean isSubtype(String child, String parent) {
        return child.equals(parent) || reach(child).contains(parent);
    }

    private java.util.Set<String> reach(String cls) {
        java.util.Set<String> known = reach.get(cls);
        if (known != null) {
            return known;
        }
        java.util.Set<String> out = new java.util.HashSet<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        work.add(cls);
        while (!work.isEmpty()) {
            String cur = work.poll();
            if (out.add(cur)) {
                hierarchyClass(cur).ifPresent(cd -> work.addAll(superClassFqns(cd)));
            }
        }
        java.util.Set<String> frozen = java.util.Set.copyOf(out);
        reach.putIfAbsent(cls, frozen);
        return frozen;
    }

    /** {@code cls} and every ancestor, breadth-first from {@code cls}
     * (nearest first), cycle-guarded; a class the index does not know
     * contributes itself and nothing above. */
    public java.util.LinkedHashSet<String> ancestorsAndSelf(String cls) {
        return ancestorsBelow(cls, null);
    }

    /** {@code cls} and its ancestors breadth-first, STOPPING at
     * {@code root}: {@code root} itself is not included and nothing is
     * climbed through it (an ancestor reached only through {@code root}
     * stays out; one reached along another parent chain stays in). */
    public java.util.LinkedHashSet<String> ancestorsBelow(String cls,
            @com.legend.base.Nullable String root) {
        java.util.LinkedHashSet<String> out = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        work.add(cls);
        while (!work.isEmpty()) {
            String cur = work.poll();
            if (cur.equals(root) || !out.add(cur)) {
                continue;
            }
            if (cur.equals(cls)) {
                // the starting class may be a name the index does not know
                // (a metamodel probe): it contributes itself and nothing above
                hierarchyClass(cur).ifPresent(cd -> work.addAll(superClassFqns(cd)));
            } else {
                work.addAll(superClassFqns(superClass(cur)));
            }
        }
        return out;
    }

    /** The classes that DIRECTLY extend {@code cls}: the graph's, off the
     * index's direct-subclass index, then the platform catalog's. A
     * natively declared FQN reads the native declaration (its edges come
     * from the native index); a primitive is not a class for the
     * calculus. */
    public List<String> directSubtypes(String cls) {
        List<String> out = new ArrayList<>();
        for (String sub : model.directSubclasses(cls)) {
            if (com.legend.builtin.Pure.findNativeClass(sub).isEmpty()
                    && com.legend.compiler.element.type.Type.Primitive.findByFqn(sub).isEmpty()) {
                out.add(sub);
            }
        }
        for (String sub : com.legend.builtin.Pure.directNativeSubclasses(cls)) {
            if (com.legend.compiler.element.type.Type.Primitive.findByFqn(sub).isEmpty()) {
                out.add(sub);
            }
        }
        return out;
    }

    /** The STRICT subtree of {@code base} (every transitive subclass,
     * graph and catalog), discovery order. Was: a per-call scan of every
     * class of the universe. */
    public java.util.Set<String> subtree(String base) {
        java.util.Set<String> subtree = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<String> frontier = new java.util.ArrayDeque<>();
        frontier.add(base);
        while (!frontier.isEmpty()) {
            String c = frontier.poll();
            for (String sub : directSubtypes(c)) {
                if (subtree.add(sub)) {
                    frontier.add(sub);
                }
            }
        }
        subtree.remove(base);
        return subtree;
    }

    /** {@code cls} and every ancestor as DEFINITIONS, nearest first — the
     * stereotype and strategy questions fold over it. LOUD for every
     * class on the way, the starting one included: a stereotype asked of
     * an unknown class is a real model gap. */
    public List<ClassDefinition> lineage(String cls) {
        List<ClassDefinition> out = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        work.add(cls);
        while (!work.isEmpty()) {
            String cur = work.poll();
            if (!seen.add(cur)) {
                continue;
            }
            ClassDefinition cd = superClass(cur);
            out.add(cd);
            work.addAll(superClassFqns(cd));
        }
        return out;
    }

    // ====================================================================
    // Properties over the hierarchy
    // ====================================================================

    /** The class behind a superclass FQN met on a hierarchy walk — LOUD
     * when unresolved: a superclass the index cannot answer is a real
     * model gap, never a silently empty answer (F7.8). */
    private ClassDefinition superClass(String fqn) {
        return hierarchyClass(fqn).orElseThrow(() -> new IllegalStateException(
                "class unresolved on a hierarchy walk (a real model gap): " + fqn));
    }

    /** The declared type of property {@code name} as seen from
     * {@code cd}: its own stored property, else an association end
     * injected onto it, else the same on each superclass in turn
     * (association ends are inherited too). Null when nothing declares
     * it; null class, null answer. */
    public @com.legend.base.Nullable TypeExpression propertyType(
            @com.legend.base.Nullable ClassDefinition cd, String name) {
        return propertyType(cd, name, new java.util.HashSet<>());
    }

    private @com.legend.base.Nullable TypeExpression propertyType(
            @com.legend.base.Nullable ClassDefinition cd, String name, java.util.Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) {
            return null;
        }
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            if (p.name().equals(name)) {
                return p.type();
            }
        }
        TypeExpression assoc = model.findAssociationProperty(cd.qualifiedName(), name).orElse(null);
        if (assoc != null) {
            return assoc;
        }
        for (String sup : superClassFqns(cd)) {
            TypeExpression inherited = propertyType(superClass(sup), name, visited);
            if (inherited != null) {
                return inherited;
            }
        }
        return null;
    }

    /** The stored-property DEFINITION {@code name} on {@code cd} or the
     * nearest superclass declaring it (no association ends). */
    public ClassDefinition.@com.legend.base.Nullable PropertyDefinition propertyDef(
            @com.legend.base.Nullable ClassDefinition cd, String name) {
        return propertyDef(cd, name, new java.util.HashSet<>());
    }

    private ClassDefinition.@com.legend.base.Nullable PropertyDefinition propertyDef(
            @com.legend.base.Nullable ClassDefinition cd, String name, java.util.Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) {
            return null;
        }
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            if (p.name().equals(name)) {
                return p;
            }
        }
        for (String sup : superClassFqns(cd)) {
            ClassDefinition.PropertyDefinition inherited = propertyDef(superClass(sup), name, visited);
            if (inherited != null) {
                return inherited;
            }
        }
        return null;
    }

    /** The declared multiplicity of stored property {@code name} on
     * {@code cd} or the nearest superclass declaring it. */
    public com.legend.protocol.@com.legend.base.Nullable Multiplicity propertyMultiplicity(
            ClassDefinition cd, String name) {
        ClassDefinition.PropertyDefinition pd = propertyDef(cd, name);
        return pd == null ? null : pd.multiplicity();
    }

    /** The zero-argument, single-expression INLINE derived property
     * {@code name} on {@code cd} or the nearest superclass declaring one
     * &mdash; the only shape a join-condition inliner serves. */
    public com.legend.protocol.@com.legend.base.Nullable DerivedPropertyDefinition derivedInline(
            @com.legend.base.Nullable ClassDefinition cd, String name) {
        return derivedInline(cd, name, new java.util.HashSet<>());
    }

    private com.legend.protocol.@com.legend.base.Nullable DerivedPropertyDefinition derivedInline(
            @com.legend.base.Nullable ClassDefinition cd, String name, java.util.Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) {
            return null;
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            if (dp.name().equals(name) && dp.parameters().isEmpty()
                    && dp.realization() instanceof com.legend.protocol.Realization.Inline inl
                    && inl.body().size() == 1) {
                return dp;
            }
        }
        for (String sup : superClassFqns(cd)) {
            DerivedPropertyDefinition r = derivedInline(superClass(sup), name, visited);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    // ====================================================================
    // Stores: tables and columns over the database include closure
    // ====================================================================

    /** A table spelled {@code default.T} is the top-level table {@code T}
     * (a database's top-level tables ARE schema 'default'). */
    public static String canonicalTable(String table) {
        return table.startsWith("default.") ? table.substring("default.".length()) : table;
    }

    /** The physical TABLE behind a mapping's table spelling in database
     * {@code dbFqn}: {@link ModelBuilder#findTableDefinition}, the one table
     * lookup ({@code SCHEMA.T} is the named schema's table only, a bare
     * name the top level's then each schema's; names compare exactly, as
     * the engine's do), the database's own tables first then its includes,
     * transitively. Empty for a null spelling, an unknown database, or a
     * view. */
    public Optional<DatabaseDefinition.TableDefinition> table(
            @com.legend.base.Nullable String dbFqn, @com.legend.base.Nullable String table) {
        if (dbFqn == null || table == null) {
            return Optional.empty();
        }
        return model.findTableDefinition(dbFqn, table);
    }

    /** The physical COLUMN {@code column} of {@link #table}, by exact name. */
    public Optional<DatabaseDefinition.ColumnDefinition> column(
            @com.legend.base.Nullable String dbFqn, @com.legend.base.Nullable String table, String column) {
        return table(dbFqn, table).flatMap(td -> td.columns().stream()
                .filter(cd -> cd.name().equals(column)).findFirst());
    }

    /** The pure KIND ("String", "Integer", …; {@link RelationalKinds}) of a
     * column of a table OR of a VIEW — a view column that reads one
     * physical column (a ColumnRef) has that column's kind, through views
     * of views; null when nothing physical is behind the name. */
    public @com.legend.base.Nullable String columnKind(String db, String table, String col) {
        return columnKind(db, table, col, new java.util.HashSet<>());
    }

    private @com.legend.base.Nullable String columnKind(String db, String table, String col,
            java.util.Set<String> seen) {
        if (!seen.add(db + "@" + table + "." + col)) {
            return null;
        }
        DatabaseDefinition.ColumnDefinition cd = column(db, table, col).orElse(null);
        if (cd != null) {
            return RelationalKinds.pureKindOf(cd.dataType());
        }
        DatabaseDefinition.ViewDefinition view = model.findView(db, table).orElse(null);
        if (view == null) {
            return null;
        }
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            if (!vc.name().equals(col)) {
                continue;
            }
            if (vc.expression() instanceof com.legend.model.RelationalOperation.ColumnRef cr) {
                String cdb = cr.databaseName() != null ? cr.databaseName() : db;
                return columnKind(cdb, cr.table(), cr.column(), seen);
            }
            // a COMPUTED view column (`HIGHEST_RATE: max(RATE.ZERO_RATE)`):
            // its kind is the expression's inferred SQL type — the
            // engine's inferRelationalType, the same rule the metamodel
            // store stamps on every relational-operation row
            com.legend.model.RelationalDataType t =
                    com.legend.compiler.element.RelationalTypeInference.infer(
                            vc.expression(), model.findDatabase(db).orElse(null), model);
            return t == null ? null : RelationalKinds.pureKindOf(t);
        }
        return null;
    }

    // ====================================================================
    // Passes over a name-resolved model
    // ====================================================================

    /**
     * Association QUALIFIED properties adopt into the class that owns
     * them (the end OPPOSITE the one the property returns &mdash; real
     * pure: an association qualified property is an alternate accessor
     * of one end, callable on the other end's class). After adoption the
     * single class-derived funnel (E.2, findProperty, $prop$ lifting)
     * covers them with no second path. The association keeps its
     * declaration (the faithful source image, like a class keeps its
     * derived bodies); the owner class gains the property.
     *
     * <p>Runs post-NameResolver, so end targets and return types are FQNs.
     * A property naming no unique owning end is a MODEL error: strict
     * builds throw; a tolerant (module) build records the association in
     * {@code wallSink} and adopts nothing from it.
     */
    public static ParsedModel adoptAssociationQualifiedProperties(ParsedModel parsed,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        Objects.requireNonNull(parsed, "parsed");
        Map<String, List<DerivedPropertyDefinition>> adoptions =
                new LinkedHashMap<>();
        for (PackageableElement el : parsed.elements()) {
            if (!(el instanceof AssociationDefinition ad)
                    || ad.derivedProperties().isEmpty()) {
                continue;
            }
            List<Map.Entry<String, DerivedPropertyDefinition>> own = new ArrayList<>();
            try {
                for (DerivedPropertyDefinition dp : ad.derivedProperties()) {
                    own.add(Map.entry(ownerOrThrow(ad, dp), dp));
                }
            } catch (ModelException e) {
                if (wallSink == null) {
                    throw e;
                }
                wallSink.putIfAbsent(ad.qualifiedName(),
                        String.valueOf(e.getMessage()).split("\n")[0]);
                continue;
            }
            for (Map.Entry<String, DerivedPropertyDefinition> e : own) {
                adoptions.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(e.getValue());
            }
        }
        if (adoptions.isEmpty()) {
            return parsed;
        }
        List<PackageableElement> out = new ArrayList<>(parsed.elements().size());
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ClassDefinition cd
                    && adoptions.containsKey(cd.qualifiedName())) {
                List<DerivedPropertyDefinition> merged =
                        new ArrayList<>(cd.derivedProperties());
                merged.addAll(adoptions.get(cd.qualifiedName()));
                out.add(new ClassDefinition(cd.qualifiedName(), cd.typeParams(), cd.typeVariables(),
                        cd.superClasses(), cd.properties(), merged, cd.constraints(),
                        cd.stereotypes(), cd.taggedValues(), cd.isNative()));
            } else {
                out.add(el);
            }
        }
        // full-arg pass-through: source/offsets/per-element imports,
        // per-element sources and unclaimed sections all survive
        return new ParsedModel(out, parsed.imports(), parsed.source(),
                parsed.elementOffsets(), parsed.elementImports(),
                parsed.elementSources(), parsed.unclaimedSections());
    }

    /**
     * The class that OWNS an association's qualified property: the end
     * opposite the one the property returns; a self-association (both
     * ends the same class) owns its qualified properties itself
     * (AssociationProcessor: leftRawType == returnType ? right : left).
     * Empty when the return type identifies no unique owning end. EXACT
     * FQN comparison &mdash; the model is name-resolved.
     */
    public static Optional<String> qualifiedPropertyOwner(AssociationDefinition ad,
            DerivedPropertyDefinition dp) {
        String t1 = rawName(ad.property1().targetClass());
        String t2 = rawName(ad.property2().targetClass());
        String ret = rawName(dp.type());
        if (t1.equals(t2)) {
            return ret.equals(t1) ? Optional.of(t1) : Optional.empty();
        }
        if (ret.equals(t1)) {
            return Optional.of(t2);
        }
        return ret.equals(t2) ? Optional.of(t1) : Optional.empty();
    }

    private static String ownerOrThrow(AssociationDefinition ad, DerivedPropertyDefinition dp) {
        Optional<String> owner = qualifiedPropertyOwner(ad, dp);
        if (owner.isPresent()) {
            return owner.get();
        }
        String t1 = rawName(ad.property1().targetClass());
        String t2 = rawName(ad.property2().targetClass());
        String ret = rawName(dp.type());
        String why = t1.equals(t2)
                ? "', which is neither end of the self-association"
                : "', which does not identify a unique owning end";
        throw new ModelException(LegendCompileException.Phase.MODEL,
                "association '" + ad.qualifiedName() + "' qualified property '"
                        + dp.name() + "' returns '" + ret + why,
                ad.qualifiedName());
    }

    private static String rawName(TypeExpression t) {
        return switch (t) {
            case TypeExpression.NameRef n -> n.name();
            case TypeExpression.Generic g -> g.name();
            default -> t.toString();
        };
    }
}
