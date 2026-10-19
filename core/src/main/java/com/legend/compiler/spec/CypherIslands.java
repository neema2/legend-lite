// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.GraphEdges;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.model.AssociationDefinition;
import com.legend.protocol.cypher.CypherToPure;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE ONE PLACE a Cypher island becomes a Relation expression (docs/CYPHER_STUDY_2026_10_15.md §5.3,
 * docs/CYPHER_PLAN_2026_10_15.md): {@code #Cypher{...}#} read by the protocol layer's translator
 * ({@link CypherToPure}) over the class model, which this seam presents as the graph the query sees (C2): a label is
 * a class, named by its simple name or its full path; a relationship type is the name of a property or association
 * end holding a class; a node's properties are its class's, own, inherited or association-injected. The result's
 * names are resolved as the resolver resolves any query, and the Typer then types it once, like any query. A
 * refusal becomes the compile error its kind says, placed in the enclosing source. Public for one reader outside the
 * typer: the Cypher parity harness's showcase dump ({@code CypherParityTest}), which pins the Pure an island
 * desugars to beside its SQL, so that a person reads it (docs/CYPHER_SHOWCASE.md).
 */
public final class CypherIslands {

    private CypherIslands() {
    }

    /** The relation expression {@code island} means over {@code model}'s classes, names resolved. */
    public static ValueSpecification desugar(SqlIsland island, ModelContext model) {
        return NameResolver.resolveQuery(translate(island, model));
    }

    /** The relation expression {@code island} means, as the translator spells it (names as a person writes them,
     *  before resolution): what the showcase dump prints. */
    public static ValueSpecification translate(SqlIsland island, ModelContext model) {
        try {
            return CypherToPure.translate(island.sql(), new Graph(model));
        } catch (SqlTranslationException e) {
            String message = island.opening() + "...}# at " + SqlIslands.position(island, e.line(), e.column()) + ": "
                    + e.getMessage();
            throw switch (e.kind()) {
                case SYNTAX -> new com.legend.error.IslandSyntaxException(message);
                case UNSUPPORTED -> new com.legend.error.NotImplementedException(message);
                case INVALID -> new TypeInferenceException(message);
            };
        }
    }

    /** The classes of the model by their simple names, computed once per model: what a label names. */
    record Labels(Map<String, List<String>> bySimpleName) {

        static Labels of(ModelContext model) {
            Map<String, List<String>> out = new TreeMap<>();
            for (String fqn : new java.util.TreeSet<>(model.elementFqns())) {
                // a synthetic class (the GraphQL companion's) is no label: unmapped, and not the user's
                if (model.findClass(fqn).isEmpty() || model.isSynthetic(fqn)) {
                    continue;
                }
                int i = fqn.lastIndexOf("::");
                String simple = i < 0 ? fqn : fqn.substring(i + 2);
                out.computeIfAbsent(simple, k -> new ArrayList<>()).add(fqn);
            }
            Map<String, List<String>> frozen = new TreeMap<>();
            out.forEach((k, v) -> frozen.put(k, List.copyOf(v)));
            return new Labels(java.util.Collections.unmodifiableMap(frozen));
        }
    }

    /** The model's associations read forward, indexed by the class their first end is injected onto (the end a
     *  relationship type names; the harness declares it first): what an untyped relationship pattern enumerates
     *  (the study's §12.17). */
    record Ends(Map<String, List<AssociationDefinition.AssociationEndDefinition>> byOwner) {

        static Ends of(ModelContext model) {
            Map<String, List<AssociationDefinition.AssociationEndDefinition>> out = new TreeMap<>();
            for (String fqn : new java.util.TreeSet<>(model.elementFqns())) {
                AssociationDefinition ad = model.findAssociation(fqn).orElse(null);
                if (ad == null) {
                    continue;
                }
                // the first end is a property of the class the second end names
                String owner = className(ad.property2().targetClass());
                if (owner != null) {
                    out.computeIfAbsent(owner, k -> new ArrayList<>()).add(ad.property1());
                }
            }
            Map<String, List<AssociationDefinition.AssociationEndDefinition>> frozen = new TreeMap<>();
            out.forEach((k, v) -> frozen.put(k, List.copyOf(v)));
            return new Ends(java.util.Collections.unmodifiableMap(frozen));
        }

        private static @com.legend.base.Nullable String className(com.legend.protocol.TypeExpression type) {
            return type instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name() : null;
        }
    }

    /** The model as the translator reads it. */
    private static final class Graph implements CypherToPure.Model {
        private final ModelContext model;

        Graph(ModelContext model) {
            this.model = model;
        }

        @Override
        public List<String> classesOf(String label) {
            // the label as a class's full path first (a backticked `demo::Person`), else as a simple name
            if (model.findClass(label).isPresent()) {
                return List.of(label);
            }
            List<String> classes = model.derived(Labels.class, Labels::of).bySimpleName().get(label);
            return classes == null ? List.of() : classes;
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            Property p = model.findProperty(classPath, name).orElse(null);
            if (p == null) {
                return;
            }
            if (p instanceof Property.Derived d && !d.parameters().isEmpty()) {
                // a qualified property takes arguments: not a node's property
                return;
            }
            report(sink, p.name(), p.type(), p.multiplicity());
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            AssociationDefinition a = model.findAssociationOf(classPath, property).orElse(null);
            if (a == null) {
                return;
            }
            // the end that injects onto classPath is the one of this property's name; the inverse is the other
            AssociationDefinition.AssociationEndDefinition other = a.property1().propertyName().equals(property)
                    ? a.property2() : a.property1();
            if (!(other.multiplicity() instanceof com.legend.protocol.Multiplicity.Concrete m)) {
                return;
            }
            Integer upper = m.upperBound();
            sink.property(other.propertyName(), other.targetClassFqn(), true, false, m.lowerBound(),
                    upper == null ? -1 : upper);
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            return classPath.equals(superPath) || model.isSubtype(classPath, superPath);
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            // the class's own stored properties, then its superclasses' (an association end is injected, not
            // declared on the class)
            java.util.Set<String> seen = new java.util.HashSet<>();
            java.util.Deque<String> todo = new java.util.ArrayDeque<>(List.of(classPath));
            while (!todo.isEmpty()) {
                com.legend.compiler.element.TypedClass tc = model.findClass(todo.poll()).orElse(null);
                if (tc == null) {
                    continue;
                }
                for (Property p : tc.properties()) {
                    if (p instanceof Property.Stored s && seen.add(s.name())) {
                        report(sink, s.name(), s.type(), s.multiplicity());
                    }
                }
                todo.addAll(tc.superClassFqns());
            }
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            // the class's own stored properties holding a class, then the associations whose first end is its (and
            // its superclasses'), in declaration order
            java.util.Set<String> seen = new java.util.HashSet<>();
            java.util.Deque<String> todo = new java.util.ArrayDeque<>(List.of(classPath));
            Ends ends = model.derived(Ends.class, Ends::of);
            // (an end holding an edge class is one of the edge's: the edge is reached by its type, the study's §5.6)
            GraphEdges edges = model.derived(GraphEdges.class, GraphEdges::of);
            while (!todo.isEmpty()) {
                String path = todo.poll();
                com.legend.compiler.element.TypedClass tc = model.findClass(path).orElse(null);
                if (tc == null) {
                    continue;
                }
                for (Property p : tc.properties()) {
                    if (p instanceof Property.Stored s && s.type() instanceof Type.ClassType c
                            && edges.edgeAt(c.fqn()) == null && seen.add(s.name())) {
                        report(sink, s.name(), s.type(), s.multiplicity());
                    }
                }
                for (AssociationDefinition.AssociationEndDefinition end : ends.byOwner().getOrDefault(path,
                        List.of())) {
                    String target = end.targetClass() instanceof com.legend.protocol.TypeExpression.NameRef nr
                            ? nr.name() : "";
                    if (!(end.multiplicity() instanceof com.legend.protocol.Multiplicity.Concrete m)
                            || edges.edgeAt(target) != null || !seen.add(end.propertyName())) {
                        continue;
                    }
                    Integer upper = m.upperBound();
                    sink.property(end.propertyName(), target, true, false, m.lowerBound(), upper == null ? -1 : upper);
                }
                todo.addAll(tc.superClassFqns());
            }
        }

        @Override
        public String labelOf(String classPath) {
            for (Map.Entry<String, List<String>> e : model.derived(Labels.class, Labels::of).bySimpleName()
                    .entrySet()) {
                if (e.getValue().contains(classPath)) {
                    return e.getKey();
                }
            }
            throw new IllegalArgumentException("no class of the model at " + classPath);
        }

        @Override
        public List<String> classes() {
            // the model's own classes: the platform's declarations (the resolver's type universe and the system
            // metamodel's elements, by their declared names) are never nodes, nor is an edge class (a relationship
            // type, the study's §5.6)
            List<String> out = new ArrayList<>();
            model.derived(Labels.class, Labels::of).bySimpleName().values().forEach(out::addAll);
            GraphEdges edges = model.derived(GraphEdges.class, GraphEdges::of);
            out.removeIf(c -> com.legend.compiler.NameResolver.platformFqns().contains(c)
                    || com.legend.builtin.SystemMetamodel.elementFqns().contains(c) || edges.edgeAt(c) != null);
            java.util.Collections.sort(out);
            return List.copyOf(out);
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
            for (GraphEdges.Edge e : model.derived(GraphEdges.class, GraphEdges::of).edges()) {
                sink.edge(e.classFqn(), e.type(), e.from().property(), e.from().classFqn(), e.from().inverse(),
                        e.from().inverseMany(), e.to().property(), e.to().classFqn(), e.to().inverse(),
                        e.to().inverseMany());
            }
        }

        private static void report(CypherToPure.PropertySink sink, String name, Type type,
                Multiplicity multiplicity) {
            int lower = 0;
            int upper = -1;
            if (multiplicity instanceof Multiplicity.Bounded b) {
                lower = b.lower();
                Integer u = b.upper();
                upper = u == null ? -1 : u;
            }
            switch (type) {
                // a Variant (a stored list: a SEMISTRUCTURED column, the study's §12.37) is a value, not a class
                case Type.ClassType c when com.legend.compiler.element.type.PlatformTypes.isVariant(c) ->
                        sink.property(name, "Variant", false, false, lower, upper);
                case Type.ClassType c -> sink.property(name, c.fqn(), true, false, lower, upper);
                case Type.EnumType e -> sink.property(name, e.fqn(), false, true, lower, upper);
                default -> sink.property(name, type.typeName(), false, false, lower, upper);
            }
        }
    }
}
