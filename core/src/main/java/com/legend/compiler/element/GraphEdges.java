// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.ProfileDefinition;
import com.legend.model.StereotypeApplication;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.Protocol;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * The model's edge classes (docs/CYPHER_STUDY_2026_10_15.md §5.6; the profile is {@code projects/graph}), read
 * from the declarations in phase F the way {@link EqualityKeys} reads {@code <<equality.Key>>}: a class carrying
 * {@code <<graph.Edge>>} (itself or a superclass) is a relationship type with properties; its two ends are the
 * properties carrying {@code <<graph.from>>} and {@code <<graph.to>>}, each to-one and typed by a class of the
 * model that is itself no edge, declared on the class, inherited, or injected by an association (whose other end
 * is then the node class's navigation to the edge, the inverse); its type is the enumeration value the derived
 * property carrying {@code <<graph.type>>} spells, else the class's simple name; every other stored property is
 * the relationship's.
 *
 * <p>The rule is checked class by class in phase F ({@link ModelIntegrity}, one wall per class in a tolerant
 * build), with an error naming the class that declares the fault: exactly one end of each kind, each {@code [1]}
 * and typed by a node class; an end or type stereotype without the class marker, or the marker without both
 * ends, is an error; both end markers on one property, two type markers on one class, or two inherited types that
 * disagree are errors; an association linking an edge class by an end carrying neither marker is a third link,
 * refused (a relationship has two ends); a to-many property holding a class is refused on an edge; a derived
 * end ({@code <<graph.from>> from() {...}}) is not read yet, refused by name; the profile at
 * {@link PlatformTypes#GRAPH_PROFILE} must be declared with exactly the eight stereotypes ({@link #STEREOTYPES}). The engine checks every
 * reference in the declaration; only lite checks the rule, since only lite runs the island. The Cypher seam reads
 * the result as a derived view of the model ({@code ModelContext.derived}).
 */
public record GraphEdges(List<Edge> edges) {

    /** An edge class: its path, its relationship type, its two ends. */
    public record Edge(String classFqn, String type, End from, End to) {
    }

    /** One end of an edge: the property on the edge class holding the node, the node's class, and, when the
     *  property is an association end declared for this very class, the inverse end the node class holds (its
     *  name and whether it holds many edges); null for an end that is the edge class's own property, or an end
     *  inherited from a superclass (whose inverse reaches the superclass's instances, not only this class's). */
    public record End(String property, String classFqn, @com.legend.base.Nullable String inverse,
            boolean inverseMany) {
    }

    /** The declarations the reader walks: the compiled model's or, in phase F, the builder's. A class is a class
     *  of the model: a platform primitive or a native is none. */
    public interface Source {
        Stream<ClassDefinition> classes();

        Stream<AssociationDefinition> associations();

        Optional<ClassDefinition> findClass(String fqn);

        Optional<ProfileDefinition> findProfile(String fqn);
    }

    /** The association ends injected onto each class (the end whose other end names the class), indexed once per
     *  read: what a lineage's classes are asked for. */
    public record Index(Map<String, List<Injected>> byOwner) {
        /** An end injected onto a class, with the association's other end (the inverse, on the class the end
         *  names). */
        public record Injected(AssociationDefinition association, AssociationDefinition.AssociationEndDefinition end,
                AssociationDefinition.AssociationEndDefinition other) {
        }

        public static Index of(Source src) {
            Map<String, List<Injected>> out = new HashMap<>();
            src.associations().forEach(ad -> {
                for (int i = 0; i < 2; i++) {
                    AssociationDefinition.AssociationEndDefinition end = i == 0 ? ad.property1() : ad.property2();
                    AssociationDefinition.AssociationEndDefinition other = i == 0 ? ad.property2() : ad.property1();
                    String owner = headName(other.targetClass());
                    if (owner != null) {
                        out.computeIfAbsent(owner, k -> new ArrayList<>()).add(new Injected(ad, end, other));
                    }
                }
            });
            return new Index(out);
        }

        List<Injected> onto(String classFqn) {
            return byOwner.getOrDefault(classFqn, List.of());
        }
    }

    public GraphEdges {
        edges = List.copyOf(edges);
    }

    /** The edge class at {@code classFqn}, or null when the class is none. */
    public @com.legend.base.Nullable Edge edgeAt(String classFqn) {
        for (Edge e : edges) {
            if (e.classFqn().equals(classFqn)) {
                return e;
            }
        }
        return null;
    }

    /** The compiled model's edge classes (the Cypher seam's derived view). */
    public static GraphEdges of(ModelContext ctx) {
        return read(new Source() {
            @Override
            public Stream<ClassDefinition> classes() {
                return new TreeSet<>(ctx.elementFqns()).stream().map(this::findClass).flatMap(Optional::stream);
            }

            @Override
            public Stream<AssociationDefinition> associations() {
                return new TreeSet<>(ctx.elementFqns()).stream().map(ctx::findAssociation).flatMap(Optional::stream);
            }

            @Override
            public Optional<ClassDefinition> findClass(String fqn) {
                // (the context's lookup reaches the native catalog too: a primitive is no class of the model)
                return ctx.findClassDefinition(fqn).filter(cd -> !cd.isNative());
            }

            @Override
            public Optional<ProfileDefinition> findProfile(String fqn) {
                return ctx.findProfile(fqn);
            }
        });
    }

    /** The builder's declarations, as phase F checks them. */
    public static Source sourceOf(com.legend.compiler.ModelBuilder model) {
        return new Source() {
            @Override
            public Stream<ClassDefinition> classes() {
                return model.classes().filter(cd -> !cd.isNative())
                        .sorted(java.util.Comparator.comparing(ClassDefinition::qualifiedName));
            }

            @Override
            public Stream<AssociationDefinition> associations() {
                return model.associations();
            }

            @Override
            public Optional<ClassDefinition> findClass(String fqn) {
                return model.findClass(fqn).filter(cd -> !cd.isNative());
            }

            @Override
            public Optional<ProfileDefinition> findProfile(String fqn) {
                return model.findProfile(fqn);
            }
        };
    }

    /** Reads every edge class of {@code src}, refusing a misdeclared one with an error naming the class at fault. */
    public static GraphEdges read(Source src) {
        Index index = Index.of(src);
        List<Edge> edges = new ArrayList<>();
        for (ClassDefinition cd : src.classes().toList()) {
            Edge e = readOne(src, index, cd);
            if (e != null) {
                edges.add(e);
            }
        }
        return new GraphEdges(edges);
    }

    /** The edge class {@code cd} is, or null when it is none; a misdeclared one is refused with an error naming
     *  the class that declares the fault (the class itself, or the superclass the faulty declaration is on). */
    public static @com.legend.base.Nullable Edge readOne(Source src, Index index, ClassDefinition cd) {
        List<String> lineage = lineage(src, cd);
        boolean marked = isMarked(src, lineage);
        // the ends: own and inherited stored properties carrying from/to (a redeclaration shadows by name), then
        // the association ends injected onto the class or a superclass
        List<EndRead> ends = new ArrayList<>();
        List<String> thirdLinks = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < lineage.size(); i++) {
            String fqn = lineage.get(i);
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c == null) {
                continue;
            }
            for (ClassDefinition.PropertyDefinition pd : c.properties()) {
                if (!seen.add(pd.name())) {
                    continue;
                }
                String kind = endKind(pd.stereotypes(), c, pd.name());
                if (kind != null) {
                    ends.add(new EndRead(pd.name(), kind, pd.type(), pd.multiplicity(), null, false, fqn, i == 0));
                }
            }
        }
        for (int i = 0; i < lineage.size(); i++) {
            String fqn = lineage.get(i);
            for (Index.Injected in : index.onto(fqn)) {
                if (!seen.add(in.end().propertyName())) {
                    continue;
                }
                ClassDefinition declaring = src.findClass(fqn).orElse(cd);
                String kind = endKind(in.end().stereotypes(), declaring, in.end().propertyName());
                if (kind == null) {
                    thirdLinks.add(in.association().qualifiedName() + "'s end " + in.end().propertyName());
                    continue;
                }
                // (the lineage's first class is the edge itself: an end declared for it has its inverse)
                ends.add(new EndRead(in.end().propertyName(), kind, in.end().targetClass(), in.end().multiplicity(),
                        in.other().propertyName(), !in.other().isToOne(), fqn, i == 0));
            }
        }
        for (String fqn : lineage) {
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c == null) {
                continue;
            }
            for (DerivedPropertyDefinition dp : c.derivedProperties()) {
                if (endKind(toApplications(dp.stereotypes()), c, dp.name()) != null) {
                    throw refuse(c, "carries an end stereotype on the derived property " + dp.name() + ": a derived"
                            + " end (a navigation of $this to the end) is not supported yet; declare the end as a"
                            + " stored property or an association end");
                }
            }
        }
        if (!marked) {
            if (!ends.isEmpty()) {
                EndRead first = ends.get(0);
                throw refuse(src.findClass(first.declaredOn()).orElse(cd), "carries " + spell(ends) + " but not <<"
                        + PlatformTypes.GRAPH_PROFILE + ".Edge>>: an end belongs to an edge class");
            }
            for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
                if (typeMarked(dp)) {
                    throw refuse(cd, "carries <<" + PlatformTypes.GRAPH_PROFILE + ".type>> on " + dp.name() + " but not"
                            + " <<" + PlatformTypes.GRAPH_PROFILE + ".Edge>>: a relationship type belongs to an edge"
                            + " class");
                }
            }
            return null;
        }
        checkProfile(src, cd);
        EndRead from = one(src, cd, ends, "from");
        EndRead to = one(src, cd, ends, "to");
        if (!thirdLinks.isEmpty()) {
            throw refuse(cd, "is linked by " + String.join(", ", thirdLinks) + ", carrying neither <<"
                    + PlatformTypes.GRAPH_PROFILE + ".from>> nor <<" + PlatformTypes.GRAPH_PROFILE + ".to>>: an edge"
                    + " has two ends and no third link");
        }
        for (String fqn : lineage) {
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c == null) {
                continue;
            }
            for (ClassDefinition.PropertyDefinition pd : c.properties()) {
                String target = headName(pd.type());
                if (target != null && src.findClass(target).isPresent() && !toOne(pd.multiplicity())
                        && endKind(pd.stereotypes(), c, pd.name()) == null) {
                    throw refuse(c, "holds many " + target + " in " + pd.name() + ": a relationship has two"
                            + " ends, each <<" + PlatformTypes.GRAPH_PROFILE + ".from>> or <<"
                            + PlatformTypes.GRAPH_PROFILE + ".to>> and [1]");
                }
            }
        }
        return new Edge(cd.qualifiedName(), typeOf(src, cd, lineage), from.end(), to.end());
    }

    /** An end as read: the property, its kind (from, to), its declared type and multiplicity, its inverse when it
     *  is an association end, the class it is declared on, and whether that class is the edge itself. */
    private record EndRead(String property, String kind, TypeExpression type, Multiplicity multiplicity,
            @com.legend.base.Nullable String inverse, boolean inverseMany, String declaredOn, boolean own) {
        /** The end: an inverse declared for another class (a superclass) reaches that class's instances, so it is
         *  none of this edge's. */
        End end() {
            return new End(property, java.util.Objects.requireNonNull(headName(type)), own ? inverse : null,
                    own && inverseMany);
        }
    }

    /** The one end of {@code kind} the class has: exactly one, to-one, typed by a class of the model that is no
     *  edge; a fault is the declaring class's. */
    private static EndRead one(Source src, ClassDefinition cd, List<EndRead> ends, String kind) {
        List<EndRead> of = ends.stream().filter(e -> e.kind().equals(kind)).toList();
        String marker = "<<" + PlatformTypes.GRAPH_PROFILE + "." + kind + ">>";
        if (of.isEmpty()) {
            throw refuse(cd, "has no property carrying " + marker + ": an edge has a " + kind + " end");
        }
        if (of.size() > 1) {
            throw refuse(cd, "carries " + marker + " on " + of.size() + " properties (" + String.join(", ",
                    of.stream().map(EndRead::property).toList()) + "): an edge has one " + kind + " end");
        }
        EndRead e = of.get(0);
        ClassDefinition at = src.findClass(e.declaredOn()).orElse(cd);
        if (!toOne(e.multiplicity()) || !(e.multiplicity() instanceof Multiplicity.Concrete c) || c.lowerBound() != 1) {
            throw refuse(at, "carries " + marker + " on " + e.property() + ", which is not [1]: a relationship always"
                    + " has its " + kind + " node");
        }
        String target = headName(e.type());
        if (target == null || src.findClass(target).isEmpty()) {
            throw refuse(at, "carries " + marker + " on " + e.property() + ", which is not typed by a class of the"
                    + " model: the " + kind + " end is a node");
        }
        if (isMarked(src, lineage(src, java.util.Objects.requireNonNull(src.findClass(target).orElse(null))))) {
            throw refuse(at, "carries " + marker + " on " + e.property() + ", which holds the edge class " + target
                    + ": an end is a node, never an edge");
        }
        return e;
    }

    /** The relationship type: the enumeration value the derived property carrying {@code <<graph.type>>} spells
     *  (the class's own, else the one its superclasses agree on), else the class's simple name. */
    private static String typeOf(Source src, ClassDefinition cd, List<String> lineage) {
        String marker = "<<" + PlatformTypes.GRAPH_PROFILE + ".type>>";
        String found = null;
        String foundOn = null;
        for (int i = 0; i < lineage.size(); i++) {
            String fqn = lineage.get(i);
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c == null) {
                continue;
            }
            List<DerivedPropertyDefinition> typed = c.derivedProperties().stream().filter(GraphEdges::typeMarked)
                    .toList();
            if (typed.isEmpty()) {
                continue;
            }
            if (typed.size() > 1) {
                throw refuse(c, "carries " + marker + " on " + typed.size() + " derived properties: an edge has one"
                        + " type");
            }
            DerivedPropertyDefinition dp = typed.get(0);
            List<ValueSpecification> body = dp.realization() instanceof com.legend.protocol.Realization.Inline inline
                    ? inline.body() : List.of();
            if (!dp.parameters().isEmpty() || body.size() != 1 || !(body.get(0) instanceof EnumValue ev)) {
                throw refuse(c, "carries " + marker + " on " + dp.name() + ", whose body is not one spelled"
                        + " enumeration value: the type is a typed constant");
            }
            if (i == 0) {
                // the class's own type: what it says
                return ev.value();
            }
            if (found != null && !found.equals(ev.value())) {
                throw refuse(cd, "inherits two relationship types, " + found + " (" + foundOn + ") and " + ev.value()
                        + " (" + fqn + "): declare its own " + marker);
            }
            found = ev.value();
            foundOn = fqn;
        }
        return found != null ? found : Protocol.splitFqn(cd.qualifiedName())[1];
    }

    /** The stereotypes the profile declares, and no tag: {@code Edge}, {@code from}, {@code to}, {@code type} (the
     *  edge classes, §5.6) and {@code Open} (2026-10-17, §12.45: on a DataSpace, the graph a Cypher island's
     *  {@code USE} names, open; the Cypher seam reads it, this reader only checks the declaration) and
     *  {@code Anonymous} (2026-10-17, §12.61: on a class, its name is no label, its instances carry its
     *  superclasses' labels alone; the Cypher seam reads it, this reader only checks the declaration) and
     *  {@code nanoseconds}, {@code timeOfDay} (2026-10-17, §12.64: on a DateTime property, its values of
     *  nanosecond precision, the store's TIMESTAMP_NS; a time of day held as the date-time at it on the epoch day;
     *  the Cypher seam reads them, this reader only checks the declaration). */
    public static final List<String> STEREOTYPES = List.of("Anonymous", "Edge", "Open", "from", "to", "type",
            "nanoseconds", "timeOfDay");

    /** The profile at its path, declaring exactly the eight stereotypes and no tag: checked for each edge class
     *  (the island reads the stereotypes by the profile's full name). Exactly, not at least (the independent review
     *  asked, 2026-10-18; kept): the path names the graph project's one profile, so a declaration with fewer is an
     *  older one, refused by name until the model declares the current one, and a stereotype beyond these would be
     *  read as nothing, where a marker the island does not know must not pass silently. */
    private static void checkProfile(Source src, ClassDefinition cd) {
        ProfileDefinition p = src.findProfile(PlatformTypes.GRAPH_PROFILE).orElse(null);
        if (p == null) {
            throw refuse(cd, "is an edge class, but the profile " + PlatformTypes.GRAPH_PROFILE + " is not declared:"
                    + " a model with edge classes depends on the graph project");
        }
        if (!new TreeSet<>(p.stereotypes()).equals(new TreeSet<>(STEREOTYPES)) || !p.tags().isEmpty()) {
            throw refuse(cd, "is an edge class, but the profile " + PlatformTypes.GRAPH_PROFILE + " declares "
                    + p.stereotypes() + " and tags " + p.tags() + ", not the stereotypes " + STEREOTYPES + " and no"
                    + " tag");
        }
    }

    /** Whether a class of {@code lineage} carries the edge marker. */
    private static boolean isMarked(Source src, List<String> lineage) {
        for (String fqn : lineage) {
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c != null && c.stereotypes().stream().anyMatch(st -> is(st, "Edge"))) {
                return true;
            }
        }
        return false;
    }

    /** The class, then its superclasses (a generic one by its head), each once (a diamond contributes once; a
     *  cycle is phase F's own error). */
    private static List<String> lineage(Source src, ClassDefinition cd) {
        List<String> out = new ArrayList<>();
        java.util.Deque<String> todo = new java.util.ArrayDeque<>(List.of(cd.qualifiedName()));
        while (!todo.isEmpty()) {
            String fqn = todo.poll();
            if (out.contains(fqn)) {
                continue;
            }
            out.add(fqn);
            ClassDefinition c = src.findClass(fqn).orElse(null);
            if (c != null) {
                for (TypeExpression sup : c.superClasses()) {
                    String name = headName(sup);
                    if (name != null) {
                        todo.add(name);
                    }
                }
            }
        }
        return out;
    }

    private static boolean is(StereotypeApplication st, String name) {
        return PlatformTypes.isProfile(st.profileName(), PlatformTypes.GRAPH_PROFILE)
                && name.equals(st.stereotypeName());
    }

    private static boolean typeMarked(DerivedPropertyDefinition dp) {
        for (Protocol.PStereotype st : dp.stereotypes()) {
            if (PlatformTypes.isProfile(st.profile(), PlatformTypes.GRAPH_PROFILE) && "type".equals(st.value())) {
                return true;
            }
        }
        return false;
    }

    private static List<StereotypeApplication> toApplications(List<Protocol.PStereotype> stereotypes) {
        List<StereotypeApplication> out = new ArrayList<>(stereotypes.size());
        for (Protocol.PStereotype st : stereotypes) {
            out.add(new StereotypeApplication(st.profile(), st.value()));
        }
        return out;
    }

    /** "from" or "to" when the stereotypes carry one of the end markers; null otherwise; both, an error of the
     *  declaring class. */
    private static @com.legend.base.Nullable String endKind(List<StereotypeApplication> stereotypes,
            ClassDefinition declaring, String property) {
        String kind = null;
        for (StereotypeApplication st : stereotypes) {
            String here = is(st, "from") ? "from" : is(st, "to") ? "to" : null;
            if (here == null) {
                continue;
            }
            if (kind != null && !kind.equals(here)) {
                throw refuse(declaring, "carries both <<" + PlatformTypes.GRAPH_PROFILE + ".from>> and <<"
                        + PlatformTypes.GRAPH_PROFILE + ".to>> on " + property + ": an end is one or the other");
            }
            kind = here;
        }
        return kind;
    }

    private static boolean toOne(Multiplicity m) {
        return m instanceof Multiplicity.Concrete c && Integer.valueOf(1).equals(c.upperBound());
    }

    /** The class a type expression names (a generic one by its head); null for a structural type. */
    private static @com.legend.base.Nullable String headName(TypeExpression type) {
        return switch (type) {
            case TypeExpression.NameRef nr -> nr.name();
            case TypeExpression.Generic g -> g.name();
            default -> null;
        };
    }

    private static String spell(List<EndRead> ends) {
        List<String> out = new ArrayList<>();
        for (EndRead e : ends) {
            out.add("<<" + PlatformTypes.GRAPH_PROFILE + "." + e.kind() + ">> on " + e.property());
        }
        return String.join(", ", out);
    }

    private static ModelException refuse(ClassDefinition cd, String what) {
        return new ModelException(LegendCompileException.Phase.MODEL, "the class " + cd.qualifiedName() + " " + what,
                cd.qualifiedName());
    }
}
