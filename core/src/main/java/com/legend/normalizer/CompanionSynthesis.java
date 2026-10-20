// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.model.StereotypeApplication;
import com.legend.protocol.graphql.Companion;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.Protocol;
import com.legend.protocol.Realization;
import com.legend.protocol.TypeExpression;
import com.legend.compiler.spec.Calls;
import com.legend.platform.CoreFn;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE COMPANION MODEL'S MEMBERS (docs/GRAPHQL_STUDY_2026_10_18.md §5.8, G15, G20; the inheritance landing, PR3):
 * generated qualified properties beside the stored ones, as milestoning generates {@code employees(businessDate)}
 * beside {@code employees}, written into the parsed classes before the one index reads them (phase E's entry), so
 * the compiler reads them as any derived property and the tree takes them. Per user class {@code C}:
 * <ul>
 *   <li>{@code __typename(): String[1]} -- the exact class of the instance: {@code if($this->instanceOf(S),
 *       |'S', |... |'C')} over the transitive subclasses, the deepest first; a spelled constant per arm, which the
 *       resolver answers from the union row's membership witness (C6.2: a token compared, never computed);</li>
 *   <li>{@code asS(): S[0..1]} per transitive subclass {@code S} -- {@code if($this->instanceOf(S),
 *       |$this->cast(@S), |[])}, the downcast as a to-one hop (G9).</li>
 * </ul>
 * Each carries the stereotype {@code meta::legend::lite::graphql.companion}, by which the schema generator knows
 * them (they are never fields of the class's type: {@code __typename} is GraphQL's own meta-field, {@code asS} an
 * input-side key) and a listing may hide them. A class that declares a member of one of these names itself is
 * refused by name (G11). The boot layer (the system metamodel) takes none. Pure callers may use them: a feature.
 */
public final class CompanionSynthesis {

    private CompanionSynthesis() {
    }

    /** The profile the companion's members carry. */
    public static final String PROFILE = com.legend.compiler.element.type.PlatformTypes.GRAPHQL_PROFILE;

    /** The stereotype marking a generated member. */
    public static final String COMPANION = com.legend.compiler.element.type.PlatformTypes.GRAPHQL_COMPANION;

    public static final String TYPENAME = "__typename";

    /** Whether {@code dp} is a member the companion generated. */
    public static boolean isCompanion(DerivedPropertyDefinition dp) {
        for (Protocol.PStereotype st : dp.stereotypes()) {
            if (st.profile().equals(PROFILE) && st.value().equals(COMPANION)) {
                return true;
            }
        }
        return false;
    }

    /** The name of the downcast member to {@code subClassFqn} ({@code asEmployee}). */
    public static String asName(String subClassFqn) {
        return "as" + simple(subClassFqn);
    }

    /**
     * {@code parsed} with the companion's members written into every non-native class: the same model when it has
     * no class. A class declaring a member of a reserved name ({@code __typename}, {@code as<Sub>} for a subclass
     * {@code Sub}) is refused: the names are the companion's (G11).
     */
    public static ParsedModel apply(ParsedModel parsed) {
        Map<String, ClassDefinition> classes = new LinkedHashMap<>();
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ClassDefinition cd && !cd.isNative()) {
                classes.put(cd.qualifiedName(), cd);
            }
        }
        if (classes.isEmpty()) {
            return parsed;
        }
        // the direct subclasses, in declaration order, by superclass name as written (resolved: the elements are
        // name-resolved before this phase)
        Map<String, List<String>> subs = new TreeMap<>();
        for (ClassDefinition cd : classes.values()) {
            for (TypeExpression sup : cd.superClasses()) {
                if (sup instanceof TypeExpression.NameRef nr && classes.containsKey(nr.name())) {
                    subs.computeIfAbsent(nr.name(), k -> new ArrayList<>()).add(cd.qualifiedName());
                }
            }
        }
        // the AGGREGATE companions (§5.5, §5.8): per class its aggregate class and fold classes; per to-many
        // property (stored, or an association end) its pAggregate() member on the owner
        List<PackageableElement> generated = new ArrayList<>();
        Map<String, List<DerivedPropertyDefinition>> aggregateMembers = new TreeMap<>();
        for (ClassDefinition cd : classes.values()) {
            generated.addAll(aggregateClasses(cd, classes));
            for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
                if (many(pd.multiplicity()) && pd.type() instanceof TypeExpression.NameRef nr
                        && classes.containsKey(nr.name())) {
                    aggregateMembers.computeIfAbsent(cd.qualifiedName(), k -> new ArrayList<>())
                            .add(aggregateMember(pd.name(), nr.name()));
                }
            }
        }
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof AssociationDefinition ad) {
                end(ad.property1(), ad.property2(), classes, aggregateMembers);
                end(ad.property2(), ad.property1(), classes, aggregateMembers);
            }
        }
        List<PackageableElement> out = new ArrayList<>(parsed.elements().size() + generated.size());
        for (PackageableElement el : parsed.elements()) {
            if (!(el instanceof ClassDefinition cd) || cd.isNative()) {
                out.add(el);
                continue;
            }
            // the members are generated on a hierarchy's ROOT (a class with no superclass in the model) and
            // inherited below it: a subclass declaring them again would carry two members of one name. A root's
            // __typename names every transitive subclass, so an inherited body answers a subclass's own rows.
            boolean root = true;
            for (TypeExpression sup : cd.superClasses()) {
                if (sup instanceof TypeExpression.NameRef nr && classes.containsKey(nr.name())) {
                    root = false;
                }
            }
            List<String> all = new ArrayList<>();
            if (root) {
                transitive(cd.qualifiedName(), subs, all, 0, new TreeMap<>());
                Map<String, Integer> depth = new TreeMap<>();
                transitive(cd.qualifiedName(), subs, new ArrayList<>(), 0, depth);
                // the deepest first: a Manager is an Employee is a Person, and names itself Manager
                all.sort((a, b) -> Integer.compare(depth.getOrDefault(b, 0), depth.getOrDefault(a, 0)));
            }
            List<DerivedPropertyDefinition> aggregates = aggregateMembers.getOrDefault(cd.qualifiedName(), List.of());
            refuseReserved(cd, all, aggregates);
            List<DerivedPropertyDefinition> merged = new ArrayList<>(cd.derivedProperties());
            if (root) {
                merged.add(typename(cd, all));
                for (String sub : all) {
                    merged.add(as(cd, sub));
                }
            }
            merged.addAll(aggregates);
            out.add(new ClassDefinition(cd.qualifiedName(), cd.typeParams(), cd.typeVariables(),
                    cd.superClasses(), cd.properties(), merged, cd.constraints(),
                    cd.stereotypes(), cd.taggedValues(), cd.isNative()));
        }
        out.addAll(generated);
        return new ParsedModel(out, parsed.imports(), parsed.source(),
                parsed.elementOffsets(), parsed.elementImports(),
                parsed.elementSources(), parsed.unclaimedSections());
    }

    private static void transitive(String fqn, Map<String, List<String>> subs, List<String> into, int level,
            Map<String, Integer> depth) {
        for (String s : subs.getOrDefault(fqn, List.of())) {
            if (!into.contains(s)) {
                into.add(s);
                depth.merge(s, level + 1, Math::max);
                transitive(s, subs, into, level + 1, depth);
            }
        }
    }

    private static void refuseReserved(ClassDefinition cd, List<String> subs,
            List<DerivedPropertyDefinition> aggregates) {
        java.util.Set<String> reserved = new java.util.TreeSet<>();
        reserved.add(TYPENAME);
        for (String s : subs) {
            reserved.add(asName(s));
        }
        for (DerivedPropertyDefinition a : aggregates) {
            reserved.add(a.name());
        }
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            if (reserved.contains(pd.name())) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        cd.qualifiedName() + "." + pd.name() + " is named like"
                        + " the member the GraphQL companion generates beside it (" + reserved + "): rename the"
                        + " property (docs/GRAPHQL_STUDY_2026_10_18.md G11)");
            }
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            if (reserved.contains(dp.name()) && !isCompanion(dp)) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        cd.qualifiedName() + "." + dp.name() + "() is named like"
                        + " the member the GraphQL companion generates beside it (" + reserved + "): rename the"
                        + " property (docs/GRAPHQL_STUDY_2026_10_18.md G11)");
            }
        }
    }

    // ---- the aggregate companions (§5.5) ----

    private static final java.util.Set<String> NUMERIC = java.util.Set.of("Integer", "Float", "Decimal", "Number",
            "meta::pure::metamodel::type::Integer", "meta::pure::metamodel::type::Float",
            "meta::pure::metamodel::type::Decimal", "meta::pure::metamodel::type::Number");
    private static final java.util.Set<String> DATES = java.util.Set.of("Date", "StrictDate", "DateTime",
            "meta::pure::metamodel::type::Date", "meta::pure::metamodel::type::StrictDate",
            "meta::pure::metamodel::type::DateTime");
    private static final TypeExpression INTEGER = new TypeExpression.NameRef("meta::pure::metamodel::type::Integer");
    private static final TypeExpression FLOAT = new TypeExpression.NameRef("meta::pure::metamodel::type::Float");

    /** A to-one scalar member the folds apply to: its name and its type as the class spells it. */
    private record Scalar(String name, TypeExpression type) {
    }

    private static boolean many(Multiplicity m) {
        return m instanceof Multiplicity.Concrete c && (c.upperBound() == null || c.upperBound() > 1);
    }

    private static boolean one(Multiplicity m) {
        return m instanceof Multiplicity.Concrete c && c.upperBound() != null && c.upperBound() == 1;
    }

    /** The class's own and inherited to-one scalars of the kinds a fold takes, declaration order, ancestors
     *  first; a parameterless derived member counts (the schema's rule). */
    private static List<Scalar> scalars(ClassDefinition cd, Map<String, ClassDefinition> classes,
            java.util.Set<String> kinds, java.util.Set<String> seen) {
        return scalars(cd, classes, kinds, seen, new java.util.HashSet<>());
    }

    private static List<Scalar> scalars(ClassDefinition cd, Map<String, ClassDefinition> classes,
            java.util.Set<String> kinds, java.util.Set<String> seen, java.util.Set<String> visited) {
        List<Scalar> out = new ArrayList<>();
        if (!visited.add(cd.qualifiedName())) {
            return out; // an inheritance cycle: the index refuses it after this pass; the walk ends here
        }
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr && classes.containsKey(nr.name())) {
                out.addAll(scalars(classes.get(nr.name()), classes, kinds, seen, visited));
            }
        }
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            if (pd.type() instanceof TypeExpression.NameRef nr && kinds.contains(nr.name()) && one(pd.multiplicity())
                    && seen.add(pd.name())) {
                out.add(new Scalar(pd.name(), pd.type()));
            }
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            if (dp.parameters().isEmpty() && !isCompanion(dp) && dp.type() instanceof TypeExpression.NameRef nr
                    && kinds.contains(nr.name()) && one(dp.multiplicity()) && seen.add(dp.name())) {
                out.add(new Scalar(dp.name(), dp.type()));
            }
        }
        return out;
    }

    /** {@code CAggregate { items: C[*]; count(); sum(); avg(); min(); max() }} and its fold classes. */
    private static List<PackageableElement> aggregateClasses(ClassDefinition cd, Map<String, ClassDefinition> classes) {
        String c = cd.qualifiedName();
        java.util.Set<String> comparable = new java.util.TreeSet<>(NUMERIC);
        comparable.addAll(DATES);
        List<Scalar> numeric = scalars(cd, classes, NUMERIC, new java.util.TreeSet<>());
        List<Scalar> ordered = scalars(cd, classes, comparable, new java.util.TreeSet<>());
        List<PackageableElement> out = new ArrayList<>();
        List<DerivedPropertyDefinition> members = new ArrayList<>();
        ValueSpecification items = new AppliedProperty(new Variable("this"), Companion.ITEMS);
        // count() { $this.items->map(x|1)->count() }: the reducer over the navigation, 0 over no items
        ValueSpecification ones = Calls.form(CoreFn.MAP, List.of(items,
                new LambdaFunction(List.of(new Variable("x")), List.of(new CInteger(1L, null)))));
        members.add(member(Companion.COUNT,
                Calls.exact(com.legend.builtin.Pure.COUNT__ANY_MANY.qualifiedName(), List.of(ones)),
                INTEGER, Multiplicity.Concrete.PURE_ONE));
        for (Companion.Fold fold : Companion.FOLDS) {
            List<Scalar> props = fold.numericOnly ? numeric : ordered;
            if (props.isEmpty()) {
                continue;
            }
            String foldFqn = Companion.foldClass(c, fold);
            List<ClassDefinition.PropertyDefinition> fields = new ArrayList<>();
            List<NewInstance.KeyBinding> bindings = new ArrayList<>();
            for (Scalar sc : props) {
                TypeExpression t = fold == Companion.Fold.AVG ? FLOAT : foldType(sc);
                fields.add(new ClassDefinition.PropertyDefinition(sc.name(), t, Multiplicity.Concrete.ZERO_ONE,
                        List.of(), List.of()));
                ValueSpecification values = new AppliedProperty(items, sc.name());
                bindings.add(new NewInstance.KeyBinding(sc.name(),
                        new KeyExpression(Calls.exact(foldFunction(fold, sc), List.of(values)), false, false)));
            }
            // every GraphQL object type answers __typename (the spec's meta-field): a generated type names itself
            // (the GraphQL hardening corpus, 2026-10-20: `ordersAggregate { __typename }` was refused)
            out.add(new ClassDefinition(foldFqn, List.of(), List.of(), List.of(), fields,
                    List.of(selfName(foldFqn)), List.of(),
                    List.of(new StereotypeApplication(PROFILE, COMPANION)), List.of(), false));
            members.add(member(fold.field, new NewInstance(foldFqn, List.of(), bindings),
                    new TypeExpression.NameRef(foldFqn), Multiplicity.Concrete.PURE_ONE));
        }
        members.add(selfName(Companion.aggregateClass(c)));
        out.add(new ClassDefinition(Companion.aggregateClass(c), List.of(), List.of(), List.of(),
                List.of(new ClassDefinition.PropertyDefinition(Companion.ITEMS, new TypeExpression.NameRef(c),
                        Multiplicity.Concrete.ZERO_MANY, List.of(), List.of())),
                members, List.of(), List.of(new StereotypeApplication(PROFILE, COMPANION)), List.of(), false));
        return out;
    }

    private static final TypeExpression NUMBER = new TypeExpression.NameRef("meta::pure::metamodel::type::Number");

    /** The scalar's kind, one of {@code Integer, Float, Decimal, Number, Date, StrictDate, DateTime}, by the
     *  simple or the metamodel spelling (the two spellings {@link #NUMERIC} and {@link #DATES} admit). */
    private static String kind(Scalar sc) {
        String n = ((TypeExpression.NameRef) sc.type()).name();
        String k = KINDS.get(n);
        if (k == null) {
            throw new IllegalStateException("not a fold's kind: " + n);
        }
        return k;
    }

    /** Both spellings of each kind to the kind. */
    private static final Map<String, String> KINDS = kinds();

    private static Map<String, String> kinds() {
        Map<String, String> m = new java.util.LinkedHashMap<>();
        for (String k : List.of("Integer", "Float", "Decimal", "Number", "Date", "StrictDate", "DateTime")) {
            m.put(k, k);
            m.put("meta::pure::metamodel::type::" + k, k);
        }
        return java.util.Collections.unmodifiableMap(m);
    }

    /** The type a fold of the scalar has, as the catalog's overload returns it: an Integer's or a Float's own,
     *  a Decimal's or a Number's {@code Number} (the catalog folds decimals through the Number overload), a
     *  date's own kind. */
    private static TypeExpression foldType(Scalar sc) {
        return switch (kind(sc)) {
            case "Decimal", "Number" -> NUMBER;
            default -> sc.type();
        };
    }

    /** The fold's function, by the catalog's overload of the scalar's kind: sum and average over numbers; min and
     *  max over numbers or dates. */
    private static String foldFunction(Companion.Fold fold, Scalar sc) {
        String kind = kind(sc);
        return switch (fold) {
            case SUM -> switch (kind) {
                case "Integer" -> com.legend.builtin.Pure.SUM__INTEGER_MANY.qualifiedName();
                case "Float" -> com.legend.builtin.Pure.SUM__FLOAT_MANY.qualifiedName();
                default -> com.legend.builtin.Pure.SUM__NUMBER_MANY.qualifiedName();
            };
            case AVG -> com.legend.builtin.Pure.AVERAGE__NUMBER_MANY.qualifiedName();
            case MIN -> switch (kind) {
                case "Integer" -> com.legend.builtin.Pure.MIN__INTEGER_MANY.qualifiedName();
                case "Float" -> com.legend.builtin.Pure.MIN__FLOAT_MANY.qualifiedName();
                case "Date" -> com.legend.builtin.Pure.MIN__DATE_MANY.qualifiedName();
                case "StrictDate" -> com.legend.builtin.Pure.MIN__STRICT_DATE_MANY.qualifiedName();
                case "DateTime" -> com.legend.builtin.Pure.MIN__DATE_TIME_MANY.qualifiedName();
                default -> com.legend.builtin.Pure.MIN__NUMBER_MANY.qualifiedName();
            };
            case MAX -> switch (kind) {
                case "Integer" -> com.legend.builtin.Pure.MAX__INTEGER_MANY.qualifiedName();
                case "Float" -> com.legend.builtin.Pure.MAX__FLOAT_MANY.qualifiedName();
                case "Date" -> com.legend.builtin.Pure.MAX__DATE_MANY.qualifiedName();
                case "StrictDate" -> com.legend.builtin.Pure.MAX__STRICT_DATE_MANY.qualifiedName();
                case "DateTime" -> com.legend.builtin.Pure.MAX__DATE_TIME_MANY.qualifiedName();
                default -> com.legend.builtin.Pure.MAX__NUMBER_MANY.qualifiedName();
            };
        };
    }

    /** {@code pAggregate() { ^TAggregate(items = $this.p) }: TAggregate[1]}. */
    private static DerivedPropertyDefinition aggregateMember(String property, String targetFqn) {
        String aggFqn = Companion.aggregateClass(targetFqn);
        ValueSpecification body = new NewInstance(aggFqn, List.of(), List.of(new NewInstance.KeyBinding(
                Companion.ITEMS, new KeyExpression(new AppliedProperty(new Variable("this"), property), false,
                        false))));
        return member(Companion.aggregateProperty(property), body, new TypeExpression.NameRef(aggFqn),
                Multiplicity.Concrete.PURE_ONE);
    }

    /** An association end injected onto the OTHER end's class gets its aggregate member there. */
    private static void end(AssociationDefinition.AssociationEndDefinition injected,
            AssociationDefinition.AssociationEndDefinition other, Map<String, ClassDefinition> classes,
            Map<String, List<DerivedPropertyDefinition>> into) {
        if (!(other.targetClass() instanceof TypeExpression.NameRef owner)
                || !(injected.targetClass() instanceof TypeExpression.NameRef target)
                || !classes.containsKey(owner.name()) || !classes.containsKey(target.name())
                || !many(injected.multiplicity())) {
            return;
        }
        into.computeIfAbsent(owner.name(), k -> new ArrayList<>())
                .add(aggregateMember(injected.propertyName(), target.name()));
    }

    /** {@code __typename() { 'OrderAggregate' }: String[1]}: a generated class's name, its GraphQL type's. */
    private static DerivedPropertyDefinition selfName(String generatedFqn) {
        return member(TYPENAME, new CString(simple(generatedFqn)),
                new TypeExpression.NameRef("meta::pure::metamodel::type::String"), Multiplicity.Concrete.PURE_ONE);
    }

    /** {@code __typename() { if($this->instanceOf(S1), |'S1', |if(..., |'C')) }: String[1]}. */
    private static DerivedPropertyDefinition typename(ClassDefinition cd, List<String> subsDeepestFirst) {
        ValueSpecification body = new CString(simple(cd.qualifiedName()));
        for (int i = subsDeepestFirst.size() - 1; i >= 0; i--) {
            String s = subsDeepestFirst.get(i);
            body = Calls.form(CoreFn.IF, List.of(instanceOf(s), thunk(new CString(simple(s))), thunk(body)));
        }
        // the m3 FQN: the elements are name-resolved before this phase, and the prelude tier spells it so
        return member(TYPENAME, body, new TypeExpression.NameRef("meta::pure::metamodel::type::String"),
                Multiplicity.Concrete.PURE_ONE);
    }

    /** {@code asS() { if($this->instanceOf(S), |$this->cast(@S), |[]) }: S[0..1]}. */
    private static DerivedPropertyDefinition as(ClassDefinition cd, String sub) {
        ValueSpecification cast = Calls.form(CoreFn.CAST, List.of(new Variable("this"),
                new TypeAnnotation.Named(new TypeExpression.NameRef(sub))));
        ValueSpecification body = Calls.form(CoreFn.IF, List.of(instanceOf(sub), thunk(cast),
                thunk(new PureCollection(List.of()))));
        return member(asName(sub), body, new TypeExpression.NameRef(sub), Multiplicity.Concrete.ZERO_ONE);
    }

    private static ValueSpecification instanceOf(String fqn) {
        return Calls.exact(com.legend.builtin.NativeFn.SubtypeForm.INSTANCE_OF.fqn(),
                List.of(new Variable("this"), new PackageableElementPtr(fqn)));
    }

    private static LambdaFunction thunk(ValueSpecification body) {
        return new LambdaFunction(List.of(), List.of(body));
    }

    private static DerivedPropertyDefinition member(String name, ValueSpecification body, TypeExpression type,
            Multiplicity multiplicity) {
        return new DerivedPropertyDefinition(name, List.of(), new Realization.Inline(List.of(body)), type,
                multiplicity, List.of(new Protocol.PStereotype(PROFILE, COMPANION, null, null)), List.of(), null);
    }

    private static String simple(String fqn) {
        int i = fqn.lastIndexOf("::");
        return i < 0 ? fqn : fqn.substring(i + 2);
    }
}
