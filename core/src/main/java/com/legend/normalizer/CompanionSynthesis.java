// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.ClassDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
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
        // (the AGGREGATE companions -- a generated aggregate class and fold classes per class, a pAggregate() member
        // per to-many property -- were deleted 2026-10-21: a summary of a collection is the relation node's table,
        // register S118; the model holds the user's classes only)
        List<PackageableElement> out = new ArrayList<>(parsed.elements().size());
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
            refuseReserved(cd, all);
            List<DerivedPropertyDefinition> merged = new ArrayList<>(cd.derivedProperties());
            if (root) {
                merged.add(typename(cd, all));
                for (String sub : all) {
                    merged.add(as(cd, sub));
                }
            }
            out.add(new ClassDefinition(cd.qualifiedName(), cd.typeParams(), cd.typeVariables(),
                    cd.superClasses(), cd.properties(), merged, cd.constraints(),
                    cd.stereotypes(), cd.taggedValues(), cd.isNative()));
        }
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

    private static void refuseReserved(ClassDefinition cd, List<String> subs) {
        java.util.Set<String> reserved = new java.util.TreeSet<>();
        reserved.add(TYPENAME);
        for (String s : subs) {
            reserved.add(asName(s));
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
