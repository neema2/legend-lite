// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.TypedEnum;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.graphql.GqlFold;
import com.legend.protocol.graphql.GqlSchema;
import com.legend.protocol.graphql.GqlSchema.EnumType;
import com.legend.protocol.graphql.GqlSchema.FieldDef;
import com.legend.protocol.graphql.GqlSchema.InputObjectType;
import com.legend.protocol.graphql.GqlSchema.InputValueDef;
import com.legend.protocol.graphql.GqlSchema.InterfaceType;
import com.legend.protocol.graphql.GqlSchema.Meaning;
import com.legend.protocol.graphql.GqlSchema.ObjectType;
import com.legend.protocol.graphql.GqlSchema.ScalarType;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.graphql.GqlSchema.TypeDef;
import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.protocol.Documentation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * THE GENERATOR (docs/GRAPHQL_STUDY_2026_10_18.md §5): a Pure model's classes in a {@link Scope} become a GraphQL
 * schema, rule by rule. Per class {@code S}: the object type (unless abstract), {@code SInterface} where the class
 * has subclasses or is abstract (G8), {@code SWhere}, {@code SListFilter}, {@code SOrderBy} (G2, G3, G9), the
 * aggregate shapes (G6) and the group shapes (G7); per to-many property its {@code pAggregate} and {@code pGroups}
 * siblings; the {@code Query} roots (G4); the scalars and enums (G5). Descriptions come from {@code doc.doc} tagged
 * values and, on a concrete type with subclasses, from the generator (G19). A model that cannot be mapped without
 * renaming, skipping or defaulting is refused by element and rule ({@link GqlSchemaException}, G11, §5.7).
 * Deterministic: classes by name, fields in declaration order (inherited first).
 */
public final class SchemaGenerator {

    private static final Pattern GRAPHQL_NAME = Pattern.compile("[_A-Za-z][_0-9A-Za-z]*");
    private static final Set<String> RESERVED_ENUM_VALUES = Set.of("true", "false", "null");
    static final String INT = "Int";
    static final String BIG_INT = "BigInt";
    static final String FLOAT = "Float";
    static final String DECIMAL = "Decimal";
    static final String STRING = "String";
    static final String BOOLEAN = "Boolean";
    static final String DATE = "Date";
    static final String DATE_TIME = "DateTime";
    static final String TIME = "Time";
    static final String SORT_DIRECTION = "SortDirection";
    static final String QUERY_MODE = "QueryMode";
    static final String DATE_UNIT = "DateUnit";


    /** One property of a class as the generator sees it: own, inherited or association-injected. */
    record Prop(String name, Kind kind, Type type, Multiplicity multiplicity, boolean derived,
            List<TypedParameter> parameters, @com.legend.base.Nullable String description, boolean deprecated) {
        boolean toMany() {
            return multiplicity.isMany();
        }

        boolean optional() {
            return !(multiplicity instanceof Multiplicity.Bounded b) || b.lower() == 0;
        }

        /** The class an object-typed property reaches. */
        String classFqn() {
            return ((Type.ClassType) type).fqn();
        }
    }

    enum Kind { SCALAR, ENUM, OBJECT }

    /** A class in scope with everything the rules read. */
    record ClassInfo(String fqn, String simple, List<Prop> props, boolean isAbstract, List<String> ancestors,
            List<String> subclasses, @com.legend.base.Nullable String description,
            com.legend.compiler.element.@com.legend.base.Nullable MilestoningStrategy temporal) {
        boolean polymorphic() {
            return isAbstract || !subclasses.isEmpty();
        }

        /** The type a position of this class is typed with: the interface where one exists (G8). */
        String refName() {
            return polymorphic() ? simple + "Interface" : simple;
        }
    }

    private final ModelContext ctx;
    private final Map<String, ClassInfo> classes = new TreeMap<>();
    private final Map<String, TypeDef> out = new LinkedHashMap<>();
    private final Set<String> enums = new TreeSet<>();
    private final Set<String> scalarsUsed = new TreeSet<>();

    private SchemaGenerator(ModelContext ctx) {
        this.ctx = ctx;
    }

    public static Schema generate(ModelContext ctx, Scope scope) {
        SchemaGenerator g = new SchemaGenerator(ctx);
        g.readClasses(scope);
        g.checkNames();
        List<TypeDef> types = new ArrayList<>();
        types.add(g.queryType());
        for (ClassInfo c : g.classes.values()) {
            g.classTypes(c);
        }
        g.enumTypes();
        g.scalarTypes();
        g.filterTypes();
        List<TypeDef> rest = new ArrayList<>(g.out.values());
        rest.sort((a, b) -> a.name().compareTo(b.name()));
        types.addAll(rest);
        return Schema.of(types);
    }

    // ---- reading the model ----

    private void readClasses(Scope scope) {
        Map<String, List<String>> subs = new TreeMap<>();
        for (String fqn : scope.classFqns()) {
            TypedClass c = ctx.findClass(fqn).orElseThrow();
            for (String sup : c.superClassFqns()) {
                if (scope.contains(sup)) {
                    subs.computeIfAbsent(sup, k -> new ArrayList<>()).add(fqn);
                }
            }
        }
        Map<String, List<AssociationDefinition.AssociationEndDefinition>> injected = injectedEnds(scope);
        for (String fqn : scope.classFqns()) {
            TypedClass c = ctx.findClass(fqn).orElseThrow();
            ClassDefinition cd = ctx.findClassDefinition(fqn).orElse(null);
            List<String> ancestors = new ArrayList<>();
            ancestorsOf(fqn, scope, ancestors);
            List<Prop> props = new ArrayList<>();
            Set<String> seen = new LinkedHashSet<>();
            for (int i = ancestors.size() - 1; i >= 0; i--) {
                ownProps(ancestors.get(i), scope, injected, props, seen);
            }
            ownProps(fqn, scope, injected, props, seen);
            List<String> all = new ArrayList<>();
            transitiveSubclasses(fqn, subs, all);
            boolean isAbstract = cd != null && cd.stereotypes().stream().anyMatch(st ->
                    PlatformTypes.isProfile(st.profileName(), PlatformTypes.TYPEMODIFIERS_PROFILE)
                            && "abstract".equals(st.stereotypeName()));
            String description = cd == null ? null : doc(cd.taggedValues().stream()
                    .filter(tv -> PlatformTypes.isProfile(tv.profileName(), Documentation.PROFILE)
                            && Documentation.TAG.equals(tv.tagName()))
                    .map(com.legend.model.TaggedValue::value).findFirst().orElse(null));
            classes.put(fqn, new ClassInfo(fqn, simple(fqn), props, isAbstract, ancestors, all, description,
                    com.legend.compiler.element.Temporal.strategyOf(ctx, fqn)));
        }
    }

    private void ancestorsOf(String fqn, Scope scope, List<String> into) {
        TypedClass c = ctx.findClass(fqn).orElseThrow();
        for (String sup : c.superClassFqns()) {
            if (!scope.contains(sup)) {
                throw new GqlSchemaException(fqn + " extends " + sup + ", which the data space's scope does not"
                        + " include: a superclass must be in scope with its subclass", fqn);
            }
            if (!into.contains(sup)) {
                into.add(sup);
                ancestorsOf(sup, scope, into);
            }
        }
    }

    private static void transitiveSubclasses(String fqn, Map<String, List<String>> subs, List<String> into) {
        for (String s : subs.getOrDefault(fqn, List.of())) {
            if (!into.contains(s)) {
                into.add(s);
                transitiveSubclasses(s, subs, into);
            }
        }
    }

    /** The first end of each association, indexed by the class the second end names (the class it is
     *  injected onto), as the Cypher seam reads them, and the second end onto the first's class. An association
     *  whose classes are not both in scope is outside the scope and injects nothing: a data space scoped to
     *  {@code Firm} has no {@code employees}. */
    private Map<String, List<AssociationDefinition.AssociationEndDefinition>> injectedEnds(Scope scope) {
        Map<String, List<AssociationDefinition.AssociationEndDefinition>> out = new TreeMap<>();
        for (String fqn : new TreeSet<>(ctx.elementFqns())) {
            AssociationDefinition ad = ctx.findAssociation(fqn).orElse(null);
            if (ad == null) {
                continue;
            }
            String owner1 = className(ad.property2().targetClass());
            String owner2 = className(ad.property1().targetClass());
            if (owner1 == null || owner2 == null || !scope.contains(owner1)
                    || !scope.contains(owner2)) {
                continue;
            }
            out.computeIfAbsent(owner1, k -> new ArrayList<>()).add(ad.property1());
            out.computeIfAbsent(owner2, k -> new ArrayList<>()).add(ad.property2());
        }
        return out;
    }

    private static @com.legend.base.Nullable String className(com.legend.protocol.TypeExpression type) {
        return type instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name() : null;
    }

    private void ownProps(String fqn, Scope scope,
            Map<String, List<AssociationDefinition.AssociationEndDefinition>> injected, List<Prop> into,
            Set<String> seen) {
        TypedClass c = ctx.findClass(fqn).orElseThrow();
        ClassDefinition cd = ctx.findClassDefinition(fqn).orElse(null);
        Set<String> companion = new TreeSet<>();
        if (cd != null) {
            for (com.legend.protocol.DerivedPropertyDefinition dp : cd.derivedProperties()) {
                if (isCompanion(dp)) {
                    companion.add(dp.name());
                }
            }
        }
        for (Property p : c.properties()) {
            if (companion.contains(p.name())) {
                // the companion's members (__typename, as<Sub>; docs/GRAPHQL_STUDY_2026_10_18.md §5.8) are the
                // schema's meta-field and input-side keys, never fields of the type
                seen.add(p.name());
                continue;
            }
            if (!seen.add(p.name())) {
                continue;
            }
            into.add(prop(fqn, p, scope, cd));
        }
        for (AssociationDefinition.AssociationEndDefinition end : injected.getOrDefault(fqn, List.of())) {
            if (!seen.add(end.propertyName())) {
                continue;
            }
            Property p = ctx.findProperty(fqn, end.propertyName()).orElseThrow(() -> new GqlSchemaException(
                    fqn + " has no property " + end.propertyName() + " its association injects", fqn));
            into.add(prop(fqn, p, scope, cd));
        }
    }

    /** Whether {@code dp} is a member the companion generated ({@code <<meta::legend::lite::graphql.companion>>}). */
    static boolean isCompanion(com.legend.protocol.DerivedPropertyDefinition dp) {
        for (com.legend.protocol.Protocol.PStereotype st : dp.stereotypes()) {
            if (st.profile().equals(PlatformTypes.GRAPHQL_PROFILE) && st.value().equals(PlatformTypes.GRAPHQL_COMPANION)) {
                return true;
            }
        }
        return false;
    }

    private Prop prop(String classFqn, Property p, Scope scope, @com.legend.base.Nullable ClassDefinition cd) {
        Kind kind = kindOf(classFqn, p.name(), p.type(), scope);
        boolean derived = p instanceof Property.Derived;
        List<TypedParameter> params = p instanceof Property.Derived d ? d.parameters() : List.of();
        String description = null;
        boolean deprecated = false;
        if (cd != null) {
            for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
                if (pd.name().equals(p.name())) {
                    description = docOf(pd.taggedValues());
                    deprecated = deprecatedOf(pd.stereotypes());
                }
            }
            for (com.legend.protocol.DerivedPropertyDefinition dp : cd.derivedProperties()) {
                if (dp.name().equals(p.name())) {
                    description = protocolDocOf(dp.taggedValues());
                    deprecated = dp.stereotypes().stream().anyMatch(st ->
                            PlatformTypes.isProfile(st.profile(), Documentation.PROFILE)
                                    && "deprecated".equals(st.value()));
                }
            }
        }
        return new Prop(p.name(), kind, p.type(), p.multiplicity(), derived, params, description, deprecated);
    }

    private Kind kindOf(String classFqn, String name, Type type, Scope scope) {
        return switch (type) {
            case Type.Primitive p -> {
                scalarOf(p, classFqn + "." + name);
                yield Kind.SCALAR;
            }
            case Type.PrecisionDecimal d -> Kind.SCALAR;
            case Type.EnumType e -> {
                enums.add(e.fqn());
                yield Kind.ENUM;
            }
            case Type.ClassType c -> {
                if (!scope.contains(c.fqn())) {
                    throw new GqlSchemaException(classFqn + "." + name + " is typed " + c.fqn() + ", which the data"
                            + " space's scope does not include: a property's class must be in scope", classFqn);
                }
                yield Kind.OBJECT;
            }
            case Type.TypeVar tv -> throw unmappable(classFqn, name, "a type variable");
            case Type.GenericType g -> throw unmappable(classFqn, name, "a generic type");
            case Type.FunctionType f -> throw unmappable(classFqn, name, "a function type");
            case Type.RelationType r -> throw unmappable(classFqn, name, "a relation type");
            case Type.SchemaAlgebra s -> throw unmappable(classFqn, name, "a schema algebra type");
        };
    }

    private static GqlSchemaException unmappable(String classFqn, String name, String what) {
        return new GqlSchemaException(classFqn + "." + name + " is " + what + ", which has no GraphQL type", classFqn);
    }

    /** The scalar a primitive maps to (G5); {@code at} names the property for the refusal. */
    private String scalarOf(Type.Primitive p, String at) {
        String s = switch (p) {
            case INTEGER -> BIG_INT;
            case FLOAT -> FLOAT;
            case DECIMAL, NUMBER -> DECIMAL;
            case STRING -> STRING;
            case BOOLEAN -> BOOLEAN;
            case STRICT_DATE -> DATE;
            case DATE, DATE_TIME -> DATE_TIME;
            case STRICT_TIME -> TIME;
            case LATEST_DATE -> throw new GqlSchemaException(at + " is a LatestDate, which has no GraphQL type", at);
            case BYTE -> throw new GqlSchemaException(at + " is a Byte, which has no GraphQL type", at);
        };
        scalarsUsed.add(s);
        return s;
    }

    private String scalarOf(Prop p) {
        return switch (p.type()) {
            case Type.Primitive prim -> scalarOf(prim, p.name());
            case Type.PrecisionDecimal d -> {
                scalarsUsed.add(DECIMAL);
                yield DECIMAL;
            }
            default -> throw new IllegalStateException(p.name() + " is not a scalar property");
        };
    }

    private static @com.legend.base.Nullable String docOf(List<com.legend.model.TaggedValue> tvs) {
        for (com.legend.model.TaggedValue tv : tvs) {
            if (PlatformTypes.isProfile(tv.profileName(), Documentation.PROFILE) && Documentation.TAG.equals(tv.tagName())) {
                return doc(tv.value());
            }
        }
        return null;
    }

    private static @com.legend.base.Nullable String protocolDocOf(List<com.legend.protocol.Protocol.PTaggedValue> tvs) {
        for (com.legend.protocol.Protocol.PTaggedValue tv : tvs) {
            if (PlatformTypes.isProfile(tv.tag().profile(), Documentation.PROFILE)
                    && Documentation.TAG.equals(tv.tag().value())) {
                return doc(tv.value());
            }
        }
        return null;
    }

    private static boolean deprecatedOf(List<com.legend.model.StereotypeApplication> sts) {
        return sts.stream().anyMatch(st -> PlatformTypes.isProfile(st.profileName(), Documentation.PROFILE)
                && "deprecated".equals(st.stereotypeName()));
    }

    private static @com.legend.base.Nullable String doc(@com.legend.base.Nullable String raw) {
        return raw == null || raw.trim().isEmpty() ? null : raw.strip();
    }

    static String simple(String fqn) {
        int i = fqn.lastIndexOf("::");
        return i < 0 ? fqn : fqn.substring(i + 2);
    }

    // ---- the reserved names (G11) ----

    private void checkNames() {
        Map<String, String> simpleToFqn = new TreeMap<>();
        for (ClassInfo c : classes.values()) {
            String dup = simpleToFqn.put(c.simple(), c.fqn());
            if (dup != null) {
                throw new GqlSchemaException("two classes of one simple name in the data space's scope, " + dup
                        + " and " + c.fqn() + ": GraphQL types are named by the simple name", c.fqn());
            }
            if (!GRAPHQL_NAME.matcher(c.simple()).matches()) {
                throw new GqlSchemaException(c.fqn() + " is not a GraphQL name", c.fqn());
            }
        }
        for (ClassInfo c : classes.values()) {
            for (String generated : generatedTypeNames(c)) {
                String other = simpleToFqn.get(generated);
                if (other != null && !generated.equals(c.simple())) {
                    throw new GqlSchemaException("the class " + other + " is named like the type the generator makes"
                            + " for " + c.fqn() + " (" + generated + "): rename the class", other);
                }
            }
            Set<String> propNames = new TreeSet<>();
            for (Prop p : c.props()) {
                if (!GRAPHQL_NAME.matcher(p.name()).matches()) {
                    throw new GqlSchemaException(c.fqn() + "." + p.name() + " is not a GraphQL name", c.fqn());
                }
                propNames.add(p.name());
            }
            for (String generated : generatedFieldNames(c)) {
                if (propNames.contains(generated)) {
                    throw new GqlSchemaException(c.fqn() + "." + generated + " is named like the field the generator"
                            + " makes beside it: rename the property", c.fqn());
                }
            }
        }
    }

    private static List<String> generatedTypeNames(ClassInfo c) {
        String s = c.simple();
        List<String> names = new ArrayList<>(List.of(s + "Interface", s + "Where", s + "ListFilter", s + "OrderBy",
                s + "Aggregate", s + "AggregateOrderBy", s + "Group", s + "GroupKey", s + "GroupHaving",
                s + "GroupOrderBy"));
        for (GqlFold fn : GqlFold.ALL) {
            String f = s + fn.suffix;
            names.add(f);
            names.add(f + "OrderBy");
            names.add(f + "Filter");
        }
        return names;
    }

    private List<String> generatedFieldNames(ClassInfo c) {
        List<String> names = new ArrayList<>();
        names.add("__typename");
        for (Prop p : c.props()) {
            if (p.kind() == Kind.OBJECT && p.toMany()) {
                names.add(p.name() + "Aggregate");
                names.add(p.name() + "Groups");
            }
        }
        for (String sub : c.subclasses()) {
            names.add("as" + simple(sub));
        }
        if (c.temporal() != null) {
            names.add(BUSINESS_DATE);
            names.add(PROCESSING_DATE);
        }
        return names;
    }

    static String capitalize(String s) {
        return s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    // ---- the output types ----

    private ObjectType queryType() {
        List<FieldDef> fields = new ArrayList<>();
        for (ClassInfo c : classes.values()) {
            String s = c.simple();
            // a TEMPORAL class's extent is as of its dates (register S119): every root over it takes them
            fields.add(new FieldDef(s, null, GqlSchema.listOf(c.refName()), withDates(c, collectionArgs(s)), null,
                    new Meaning.Root(c.fqn())));
            fields.add(new FieldDef(s + "Aggregate", null, GqlSchema.named(s + "Aggregate", false),
                    withDates(c, List.of(new InputValueDef("where", GqlSchema.named(s + "Where", true)))), null,
                    new Meaning.AggregateRoot(c.fqn())));
            fields.add(new FieldDef(s + "Groups", null, GqlSchema.listOf(s + "Group"), withDates(c, groupsArgs(s)),
                    null, new Meaning.GroupsRoot(c.fqn())));
        }
        return new ObjectType("Query", null, List.of(), fields, null);
    }

    /**
     * The DATE ARGUMENTS of a temporal class's extent (register S119), before {@code rest}: a business-temporal class
     * takes a {@code businessDate: Date!}, a processing-temporal one a {@code processingDate: DateTime!}, a bitemporal
     * one both, the processing date first (Pure's {@code all(processingDate, businessDate)} and {@code
     * prop(processingDate, businessDate)}). Required wherever a temporal class is reached -- a root, a field -- so a
     * query always names the version it reads (the engine's propagation of a temporal owner's dates is not offered
     * yet); a non-temporal class takes none.
     */
    private List<InputValueDef> withDates(ClassInfo target, List<InputValueDef> rest) {
        if (target.temporal() == null) {
            return rest;
        }
        List<InputValueDef> out = new ArrayList<>();
        if (target.temporal().has(com.legend.compiler.element.MilestoningStrategy.Dimension.PROCESSING)) {
            scalarsUsed.add(DATE_TIME);
            out.add(new InputValueDef(PROCESSING_DATE, GqlSchema.named(DATE_TIME, false),
                    "The processing date: the version as recorded then."));
        }
        if (target.temporal().has(com.legend.compiler.element.MilestoningStrategy.Dimension.BUSINESS)) {
            scalarsUsed.add(DATE);
            out.add(new InputValueDef(BUSINESS_DATE, GqlSchema.named(DATE, false),
                    "The business date: the version holding on it."));
        }
        out.addAll(rest);
        return out;
    }

    static final String BUSINESS_DATE = "businessDate";
    static final String PROCESSING_DATE = "processingDate";

    /** Whether a property reaches a temporal class: it takes its dates as a field, so no input object (a
     *  condition, a sort key, a group key) and no summary of it ({@code pAggregate}, {@code pGroups}) can name it
     *  (register S119). */
    private boolean reachesTemporal(Prop p) {
        return p.kind() == Kind.OBJECT && classOf(p.classFqn()).temporal() != null;
    }

    /** {@code where, orderBy, limit, offset} (G3). */
    private static List<InputValueDef> collectionArgs(String s) {
        return List.of(new InputValueDef("where", GqlSchema.named(s + "Where", true)),
                new InputValueDef("orderBy", GqlSchema.nullableListOf(s + "OrderBy")),
                new InputValueDef("limit", GqlSchema.named(INT, true)),
                new InputValueDef("offset", GqlSchema.named(INT, true)));
    }

    private static List<InputValueDef> groupsArgs(String s) {
        return List.of(new InputValueDef("where", GqlSchema.named(s + "Where", true)),
                new InputValueDef("having", GqlSchema.named(s + "GroupHaving", true)),
                new InputValueDef("orderBy", GqlSchema.nullableListOf(s + "GroupOrderBy")),
                new InputValueDef("limit", GqlSchema.named(INT, true)),
                new InputValueDef("offset", GqlSchema.named(INT, true)));
    }

    private void classTypes(ClassInfo c) {
        List<FieldDef> fields = new ArrayList<>();
        for (Prop p : c.props()) {
            fields.add(propertyField(c, p));
            if (p.kind() == Kind.OBJECT && p.toMany() && !reachesTemporal(p)) {
                String t = classOf(p.classFqn()).simple();
                fields.add(new FieldDef(p.name() + "Aggregate", null, GqlSchema.named(t + "Aggregate", false),
                        List.of(new InputValueDef("where", GqlSchema.named(t + "Where", true))), null,
                        new Meaning.AggregateOf(c.fqn(), p.name())));
                fields.add(new FieldDef(p.name() + "Groups", null, GqlSchema.listOf(t + "Group"), groupsArgs(t), null,
                        new Meaning.GroupsOf(c.fqn(), p.name())));
            }
        }
        // a temporal object answers the dates it was read as of (Pure's generated members; register S119)
        if (c.temporal() != null) {
            if (c.temporal().has(com.legend.compiler.element.MilestoningStrategy.Dimension.PROCESSING)) {
                fields.add(new FieldDef(PROCESSING_DATE, "The processing date this version was read as of.",
                        GqlSchema.named(DATE_TIME, false), List.of(), null,
                        new Meaning.Property(c.fqn(), PROCESSING_DATE, false, false)));
            }
            if (c.temporal().has(com.legend.compiler.element.MilestoningStrategy.Dimension.BUSINESS)) {
                fields.add(new FieldDef(BUSINESS_DATE, "The business date this version was read as of.",
                        GqlSchema.named(DATE, false), List.of(), null,
                        new Meaning.Property(c.fqn(), BUSINESS_DATE, false, false)));
            }
        }
        List<String> interfaces = new ArrayList<>();
        for (String anc : c.ancestors()) {
            ClassInfo a = classOf(anc);
            if (a.polymorphic()) {
                interfaces.add(a.simple() + "Interface");
            }
        }
        if (c.polymorphic()) {
            String subs = String.join(", ", c.subclasses().stream().map(SchemaGenerator::simple).toList());
            String idesc = c.description() == null ? "" : c.description() + "\n";
            idesc += "Any " + c.simple() + ": " + (c.isAbstract() ? "" : c.simple() + " itself or ")
                    + "a subclass (" + subs + ").";
            put(new InterfaceType(c.simple() + "Interface", idesc, interfaces, fields, c.fqn()));
            if (!c.isAbstract()) {
                String desc = (c.description() == null ? "" : c.description() + "\n") + "Exact " + c.simple()
                        + " instances only; spread on " + c.simple() + "Interface to include " + subs + ".";
                List<String> impl = new ArrayList<>(List.of(c.simple() + "Interface"));
                impl.addAll(interfaces);
                put(new ObjectType(c.simple(), desc, impl, fields, c.fqn()));
            }
        } else {
            put(new ObjectType(c.simple(), c.description(), interfaces, fields, c.fqn()));
        }
        inputTypes(c);
        aggregateTypes(c);
        groupTypes(c);
    }

    private FieldDef propertyField(ClassInfo c, Prop p) {
        GqlSchema.TypeRef type;
        List<InputValueDef> args = new ArrayList<>();
        for (TypedParameter tp : p.parameters()) {
            args.add(new InputValueDef(tp.name(), inputRef(c.fqn() + "." + p.name(), tp.type(), tp.multiplicity())));
        }
        switch (p.kind()) {
            case SCALAR -> type = p.toMany() ? GqlSchema.listOf(scalarOf(p)) : GqlSchema.named(scalarOf(p), p.optional());
            case ENUM -> type = p.toMany() ? GqlSchema.listOf(enumName(p)) : GqlSchema.named(enumName(p), p.optional());
            case OBJECT -> {
                ClassInfo t = classOf(p.classFqn());
                // a temporal target's dates are the field's arguments (register S119); its list takes no clause
                // yet (a clause over a dated navigation is not built)
                args.addAll(withDates(t, List.of()));
                if (p.toMany()) {
                    type = GqlSchema.listOf(t.refName());
                    if (t.temporal() == null) {
                        args.addAll(collectionArgs(t.simple()));
                    }
                } else {
                    type = GqlSchema.named(t.refName(), p.optional());
                }
            }
            default -> throw new IllegalStateException("kind " + p.kind());
        }
        return new FieldDef(p.name(), p.description(), type, args, p.deprecated() ? "deprecated" : null,
                new Meaning.Property(c.fqn(), p.name(), p.derived(), p.kind() == Kind.OBJECT && p.toMany()));
    }

    /** A derived property's parameter as an argument: scalars and enums only (§5.1). */
    private GqlSchema.TypeRef inputRef(String at, Type type, Multiplicity m) {
        String name = switch (type) {
            case Type.Primitive prim -> scalarOf(prim, at);
            case Type.PrecisionDecimal d -> DECIMAL;
            case Type.EnumType e -> {
                enums.add(e.fqn());
                yield simple(e.fqn());
            }
            default -> throw new GqlSchemaException(at + " takes a parameter typed " + type.typeName()
                    + ", which has no GraphQL input type: a derived property's arguments are scalars or enumerations",
                    at);
        };
        boolean optional = !(m instanceof Multiplicity.Bounded b) || b.lower() == 0;
        return m.isMany() ? GqlSchema.nullableListOf(name) : GqlSchema.named(name, optional);
    }

    private String enumName(Prop p) {
        return simple(((Type.EnumType) p.type()).fqn());
    }

    /** The class in scope, by its full name; absent only through a generator bug. */
    private ClassInfo classOf(String fqn) {
        ClassInfo c = classes.get(fqn);
        if (c == null) {
            throw new IllegalStateException(fqn + " is not in the schema's scope");
        }
        return c;
    }

    private void put(TypeDef t) {
        TypeDef dup = out.put(t.name(), t);
        if (dup != null) {
            throw new IllegalStateException("generated twice: " + t.name());
        }
    }

    // ---- the input types (G2, G3, G9) ----

    private void inputTypes(ClassInfo c) {
        String s = c.simple();
        List<InputValueDef> where = new ArrayList<>();
        List<InputValueDef> orderBy = new ArrayList<>();
        for (Prop p : c.props()) {
            if (!p.parameters().isEmpty()) {
                continue;
            }
            switch (p.kind()) {
                case SCALAR -> {
                    if (!p.toMany()) {
                        where.add(new InputValueDef(p.name(), GqlSchema.named(scalarOf(p) + "Filter", true)));
                        orderBy.add(new InputValueDef(p.name(), GqlSchema.named(SORT_DIRECTION, true)));
                    }
                }
                case ENUM -> {
                    if (!p.toMany()) {
                        where.add(new InputValueDef(p.name(), GqlSchema.named(enumName(p) + "Filter", true)));
                        orderBy.add(new InputValueDef(p.name(), GqlSchema.named(SORT_DIRECTION, true)));
                    }
                }
                case OBJECT -> {
                    if (reachesTemporal(p)) {
                        continue;
                    }
                    String t = classOf(p.classFqn()).simple();
                    if (p.toMany()) {
                        where.add(new InputValueDef(p.name(), GqlSchema.named(t + "ListFilter", true)));
                        orderBy.add(new InputValueDef(p.name() + "Aggregate",
                                GqlSchema.named(t + "AggregateOrderBy", true)));
                    } else {
                        where.add(new InputValueDef(p.name(), GqlSchema.named(t + "Where", true)));
                        orderBy.add(new InputValueDef(p.name(), GqlSchema.named(t + "OrderBy", true)));
                    }
                }
                default -> throw new IllegalStateException("kind " + p.kind());
            }
        }
        for (String sub : c.subclasses()) {
            String t = simple(sub);
            where.add(new InputValueDef("as" + t, GqlSchema.named(t + "Where", true),
                    "The row as a " + t + ": its conditions, or `isNot: null` for membership (G9)."));
            orderBy.add(new InputValueDef("as" + t, GqlSchema.named(t + "OrderBy", true)));
        }
        where.add(new InputValueDef("is", GqlSchema.named(s + "Where", true)));
        where.add(new InputValueDef("isNot", GqlSchema.named(s + "Where", true)));
        where.add(new InputValueDef("AND", GqlSchema.nullableListOf(s + "Where")));
        where.add(new InputValueDef("OR", GqlSchema.nullableListOf(s + "Where")));
        where.add(new InputValueDef("NOT", GqlSchema.named(s + "Where", true)));
        put(new InputObjectType(s + "Where", where));
        put(new InputObjectType(s + "ListFilter", List.of(
                new InputValueDef("some", GqlSchema.named(s + "Where", true)),
                new InputValueDef("every", GqlSchema.named(s + "Where", true)),
                new InputValueDef("none", GqlSchema.named(s + "Where", true)))));
        put(new InputObjectType(s + "OrderBy", orderBy));
    }

    // ---- the aggregate shapes (G6) ----

    private List<Prop> numericProps(ClassInfo c) {
        return c.props().stream().filter(p -> p.kind() == Kind.SCALAR && !p.toMany() && p.parameters().isEmpty()
                && isNumeric(p)).toList();
    }

    /** The properties {@code min} and {@code max} fold: numbers, dates and strings (a string's by code point, the
     *  collection's own {@code min}/{@code max}: register S104). */
    private List<Prop> comparableProps(ClassInfo c) {
        return c.props().stream().filter(p -> p.kind() == Kind.SCALAR && !p.toMany() && p.parameters().isEmpty()
                && (isNumeric(p) || DATE.equals(scalarOf(p)) || DATE_TIME.equals(scalarOf(p))
                        || STRING.equals(scalarOf(p)))).toList();
    }

    private boolean isNumeric(Prop p) {
        String s = scalarOf(p);
        return BIG_INT.equals(s) || FLOAT.equals(s) || DECIMAL.equals(s);
    }

    private void aggregateTypes(ClassInfo c) {
        String s = c.simple();
        List<FieldDef> agg = new ArrayList<>();
        List<InputValueDef> aggOrder = new ArrayList<>();
        agg.add(new FieldDef("count", null, GqlSchema.named(BIG_INT, false), List.of(), null, new Meaning.Count()));
        aggOrder.add(new InputValueDef("count", GqlSchema.named(SORT_DIRECTION, true)));
        scalarsUsed.add(BIG_INT);
        for (GqlFold fn : GqlFold.ALL) {
            List<Prop> props = fn.numericOnly ? numericProps(c) : comparableProps(c);
            if (props.isEmpty()) {
                continue;
            }
            String t = s + fn.suffix;
            List<FieldDef> fields = new ArrayList<>();
            List<InputValueDef> order = new ArrayList<>();
            List<InputValueDef> filter = new ArrayList<>();
            for (Prop p : props) {
                String scalar = fn == GqlFold.AVG ? FLOAT : scalarOf(p);
                scalarsUsed.add(scalar);
                fields.add(new FieldDef(p.name(), null, GqlSchema.named(scalar, true), List.of(), null,
                        new Meaning.FoldOf(fn, c.fqn(), p.name())));
                order.add(new InputValueDef(p.name(), GqlSchema.named(SORT_DIRECTION, true)));
                filter.add(new InputValueDef(p.name(), GqlSchema.named(scalar + "Filter", true)));
            }
            put(new ObjectType(t, null, List.of(), fields, null));
            put(new InputObjectType(t + "OrderBy", order));
            put(new InputObjectType(t + "Filter", filter));
            agg.add(new FieldDef(fn.field, null, GqlSchema.named(t, true), List.of(), null,
                    new Meaning.Fold(fn)));
            aggOrder.add(new InputValueDef(fn.field, GqlSchema.named(t + "OrderBy", true)));
        }
        put(new ObjectType(s + "Aggregate", null, List.of(), agg, null));
        put(new InputObjectType(s + "AggregateOrderBy", aggOrder));
    }

    // ---- the group shapes (G7) ----

    private void groupTypes(ClassInfo c) {
        String s = c.simple();
        List<FieldDef> key = new ArrayList<>();
        for (Prop p : c.props()) {
            if (!p.parameters().isEmpty() || p.toMany()) {
                continue;
            }
            switch (p.kind()) {
                case SCALAR -> {
                    String scalar = scalarOf(p);
                    List<InputValueDef> args = DATE.equals(scalar) || DATE_TIME.equals(scalar)
                            ? List.of(new InputValueDef("truncate", GqlSchema.named(DATE_UNIT, true))) : List.of();
                    key.add(new FieldDef(p.name(), null, GqlSchema.named(scalar, true), args, null,
                            new Meaning.GroupKeyProperty(c.fqn(), p.name(), false)));
                }
                case ENUM -> key.add(new FieldDef(p.name(), null, GqlSchema.named(enumName(p), true), List.of(), null,
                        new Meaning.GroupKeyProperty(c.fqn(), p.name(), false)));
                case OBJECT -> {
                    if (!reachesTemporal(p)) {
                        key.add(new FieldDef(p.name(), null,
                                GqlSchema.named(classOf(p.classFqn()).simple() + "GroupKey", true), List.of(), null,
                                new Meaning.GroupKeyProperty(c.fqn(), p.name(), true)));
                    }
                }
                default -> throw new IllegalStateException("kind " + p.kind());
            }
        }
        put(new ObjectType(s + "GroupKey", null, List.of(), key, null));
        List<FieldDef> group = new ArrayList<>();
        List<InputValueDef> having = new ArrayList<>();
        List<InputValueDef> order = new ArrayList<>();
        group.add(new FieldDef("key", null, GqlSchema.named(s + "GroupKey", false), List.of(), null,
                new Meaning.GroupKey()));
        group.add(new FieldDef("count", null, GqlSchema.named(BIG_INT, false), List.of(), null, new Meaning.Count()));
        having.add(new InputValueDef("count", GqlSchema.named(BIG_INT + "Filter", true)));
        order.add(new InputValueDef("count", GqlSchema.named(SORT_DIRECTION, true)));
        order.add(new InputValueDef("key", GqlSchema.named(s + "OrderBy", true)));
        for (GqlFold fn : GqlFold.ALL) {
            String t = s + fn.suffix;
            if (!out.containsKey(t)) {
                continue;
            }
            group.add(new FieldDef(fn.field, null, GqlSchema.named(t, true), List.of(), null,
                    new Meaning.Fold(fn)));
            having.add(new InputValueDef(fn.field, GqlSchema.named(t + "Filter", true)));
            order.add(new InputValueDef(fn.field, GqlSchema.named(t + "OrderBy", true)));
        }
        having.add(new InputValueDef("AND", GqlSchema.nullableListOf(s + "GroupHaving")));
        having.add(new InputValueDef("OR", GqlSchema.nullableListOf(s + "GroupHaving")));
        having.add(new InputValueDef("NOT", GqlSchema.named(s + "GroupHaving", true)));
        put(new ObjectType(s + "Group", null, List.of(), group, null));
        put(new InputObjectType(s + "GroupHaving", having));
        put(new InputObjectType(s + "GroupOrderBy", order));
    }

    // ---- enums, scalars, filters (G2, G5) ----

    private void enumTypes() {
        put(new EnumType(SORT_DIRECTION, null, List.of("ASC", "DESC"), null));
        put(new EnumType(QUERY_MODE, "How a string comparison matches: Prisma's spelling.",
                List.of("default", "insensitive"), null));
        put(new EnumType(DATE_UNIT, "The unit a date key is truncated to under `key` (G7).",
                List.of("YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR"), null));
        for (String fqn : enums) {
            TypedEnum e = ctx.findEnum(fqn).orElseThrow(() -> new GqlSchemaException(fqn + " is not an enumeration",
                    fqn));
            String name = simple(fqn);
            if (classes.containsKey(fqn) || out.containsKey(name) || out.containsKey(name + "Filter")) {
                throw new GqlSchemaException(fqn + " is named like a class or a generated type of the data space",
                        fqn);
            }
            for (String v : e.values()) {
                if (!GRAPHQL_NAME.matcher(v).matches() || RESERVED_ENUM_VALUES.contains(v)) {
                    throw new GqlSchemaException(fqn + "." + v + " is not a GraphQL enum value", fqn);
                }
            }
            put(new EnumType(name, null, e.values(), fqn));
            put(new InputObjectType(name + "Filter", List.of(
                    new InputValueDef("equals", GqlSchema.named(name, true)),
                    new InputValueDef("not", GqlSchema.named(name + "Filter", true)),
                    new InputValueDef("in", GqlSchema.nullableListOf(name)),
                    new InputValueDef("notIn", GqlSchema.nullableListOf(name)))));
        }
    }

    private void scalarTypes() {
        Map<String, String> descriptions = new LinkedHashMap<>();
        descriptions.put(BIG_INT, "Pure's Integer: a signed 64-bit integer, written as an integer literal, carried as a"
                + " JSON number (G5).");
        descriptions.put(DECIMAL, "Pure's Decimal or Number: an exact decimal, written as a numeric literal.");
        descriptions.put(DATE, "Pure's StrictDate: an ISO-8601 date, \"2024-01-01\".");
        descriptions.put(DATE_TIME, "Pure's DateTime or Date: an ISO-8601 date-time, \"2024-01-01T10:30:00\".");
        descriptions.put(TIME, "Pure's StrictTime: an ISO-8601 time, \"10:30:00\".");
        for (Map.Entry<String, String> e : descriptions.entrySet()) {
            if (scalarsUsed.contains(e.getKey())) {
                put(new ScalarType(e.getKey(), e.getValue()));
            }
        }
    }

    /** One comparison input per scalar in use (G2): Prisma's names. */
    private void filterTypes() {
        for (String scalar : new TreeSet<>(scalarsUsed)) {
            List<InputValueDef> f = new ArrayList<>();
            f.add(new InputValueDef("equals", GqlSchema.named(scalar, true)));
            f.add(new InputValueDef("not", GqlSchema.named(scalar + "Filter", true)));
            if (!BOOLEAN.equals(scalar)) {
                f.add(new InputValueDef("in", GqlSchema.nullableListOf(scalar)));
                f.add(new InputValueDef("notIn", GqlSchema.nullableListOf(scalar)));
                f.add(new InputValueDef("lt", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("lte", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("gt", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("gte", GqlSchema.named(scalar, true)));
            }
            if (STRING.equals(scalar)) {
                f.add(new InputValueDef("contains", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("startsWith", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("endsWith", GqlSchema.named(scalar, true)));
                f.add(new InputValueDef("mode", GqlSchema.named(QUERY_MODE, true)));
            }
            put(new InputObjectType(scalar + "Filter", f));
        }
    }
}
