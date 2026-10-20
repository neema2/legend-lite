// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.arrOf;
import static com.legend.server.ModelChanges.camelCase;
import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.identifier;
import static com.legend.server.ModelChanges.objOf;
import static com.legend.server.ModelChanges.quoted;
import static com.legend.server.ModelChanges.quotedIfNeeded;
import static com.legend.server.ModelChanges.refused;
import static com.legend.server.ModelChanges.upperFirst;
import static com.legend.server.ModelChanges.with;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.error.LegendCompileException;
import com.legend.json.Json;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * MAP A CLASS WITH SQL (docs/MODEL_IMPORT_STUDY_2026_10_20.md §5; an action of {@link ModelChanges}): a class of the
 * project -- imported ({@link ImportSchema}), written in Studio, made of a grid -- given its rows by a Postgres SELECT.
 * Nothing new in Pure: the SELECT is the body of a function, a {@code #SQL{...}#} island (docs/LEGEND_SQL_STUDY_2026_10_11.md
 * §9), and the class is mapped to the function as upstream's promotion maps one, {@code Relation { ~func ... }}, in
 * Create a Class's layout ({@code <pkg>::function::<Name>Fn}, {@code <pkg>::mapping::<Name>Mapping}, included in
 * {@code <pkg>::mapping::Model}). The SQL names the class's tables as the island does ({@code tb('store::DB.TRADES')}),
 * of a store a runtime of the project connects.
 *
 * <p><b>The SELECT must fit the class</b>, checked here, every misfit named at once before anything is written:
 * <ul>
 *   <li>each property the class has (its own and inherited) reads one column: of its name, else of its name in another
 *       case (Postgres folds {@code AS customerId} to {@code customerid}), else the one whose name upstream's camel case
 *       makes it ({@code trade_id} is {@code tradeId}); each unique, or the property is refused by name;</li>
 *   <li>of its type: the column's type is the property's or one of its kinds (an {@code Integer} column for a
 *       {@code Number} property), as the compiler takes it; an enumeration's property reads a text column through an
 *       enumeration mapping, each value from its own name unless the request says which text is which;</li>
 *   <li>a property that holds many values, or a class, is not one column: it is left unmapped by name
 *       ({@code "unmapped"}), or refused; a class is reached through an association, joined
 *       ({@link JoinAssociation}).</li>
 * </ul>
 * Every column no property reads is a KEY of the mapping, a local property ({@code +customerId: Integer[0..1]:
 * customer_id}) an association joins on, as Create a Class keeps one. A {@code [1]} property read from a column the
 * compiler cannot show is never empty is noted, not refused: the SQL says what is there, the rows say whether it is.
 */
final class MapClass {

    private MapClass() {
    }

    /** The class mapped: its path, the query that reads it back, the runtime, each property's column, the keys. */
    record Made(String className, String mapping, String runtime, String query, List<Map<String, Object>> properties,
            List<Map<String, Object>> keys) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("class", className);
            out.put("mapping", mapping);
            out.put("runtime", runtime);
            out.put("query", query);
            out.put("properties", properties);
            out.put("keys", keys);
            return out;
        }
    }

    /** A property of the class as it reads its column: by name, or through an enumeration mapping. */
    private record Read(String property, String column, @com.legend.base.Nullable String enumeration) {
    }

    /**
     * One class, of {@code pkg}: {@code spec} is {@code {"class", "sql", "runtime"?, "unmapped"?: [property, ...],
     * "enumerations"?: {enumeration: {value: [text, ...]}}}} -- {@code runtime} the runtime the SQL runs on (the one
     * of the project naming the model's mapping when absent; named, it is made to name it).
     */
    static Made apply(ModelChanges m, String pkg, Json.Obj spec) {
        String className = spec.getString("class");
        Json.Obj cls = m.get(className);
        if (cls == null || !"class".equals(cls.getStringOr("_type", ""))) {
            throw refused("the project has no class " + className);
        }
        String name = cls.getString("name");
        String sql = spec.getString("sql").strip();
        if (sql.contains("}#")) {
            throw refused("the SQL holds '}#', which ends the island it is written in");
        }
        String function = pkg + "::function::" + name + "Fn";
        String classMapping = pkg + "::mapping::" + name + "Mapping";
        String modelMapping = pkg + "::mapping::Model";

        // THE SELECT'S COLUMNS, as the compiler types the island over the project
        ModelContext ctx = com.legend.Compiler.compileModel(m.afterText());
        Type type;
        try {
            type = com.legend.Compiler.query(ctx, "|#SQL{" + sql + "}#").resultType().type();
        } catch (LegendCompileException e) {
            throw refused("the SQL does not compile: " + e.getMessage());
        }
        Type.RelationType relation = Type.schemaView(type);
        if (relation == null) {
            throw refused("the SQL returns " + type.typeName() + ", not rows");
        }
        if (!relation.dynamicColumns().isEmpty()) {
            throw refused("a class has fixed properties, and this SELECT's columns are known only when it runs");
        }
        List<Type.Column> columns = relation.columns();

        Set<String> unmapped = new HashSet<>(spec.getStringArrayOr("unmapped", List.of()));
        List<Property.Stored> properties = properties(ctx, className);
        Set<String> names = new HashSet<>();
        for (Property.Stored p : properties) {
            names.add(p.name());
        }
        for (String u : unmapped) {
            if (!names.contains(u)) {
                throw refused(className + " has no property '" + u + "' to leave unmapped");
            }
        }
        Json.Obj enumerations = objOf(spec, "enumerations");

        // EACH PROPERTY'S COLUMN, every misfit named
        List<String> misfits = new ArrayList<>();
        List<Read> reads = new ArrayList<>();
        Set<String> used = new HashSet<>();
        List<String> enumerationMappings = new ArrayList<>();
        for (Property.Stored p : properties) {
            if (unmapped.contains(p.name())) {
                continue;
            }
            String at = className + "." + p.name();
            if (p.multiplicity().isMany()) {
                misfits.add(at + " holds many values, which one column does not: leave it unmapped, or import lists as "
                        + "Variants ('lists': 'variant') to read the list from a JSON column");
                continue;
            }
            if (p.type() instanceof Type.ClassType target && !PlatformTypes.isVariant(p.type())) {
                misfits.add(at + " is a " + target.fqn() + ", reached through an association: leave it unmapped");
                continue;
            }
            Type.Column column = columnOf(p.name(), columns, misfits, at);
            if (column == null) {
                continue;
            }
            used.add(column.name());
            if (p.type() instanceof Type.EnumType e) {
                if (!(column.type() == Type.Primitive.STRING)) {
                    misfits.add(at + " is the enumeration " + e.fqn() + ", read from text; its column '" + column.name()
                            + "' is " + column.type().typeName());
                    continue;
                }
                String id = name + upperFirst(p.name());
                enumerationMappings.add(enumerationMapping(ctx, e.fqn(), id, objOf(enumerations, e.fqn()), misfits));
                reads.add(new Read(p.name(), column.name(), id));
                continue;
            }
            if (!fits(p.type(), column.type())) {
                misfits.add(at + " is " + typeName(p.type()) + ", its column '" + column.name() + "' is "
                        + column.type().typeName() + ": cast it in the SQL");
                continue;
            }
            if (!required(column.multiplicity()) && required(p.multiplicity())) {
                m.notes.add(at + " is [1], and its column '" + column.name() + "' may be empty: a row without one "
                        + "reads as no value");
            }
            reads.add(new Read(p.name(), column.name(), null));
        }
        for (String e : enumerations.fields().keySet()) {
            Type named = ctx.findType(e).orElse(null);
            if (!(named instanceof Type.EnumType)) {
                misfits.add("the project has no enumeration " + e);
            } else if (properties.stream().noneMatch(p -> p.type().equals(named))) {
                misfits.add("no property of " + className + " is of the enumeration " + e);
            }
        }
        if (!misfits.isEmpty()) {
            throw refused("the SQL does not fit " + className + ":\n- " + String.join("\n- ", misfits));
        }

        // EVERY OTHER COLUMN, a key: named by upstream's rule, apart from the class's properties and ends
        List<String> lines = new ArrayList<>();
        for (Read r : reads) {
            lines.add("    " + r.property() + ": " + (r.enumeration() == null ? "" : "EnumerationMapping "
                    + r.enumeration() + ": ") + quoted(r.column()));
        }
        List<Map<String, Object>> keys = new ArrayList<>();
        Set<String> taken = new HashSet<>(names);
        for (Type.Column c : columns) {
            if (used.contains(c.name())) {
                continue;
            }
            String keyType = holdable(c.type());
            if (keyType == null) {
                m.notes.add("the column '" + c.name() + "' is " + c.type().typeName() + ", which a key cannot hold: "
                        + "it is not kept");
                continue;
            }
            String base = identifier(camelCase(c.name().replaceAll("[ /@~<>.]", "_").replace("\"", ""), false));
            String key = base;
            for (int n = 2; !taken.add(key) || ctx.findProperty(className, key).isPresent(); n++) {
                key = base + n;
            }
            lines.add("    +" + key + ": " + keyType + "[0..1]: " + quoted(c.name()));
            Map<String, Object> k = new LinkedHashMap<>();
            k.put("key", key);
            k.put("column", c.name());
            k.put("type", keyType);
            keys.add(k);
        }

        List<String> cols = new ArrayList<>();
        for (Type.Column c : columns) {
            String t = holdable(c.type());
            cols.add(quotedIfNeeded(c.name()) + ":" + (t == null ? c.type().typeName() : t));
        }
        m.put(element("###Pure\nfunction " + function + "(): meta::pure::metamodel::relation::Relation<("
                + String.join(", ", cols) + ")>[1]\n{\n  #SQL{" + sql + "}#\n}\n"));
        m.put(element("###Mapping\nMapping " + classMapping + "\n(\n" + String.join("", enumerationMappings) + "  *"
                + className + ": Relation\n  {\n    ~func " + function + "():Relation<Any>[1]\n" + String.join(",\n", lines)
                + "\n  }\n)\n"));
        m.put(modelMapping(m.get(modelMapping), modelMapping, classMapping));
        String runtime = runtime(m, spec.getStringOr("runtime", null), modelMapping);

        List<Map<String, Object>> mapped = new ArrayList<>();
        List<String> project = new ArrayList<>();
        for (Read r : reads) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("property", r.property());
            p.put("column", r.column());
            mapped.add(p);
            project.add(quotedIfNeeded(r.property()) + ": x|$x." + r.property());
        }
        if (project.isEmpty()) {
            throw refused(className + " has no property this SQL maps");
        }
        return new Made(className, classMapping, runtime, className + ".all()->project(~[" + String.join(", ", project)
                + "])", mapped, keys);
    }

    /** The class's stored properties, its superclasses' first, as the compiled model holds them. */
    private static List<Property.Stored> properties(ModelContext ctx, String className) {
        List<Property.Stored> out = new ArrayList<>();
        collect(ctx, className, out, new HashSet<>());
        return out;
    }

    private static void collect(ModelContext ctx, String className, List<Property.Stored> out, Set<String> seen) {
        if (!seen.add(className)) {
            return;
        }
        TypedClass c = ctx.findClass(className).orElseThrow(() -> refused("the project has no class " + className));
        for (String s : c.superClassFqns()) {
            collect(ctx, s, out, seen);
        }
        for (Property p : c.properties()) {
            if (p instanceof Property.Stored s && out.stream().noneMatch(o -> o.name().equals(s.name()))) {
                out.add(s);
            }
        }
    }

    /**
     * The column a property reads: of its name; else of its name in another case; else the one whose name upstream's
     * camel case makes it -- each the only one, or the property is refused (a misfit), named.
     */
    private static @com.legend.base.Nullable Type.Column columnOf(String property, List<Type.Column> columns,
            List<String> misfits, String at) {
        for (Type.Column c : columns) {
            if (c.name().equals(property)) {
                return c;
            }
        }
        List<Type.Column> folded = new ArrayList<>();
        List<Type.Column> camel = new ArrayList<>();
        for (Type.Column c : columns) {
            if (c.name().toLowerCase(Locale.ROOT).equals(property.toLowerCase(Locale.ROOT))) {
                folded.add(c);
            }
            if (camelCase(c.name(), false).equals(property)) {
                camel.add(c);
            }
        }
        List<Type.Column> found = !folded.isEmpty() ? folded : camel;
        if (found.size() == 1) {
            return found.get(0);
        }
        List<String> names = new ArrayList<>();
        for (Type.Column c : columns) {
            names.add(c.name());
        }
        misfits.add(found.isEmpty() ? at + " has no column (the SELECT's: " + String.join(", ", names) + ")"
                : at + " could be read from several columns (" + String.join(", ", found.stream().map(Type.Column::name)
                        .toList()) + "): name one apart");
        return null;
    }

    /** Whether a property of type {@code property} takes a column of type {@code column}: its own, or a kind of it. */
    private static boolean fits(Type property, Type column) {
        if (property.equals(column)) {
            return true;
        }
        if (PlatformTypes.isVariant(property)) {
            return PlatformTypes.isVariant(column);
        }
        if (property == Type.Primitive.NUMBER) {
            return column instanceof Type.PrecisionDecimal
                    || (column instanceof Type.Primitive p && p.isNumeric());
        }
        if (property == Type.Primitive.DECIMAL) {
            return column instanceof Type.PrecisionDecimal;
        }
        if (property == Type.Primitive.DATE) {
            return column == Type.Primitive.STRICT_DATE || column == Type.Primitive.DATE_TIME;
        }
        return false;
    }

    /** A type as a person reads it: a primitive's name, a class's or enumeration's path. */
    private static String typeName(Type t) {
        return t instanceof Type.ClassType c ? c.fqn() : t instanceof Type.EnumType e ? e.fqn() : t.typeName();
    }

    /** A column's type as a property holds it, or null when none can. */
    private static @com.legend.base.Nullable String holdable(Type t) {
        if (t instanceof Type.Primitive p) {
            return p.typeName();
        }
        if (t instanceof Type.PrecisionDecimal) {
            return "Decimal";
        }
        if (t instanceof Type.EnumType e) {
            return e.fqn();
        }
        return PlatformTypes.isVariant(t) ? PlatformTypes.VARIANT : null;
    }

    private static boolean required(Multiplicity m) {
        return m instanceof Multiplicity.Bounded b && b.lower() >= 1;
    }

    /**
     * The enumeration mapping {@code id} of {@code enumeration}: each value read from its own name, or from the texts
     * {@code given} says ({@code {"BUY": ["B"]}}); a value {@code given} names that the enumeration does not have is a
     * misfit.
     */
    private static String enumerationMapping(ModelContext ctx, String enumeration, String id, Json.Obj given,
            List<String> misfits) {
        List<String> values = ctx.findEnum(enumeration).orElseThrow().values();
        for (String v : given.fields().keySet()) {
            if (!values.contains(v)) {
                misfits.add(enumeration + " has no value '" + v + "'");
            }
        }
        List<String> lines = new ArrayList<>();
        for (String v : values) {
            List<String> texts = given.has(v) ? given.getStringArray(v) : List.of(v);
            List<String> literals = new ArrayList<>();
            for (String t : texts) {
                literals.add(ModelChanges.literal(t));
            }
            lines.add("    " + v + ": [" + String.join(", ", literals) + "]");
        }
        return "  " + enumeration + ": EnumerationMapping " + id + "\n  {\n" + String.join(",\n", lines) + "\n  }\n";
    }

    /** The model's mapping, made or as the project holds it, including the class's mapping. */
    private static Json.Obj modelMapping(@com.legend.base.Nullable Json.Obj existing, String modelMapping,
            String classMapping) {
        Json.Obj mm = existing != null ? existing : element("###Mapping\nMapping " + modelMapping + "\n(\n)\n");
        List<Json.Node> includes = new ArrayList<>(arrOf(mm, "includedMappings").items());
        Json.Node include = Json.of(Map.of("_type", "mappingIncludeMapping", "includedMapping", classMapping));
        if (!includes.contains(include)) {
            includes.add(include);
        }
        return with(mm, "includedMappings", new Json.Arr(includes));
    }

    /**
     * The runtime the SQL runs on: the one named, made to name the model's mapping when it does not; or, named none,
     * the one runtime of the project naming the model's mapping.
     */
    private static String runtime(ModelChanges m, @com.legend.base.Nullable String named, String modelMapping) {
        if (named == null) {
            List<String> found = new ArrayList<>();
            for (Json.Obj e : m.all()) {
                if ("runtime".equals(e.getStringOr("_type", "")) && namesMapping(e, modelMapping)) {
                    found.add(ModelChanges.pathOf(e));
                }
            }
            if (found.size() != 1) {
                throw refused((found.isEmpty() ? "no runtime of the project names " : "several runtimes name ")
                        + modelMapping + ": name the runtime the SQL runs on ('runtime')");
            }
            return found.get(0);
        }
        Json.Obj rt = m.get(named);
        if (rt == null || !"runtime".equals(rt.getStringOr("_type", ""))) {
            throw refused("the project has no runtime " + named);
        }
        if (!namesMapping(rt, modelMapping)) {
            Json.Obj value = rt.getObj("runtimeValue");
            List<Json.Node> mappings = new ArrayList<>(arrOf(value, "mappings").items());
            mappings.add(Json.of(Map.of("path", modelMapping, "type", "MAPPING")));
            m.put(with(rt, "runtimeValue", with(value, "mappings", new Json.Arr(mappings))));
        }
        return named;
    }

    private static boolean namesMapping(Json.Obj runtime, String mapping) {
        for (Json.Node n : arrOf(objOf(runtime, "runtimeValue"), "mappings").items()) {
            if (mapping.equals(((Json.Obj) n).getStringOr("path", ""))) {
                return true;
            }
        }
        return false;
    }
}
