// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.literal;
import static com.legend.server.ModelChanges.lowerFirst;
import static com.legend.server.ModelChanges.refused;
import static com.legend.server.ModelChanges.upperFirst;

import com.legend.json.Json;
import com.legend.protocol.schema.ImportedModel;
import com.legend.protocol.schema.ImportedModel.All;
import com.legend.protocol.schema.ImportedModel.Any;
import com.legend.protocol.schema.ImportedModel.Bound;
import com.legend.protocol.schema.ImportedModel.Condition;
import com.legend.protocol.schema.ImportedModel.Has;
import com.legend.protocol.schema.ImportedModel.Holds;
import com.legend.protocol.schema.ImportedModel.ImportedClass;
import com.legend.protocol.schema.ImportedModel.ImportedEnum;
import com.legend.protocol.schema.ImportedModel.ImportedProperty;
import com.legend.protocol.schema.ImportedModel.Kind;
import com.legend.protocol.schema.ImportedModel.Length;
import com.legend.protocol.schema.ImportedModel.Logic;
import com.legend.protocol.schema.ImportedModel.MultipleOf;
import com.legend.protocol.schema.ImportedModel.Not;
import com.legend.protocol.schema.ImportedModel.OneOf;
import com.legend.protocol.schema.ImportedModel.Pattern;
import com.legend.protocol.schema.ImportedModel.Requires;
import com.legend.protocol.schema.ImportedModel.Rule;
import com.legend.protocol.schema.ImportedModel.Unique;
import com.legend.protocol.schema.JsonSchemaReader;
import com.legend.protocol.schema.SchemaImportException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IMPORT A SCHEMA (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4; an action of {@link ModelChanges}): the classes and
 * enumerations a schema language defines, written into the project as Pure, in {@code <pkg>::domain} where Create a
 * Class writes its classes -- so the classes are mapped next ({@link MapClass}) and their associations joined
 * ({@link JoinAssociation}) as any class of the model is. Written ONCE: the Pure is the model from then on, owned and
 * edited as Pure; importing again writes the same elements over the ones it wrote.
 *
 * <p>A property typed by another class becomes an ASSOCIATION, {@code <Owner>_<Property>} (numbered when the name is
 * another element's): the property, as the schema names it, an end of it, and the way back the other end -- named
 * after the owner, plural ({@code trades: Trade[*]}), or, for a class made of an inline object (a value its owner
 * holds), singular and to one ({@code order: Order[0..1]}); {@code <plural>As<Property>} when the plural is taken
 * ({@code tradesAsBuyer}, {@code tradesAsSeller}). An association, not a property of the class, because a
 * Relation-mapped class reaches another through a ModelJoin, and a ModelJoin joins an association.
 *
 * <p>Each RULE a schema's validations make is a class CONSTRAINT ({@code quantityMinimum: $this.quantity >= 0}), an
 * optional property's guarded ({@code $this.p->isEmpty() || ...}), a list's each value's ({@code ->forAll(...)}); a
 * default is the property's ({@code = 'x'}); {@code deprecated} is {@code <<doc.deprecated>>}; a description is
 * {@code doc.doc}, an enumeration value's too. The elements written are compiled, constraints included, before
 * anything is: a constraint that would not compile is the importer's defect, refused by name.
 */
final class ImportSchema {

    private ImportSchema() {
    }

    private static final String DOC = "meta::pure::profiles::doc.doc";

    private static final String DEPRECATED = "<<meta::pure::profiles::doc.deprecated>> ";

    /** An import made: the elements written, by kind. */
    record Made(List<String> classes, List<String> enumerations, List<String> associations) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("classes", classes);
            out.put("enumerations", enumerations);
            out.put("associations", associations);
            return out;
        }
    }

    /**
     * One schema, into {@code pkg}: {@code spec} is {@code {"format": "jsonSchema"|"openApi", "content", "name"?,
     * "schemas"?, "lists"?, "operations"?, "documents"?}} -- {@code content} the document's text, JSON or YAML,
     * {@code name} its root object's class name when it has no title, {@code schemas} the schemas to import (each with
     * the schemas it reaches), every one when absent, {@code lists} {@code "many"} or {@code "variant"}
     * ({@link #listsAsVariant}), {@code operations} whether an OpenAPI document's operations' bodies are read (by
     * default, only when it names no component schema), {@code documents} the other documents its references name,
     * each text by the name it is referenced by.
     */
    static Made apply(ModelChanges m, String pkg, Json.Obj spec) {
        String format = spec.getString("format");
        ImportedModel model;
        try {
            // each schema language its reader; an OpenAPI document's schemas are JSON Schema's
            model = switch (format) {
                case "jsonSchema", "openApi" -> JsonSchemaReader.read(spec.getString("content"),
                        JsonSchemaReader.Options.of(spec.getStringOr("name", null),
                                spec.getStringArrayOr("schemas", List.of()), listsAsVariant(spec),
                                spec.has("operations") ? Boolean.valueOf(spec.getBool("operations")) : null,
                                documents(spec)));
                default -> throw refused("a schema is read as jsonSchema or openApi, not '" + format + "'");
            };
        } catch (SchemaImportException e) {
            throw refused("the schema cannot be imported:\n- " + String.join("\n- ", e.reasons()));
        }
        m.notes.addAll(model.notes());
        String domain = pkg + "::domain";
        List<String> written = new ArrayList<>();

        List<String> enumerations = new ArrayList<>();
        Map<String, ImportedEnum> enums = new HashMap<>();
        for (ImportedEnum e : model.enumerations()) {
            String path = domain + "::" + e.name();
            List<String> values = new ArrayList<>();
            for (int i = 0; i < e.values().size(); i++) {
                values.add(docOf(e.valueDocs().get(i)) + e.values().get(i));
            }
            written.add(put(m, "Enum " + docOf(e.doc()) + path + "\n{\n  " + String.join(",\n  ", values) + "\n}\n"));
            enumerations.add(path);
            enums.put(e.name(), e);
        }

        Map<String, ImportedClass> byName = new HashMap<>();
        for (ImportedClass c : model.classes()) {
            byName.put(c.name(), c);
        }
        Writing w = new Writing(domain, enums, byName, m.notes);
        List<String> classes = new ArrayList<>();
        List<Reference> references = new ArrayList<>();
        Set<String> owned = new HashSet<>();
        for (ImportedClass c : model.classes()) {
            if (c.owned()) {
                owned.add(c.name());
            }
        }
        for (ImportedClass c : model.classes()) {
            String path = domain + "::" + c.name();
            StringBuilder text = new StringBuilder("Class ").append(c.deprecated() ? DEPRECATED : "")
                    .append(docOf(c.doc())).append(path);
            if (!c.supers().isEmpty()) {
                List<String> supers = new ArrayList<>();
                for (String s : c.supers()) {
                    supers.add(domain + "::" + s);
                }
                text.append(" extends ").append(String.join(", ", supers));
            }
            Map<String, ImportedProperty> own = new LinkedHashMap<>();
            for (ImportedProperty p : c.properties()) {
                own.put(p.name(), p);
            }
            List<String> constraints = constraints(c, own, w);
            if (!constraints.isEmpty()) {
                text.append("\n[\n  ").append(String.join(",\n  ", constraints)).append("\n]");
            }
            text.append("\n{\n");
            for (ImportedProperty p : c.properties()) {
                if (p.kind() == Kind.CLASS) {
                    references.add(new Reference(c.name(), p, owned.contains(p.type())));
                    continue;
                }
                String type = p.kind() == Kind.ENUMERATION ? domain + "::" + p.type() : p.type();
                text.append("  ").append(p.deprecated() ? DEPRECATED : "").append(docOf(p.doc())).append(p.name())
                        .append(": ").append(type).append(p.multiplicity()).append(defaultOf(p, domain)).append(";\n");
            }
            written.add(put(m, text.append("}\n").toString()));
            classes.add(path);
        }

        List<String> associations = new ArrayList<>();
        Map<String, Set<String>> ways = new HashMap<>();
        for (Reference r : references) {
            String text = associate(m, domain, r, references, ways);
            written.add(text);
            associations.add(ModelChanges.pathOf(element("###Pure\n" + text)));
        }

        // THE ELEMENTS WRITTEN, compiled on their own, bodies (the constraints) included: they name nothing else
        List<String> errors = com.legend.Compiler.compileErrors("###Pure\n" + String.join("\n", written));
        if (!errors.isEmpty()) {
            throw refused("the import would not compile, the importer's defect: " + String.join("; ", errors));
        }
        return new Made(classes, enumerations, associations);
    }

    /** One element written into the project, as Pure text; its text, as written. */
    private static String put(ModelChanges m, String text) {
        m.put(element("###Pure\n" + text));
        return text;
    }

    /** {@code "lists"}: {@code "many"} (the default), each list a property of many values; or {@code "variant"}, a list
     *  of primitives or enumeration values one {@code Variant}, the JSON list a column holds. */
    private static boolean listsAsVariant(Json.Obj spec) {
        String lists = spec.has("lists") ? spec.getString("lists") : "many";
        return switch (lists) {
            case "many" -> false;
            case "variant" -> true;
            default -> throw refused("a list is read as 'many' or 'variant', not '" + lists + "'");
        };
    }

    /** {@code "documents"}: each other document's text, by the name a reference names it by. */
    private static Map<String, String> documents(Json.Obj spec) {
        Map<String, String> out = new LinkedHashMap<>();
        Json.Obj docs = spec.getObjOr("documents", null);
        if (docs != null) {
            for (String name : docs.fields().keySet()) {
                out.put(name, docs.getString(name));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // Constraints and defaults
    // ---------------------------------------------------------------------

    /** Each of a class's rules as a constraint, {@code <property><Rule>: <expression>}; one that cannot be said is
     *  noted. */
    private static List<String> constraints(ImportedClass c, Map<String, ImportedProperty> own, Writing w) {
        List<String> out = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (Rule r : c.rules()) {
            ImportedProperty p = own.get(r.property());
            String value = "$this." + r.property();
            // a list's own rules, and one property's requiring others, say no value's condition
            String whole = switch (r) {
                case Unique u -> value + "->removeDuplicates()->size() == " + value + "->size()";
                case Requires q -> {
                    List<String> present = new ArrayList<>();
                    for (String other : q.others()) {
                        present.add("$this." + other + "->isNotEmpty()");
                    }
                    yield value + "->isEmpty() || (" + String.join(" && ", present) + ")";
                }
                default -> null;
            };
            if (p == null || (whole == null && p.kind() != Kind.PRIMITIVE)
                    || (r instanceof Unique && p.kind() == Kind.CLASS)) {
                continue;
            }
            String condition = whole != null ? "" : condition(r, p, w, c.name());
            if (condition == null) {
                continue;
            }
            String base = r.property() + switch (r) {
                case Bound b -> (b.exclusive() ? "Exclusive" : "") + (b.lower() ? "Minimum" : "Maximum");
                case Length l -> l.lower() ? "MinLength" : "MaxLength";
                case Pattern x -> "Pattern";
                case MultipleOf x -> "MultipleOf";
                case OneOf x -> "OneOf";
                case Unique x -> "Unique";
                case Requires x -> "Requires";
            };
            String name = base;
            for (int n = 2; !names.add(name); n++) {
                name = base + n;
            }
            String expression;
            if (whole != null) {
                expression = whole;
            } else if (r.each()) {
                expression = value + "->forAll(v|" + condition.replace("%v", "$v") + ")";
            } else if (p.lower() >= 1) {
                expression = condition.replace("%v", value);
            } else {
                expression = value + "->isEmpty() || " + condition.replace("%v", value + "->toOne()");
            }
            out.add(name + ": " + expression);
        }
        for (Logic l : c.logic()) {
            String expression = expression(l.condition(), c, w);
            if (expression == null) {
                w.notes().add(c.name() + ": its condition " + l.name() + " says what Pure's constraint cannot, and is "
                        + "not carried");
                continue;
            }
            String name = l.name();
            for (int n = 2; !names.add(name); n++) {
                name = l.name() + n;
            }
            out.add(name + ": " + expression);
        }
        return out;
    }

    /** What the writing of a model's constraints reads: its package, its enumerations and classes by name, its
     *  notes. */
    private record Writing(String domain, Map<String, ImportedEnum> enums, Map<String, ImportedClass> classes,
            List<String> notes) {
    }

    /** A class's property, its own or a superclass's, or one at the end of a path ({@code address.city}) of
     *  object-typed properties; null when it has none of the name. */
    private static @com.legend.base.Nullable ImportedProperty property(ImportedClass c, String name, Writing w,
            Set<String> seen) {
        int dot = name.indexOf('.');
        if (dot > 0) {
            ImportedProperty first = property(c, name.substring(0, dot), w, seen);
            ImportedClass next = first == null || first.kind() != Kind.CLASS ? null : w.classes().get(first.type());
            return next == null ? null : property(next, name.substring(dot + 1), w, new HashSet<>());
        }
        for (ImportedProperty p : c.properties()) {
            if (p.name().equals(name)) {
                return p;
            }
        }
        for (String s : c.supers()) {
            ImportedClass sup = w.classes().get(s);
            if (sup != null && seen.add(s)) {
                ImportedProperty p = property(sup, name, w, seen);
                if (p != null) {
                    return p;
                }
            }
        }
        return null;
    }

    /** A condition as a Pure expression on {@code $this}; null (noted) when a part of it cannot be said. A value is
     *  read ({@code toOne}) only where a test of its presence before it has held. */
    private static @com.legend.base.Nullable String expression(Condition condition, ImportedClass c, Writing w) {
        return switch (condition) {
            case Has h -> property(c, h.property(), w, new HashSet<>()) == null ? null
                    : "$this." + h.property() + "->isNotEmpty()";
            case Not n when n.condition() instanceof Has h -> property(c, h.property(), w, new HashSet<>()) == null
                    ? null : "$this." + h.property() + "->isEmpty()";
            case Not n -> {
                String inner = expression(n.condition(), c, w);
                yield inner == null ? null : "!" + grouped(n.condition(), inner);
            }
            case All a -> joined(a.conditions(), " && ", "true", c, w);
            case Any a -> joined(a.conditions(), " || ", "false", c, w);
            case Holds h -> {
                Rule r = h.rule();
                ImportedProperty p = property(c, r.property(), w, new HashSet<>());
                if (p == null) {
                    yield null;
                }
                String value = "$this." + r.property();
                if (r instanceof Unique) {
                    yield "(" + value + "->removeDuplicates()->size() == " + value + "->size())";
                }
                String cond = condition(r, p, w, c.name());
                yield cond == null ? null : r.each() ? value + "->forAll(v|" + cond.replace("%v", "$v") + ")"
                        : cond.replace("%v", value + "->toOne()");
            }
        };
    }

    private static @com.legend.base.Nullable String joined(List<Condition> parts, String operator, String none,
            ImportedClass c, Writing w) {
        if (parts.isEmpty()) {
            return none;
        }
        List<String> out = new ArrayList<>();
        for (Condition part : parts) {
            String e = expression(part, c, w);
            if (e == null) {
                return null;
            }
            out.add(parts.size() > 1 ? grouped(part, e) : e);
        }
        return String.join(operator, out);
    }

    /** An expression parenthesized when it is a conjunction or disjunction. */
    private static String grouped(Condition c, String expression) {
        return c instanceof All || c instanceof Any ? "(" + expression + ")" : expression;
    }

    /** A rule's condition on one value {@code %v} of a primitive of {@code type}, or null (noted) when Pure cannot say
     *  it. */
    private static @com.legend.base.Nullable String condition(Rule r, ImportedProperty p, Writing w, String cls) {
        String type = p.type();
        List<String> notes = w.notes();
        return switch (r) {
            case Bound b -> "(%v " + (b.lower() ? ">" : "<") + (b.exclusive() ? "" : "=") + " " + number(b.value())
                    + ")";
            case Length l -> "(%v->length() " + (l.lower() ? ">=" : "<=") + " " + l.value() + ")";
            case MultipleOf m -> "(rem(%v, " + number(m.value()) + ") == 0)";
            case Pattern x -> {
                try {
                    java.util.regex.Pattern.compile(x.regex());
                } catch (java.util.regex.PatternSyntaxException e) {
                    notes.add(cls + "." + x.property() + ": its pattern is not a regular expression Pure reads, and is "
                            + "not carried (" + x.regex() + ")");
                    yield null;
                }
                // JSON Schema's pattern matches anywhere in the text, its own ^ and $ anchoring it; this reads so
                // whether matches is the whole text's or any part's
                String whole = "(?s).*(?:" + x.regex() + ").*";
                yield "%v->matches(" + literal(whole) + ")";
            }
            case OneOf o -> {
                List<String> values = new ArrayList<>();
                for (Json.Node v : o.values()) {
                    String lit = p.kind() == Kind.ENUMERATION ? enumLiteral(v, p, w) : valueLiteral(v, type);
                    if (lit == null) {
                        notes.add(cls + "." + o.property() + ": a value of its const or enum that is not of its type "
                                + "is not carried (" + Json.toCompact(v) + ")");
                        yield null;
                    }
                    values.add(lit);
                }
                yield "%v->in([" + String.join(", ", values) + "])";
            }
            case Unique u -> throw new IllegalStateException("a list's rule is no value's condition");
            case Requires q -> throw new IllegalStateException("a property's requiring others is no value's condition");
        };
    }

    private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

    /** A number as a Pure literal: a whole number Pure's 64-bit Integer cannot hold spelled as a Float. */
    private static String number(BigDecimal value) {
        String plain = value.stripTrailingZeros().toPlainString();
        boolean whole = !plain.contains(".");
        return whole && (value.compareTo(LONG_MIN) <= 0 || value.compareTo(LONG_MAX) > 0) ? plain + ".0" : plain;
    }

    /** A JSON text as a value of the enumeration a property is typed by, or null when it is none of its values. */
    private static @com.legend.base.Nullable String enumLiteral(Json.Node v, ImportedProperty p, Writing w) {
        ImportedEnum e = w.enums().get(p.type());
        int i = e != null && v instanceof Json.Str s ? e.texts().indexOf(s.value()) : -1;
        return i < 0 || e == null ? null : w.domain() + "::" + e.name() + "." + e.values().get(i);
    }

    /** A JSON value as a Pure literal of {@code type}, or null when it is not one. */
    private static @com.legend.base.Nullable String valueLiteral(Json.Node v, String type) {
        return switch (type) {
            case "String" -> v instanceof Json.Str s ? literal(s.value()) : null;
            case "Boolean" -> v instanceof Json.Bool b ? Boolean.toString(b.value()) : null;
            case "Integer", "Float", "Decimal", "Number" -> v instanceof Json.Num n ? number(n.decimalValue() != null
                    ? n.decimalValue() : n.isInteger() ? BigDecimal.valueOf(n.longValue())
                    : new BigDecimal(Double.toString(n.doubleValue()))) : null;
            default -> null;
        };
    }

    /** A property's default, {@code " = <literal>"}, or nothing. */
    private static String defaultOf(ImportedProperty p, String domain) {
        Json.Node d = p.defaultValue();
        if (d == null) {
            return "";
        }
        if (p.kind() == Kind.ENUMERATION && d instanceof Json.Str s) {
            return " = " + domain + "::" + p.type() + "." + s.value();
        }
        String lit = valueLiteral(d, p.type());
        if (lit == null) {
            return "";
        }
        // a whole number a Float's default spelled as a Float
        return " = " + (p.type().equals("Float") && !lit.contains(".") && !lit.contains("E") ? lit + ".0" : lit);
    }

    // ---------------------------------------------------------------------
    // Associations
    // ---------------------------------------------------------------------

    /** A property of {@code owner} typed by a class; {@code owned} when that class is a value the owner holds. */
    private record Reference(String owner, ImportedProperty property, boolean owned) {
    }

    /** Whether a path holds an element that is not an association: one the import's association cannot take. */
    private static boolean isOtherElement(Json.@com.legend.base.Nullable Obj held) {
        return held != null && !"association".equals(held.getStringOr("_type", ""));
    }

    /** The association a reference is made: its text, written. Its name numbered when it is another element's; its
     *  way back numbered when the class reached has one of that name already. */
    private static String associate(ModelChanges m, String domain, Reference r, List<Reference> all,
            Map<String, Set<String>> ways) {
        ImportedProperty p = r.property();
        String base = domain + "::" + r.owner() + "_" + upperFirst(p.name());
        String path = base;
        for (int n = 2; isOtherElement(m.get(path)); n++) {
            path = base + "_" + n;
        }
        String back;
        String backMultiplicity;
        if (r.owned()) {
            back = lowerFirst(r.owner());
            backMultiplicity = "[0..1]";
        } else {
            back = lowerFirst(CreateAssociation.plural(r.owner()));
            backMultiplicity = "[*]";
            // the plural is the way back of ONE reference from the owner to the class, not its own: taken by the
            // class itself, or by another of the owner's references to the class, each is named after its property
            boolean shared = r.owner().equals(p.type());
            for (Reference other : all) {
                shared |= other != r && other.owner().equals(r.owner()) && other.property().type().equals(p.type());
            }
            if (shared) {
                back = back + "As" + upperFirst(p.name());
            }
        }
        // the class reached holds the way back as a property: one of its own, or another way back, is not taken
        String target = domain + "::" + p.type();
        Set<String> used = ways.computeIfAbsent(target, k -> propertyNames(m, target));
        String held = back;
        for (int n = 2; !used.add(held); n++) {
            held = back + n;
        }
        String text = "Association " + path + "\n{\n  " + (p.deprecated() ? DEPRECATED : "") + docOf(p.doc()) + p.name()
                + ": " + target + p.multiplicity() + ";\n  " + held + ": " + domain + "::" + r.owner()
                + backMultiplicity + ";\n}\n";
        m.put(element("###Pure\n" + text));
        return text;
    }

    /** The names of the properties a class of the project declares. */
    private static Set<String> propertyNames(ModelChanges m, String cls) {
        Set<String> out = new HashSet<>();
        Json.Obj c = m.get(cls);
        if (c != null) {
            for (Json.Node n : ModelChanges.arrOf(c, "properties").items()) {
                out.add(((Json.Obj) n).getString("name"));
            }
        }
        return out;
    }

    private static String docOf(@com.legend.base.Nullable String doc) {
        return doc == null || doc.strip().isEmpty() ? "" : "{" + DOC + " = " + literal(doc.strip()) + "} ";
    }
}
