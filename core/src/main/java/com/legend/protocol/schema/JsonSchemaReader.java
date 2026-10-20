// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import com.legend.json.Json;
import com.legend.json.Yaml;
import com.legend.protocol.schema.ImportedModel.ImportedClass;
import com.legend.protocol.schema.ImportedModel.ImportedEnum;
import com.legend.protocol.schema.ImportedModel.ImportedProperty;
import com.legend.protocol.schema.ImportedModel.Kind;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * JSON SCHEMA AND OPENAPI, READ AS A MODEL (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4): a JSON Schema document (its
 * root, {@code $defs}, {@code definitions}), an OpenAPI 3 document ({@code components.schemas}) or a Swagger 2 one
 * ({@code definitions}), written in JSON or YAML ({@link Yaml}), to an {@link ImportedModel}.
 *
 * <ul>
 *   <li>A schema with {@code properties}, or an {@code allOf} naming an object schema, is a <b>class</b>; each
 *       {@code allOf} reference to an object schema a superclass, each inline part's properties its own.</li>
 *   <li>A <b>union of objects</b> ({@code oneOf} or {@code anyOf} whose alternatives but {@code null} are two or more
 *       objects) is a class too, a superclass its alternatives extend: {@code allOf} says "I am a Base", a union
 *       "I am one of these", and Pure says both with a superclass. A union on a property is a superclass named after
 *       its owner and the property.</li>
 *   <li>A schema with {@code enum} (of text) is an <b>enumeration</b>.</li>
 *   <li>Any other named schema (a number, a list, a free-form object) is no element: a reference to it reads as what
 *       it is.</li>
 *   <li>A property: {@code string} String ({@code format: date} StrictDate, {@code date-time} DateTime),
 *       {@code integer} Integer, {@code number} Float ({@code format: decimal} Decimal), {@code boolean} Boolean; a
 *       reference to a class, or an inline object (a class of its own, named after its owner and the property), a
 *       reference the writer makes an association; an {@code array} many (or, asked, one {@code Variant} holding the
 *       list). A JSON value of no one Pure type is a {@code Variant}, the type Pure holds JSON in: a free-form object,
 *       no type at all, several types, a union not of objects, a tuple, a list of lists (each noted). {@code [1]} when required and not nullable ({@code type: [..., "null"]}, {@code nullable},
 *       an {@code anyOf} of a schema and {@code null}), else {@code [0..1]}; an array {@code [*]}, its bounds its
 *       {@code minItems} (when required) and {@code maxItems} ({@code [1..*]}, {@code [0..3]}).</li>
 * </ul>
 *
 * <p>What has no Pure form -- a reference outside the document, a keyword this reader does not know ({@code not},
 * {@code if}, ...), an enumeration not of text, a class that would be its own superclass -- is REFUSED, each where the
 * schema has it, all in one answer
 * ({@link SchemaImportException}). A validation keyword ({@code minimum}, {@code pattern}, ...) is not a shape and is
 * not carried, said in the model's notes; an annotation ({@code description}, {@code example}, {@code x-...}) is read
 * or ignored. A name Pure cannot hold is made one (upstream's camel case) and the rename noted.
 */
public final class JsonSchemaReader {

    private static final String VARIANT = "meta::pure::metamodel::variant::Variant";

    /** The keywords that make a schema's shape: read. */
    private static final Set<String> SHAPE = Set.of("type", "properties", "required", "items", "minItems", "maxItems",
            "$ref", "enum", "allOf", "anyOf", "oneOf", "nullable", "additionalProperties", "format", "$defs",
            "definitions");

    /** The keywords that say something of a schema and nothing of its shape: a description is read, the rest
     *  ignored. */
    private static final Set<String> ANNOTATIONS = Set.of("title", "description", "$schema", "$id", "id", "$comment",
            "examples", "example", "default", "deprecated", "readOnly", "writeOnly", "xml", "externalDocs",
            "discriminator", "$anchor", "contentMediaType", "contentEncoding", "unevaluatedProperties");

    /** The keywords that validate a value of the shape: not carried, said in the notes. */
    private static final Set<String> VALIDATIONS = Set.of("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
            "multipleOf", "minLength", "maxLength", "pattern", "uniqueItems", "minProperties",
            "maxProperties", "const", "contains", "minContains", "maxContains", "dependentRequired", "propertyNames");

    /** The words Pure reads as literals, never as a name. */
    private static final Set<String> LITERAL_WORDS = Set.of("true", "false");

    /** A JSON document nests deeper than a model message does. */
    private static final Json.Config DEPTH = new Json.Config(512);

    /** What a named schema is made. */
    private enum Shape { CLASS, ENUMERATION, ALIAS }

    /** A schema of the document's own, by name: its JSON pointer, its key, its Pure name, the schema. */
    private record Named(String pointer, String key, String name, Json.Obj schema) {
    }

    /** A property's type as read: its kind and type, whether many, whether it may be empty, the least and most
     *  number of a list's items. */
    private record Resolved(Kind kind, String type, boolean many, boolean nullable, int minItems,
            @com.legend.base.Nullable Integer maxItems) {
        Resolved(Kind kind, String type, boolean nullable) {
            this(kind, type, false, nullable, 0, null);
        }

        Resolved orEmpty(boolean empty) {
            return empty ? new Resolved(kind, type, many, true, minItems, maxItems) : this;
        }
    }

    private final Json.Obj document;
    private final Map<String, Named> named = new LinkedHashMap<>();
    private final Map<String, Shape> shapes = new HashMap<>();
    private final Map<String, Resolved> aliases = new HashMap<>();
    private final Set<String> resolving = new HashSet<>();
    private final Set<String> taken = new HashSet<>();
    private final List<ImportedClass> classes = new ArrayList<>();
    private final List<ImportedEnum> enumerations = new ArrayList<>();
    private final Set<String> notes = new LinkedHashSet<>();
    private final List<String> refusals = new ArrayList<>();
    /** The superclasses a class gains as an alternative of a union, by the class's name. */
    private final Map<String, Set<String>> unionSupers = new LinkedHashMap<>();
    private final boolean listsAsVariant;

    private JsonSchemaReader(Json.Obj document, boolean listsAsVariant) {
        this.document = document;
        this.listsAsVariant = listsAsVariant;
    }

    /**
     * The model {@code text} defines: every schema it names, or those of {@code only} (by key or Pure name) and the
     * schemas they reach. {@code rootName} names a JSON Schema's root object when it has no {@code title}.
     * {@code listsAsVariant}: a list of primitives or enumeration values is one {@code Variant} (the JSON list a
     * column holds) rather than a property of many values.
     *
     * @throws SchemaImportException naming everything that has no Pure form
     */
    public static ImportedModel read(String text, @com.legend.base.Nullable String rootName, List<String> only,
            boolean listsAsVariant) {
        String trimmed = text.strip();
        // a document of JSON, or of YAML (most OpenAPI documents are): read to the same tree
        boolean json = trimmed.startsWith("{");
        Json.Node parsed;
        try {
            parsed = json ? Json.parse(trimmed, DEPTH) : Yaml.parse(text);
        } catch (IllegalArgumentException e) {
            throw new SchemaImportException(List.of("the schema is not " + (json ? "JSON" : "YAML") + ": "
                    + e.getMessage()));
        }
        if (!(parsed instanceof Json.Obj document)) {
            throw new SchemaImportException(List.of("the schema is a document whose root is a mapping, not "
                    + Json.toCompact(parsed)));
        }
        JsonSchemaReader reader = new JsonSchemaReader(document, listsAsVariant);
        reader.collect(rootName);
        reader.build(only);
        reader.unite();
        if (!reader.refusals.isEmpty()) {
            throw new SchemaImportException(reader.refusals);
        }
        return new ImportedModel(reader.classes, reader.enumerations, new ArrayList<>(reader.notes));
    }

    // ---------------------------------------------------------------------
    // The document's named schemas
    // ---------------------------------------------------------------------

    private void collect(@com.legend.base.Nullable String rootName) {
        if (document.has("openapi") || document.has("swagger")) {
            Json.Obj components = objectAt(document, "components");
            collectFrom(components == null ? null : objectAt(components, "schemas"), "#/components/schemas");
            collectFrom(objectAt(document, "definitions"), "#/definitions");
            if (named.isEmpty()) {
                refusals.add("the OpenAPI document names no schema (components.schemas, or a Swagger document's "
                        + "definitions)");
            }
            return;
        }
        collectFrom(objectAt(document, "$defs"), "#/$defs");
        collectFrom(objectAt(document, "definitions"), "#/definitions");
        if (document.has("properties") || document.has("allOf")) {
            String title = text(document, "title");
            String key = title != null ? title : rootName;
            if (key == null) {
                refusals.add("#: the root schema is an object with no title: name it (the request's 'name')");
                return;
            }
            add("#", key);
        } else if (named.isEmpty()) {
            refusals.add("#: the document defines no object schema (no properties at its root, no $defs or "
                    + "definitions)");
        }
    }

    private void collectFrom(@com.legend.base.Nullable Json.Obj container, String at) {
        if (container == null) {
            return;
        }
        for (Map.Entry<String, Json.Node> e : container.fields().entrySet()) {
            if (e.getValue() instanceof Json.Obj) {
                add(at + "/" + pointerPart(e.getKey()), e.getKey());
            } else {
                refusals.add(at + "/" + pointerPart(e.getKey()) + ": a schema is a JSON object");
            }
        }
    }

    private void add(String pointer, String key) {
        String name = pureName(key, true, pointer);
        if (!taken.add(name)) {
            refusals.add(pointer + ": its name '" + name + "' is another schema's too");
            return;
        }
        if (at(pointer) instanceof Json.Obj schema) {
            named.put(pointer, new Named(pointer, key, name, schema));
        }
    }

    /** The node at a local JSON pointer ({@code #/components/schemas/Trade}), or null. */
    private @com.legend.base.Nullable Json.Node at(String pointer) {
        Json.Node node = document;
        if ("#".equals(pointer)) {
            return node;
        }
        for (String part : pointer.substring(2).split("/", -1)) {
            String key = part.replace("~1", "/").replace("~0", "~");
            if (!(node instanceof Json.Obj o) || !o.has(key)) {
                return null;
            }
            node = o.get(key);
        }
        return node;
    }

    private static String pointerPart(String key) {
        return key.replace("~", "~0").replace("/", "~1");
    }

    private Shape shape(Named n) {
        Shape known = shapes.get(n.pointer());
        if (known != null) {
            return known;
        }
        // a schema that reaches itself through allOf alone is no class: its cycle is refused where it is read
        shapes.put(n.pointer(), Shape.ALIAS);
        Shape s = shapeOf(n.schema());
        shapes.put(n.pointer(), s);
        return s;
    }

    private Shape shapeOf(Json.Obj schema) {
        if (schema.has("enum")) {
            return Shape.ENUMERATION;
        }
        if (schema.has("properties")) {
            return Shape.CLASS;
        }
        for (Json.Obj part : objects(schema, "allOf")) {
            if (isObject(part)) {
                return Shape.CLASS;
            }
        }
        return unionOfObjects(schema) != null ? Shape.CLASS : Shape.ALIAS;
    }

    /** Whether an alternative or part is an object: a reference to an object schema, or one written in place. */
    private boolean isObject(Json.Obj part) {
        if (part.has("properties")) {
            return true;
        }
        Named target = part.has("$ref") ? named.get(text(part, "$ref")) : null;
        if (target != null) {
            return shape(target) == Shape.CLASS;
        }
        for (Json.Obj p : objects(part, "allOf")) {
            if (isObject(p)) {
                return true;
            }
        }
        return false;
    }

    /** The keyword of a union ({@code oneOf} or {@code anyOf}) whose alternatives but {@code null} are two or more
     *  objects, or null when the schema is no such union. */
    private @com.legend.base.Nullable String unionOfObjects(Json.Obj schema) {
        for (String union : List.of("oneOf", "anyOf")) {
            List<Json.Obj> parts = objects(schema, union);
            List<Json.Obj> values = new ArrayList<>();
            for (Json.Obj part : parts) {
                if (!isNull(part)) {
                    values.add(part);
                }
            }
            if (values.size() >= 2 && values.stream().allMatch(this::isObject)) {
                return union;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------------
    // The model
    // ---------------------------------------------------------------------

    private void build(List<String> only) {
        Set<String> chosen = only.isEmpty() ? named.keySet() : reached(only);
        for (Named n : named.values()) {
            if (!chosen.contains(n.pointer())) {
                continue;
            }
            switch (shape(n)) {
                case CLASS -> buildClass(n.name(), n.schema(), n.pointer(), false);
                case ENUMERATION -> enumerations.add(enumeration(n.name(), n.schema(), n.pointer()));
                case ALIAS -> {
                    // no element: each reference reads it as what it is
                }
            }
        }
    }

    /** The schemas {@code only} names and every schema they reach, by pointer. */
    private Set<String> reached(List<String> only) {
        Set<String> out = new LinkedHashSet<>();
        Deque<String> work = new ArrayDeque<>();
        for (String wanted : only) {
            Named found = null;
            for (Named n : named.values()) {
                if (n.key().equals(wanted) || n.name().equals(wanted)) {
                    found = n;
                }
            }
            if (found == null) {
                refusals.add("the schema has no '" + wanted + "' to import");
            } else {
                work.add(found.pointer());
            }
        }
        while (!work.isEmpty()) {
            String pointer = work.pop();
            Named n = named.get(pointer);
            if (n != null && out.add(pointer)) {
                List<String> refs = new ArrayList<>();
                refsIn(n.schema(), refs);
                for (String ref : refs) {
                    if (named.containsKey(ref)) {
                        work.add(ref);
                    }
                }
            }
        }
        return out;
    }

    private static void refsIn(Json.Node node, List<String> out) {
        if (node instanceof Json.Obj o) {
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                if ("$ref".equals(e.getKey()) && e.getValue() instanceof Json.Str s) {
                    out.add(s.value());
                } else {
                    refsIn(e.getValue(), out);
                }
            }
        } else if (node instanceof Json.Arr a) {
            for (Json.Node item : a.items()) {
                refsIn(item, out);
            }
        }
    }

    /** A property's schema as the class gathers it: the schema, and where the document has it. */
    private record Gathered(Json.Obj schema, String where) {
    }

    /** A class: its superclasses and properties, gathered through {@code allOf}; added before any class it owns. */
    private void buildClass(String name, Json.Obj schema, String where, boolean owned) {
        int position = classes.size();
        List<String> supers = new ArrayList<>();
        Map<String, Gathered> properties = new LinkedHashMap<>();
        Set<String> required = new HashSet<>();
        gather(schema, where, supers, properties, required);
        List<ImportedProperty> out = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (Map.Entry<String, Gathered> e : properties.entrySet()) {
            String at = e.getValue().where();
            String property = pureName(e.getKey(), false, at);
            if (!names.add(property)) {
                refusals.add(at + ": its name '" + property + "' is another property's too");
                continue;
            }
            Resolved r = resolve(e.getValue().schema(), at, name, property);
            if (r == null) {
                continue;
            }
            boolean mustHave = required.contains(e.getKey()) && !r.nullable();
            int lower = r.many() ? (mustHave ? r.minItems() : 0) : (mustHave ? 1 : 0);
            out.add(new ImportedProperty(property, text(e.getValue().schema(), "description"), r.kind(), r.type(), lower,
                    r.many() ? r.maxItems() : Integer.valueOf(1)));
        }
        classes.add(position, new ImportedClass(name, text(schema, "description"), supers, out, owned));
        String union = unionOfObjects(schema);
        if (union != null) {
            alternatives(name, schema, union, where);
        }
    }

    /**
     * A UNION OF OBJECTS read as inheritance (the study's §4.2): each alternative a subclass of {@code superclass} --
     * a referenced class gains it as a superclass, an object written in place is a class of its own, named after the
     * union and its title (or {@code Option1}, {@code Option2}, ...). Pure has no union type; a superclass and its
     * subclasses is how an object model says "one of these". An anyOf is read as a oneOf: an instance is one class.
     *
     * @return whether every alternative is written in place (each a value its owner holds)
     */
    private boolean alternatives(String superclass, Json.Obj schema, String union, String where) {
        if (union.equals("anyOf")) {
            notes.add(where + ": anyOf is read as oneOf (an instance is one of its alternatives)");
        }
        List<Json.Obj> parts = objects(schema, union);
        boolean inPlace = true;
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj part = parts.get(i);
            String at = where + "/" + union + "/" + i;
            if (isNull(part)) {
                continue;
            }
            if (part.has("$ref")) {
                keywords(part, at);
                Named target = target(part, at);
                if (target != null) {
                    unionSupers.computeIfAbsent(target.name(), k -> new LinkedHashSet<>()).add(superclass);
                }
                inPlace = false;
            } else {
                String title = text(part, "title");
                String name = unique(superclass + (title != null ? pureName(title, true, at) : "Option" + (i + 1)));
                buildClass(name, part, at, true);
                unionSupers.computeIfAbsent(name, k -> new LinkedHashSet<>()).add(superclass);
            }
        }
        return inPlace;
    }

    /** Each class's superclasses as an alternative of a union, added; a class that would be its own superclass is
     *  refused by name. */
    private void unite() {
        for (int i = 0; i < classes.size(); i++) {
            ImportedClass c = classes.get(i);
            Set<String> extra = unionSupers.get(c.name());
            if (extra == null) {
                continue;
            }
            List<String> supers = new ArrayList<>(c.supers());
            for (String s : extra) {
                if (!s.equals(c.name()) && !supers.contains(s)) {
                    supers.add(s);
                }
            }
            classes.set(i, new ImportedClass(c.name(), c.doc(), supers, c.properties(), c.owned()));
        }
        Map<String, List<String>> graph = new HashMap<>();
        for (ImportedClass c : classes) {
            graph.put(c.name(), c.supers());
        }
        for (ImportedClass c : classes) {
            List<String> path = cycle(c.name(), c.name(), graph, new ArrayList<>(), new HashSet<>());
            if (path != null) {
                refusals.add("the class " + c.name() + " would be its own superclass (" + String.join(" extends ", path)
                        + "): a union and an allOf name each other");
                return;
            }
        }
    }

    private static @com.legend.base.Nullable List<String> cycle(String start, String at, Map<String, List<String>> graph,
            List<String> path, Set<String> seen) {
        path.add(at);
        for (String s : graph.getOrDefault(at, List.of())) {
            if (s.equals(start)) {
                List<String> out = new ArrayList<>(path);
                out.add(s);
                return out;
            }
            if (seen.add(s)) {
                List<String> found = cycle(start, s, graph, path, seen);
                if (found != null) {
                    return found;
                }
            }
        }
        path.remove(path.size() - 1);
        return null;
    }

    private void gather(Json.Obj schema, String where, List<String> supers, Map<String, Gathered> properties,
            Set<String> required) {
        keywords(schema, where);
        List<Json.Obj> parts = objects(schema, "allOf");
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj part = parts.get(i);
            String at = where + "/allOf/" + i;
            if (part.has("$ref")) {
                keywords(part, at);
                Named target = target(part, at);
                if (target == null) {
                    continue;
                }
                if (shape(target) == Shape.CLASS) {
                    supers.add(target.name());
                } else {
                    refusals.add(at + ": allOf names " + target.pointer() + ", which is not an object schema");
                }
            } else {
                gather(part, at, supers, properties, required);
            }
        }
        Json.Obj props = objectAt(schema, "properties");
        if (props != null) {
            for (Map.Entry<String, Json.Node> e : props.fields().entrySet()) {
                String at = where + "/properties/" + pointerPart(e.getKey());
                if (e.getValue() instanceof Json.Obj p) {
                    properties.put(e.getKey(), new Gathered(p, at));
                } else {
                    refusals.add(at + ": a property's schema is a JSON object");
                }
            }
        }
        if (schema.getOr("required", null) instanceof Json.Arr names) {
            for (Json.Node n : names.items()) {
                if (n instanceof Json.Str s) {
                    required.add(s.value());
                }
            }
        }
        if (schema.getOr("additionalProperties", null) instanceof Json.Obj) {
            notes.add(where + ": additionalProperties is not carried (a class has the properties it names)");
        }
    }

    /** A property's type, or null when it has no Pure form (refused, named). */
    private @com.legend.base.Nullable Resolved resolve(Json.Obj schema, String where, String owner, String property) {
        keywords(schema, where);
        boolean empty = schema.getOr("nullable", null) instanceof Json.Bool b && b.value();
        if (schema.has("$ref")) {
            Named target = target(schema, where);
            if (target == null) {
                return null;
            }
            return switch (shape(target)) {
                case CLASS -> new Resolved(Kind.CLASS, target.name(), empty);
                case ENUMERATION -> new Resolved(Kind.ENUMERATION, target.name(), empty || hasNull(target.schema()));
                case ALIAS -> {
                    Resolved r = alias(target);
                    yield r == null ? null : r.orEmpty(empty);
                }
            };
        }
        if (schema.has("enum")) {
            String name = unique(owner + upperFirst(property));
            enumerations.add(enumeration(name, schema, where));
            return new Resolved(Kind.ENUMERATION, name, empty || hasNull(schema));
        }
        for (String union : List.of("anyOf", "oneOf")) {
            List<Json.Obj> parts = objects(schema, union);
            if (parts.isEmpty()) {
                continue;
            }
            int values = 0;
            int value = -1;
            for (int i = 0; i < parts.size(); i++) {
                if (!isNull(parts.get(i))) {
                    values++;
                    value = i;
                }
            }
            boolean mayBeNull = values < parts.size();
            if (values == 1) {
                // one alternative and null: that alternative, which may be empty
                Resolved r = resolve(parts.get(value), where + "/" + union + "/" + value, owner, property);
                return r == null ? null : r.orEmpty(mayBeNull || empty);
            }
            if (unionOfObjects(schema) != null) {
                // a union of objects: a superclass of its own, its alternatives its subclasses
                String name = unique(owner + upperFirst(property));
                int position = classes.size();
                boolean inPlace = alternatives(name, schema, union, where);
                classes.add(position, new ImportedClass(name, text(schema, "description"), List.of(), List.of(), inPlace));
                return new Resolved(Kind.CLASS, name, mayBeNull || empty);
            }
            notes.add(where + ": a union (" + union + ") of values that are not all objects is a Variant (Pure has no "
                    + "union type)");
            return new Resolved(Kind.PRIMITIVE, VARIANT, mayBeNull || empty);
        }
        List<Json.Obj> all = objects(schema, "allOf");
        if (all.size() == 1 && !schema.has("properties")) {
            Resolved r = resolve(all.get(0), where + "/allOf/0", owner, property);
            return r == null ? null : r.orEmpty(empty);
        }
        if (schema.has("properties") || !all.isEmpty()) {
            String name = unique(owner + upperFirst(property));
            buildClass(name, schema, where, true);
            return new Resolved(Kind.CLASS, name, empty);
        }
        List<String> types = new ArrayList<>();
        Json.Node type = schema.getOr("type", null);
        if (type instanceof Json.Str s) {
            types.add(s.value());
        } else if (type instanceof Json.Arr a) {
            for (Json.Node n : a.items()) {
                if (n instanceof Json.Str s) {
                    types.add(s.value());
                }
            }
        }
        if (types.remove("null")) {
            empty = true;
            if (types.isEmpty()) {
                refusals.add(where + ": a value that is always null has no Pure form");
                return null;
            }
        }
        if (types.size() > 1) {
            notes.add(where + ": a value of several types (" + String.join(", ", types) + ") is a Variant (Pure has no "
                    + "union type)");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        String t = types.isEmpty() ? (schema.has("items") ? "array" : "") : types.get(0);
        return switch (t) {
            case "array" -> {
                Json.Node items = schema.getOr("items", null);
                if (items instanceof Json.Arr) {
                    notes.add(where + ": a tuple (items as a list) is a Variant, the JSON list it is");
                    yield new Resolved(Kind.PRIMITIVE, VARIANT, empty);
                }
                int least = schema.getOr("minItems", null) instanceof Json.Num n ? (int) n.longValue() : 0;
                Integer most = schema.getOr("maxItems", null) instanceof Json.Num n ? Integer.valueOf((int) n.longValue())
                        : null;
                if (items == null) {
                    notes.add(where + ": a list of any values is one Variant, the JSON list it is");
                    yield new Resolved(Kind.PRIMITIVE, VARIANT, empty);
                }
                Resolved item = resolve((Json.Obj) items, where + "/items", owner, singular(property));
                if (item == null) {
                    yield null;
                }
                if (item.many() || (item.kind() == Kind.PRIMITIVE && item.type().equals(VARIANT))) {
                    // a list of lists, or of values of no one type: the JSON list itself
                    notes.add(where + ": a list of " + (item.many() ? "lists" : "Variants") + " is one Variant, the "
                            + "JSON list it is");
                    yield new Resolved(Kind.PRIMITIVE, VARIANT, empty);
                }
                if (listsAsVariant && item.kind() != Kind.CLASS) {
                    notes.add(where + ": a list of " + item.type() + " is one Variant, the JSON list a column holds "
                            + "('lists': 'variant')");
                    yield new Resolved(Kind.PRIMITIVE, VARIANT, empty);
                }
                yield new Resolved(item.kind(), item.type(), true, empty, least, most);
            }
            case "string" -> new Resolved(Kind.PRIMITIVE, switch (String.valueOf(text(schema, "format"))) {
                case "date" -> "StrictDate";
                case "date-time" -> "DateTime";
                default -> "String";
            }, empty);
            case "integer" -> new Resolved(Kind.PRIMITIVE, "Integer", empty);
            case "number" -> new Resolved(Kind.PRIMITIVE, "decimal".equals(text(schema, "format")) ? "Decimal" : "Float",
                    empty);
            case "boolean" -> new Resolved(Kind.PRIMITIVE, "Boolean", empty);
            // a free-form object, or any value at all: semi-structured, as Pure holds JSON
            case "object", "" -> new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            default -> {
                refusals.add(where + ": the type '" + t + "' is not JSON Schema's");
                yield null;
            }
        };
    }

    /** A named schema that is no element, read as what it is: once, wherever it is referenced. */
    private @com.legend.base.Nullable Resolved alias(Named target) {
        if (aliases.containsKey(target.pointer())) {
            return aliases.get(target.pointer());
        }
        if (!resolving.add(target.pointer())) {
            refusals.add(target.pointer() + ": a schema that is itself, through references alone, has no Pure form");
            return null;
        }
        Resolved r = resolve(target.schema(), target.pointer(), target.name(), "");
        resolving.remove(target.pointer());
        aliases.put(target.pointer(), r);
        return r;
    }

    /** The named schema a {@code $ref} names, or null (refused, named). */
    private @com.legend.base.Nullable Named target(Json.Obj schema, String where) {
        String ref = text(schema, "$ref");
        if (ref == null || !ref.startsWith("#")) {
            refusals.add(where + ": the reference '" + ref + "' is outside the document; only the document's own "
                    + "schemas are read");
            return null;
        }
        Named target = named.get(ref);
        if (target == null) {
            refusals.add(where + ": the reference '" + ref + "' names no schema of the document's (a schema of "
                    + "components.schemas, $defs or definitions)");
        }
        return target;
    }

    private ImportedEnum enumeration(String name, Json.Obj schema, String where) {
        keywords(schema, where);
        List<String> values = new ArrayList<>();
        for (Json.Node n : schema.getArr("enum").items()) {
            if (n instanceof Json.Null) {
                continue;
            }
            if (!(n instanceof Json.Str s)) {
                refusals.add(where + ": an enumeration of values that are not text (" + Json.toCompact(n)
                        + ") has no Pure form yet");
                continue;
            }
            String value = s.value();
            String held = isName(value) ? value : valueName(value);
            if (!held.equals(value)) {
                notes.add(where + ": the value '" + value + "' is named " + held);
            }
            if (values.contains(held)) {
                refusals.add(where + ": two values are named '" + held + "'");
            }
            values.add(held);
        }
        if (values.isEmpty()) {
            refusals.add(where + ": an enumeration with no value");
        }
        return new ImportedEnum(name, text(schema, "description"), values);
    }

    /** Each keyword of a schema read, ignored, noted (a validation) or refused (unknown). */
    private void keywords(Json.Obj schema, String where) {
        for (String key : schema.fields().keySet()) {
            if (key.startsWith("x-") || SHAPE.contains(key) || ANNOTATIONS.contains(key)) {
                continue;
            }
            if (VALIDATIONS.contains(key)) {
                notes.add(where + ": " + key + " is not carried (a validation, not a shape)");
            } else {
                refusals.add(where + ": the keyword '" + key + "' has no Pure form yet");
            }
        }
    }

    // ---------------------------------------------------------------------
    // Names
    // ---------------------------------------------------------------------

    /** A schema's or property's name as Pure holds it: as it is when it can be, else camel-cased (noted). */
    private String pureName(String raw, boolean type, String where) {
        if (isName(raw) && (!type || Character.isUpperCase(raw.charAt(0)) || !Character.isLetter(raw.charAt(0)))) {
            return raw;
        }
        String held = camelCase(raw, type);
        notes.add(where + ": '" + raw + "' is named " + held);
        return held;
    }

    /** A name not yet a class's or enumeration's: {@code name}, else {@code name2}, {@code name3}, ... */
    private String unique(String name) {
        String out = name;
        for (int n = 2; !taken.add(out); n++) {
            out = name + n;
        }
        return out;
    }

    private static boolean isName(String name) {
        return name.matches("[A-Za-z_][A-Za-z0-9_]*") && !LITERAL_WORDS.contains(name);
    }

    /** Upstream's camel case ({@code makeCamelCase}) of the parts between characters a name cannot hold. */
    static String camelCase(String raw, boolean upperFirst) {
        StringBuilder out = new StringBuilder();
        for (String part : raw.split("[^A-Za-z0-9]+")) {
            if (part.isEmpty()) {
                continue;
            }
            String rest = part.substring(1);
            boolean first = out.length() == 0;
            out.append(upperFirst || !first ? part.substring(0, 1).toUpperCase(Locale.ROOT)
                    : part.substring(0, 1).toLowerCase(Locale.ROOT))
                    .append(rest.equals(rest.toUpperCase(Locale.ROOT)) ? rest.toLowerCase(Locale.ROOT) : rest);
        }
        String held = out.length() == 0 ? "_" : out.toString();
        held = Character.isDigit(held.charAt(0)) ? "_" + held : held;
        return LITERAL_WORDS.contains(held) ? held + "_" : held;
    }

    /** An enumeration value as Pure holds it: each character a name cannot hold made {@code _}. */
    private static String valueName(String value) {
        String held = value.replaceAll("[^A-Za-z0-9_]", "_");
        if (held.isEmpty()) {
            return "_";
        }
        held = Character.isDigit(held.charAt(0)) ? "_" + held : held;
        return LITERAL_WORDS.contains(held) ? held + "_" : held;
    }

    private static String upperFirst(String name) {
        return name.isEmpty() ? name : name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
    }

    /** One of a list property's items, named for a class or enumeration of its own: {@code lines} a line. */
    static String singular(String plural) {
        if (plural.endsWith("ies") && plural.length() > 3) {
            return plural.substring(0, plural.length() - 3) + "y";
        }
        if (plural.endsWith("sses") || plural.endsWith("xes") || plural.endsWith("ches") || plural.endsWith("shes")) {
            return plural.substring(0, plural.length() - 2);
        }
        if (plural.endsWith("s") && !plural.endsWith("ss") && plural.length() > 1) {
            return plural.substring(0, plural.length() - 1);
        }
        return plural;
    }

    // ---------------------------------------------------------------------
    // JSON
    // ---------------------------------------------------------------------

    private static @com.legend.base.Nullable String text(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Str s ? s.value() : null;
    }

    private static @com.legend.base.Nullable Json.Obj objectAt(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Obj v ? v : null;
    }

    private static List<Json.Obj> objects(Json.Obj o, String key) {
        List<Json.Obj> out = new ArrayList<>();
        if (o.getOr(key, null) instanceof Json.Arr a) {
            for (Json.Node n : a.items()) {
                if (n instanceof Json.Obj item) {
                    out.add(item);
                }
            }
        }
        return out;
    }

    private static boolean isNull(Json.Obj schema) {
        return schema.getOr("type", null) instanceof Json.Str s && "null".equals(s.value());
    }

    private static boolean hasNull(Json.Obj schema) {
        return schema.getOr("enum", null) instanceof Json.Arr a && a.items().contains(Json.nil());
    }
}
