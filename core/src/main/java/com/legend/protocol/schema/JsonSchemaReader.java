// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import com.legend.json.Json;
import com.legend.json.Yaml;
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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
 * root, {@code $defs}, {@code definitions}; drafts 4 to 2020-12), an OpenAPI 3 document ({@code components.schemas},
 * and its operations' bodies) or a Swagger 2 one ({@code definitions}), written in JSON or YAML ({@link Yaml}), and the
 * documents it references, to an {@link ImportedModel}.
 *
 * <ul>
 *   <li><b>A class</b>: a schema with properties (its own, or an {@code if}/{@code then}/{@code else}'s or a
 *       {@code dependentSchemas}' -- each read as an optional property, a reference or a union inside a condition
 *       too), an {@code allOf} naming an object schema (each such reference a superclass, each part written in place
 *       its properties), or a <b>union of objects</b> ({@code oneOf}/{@code anyOf} of two or more objects: the
 *       superclass its alternatives extend, the study's §4.2). A root that is a list of objects: its item a class; a
 *       root that is a map ({@code additionalProperties}, {@code patternProperties}): a class of its entries, each its
 *       {@code key} and its {@code value}; a root that is a reference alone: what it names.</li>
 *   <li><b>An enumeration</b>: an {@code enum} of two or more texts (one, of a schema the document names), or a union
 *       of {@code const} texts (each value's description its own); its values' names from {@code x-enum-varnames}
 *       (or jsonschema2pojo's, json-schema-to-typescript's) when given, documented by {@code x-enum-descriptions}
 *       (or VS Code's).</li>
 *   <li><b>A property</b>: {@code string} String ({@code format} {@code date} StrictDate, {@code date-time} DateTime,
 *       {@code time} StrictTime), {@code integer} Integer, {@code number} Float ({@code decimal} Decimal; integer and
 *       number together Number), {@code boolean} Boolean; a reference to a class or an object written in place an
 *       association; an {@code array} many (or, asked, one {@code Variant} holding the list). A JSON value of no one
 *       Pure type is a {@code Variant}: a free-form object or map, a union not of objects, several types, a tuple, a
 *       list of lists, {@code true}. {@code [1]} when required and not nullable, else {@code [0..1]}; a list's bounds
 *       its {@code minItems} and {@code maxItems}. A property that can hold no value ({@code false}, a {@code null}
 *       type) is none.</li>
 *   <li><b>A rule</b> for each validation Pure states as a constraint: {@code minimum}, {@code maximum} (and
 *       exclusive, draft 4's boolean spelling too), {@code minLength}, {@code maxLength}, {@code pattern}, an
 *       integer's {@code multipleOf}, a {@code const} or an {@code enum} not made an enumeration,
 *       {@code uniqueItems}, {@code dependentRequired} (draft 7's {@code dependencies} naming properties); an item's
 *       rules each item's.</li>
 *   <li><b>Logic</b>, a schema's validation of a whole value, as a {@link ImportedModel.Condition} on the class's
 *       properties: {@code if}/{@code then}/{@code else}, {@code not}, a {@code oneOf} or {@code anyOf} of
 *       requirements, {@code dependentSchemas}, and a property's own {@code not} and {@code if}; into an
 *       object-typed property by its path ({@code config.mode}). What a condition says that Pure's constraint
 *       cannot (a Variant's type, the properties of each object of a list, ...) is noted, and the condition not
 *       carried.</li>
 *   <li>A default ({@code default}) and {@code deprecated} (VS Code's {@code deprecationMessage} too), carried;
 *       {@code description} (VS Code's {@code markdownDescription}; else {@code title}) the element's
 *       documentation.</li>
 * </ul>
 *
 * <p><b>References</b> reach any place in the document by its JSON pointer ({@code #/definitions/A/properties/b},
 * percent-encoded), an {@code $anchor} ({@code #name}), an {@code $id} (draft 4's {@code id}), {@code $dynamicRef}
 * and {@code $recursiveRef}, and another document the request gives ({@code documents}). A place written in two
 * places is one class.
 *
 * <p><b>Notes, not refusals</b>: JSON Schema's own rule is that a keyword a reader does not know is an annotation, so
 * an unknown keyword is not read, and noted (another vocabulary's, VS Code's or jsonschema2pojo's, says nothing of a
 * shape and is not); a validation Pure cannot state ({@code minProperties}, {@code contains}, ...) is noted; a
 * reference to a document not given, or to nothing, is a {@code Variant}, noted; a name Pure cannot hold is made one,
 * and a name taken twice is numbered, noted; a class that would be its own superclass loses the superclass closing
 * the circle, noted. Each note is said once, with where it was first met and how many times. <b>Refused</b>
 * ({@link SchemaImportException}, every reason at once): a document that is not JSON or YAML, one that defines no
 * object (a root that is a reference to a document not given says which), and a schema asked for that the document
 * does not have.
 */
public final class JsonSchemaReader {

    private static final String VARIANT = "meta::pure::metamodel::variant::Variant";

    /** The keywords read: the shape, its documentation, and the validations made rules. */
    private static final Set<String> READ = Set.of("type", "properties", "required", "items", "prefixItems",
            "minItems", "maxItems", "$ref", "$dynamicRef", "$recursiveRef", "enum", "const", "allOf", "anyOf", "oneOf",
            "nullable", "x-nullable", "additionalProperties", "patternProperties", "format", "$defs", "definitions",
            "if", "then", "else", "dependentSchemas", "dependencies", "title", "description", "default", "deprecated",
            "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength",
            "pattern", "uniqueItems", "dependentRequired", "not");

    /** The keywords that say nothing of a shape: identifiers, examples, media, and OpenAPI's annotations. */
    private static final Set<String> ANNOTATIONS = Set.of("$schema", "$id", "id", "$comment", "$anchor",
            "$dynamicAnchor", "$recursiveAnchor", "$vocabulary", "examples", "example", "readOnly", "writeOnly", "xml",
            "externalDocs", "discriminator", "contentMediaType", "contentEncoding", "contentSchema", "additionalItems",
            "unevaluatedItems", "unevaluatedProperties");

    /** The keywords of vocabularies beside JSON Schema's that say nothing of a shape: VS Code's JSON language service
     *  (its {@code markdownDescription} a description, its {@code deprecationMessage} a deprecation), jsonschema2pojo,
     *  json-schema-to-typescript, Hyper-Schema. */
    private static final Set<String> VOCABULARIES = Set.of("markdownDescription", "deprecationMessage",
            "defaultSnippets", "allowComments", "allowTrailingCommas", "errorMessage", "patternErrorMessage",
            "doNotSuggest", "suggestSortText", "enumDescriptions", "markdownEnumDescriptions", "enumItemLabels",
            "javaType", "existingJavaType", "javaInterfaces", "javaName", "javaEnumNames", "javaEnums", "javaJsonView",
            "customDateTimePattern", "customPattern", "customTimezone", "excludedFromEqualsAndHashCode", "tsType",
            "tsEnumNames", "links", "base", "media", "fileMatch");

    /** The validations a map at the root carries: its keys' rules. */
    private static final Set<String> CARRIED_AT_A_MAP = Set.of("propertyNames");

    /** The validations Pure does not state as a constraint here: noted. */
    private static final Set<String> VALIDATIONS = Set.of("minProperties", "maxProperties", "contains",
            "minContains", "maxContains", "propertyNames");

    /** The words Pure reads as literals, never as a name. */
    private static final Set<String> LITERAL_WORDS = Set.of("true", "false");

    /** The extensions that name an enumeration's values, and that document them. */
    private static final List<String> VALUE_NAMES = List.of("x-enum-varnames", "x-enumNames", "x-enum-names",
            "javaEnumNames", "tsEnumNames");
    private static final List<String> VALUE_DOCS = List.of("x-enum-descriptions", "x-enumDescriptions",
            "enumDescriptions", "markdownEnumDescriptions");

    /** A JSON document nests deeper than a model message does. */
    private static final Json.Config DEPTH = new Json.Config(512);

    /** How a document is read. */
    public record Options(@com.legend.base.Nullable String rootName, List<String> only, boolean listsAsVariant,
            @com.legend.base.Nullable Boolean operations, Map<String, String> documents) {
        public Options {
            only = List.copyOf(only);
            documents = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(documents));
        }

        /**
         * @param rootName       a JSON Schema's root object's name when it has no {@code title}
         * @param only           the schemas to import (by key or Pure name), each with the schemas it reaches; every
         *                       one when empty
         * @param listsAsVariant a list of primitives or enumeration values one {@code Variant}, the JSON list a column
         *                       holds
         * @param operations     whether an OpenAPI document's operations' bodies are read; when null, only when it
         *                       names no component schema
         * @param documents      the other documents a reference may name, by the name it names them ({@code
         *                       common.yaml})
         */
        public static Options of(@com.legend.base.Nullable String rootName, List<String> only, boolean listsAsVariant,
                @com.legend.base.Nullable Boolean operations, Map<String, String> documents) {
            return new Options(rootName, only, listsAsVariant, operations, documents);
        }
    }

    /** What a place in a document is made. */
    private enum Shape { CLASS, ENUMERATION, ALIAS }

    /** A schema the document names: its place, its key, its Pure name. */
    private record Named(String loc, String key, String name) {
    }

    /** A property's type as read. {@code rules} are its values' rules, of no property yet. */
    private record Resolved(Kind kind, String type, boolean many, boolean nullable, int minItems,
            @com.legend.base.Nullable Integer maxItems, List<Rule> rules) {
        Resolved(Kind kind, String type, boolean nullable) {
            this(kind, type, false, nullable, 0, null, List.of());
        }

        Resolved orEmpty(boolean empty) {
            return empty ? new Resolved(kind, type, many, true, minItems, maxItems, rules) : this;
        }

        Resolved withRules(List<Rule> more) {
            if (more.isEmpty()) {
                return this;
            }
            List<Rule> all = new ArrayList<>(rules);
            all.addAll(more);
            return new Resolved(kind, type, many, nullable, minItems, maxItems, all);
        }

        boolean isVariant() {
            return kind == Kind.PRIMITIVE && type.equals(VARIANT);
        }
    }

    /** The value a property can never hold: a {@code false} schema, an empty {@code enum}, a {@code null} type. */
    private static final Resolved NEVER = new Resolved(Kind.PRIMITIVE, "", false);

    /** A note, said once: where it was first met, and how many times. */
    private static final class Note {
        final String where;
        int count = 1;

        Note(String where) {
            this.where = where;
        }
    }

    /** A property's schema, and its place. */
    private record PropertyAt(Json.Node schema, String loc) {
    }

    /** A class's properties as its schema and its parts gather them. */
    private static final class Gathered {
        /** The class's own place: an allOf naming it names no superclass. */
        final String self;
        final List<String> supers = new ArrayList<>();
        final Map<String, PropertyAt> properties = new LinkedHashMap<>();
        final Set<String> required = new HashSet<>();
        final Set<String> conditional = new HashSet<>();
        /** Each property another's presence requires, by their JSON names. */
        final Map<String, List<String>> requires = new LinkedHashMap<>();
        /** The places gathered: each once. */
        final Set<String> visited = new HashSet<>();
        /** The places of the conditions met (then, else, a dependentSchemas schema), gathered after the rest. */
        final List<String> conditions = new ArrayList<>();
        /** The places of the class's logic over a whole value, each with what its constraint is named after. */
        final Map<String, String> logic = new LinkedHashMap<>();

        Gathered(String self) {
            this.self = self;
        }
    }

    private final Options options;
    private final Map<String, Json.Node> documents = new LinkedHashMap<>();
    private final Map<String, Map<String, String>> anchors = new HashMap<>();
    private final Map<String, String> ids = new LinkedHashMap<>();
    private final Map<String, Named> named = new LinkedHashMap<>();
    private final Map<String, Shape> shapes = new HashMap<>();
    private final Map<String, String> classAt = new HashMap<>();
    /** Each class's properties, its superclasses' too, by their JSON names: what its logic can name. */
    private final Map<String, Map<String, ImportedProperty>> keysOf = new HashMap<>();
    /** The places that are a property's own schema: their not and if/then/else are the class's logic. */
    private final Set<String> directs = new HashSet<>();
    private final Map<String, String> enumAt = new HashMap<>();
    private final Map<String, Resolved> aliases = new HashMap<>();
    private final Set<String> resolving = new HashSet<>();
    private final Set<String> scanned = new HashSet<>();
    private final Set<String> taken = new HashSet<>();
    private final Set<String> shared = new HashSet<>();
    private final List<ImportedClass> classes = new ArrayList<>();
    private final List<ImportedEnum> enumerations = new ArrayList<>();
    private final Map<String, Set<String>> unionSupers = new LinkedHashMap<>();
    private final Map<String, Note> notes = new LinkedHashMap<>();
    private final List<String> refusals = new ArrayList<>();
    /** The place of a map at the root, made a class of its entries: none, or one. */
    private final Set<String> mapRoots = new HashSet<>();

    private JsonSchemaReader(Options options) {
        this.options = options;
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
        return read(text, Options.of(rootName, only, listsAsVariant, null, Map.of()));
    }

    /**
     * The model {@code text} defines, read as {@code options} say.
     *
     * @throws SchemaImportException naming everything that has no Pure form
     */
    public static ImportedModel read(String text, Options options) {
        JsonSchemaReader reader = new JsonSchemaReader(options);
        reader.documents.put("", parse(text, "the schema"));
        if (!(reader.documents.get("") instanceof Json.Obj)) {
            throw new SchemaImportException(List.of("the schema is a document whose root is a mapping, not "
                    + Json.toCompact(reader.documents.get(""))));
        }
        for (Map.Entry<String, String> d : options.documents().entrySet()) {
            reader.documents.put(normalized(d.getKey()), parse(d.getValue(), "the document '" + d.getKey() + "'"));
        }
        for (String doc : reader.documents.keySet()) {
            Json.Node d = reader.documents.get(doc);
            // draft 4 and before: a schema's identifier is its "id"
            String dialect = d instanceof Json.Obj o ? text(o, "$schema") : null;
            boolean legacy = dialect != null && (dialect.contains("draft-04") || dialect.contains("draft-03"));
            reader.index(doc, "", d, legacy);
        }
        reader.collect();
        reader.build();
        reader.unite();
        if (!reader.refusals.isEmpty()) {
            throw new SchemaImportException(reader.refusals);
        }
        List<String> notes = new ArrayList<>();
        for (Map.Entry<String, Note> n : reader.notes.entrySet()) {
            Note note = n.getValue();
            notes.add(note.where + ": " + n.getKey() + (note.count > 1 ? " (" + note.count + " times)" : ""));
        }
        return new ImportedModel(reader.classes, reader.enumerations, notes);
    }

    /** A document of JSON, or of YAML (most OpenAPI documents are): read to the same tree. */
    private static Json.Node parse(String text, String what) {
        boolean json = text.strip().startsWith("{");
        try {
            return json ? Json.parse(text.strip(), DEPTH) : Yaml.parse(text);
        } catch (IllegalArgumentException e) {
            throw new SchemaImportException(List.of(what + " is not " + (json ? "JSON" : "YAML") + ": "
                    + e.getMessage()));
        }
    }

    // ---------------------------------------------------------------------
    // Places: a document and a JSON pointer in it, "doc#/a/b"; the main document's name is empty
    // ---------------------------------------------------------------------

    private static String loc(String doc, String pointer) {
        return doc + "#" + pointer;
    }

    private static String docOf(String loc) {
        return loc.substring(0, loc.indexOf('#'));
    }

    private static String child(String loc, String... parts) {
        StringBuilder out = new StringBuilder(loc);
        for (String p : parts) {
            out.append('/').append(p.replace("~", "~0").replace("/", "~1"));
        }
        return out.toString();
    }

    /** The schema at a place known to be one (a JSON object); an empty one if it is not. */
    private Json.Obj schemaAt(String loc) {
        return at(loc) instanceof Json.Obj o ? o : Json.obj();
    }

    /** The node at a place, or null. */
    private @com.legend.base.Nullable Json.Node at(String loc) {
        int hash = loc.indexOf('#');
        Json.Node node = documents.get(loc.substring(0, hash));
        String pointer = loc.substring(hash + 1);
        if (node == null || pointer.isEmpty()) {
            return node;
        }
        for (String part : parts(pointer.substring(1), '/')) {
            String key = part.replace("~1", "/").replace("~0", "~");
            if (node instanceof Json.Obj o && o.has(key)) {
                node = o.get(key);
            } else if (node instanceof Json.Arr a && digits(key)
                    && Integer.parseInt(key) < a.items().size()) {
                node = a.items().get(Integer.parseInt(key));
            } else {
                return null;
            }
        }
        return node;
    }

    /** Every {@code $id} and {@code $anchor} of a document, by the place it names; instance values not walked. */
    private void index(String doc, String pointer, @com.legend.base.Nullable Json.Node node, boolean legacy) {
        if (node instanceof Json.Obj o) {
            if (o.getOr(legacy ? "id" : "$id", null) instanceof Json.Str id && !id.value().startsWith("#")) {
                ids.put(stripHash(id.value()), loc(doc, pointer));
            } else if (legacy && o.getOr("id", null) instanceof Json.Str id && id.value().length() > 1) {
                // draft 4's plain-name fragment, "#name": an anchor
                anchors.computeIfAbsent(doc, k -> new HashMap<>()).put(id.value().substring(1), pointer);
            }
            for (String a : List.of("$anchor", "$dynamicAnchor")) {
                if (o.getOr(a, null) instanceof Json.Str name) {
                    anchors.computeIfAbsent(doc, k -> new HashMap<>()).put(name.value(), pointer);
                }
            }
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                if (!Set.of("enum", "const", "default", "examples", "example").contains(e.getKey())) {
                    index(doc, pointer + "/" + e.getKey().replace("~", "~0").replace("/", "~1"), e.getValue(),
                            legacy);
                }
            }
        } else if (node instanceof Json.Arr a) {
            for (int i = 0; i < a.items().size(); i++) {
                index(doc, pointer + "/" + i, a.items().get(i), legacy);
            }
        }
    }

    private static String stripHash(String id) {
        return id.endsWith("#") ? id.substring(0, id.length() - 1) : id;
    }

    /** A document's name as a reference spells it, {@code ./common.yaml} as {@code common.yaml}. */
    private static String normalized(String name) {
        String n = name;
        while (n.startsWith("./")) {
            n = n.substring(2);
        }
        return n;
    }

    /** The reference a schema makes: its {@code $ref}, else its {@code $dynamicRef} or {@code $recursiveRef}. */
    private static @com.legend.base.Nullable String refOf(Json.Obj schema) {
        for (String k : List.of("$ref", "$dynamicRef", "$recursiveRef")) {
            if (schema.getOr(k, null) instanceof Json.Str s) {
                return s.value();
            }
        }
        return null;
    }

    /**
     * The place a reference from {@code from} names: a JSON pointer, an anchor, an {@code $id}, or a document given;
     * null when it names a document not given, or nothing.
     */
    private @com.legend.base.Nullable String target(String from, String ref) {
        int hash = ref.indexOf('#');
        String uri = hash < 0 ? ref : ref.substring(0, hash);
        String fragment = decoded(hash < 0 ? "" : ref.substring(hash + 1));
        String base;
        if (uri.isEmpty()) {
            base = loc(docOf(from), "");
        } else {
            String found = ids.get(stripHash(uri));
            if (found == null) {
                for (Map.Entry<String, String> id : ids.entrySet()) {
                    if (id.getKey().endsWith("/" + normalized(uri))) {
                        found = id.getValue();
                    }
                }
            }
            if (found == null) {
                String doc = normalized(uri);
                if (!documents.containsKey(doc)) {
                    String last = doc.substring(doc.lastIndexOf('/') + 1);
                    doc = documents.containsKey(last) ? last : null;
                }
                found = doc == null ? null : loc(doc, "");
            }
            if (found == null) {
                return null;
            }
            base = found;
        }
        if (fragment.isEmpty()) {
            return base;
        }
        if (fragment.startsWith("/")) {
            String placed = base + fragment;
            return at(placed) == null ? null : placed;
        }
        Map<String, String> named = anchors.get(docOf(base));
        String pointer = named == null ? null : named.get(fragment);
        return pointer == null ? null : loc(docOf(base), pointer);
    }

    /** A URI fragment's percent-encoded bytes decoded, as UTF-8. */
    private static String decoded(String fragment) {
        if (fragment.indexOf('%') < 0) {
            return fragment;
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < fragment.length(); i++) {
            char c = fragment.charAt(i);
            if (c == '%' && i + 2 < fragment.length() && hex(fragment.charAt(i + 1)) && hex(fragment.charAt(i + 2))) {
                out.write(Integer.parseInt(fragment.substring(i + 1, i + 3), 16));
                i += 2;
            } else {
                byte[] b = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                out.write(b, 0, b.length);
            }
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------------
    // The documents' named schemas
    // ---------------------------------------------------------------------

    private void collect() {
        Json.Obj main = documents.get("") instanceof Json.Obj o ? o : Json.obj();
        boolean openApi = main.has("openapi") || main.has("swagger");
        for (String doc : documents.keySet()) {
            Json.Obj d = documents.get(doc) instanceof Json.Obj o ? o : Json.obj();
            Json.Obj components = objectAt(d, "components");
            collectFrom(doc, components == null ? null : objectAt(components, "schemas"), "/components/schemas");
            collectFrom(doc, objectAt(d, "definitions"), "/definitions");
            collectFrom(doc, objectAt(d, "$defs"), "/$defs");
        }
        if (openApi) {
            boolean read = options.operations() != null ? options.operations() : named.isEmpty();
            if (read) {
                operations(main);
            }
            if (named.isEmpty()) {
                refusals.add("the OpenAPI document names no schema (components.schemas, a Swagger document's "
                        + "definitions) and no operation has a body of its own");
            }
            return;
        }
        String title = text(main, "title");
        // a root that is a reference alone: what it names is the root
        List<String> unfollowed = new ArrayList<>();
        String root = followed(loc("", ""), unfollowed);
        if (root == null) {
            String ref = unfollowed.get(0);
            if (named.isEmpty()) {
                refusals.add("#: the schema is the reference '" + ref + "' alone, to a document not given: give that "
                        + "document in 'documents'");
            } else {
                note("#", "the root is the reference '" + ref + "' alone, to a document not given: no class is made "
                        + "of it");
            }
            return;
        }
        if (title == null && at(root) instanceof Json.Obj o) {
            title = text(o, "title");
        }
        String rootName = title != null ? title : options.rootName();
        Shape shape = shapeAt(root);
        if (named.containsKey(root)) {
            // the root names a schema the document names already
            return;
        }
        if (shape != Shape.ALIAS) {
            if (rootName == null) {
                refusals.add("#: the root schema is " + (shape == Shape.CLASS ? "an object" : "an enumeration")
                        + " with no title: name it (the request's 'name')");
                return;
            }
            add(root, rootName);
        } else if (at(root) instanceof Json.Obj rootSchema) {
            // a list of objects at the root: its item a class; a map: its entries
            String item = rootSchema.has("items") ? followed(child(root, "items"), new ArrayList<>()) : null;
            boolean list = item != null && shapeAt(item) == Shape.CLASS && !named.containsKey(item);
            boolean map = !list && !mapValues(rootSchema, root).isEmpty();
            if ((list || map) && rootName == null) {
                refusals.add("#: the root schema is " + (list ? "a list" : "a map") + " with no title: name it (the "
                        + "request's 'name')");
                return;
            }
            if (rootName != null && list && item != null) {
                add(item, pureName(rootName, true, where(root)) + "Item");
            } else if (rootName != null && map) {
                mapRoots.add(root);
                add(root, pureName(rootName, true, where(root)) + "Entry");
            }
        }
        if (named.isEmpty()) {
            refusals.add("#: the document defines no object schema (no properties at its root, no $defs or "
                    + "definitions)");
        }
    }

    /** A place that is a reference alone, followed to what it names; null when a reference names a document not
     *  given (that reference added to {@code unfollowed}). */
    private @com.legend.base.Nullable String followed(String loc, List<String> unfollowed) {
        String place = loc;
        Set<String> seen = new HashSet<>();
        while (at(place) instanceof Json.Obj o && !declaresProperties(o) && objects(o, "allOf").isEmpty()
                && seen.add(place)) {
            String ref = refOf(o);
            if (ref == null) {
                break;
            }
            String t = target(place, ref);
            if (t == null) {
                unfollowed.add(ref);
                return null;
            }
            place = t;
        }
        return place;
    }

    /** The places of a map's values: each {@code patternProperties} schema, then an {@code additionalProperties}
     *  schema; none when a schema is no map. */
    private List<String> mapValues(Json.Obj schema, String loc) {
        List<String> out = new ArrayList<>();
        if (schema.getOr("patternProperties", null) instanceof Json.Obj patterns) {
            for (String key : patterns.fields().keySet()) {
                out.add(child(loc, "patternProperties", key));
            }
        }
        if (schema.getOr("additionalProperties", null) instanceof Json.Obj) {
            out.add(child(loc, "additionalProperties"));
        }
        return out;
    }

    private void collectFrom(String doc, @com.legend.base.Nullable Json.Obj container, String pointer) {
        if (container == null) {
            return;
        }
        for (String key : container.fields().keySet()) {
            add(child(loc(doc, pointer), key), key);
        }
    }

    /** A named schema: its Pure name, numbered when another's (noted). */
    private void add(String loc, String key) {
        String name = pureName(key, true, where(loc));
        String held = unique(name);
        if (!held.equals(name)) {
            note(where(loc), "its name '" + name + "' is another schema's, so it is named " + held);
        }
        named.put(loc, new Named(loc, key, held));
    }

    /** An OpenAPI document's operations: each request and response body written in place, a class named after the
     *  operation. */
    private void operations(Json.Obj doc) {
        Json.Obj paths = objectAt(doc, "paths");
        if (paths == null) {
            return;
        }
        for (Map.Entry<String, Json.Node> p : paths.fields().entrySet()) {
            if (!(p.getValue() instanceof Json.Obj item)) {
                continue;
            }
            for (String method : List.of("get", "put", "post", "delete", "options", "head", "patch", "trace")) {
                if (!(item.getOr(method, null) instanceof Json.Obj op)) {
                    continue;
                }
                String opLoc = child(loc("", ""), "paths", p.getKey(), method);
                String id = text(op, "operationId");
                String base = camelCase(id != null ? id : method + " " + p.getKey(), true);
                // the request: OpenAPI 3's requestBody, Swagger 2's body parameter
                String request = body(child(opLoc, "requestBody"));
                if (request == null && op.getOr("parameters", null) instanceof Json.Arr params) {
                    for (int i = 0; i < params.items().size(); i++) {
                        if (params.items().get(i) instanceof Json.Obj param && "body".equals(text(param, "in"))
                                && param.has("schema")) {
                            request = child(opLoc, "parameters", Integer.toString(i), "schema");
                        }
                    }
                }
                addBody(request, base + "Request");
                Json.Obj responses = objectAt(op, "responses");
                if (responses != null) {
                    for (String code : responses.fields().keySet()) {
                        if (code.startsWith("2")) {
                            String response = child(opLoc, "responses", code);
                            Json.Node r = at(response);
                            if (r instanceof Json.Obj ro && ro.has("schema")) {
                                addBody(child(response, "schema"), base + "Response");
                            } else {
                                addBody(body(response), base + "Response");
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /** The schema of a request or response body (OpenAPI 3: its content's JSON media type, else the first). */
    private @com.legend.base.Nullable String body(String loc) {
        String place = loc;
        Json.Node node = at(place);
        String ref = node instanceof Json.Obj o ? refOf(o) : null;
        if (ref != null) {
            String t = target(place, ref);
            if (t == null) {
                return null;
            }
            place = t;
            node = at(place);
        }
        if (!(node instanceof Json.Obj o) || !(o.getOr("content", null) instanceof Json.Obj content)) {
            return null;
        }
        String chosen = null;
        for (String media : content.fields().keySet()) {
            if (content.get(media) instanceof Json.Obj m && m.has("schema")
                    && (chosen == null || media.contains("json"))) {
                chosen = child(place, "content", media, "schema");
                if (media.contains("json")) {
                    break;
                }
            }
        }
        return chosen;
    }

    /** An operation's body: a class when it is an object written in place, its item's when a list of one. */
    private void addBody(@com.legend.base.Nullable String loc, String name) {
        if (loc == null || !(at(loc) instanceof Json.Obj s) || refOf(s) != null) {
            return;
        }
        if (shapeAt(loc) == Shape.CLASS) {
            add(loc, name);
        } else if (at(child(loc, "items")) instanceof Json.Obj items && refOf(items) == null
                && shapeAt(child(loc, "items")) == Shape.CLASS) {
            add(child(loc, "items"), name + "Item");
        }
    }

    // ---------------------------------------------------------------------
    // Shapes
    // ---------------------------------------------------------------------

    private Shape shapeAt(String loc) {
        Shape known = shapes.get(loc);
        if (known != null) {
            return known;
        }
        // a place that reaches itself through references alone is no class
        shapes.put(loc, Shape.ALIAS);
        Json.Node node = at(loc);
        Shape s = !(node instanceof Json.Obj o) ? Shape.ALIAS
                : textEnum(o, named.containsKey(loc) ? 1 : 2) || constAlternatives(o) != null ? Shape.ENUMERATION
                : objectShaped(o, loc) ? Shape.CLASS : Shape.ALIAS;
        shapes.put(loc, s);
        return s;
    }

    /** Whether a schema is an object: properties of its own or of a condition, an allOf naming an object, a union of
     *  objects, or a reference to a class beside properties. */
    private boolean objectShaped(Json.Obj schema, String loc) {
        if (declaresProperties(schema)) {
            return true;
        }
        List<Json.Obj> parts = objects(schema, "allOf");
        for (int i = 0; i < parts.size(); i++) {
            if (isObject(parts.get(i), child(loc, "allOf", Integer.toString(i)))) {
                return true;
            }
        }
        return unionOfObjects(schema, loc) != null;
    }

    private static boolean declaresProperties(Json.Obj schema) {
        if (schema.getOr("properties", null) instanceof Json.Obj p && !p.fields().isEmpty()) {
            return true;
        }
        for (Json.Obj c : conditions(schema)) {
            if (c.getOr("properties", null) instanceof Json.Obj p && !p.fields().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** The schemas a schema applies when a condition holds: {@code then}, {@code else}, {@code dependentSchemas}' and
     *  {@code dependencies}' schemas. */
    private static List<Json.Obj> conditions(Json.Obj schema) {
        List<Json.Obj> out = new ArrayList<>();
        for (String k : List.of("then", "else")) {
            if (schema.getOr(k, null) instanceof Json.Obj c) {
                out.add(c);
            }
        }
        for (String k : List.of("dependentSchemas", "dependencies")) {
            if (schema.getOr(k, null) instanceof Json.Obj d) {
                for (Json.Node v : d.fields().values()) {
                    if (v instanceof Json.Obj c) {
                        out.add(c);
                    }
                }
            }
        }
        return out;
    }

    /** Whether a part of an allOf or a union is an object: a reference to a class, or an object written in place. */
    private boolean isObject(Json.Obj part, String loc) {
        String ref = refOf(part);
        if (ref != null) {
            String t = target(loc, ref);
            return (t != null && shapeAt(t) == Shape.CLASS) || declaresProperties(part);
        }
        return objectShaped(part, loc);
    }

    /** The union keyword ({@code oneOf}, {@code anyOf}) whose alternatives but {@code null} are two or more objects. */
    private @com.legend.base.Nullable String unionOfObjects(Json.Obj schema, String loc) {
        for (String union : List.of("oneOf", "anyOf")) {
            List<Json.Obj> parts = objects(schema, union);
            int values = 0;
            boolean all = true;
            for (int i = 0; i < parts.size(); i++) {
                if (isNull(parts.get(i))) {
                    continue;
                }
                values++;
                all &= isObject(parts.get(i), child(loc, union, Integer.toString(i)));
            }
            if (values >= 2 && all) {
                return union;
            }
        }
        return null;
    }

    /** Whether a schema is an enumeration of {@code least} texts or more (and maybe null): two, inline, where one is a
     *  constant; one, of a schema the document names. */
    private static boolean textEnum(Json.Obj schema, int least) {
        if (!(schema.getOr("enum", null) instanceof Json.Arr values)) {
            return false;
        }
        Set<String> texts = new LinkedHashSet<>();
        for (Json.Node v : values.items()) {
            if (v instanceof Json.Str s) {
                texts.add(s.value());
            } else if (!(v instanceof Json.Null)) {
                return false;
            }
        }
        return texts.size() >= least;
    }

    /** A union's alternatives when each but null is one text ({@code const}, or an {@code enum} of one), two or more:
     *  an enumeration whose values are documented one by one. */
    private static @com.legend.base.Nullable List<Json.Obj> constAlternatives(Json.Obj schema) {
        for (String union : List.of("oneOf", "anyOf")) {
            List<Json.Obj> parts = objects(schema, union);
            List<Json.Obj> out = new ArrayList<>();
            for (Json.Obj p : parts) {
                if (isNull(p)) {
                    continue;
                }
                if (constText(p) == null) {
                    out = null;
                    break;
                }
                out.add(p);
            }
            if (out != null && out.size() >= 2) {
                return out;
            }
        }
        return null;
    }

    private static @com.legend.base.Nullable String constText(Json.Obj schema) {
        if (schema.getOr("const", null) instanceof Json.Str s) {
            return s.value();
        }
        if (schema.getOr("enum", null) instanceof Json.Arr a && a.items().size() == 1
                && a.items().get(0) instanceof Json.Str s) {
            return s.value();
        }
        return null;
    }

    // ---------------------------------------------------------------------
    // The model
    // ---------------------------------------------------------------------

    private void build() {
        Set<String> chosen = options.only().isEmpty() ? named.keySet() : reached(options.only());
        for (Named n : named.values()) {
            if (!chosen.contains(n.loc())) {
                continue;
            }
            if (mapRoots.contains(n.loc())) {
                entries(n.name(), n.loc());
                continue;
            }
            switch (shapeAt(n.loc())) {
                case CLASS -> classFor(n.loc(), n.name(), false);
                case ENUMERATION -> enumFor(n.loc(), n.name());
                case ALIAS -> {
                    // no element: each reference reads it as what it is; read once, for its notes
                    alias(n.loc(), "", n.name());
                }
            }
        }
    }

    /** The schemas {@code only} names and every named schema they reach, by place. */
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
                work.add(found.loc());
            }
        }
        Set<String> walked = new HashSet<>();
        while (!work.isEmpty()) {
            String place = work.pop();
            if (!walked.add(place)) {
                continue;
            }
            if (named.containsKey(place)) {
                out.add(place);
            }
            List<String> refs = new ArrayList<>();
            refsIn(place, at(place), refs);
            for (String t : refs) {
                // a reference into another schema's insides reaches what it names, and what that reaches
                work.add(t);
            }
        }
        return out;
    }

    private void refsIn(String loc, @com.legend.base.Nullable Json.Node node, List<String> out) {
        if (node instanceof Json.Obj o) {
            String ref = refOf(o);
            if (ref != null) {
                String t = target(loc, ref);
                if (t != null) {
                    out.add(t);
                }
            }
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                if (!Set.of("enum", "const", "default", "examples", "example").contains(e.getKey())) {
                    refsIn(child(loc, e.getKey()), e.getValue(), out);
                }
            }
        } else if (node instanceof Json.Arr a) {
            for (int i = 0; i < a.items().size(); i++) {
                refsIn(child(loc, Integer.toString(i)), a.items().get(i), out);
            }
        }
    }

    /** The class at a place: made once, however many places name it; {@code name} its name when it is new. */
    private String classFor(String loc, @com.legend.base.Nullable String name, boolean owned) {
        String known = classAt.get(loc);
        if (known != null) {
            if (!owned) {
                shared.add(known);
            }
            return known;
        }
        Named n = named.get(loc);
        String held = n != null ? n.name() : unique(name != null ? name : nameFor(loc));
        classAt.put(loc, held);
        buildClass(held, schemaAt(loc), loc, owned && n == null);
        return held;
    }

    /** A name for a place no schema names: its last meaningful parts ({@code #/definitions/A/properties/b} is AB). */
    private String nameFor(String loc) {
        String pointer = loc.substring(loc.indexOf('#') + 1);
        List<String> parts = new ArrayList<>();
        for (String p : parts(pointer, '/')) {
            String key = p.replace("~1", "/").replace("~0", "~");
            if (!key.isEmpty() && !Set.of("definitions", "$defs", "properties", "components", "schemas", "items",
                    "allOf", "oneOf", "anyOf", "additionalProperties", "then", "else").contains(key)
                    && !digits(key)) {
                parts.add(key);
            }
        }
        if (parts.isEmpty()) {
            String doc = docOf(loc);
            parts.add(doc.isEmpty() ? "Root" : withoutExtension(doc.substring(doc.lastIndexOf('/') + 1)));
        }
        String joined = String.join(" ", parts.subList(Math.max(0, parts.size() - 2), parts.size()));
        return camelCase(joined, true);
    }

    /** A class: its superclasses and properties, gathered through allOf and its conditions; added before any class
     *  it owns. */
    private void buildClass(String name, Json.Obj schema, String loc, boolean owned) {
        int position = classes.size();
        Gathered g = new Gathered(loc);
        gather(schema, loc, g, false);
        // then the conditions' properties, after the class's own
        int before = g.properties.size();
        for (int i = 0; i < g.conditions.size(); i++) {
            gather(schemaAt(g.conditions.get(i)), g.conditions.get(i), g, true);
        }
        if (g.properties.size() > before) {
            note(where(loc), "the properties of a condition (if/then/else, dependentSchemas) are read as optional "
                    + "properties");
        }
        List<ImportedProperty> out = new ArrayList<>();
        List<Rule> rules = new ArrayList<>();
        Set<String> names = new HashSet<>();
        Map<String, String> held = new HashMap<>();
        Set<String> nevers = new HashSet<>();
        for (Map.Entry<String, PropertyAt> e : g.properties.entrySet()) {
            Json.Node ps = e.getValue().schema();
            String at = e.getValue().loc();
            String property = pureName(e.getKey(), false, where(at));
            String pureName = property;
            for (int n = 2; !names.add(pureName); n++) {
                pureName = property + n;
            }
            if (!pureName.equals(property)) {
                note(where(at), "its name '" + property + "' is another property's, so it is named " + pureName);
            }
            directs.add(at);
            Resolved r = resolve(ps, at, name, pureName);
            if (r == null) {
                continue;
            }
            if (r == NEVER) {
                note(where(at), "a property that can hold no value is not a property");
                nevers.add(e.getKey());
                continue;
            }
            boolean mustHave = g.required.contains(e.getKey()) && !g.conditional.contains(e.getKey()) && !r.nullable();
            int lower = r.many() ? (mustHave ? r.minItems() : 0) : (mustHave ? 1 : 0);
            Json.Obj pso = ps instanceof Json.Obj o ? o : Json.obj();
            out.add(new ImportedProperty(pureName, doc(pso), r.kind(), r.type(), lower, r.many() ? r.maxItems()
                    : Integer.valueOf(1), deprecated(pso), defaultOf(pso, r, at)));
            held.put(e.getKey(), pureName);
            for (Rule rule : r.rules()) {
                rules.add(of(rule, pureName));
            }
        }
        // the properties the class's logic names: its own, and its superclasses', by their JSON names
        Map<String, ImportedProperty> scope = new LinkedHashMap<>();
        for (String s : g.supers) {
            scope.putAll(keysOf.getOrDefault(s, Map.of()));
        }
        for (Map.Entry<String, String> h : held.entrySet()) {
            for (ImportedProperty p : out) {
                if (p.name().equals(h.getValue())) {
                    scope.put(h.getKey(), p);
                }
            }
        }
        for (String never : nevers) {
            // never present: its logic's requiring it false, its properties' rules of it nothing
            scope.put(never, new ImportedProperty("", null, Kind.PRIMITIVE, "", 0, 0));
        }
        keysOf.put(name, scope);
        List<Logic> logic = new ArrayList<>();
        for (Map.Entry<String, String> l : g.logic.entrySet()) {
            String place = l.getKey().substring(0, l.getKey().lastIndexOf('?'));
            String kind = l.getKey().substring(l.getKey().lastIndexOf('?') + 1);
            Json.Obj at = at(place) instanceof Json.Obj o ? o : Json.obj();
            ImportedProperty depending = scope.get(l.getValue());
            String named = switch (kind) {
                case "dependency" -> (depending != null ? depending.name() : camelCase(l.getValue(), false))
                        + "Dependency";
                default -> l.getValue();
            };
            logic(logic, named, place, kind, () -> switch (kind) {
                case "if" -> whole(only(at, "if", "then", "else"), place, scope, new HashSet<>());
                case "not" -> not(whole(at, place, scope, new HashSet<>()));
                case "dependency" -> {
                    ImportedProperty p = property(scope, l.getValue());
                    yield any(List.of(not(has(p)), whole(at, place, scope, new HashSet<>())));
                }
                default -> whole(only(at, kind), place, scope, new HashSet<>());
            });
        }
        // a property's own logic over its value: its not, its if/then/else
        for (Map.Entry<String, PropertyAt> e : g.properties.entrySet()) {
            ImportedProperty p = scope.get(e.getKey());
            if (p == null || !held.containsKey(e.getKey()) || !(e.getValue().schema() instanceof Json.Obj ps)) {
                continue;
            }
            String at = e.getValue().loc();
            if (ps.has("not")) {
                logic(logic, p.name() + "Not", child(at, "not"), "not", () -> any(List.of(not(has(p)),
                        not(value(p, ps.get("not"), child(at, "not"), new HashSet<>(), false)))));
            }
            if (ps.has("if")) {
                logic(logic, p.name() + "Condition", at, "if", () -> any(List.of(not(has(p)),
                        value(p, only(ps, "if", "then", "else"), at, new HashSet<>(), false))));
            }
        }
        for (Map.Entry<String, List<String>> q : g.requires.entrySet()) {
            String property = held.get(q.getKey());
            List<String> others = new ArrayList<>();
            for (String other : q.getValue()) {
                String h = held.get(other);
                if (h != null) {
                    others.add(h);
                } else {
                    property = null;
                }
            }
            if (property == null) {
                note(where(loc), "a dependentRequired naming a property the schema does not declare of its own is not "
                        + "carried");
            } else if (!others.isEmpty()) {
                rules.add(new Requires(property, false, others));
            }
        }
        // a union of objects: its alternatives subclasses; written in place, each a value the owner holds
        String union = unionOfObjects(schema, loc);
        boolean inPlace = union == null || alternatives(name, schema, union, loc);
        classes.add(position, new ImportedClass(name, doc(schema), g.supers, out, owned && inPlace, deprecated(schema),
                rules, logic));
    }

    /** A map at the root: a class of its entries, each its key and its value ({@code <Root>Value} when the value is an
     *  object of its own); the key's pattern ({@code patternProperties}' one, {@code propertyNames}') its rules. */
    private void entries(String name, String loc) {
        int position = classes.size();
        Json.Obj schema = schemaAt(loc);
        keywords(schema, loc);
        note(where(loc), "a map at the root is a class of its entries, each its key and its value");
        String owner = name.endsWith("Entry") ? name.substring(0, name.length() - "Entry".length()) : name;
        List<ImportedProperty> properties = new ArrayList<>();
        List<Rule> rules = new ArrayList<>();
        properties.add(new ImportedProperty("key", "The entry's key: the name the map holds its value by.",
                Kind.PRIMITIVE, "String", 1, 1));
        List<String> values = mapValues(schema, loc);
        if (values.size() == 1 && values.get(0).startsWith(child(loc, "patternProperties"))
                && schema.getOr("patternProperties", null) instanceof Json.Obj patterns) {
            rules.add(new Pattern("key", false, patterns.fields().keySet().iterator().next()));
        }
        if (schema.getOr("propertyNames", null) instanceof Json.Obj names) {
            for (Rule r : rules(names, "String", child(loc, "propertyNames"))) {
                rules.add(of(r, "key"));
            }
        }
        Resolved r;
        Json.Obj valueSchema = Json.obj();
        if (values.size() == 1) {
            Json.Node v = at(values.get(0));
            valueSchema = v instanceof Json.Obj o ? o : valueSchema;
            r = resolve(v == null ? Json.bool(true) : v, values.get(0), owner, "value");
        } else {
            note(where(loc), "a map whose values are of several schemas (patternProperties) holds each a Variant");
            r = new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        if (r != null && r != NEVER) {
            int lower = r.nullable() ? 0 : r.many() ? r.minItems() : 1;
            properties.add(new ImportedProperty("value", doc(valueSchema), r.kind(), r.type(), lower,
                    r.many() ? r.maxItems() : Integer.valueOf(1), deprecated(valueSchema), null));
            for (Rule rule : r.rules()) {
                rules.add(of(rule, "value"));
            }
        }
        classes.add(position, new ImportedClass(name, doc(schema), List.of(), properties, false, deprecated(schema),
                rules, List.of()));
    }

    /** A rule of the property {@code property}. */
    private static Rule of(Rule r, String property) {
        return switch (r) {
            case Bound b -> new Bound(property, b.each(), b.value(), b.lower(), b.exclusive());
            case Length l -> new Length(property, l.each(), l.value(), l.lower());
            case Pattern p -> new Pattern(property, p.each(), p.regex());
            case MultipleOf m -> new MultipleOf(property, m.each(), m.value());
            case OneOf o -> new OneOf(property, o.each(), o.values());
            case Unique u -> new Unique(property, u.each());
            case Requires q -> new Requires(property, q.each(), q.others());
        };
    }

    /** A rule of each value of a list. */
    private static Rule each(Rule r) {
        return switch (r) {
            case Bound b -> new Bound(b.property(), true, b.value(), b.lower(), b.exclusive());
            case Length l -> new Length(l.property(), true, l.value(), l.lower());
            case Pattern p -> new Pattern(p.property(), true, p.regex());
            case MultipleOf m -> new MultipleOf(m.property(), true, m.value());
            case OneOf o -> new OneOf(o.property(), true, o.values());
            // the list's own, never its values': a list of lists is a Variant
            case Unique u -> u;
            case Requires q -> q;
        };
    }

    private void gather(Json.Obj schema, String loc, Gathered g, boolean conditional) {
        keywords(schema, loc);
        if (!g.visited.add(loc)) {
            return;
        }
        String ref = refOf(schema);
        if (ref != null) {
            reference(g, ref, loc, conditional);
        }
        List<Json.Obj> parts = objects(schema, "allOf");
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj part = parts.get(i);
            String at = child(loc, "allOf", Integer.toString(i));
            String partRef = refOf(part);
            if (partRef != null) {
                keywords(part, at);
                reference(g, partRef, at, conditional);
                if (declaresProperties(part)) {
                    gatherProperties(part, at, g, conditional);
                }
            } else {
                gather(part, at, g, conditional);
            }
        }
        gatherProperties(schema, loc, g, conditional);
        if (conditional) {
            // a union inside a condition: each alternative's properties optional too
            for (String union : List.of("oneOf", "anyOf")) {
                List<Json.Obj> alternatives = objects(schema, union);
                for (int i = 0; i < alternatives.size(); i++) {
                    gather(alternatives.get(i), child(loc, union, Integer.toString(i)), g, true);
                }
            }
        }
        for (String k : List.of("then", "else")) {
            if (schema.getOr(k, null) instanceof Json.Obj) {
                g.conditions.add(child(loc, k));
            }
        }
        for (String k : List.of("dependentSchemas", "dependencies")) {
            if (schema.getOr(k, null) instanceof Json.Obj d) {
                for (Map.Entry<String, Json.Node> e : d.fields().entrySet()) {
                    if (e.getValue() instanceof Json.Obj) {
                        g.conditions.add(child(loc, k, e.getKey()));
                    }
                }
            }
        }
        if (!conditional) {
            // the logic over a whole value: a union of objects is the class's alternatives, not its logic
            String alternatives = unionOfObjects(schema, loc);
            if (schema.has("if")) {
                g.logic.put(loc + "?if", "condition");
            }
            if (schema.has("not")) {
                g.logic.put(child(loc, "not") + "?not", "exclusion");
            }
            for (String union : List.of("oneOf", "anyOf")) {
                if (schema.getOr(union, null) instanceof Json.Arr && !union.equals(alternatives)) {
                    g.logic.put(loc + "?" + union, union);
                }
            }
            for (String k : List.of("dependentSchemas", "dependencies")) {
                if (schema.getOr(k, null) instanceof Json.Obj d) {
                    for (Map.Entry<String, Json.Node> e : d.fields().entrySet()) {
                        if (!(e.getValue() instanceof Json.Arr)) {
                            g.logic.put(child(loc, k, e.getKey()) + "?dependency", e.getKey());
                        }
                    }
                }
            }
            for (String k : List.of("dependentRequired", "dependencies")) {
                if (schema.getOr(k, null) instanceof Json.Obj d) {
                    for (Map.Entry<String, Json.Node> e : d.fields().entrySet()) {
                        if (e.getValue() instanceof Json.Arr names) {
                            List<String> others = g.requires.computeIfAbsent(e.getKey(), x -> new ArrayList<>());
                            for (Json.Node n : names.items()) {
                                if (n instanceof Json.Str name && !others.contains(name.value())) {
                                    others.add(name.value());
                                }
                            }
                        }
                    }
                }
            }
        }
        boolean hasOwn = schema.getOr("properties", null) instanceof Json.Obj;
        if (hasOwn && (schema.getOr("additionalProperties", null) instanceof Json.Obj
                || schema.has("patternProperties"))) {
            note(where(loc), "a map beside the properties (additionalProperties, patternProperties) is not carried: a "
                    + "class has the properties it names");
        }
    }

    private void gatherProperties(Json.Obj schema, String loc, Gathered g, boolean conditional) {
        Json.Obj props = objectAt(schema, "properties");
        if (props != null) {
            for (Map.Entry<String, Json.Node> e : props.fields().entrySet()) {
                PropertyAt known = g.properties.get(e.getKey());
                if (known != null) {
                    if (!conditional) {
                        g.conditional.remove(e.getKey());
                    }
                    // a branch forbidding the property (null, false) does not hide another's declaring it
                    if (forbids(known.schema()) && !forbids(e.getValue())) {
                        g.properties.put(e.getKey(), new PropertyAt(e.getValue(), child(loc, "properties",
                                e.getKey())));
                    }
                    continue;
                }
                g.properties.put(e.getKey(), new PropertyAt(e.getValue(), child(loc, "properties", e.getKey())));
                if (conditional) {
                    g.conditional.add(e.getKey());
                }
                // draft 3: a property says it is required
                if (!conditional && e.getValue() instanceof Json.Obj ps && isTrue(ps, "required")) {
                    g.required.add(e.getKey());
                }
            }
        }
        if (!conditional && schema.getOr("required", null) instanceof Json.Arr names) {
            for (Json.Node n : names.items()) {
                if (n instanceof Json.Str s) {
                    g.required.add(s.value());
                }
            }
        }
    }

    /** Whether a property's schema forbids it a value: {@code false}, a {@code null} type, {@code not: {}}. */
    private static boolean forbids(Json.Node schema) {
        return schema instanceof Json.Bool b ? !b.value()
                : schema instanceof Json.Obj o && (isNull(o) || (o.getOr("not", null) instanceof Json.Obj n
                        && n.fields().isEmpty()));
    }

    /** A reference in an allOf or beside properties: a superclass; inside a condition, its properties optional ones
     *  (a value keeps it only when the condition holds). */
    private void reference(Gathered g, String ref, String at, boolean conditional) {
        if (!conditional) {
            addSuper(g, ref, at, at);
            return;
        }
        String t = target(at, ref);
        if (t == null) {
            note(where(at), "the reference '" + ref + "' names nothing in the documents given: its properties are not "
                    + "read");
        } else if (at(t) instanceof Json.Obj o) {
            gather(o, t, g, true);
        }
    }

    /** A superclass from a reference in an allOf or beside properties: a class's; else noted. */
    private void addSuper(Gathered g, String ref, String from, String at) {
        String t = target(from, ref);
        if (t == null) {
            note(where(at), "the reference '" + ref + "' names nothing in the documents given: its properties are not "
                    + "inherited");
            return;
        }
        if (t.equals(g.self)) {
            note(where(at), "an allOf naming its own schema names no superclass, and is skipped");
            return;
        }
        if (shapeAt(t) == Shape.CLASS) {
            String s = classFor(t, null, false);
            if (!g.supers.contains(s)) {
                g.supers.add(s);
            }
        } else {
            note(where(at), "an allOf part that is not an object (a map, a primitive) is not carried: a class has the "
                    + "properties it names");
        }
    }

    /**
     * A UNION OF OBJECTS read as inheritance (the study's §4.2): each alternative a subclass of {@code superclass} --
     * a referenced class gains it as a superclass, an object written in place is a class of its own, named after the
     * union and its title (or {@code Option1}, {@code Option2}, ...). An anyOf is read as a oneOf: an instance is one
     * class.
     *
     * @return whether every alternative is written in place (each a value its owner holds)
     */
    private boolean alternatives(String superclass, Json.Obj schema, String union, String loc) {
        if (union.equals("anyOf")) {
            note(where(loc), "anyOf is read as oneOf (an instance is one of its alternatives)");
        }
        List<Json.Obj> parts = objects(schema, union);
        boolean inPlace = true;
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj part = parts.get(i);
            String at = child(loc, union, Integer.toString(i));
            if (isNull(part)) {
                continue;
            }
            String ref = refOf(part);
            String sub;
            if (ref != null && !declaresProperties(part)) {
                keywords(part, at);
                String t = target(at, ref);
                if (t == null) {
                    continue;
                }
                sub = classFor(t, null, false);
                inPlace = false;
            } else {
                String title = text(part, "title");
                sub = classFor(at, superclass + (title != null ? pureName(title, true, where(at)) : "Option" + (i + 1)),
                        true);
            }
            unionSupers.computeIfAbsent(sub, k -> new LinkedHashSet<>()).add(superclass);
        }
        return inPlace;
    }

    /** Each class's superclasses as an alternative of a union, added; each class a place outside its owner names
     *  marked shared; a class that would be its own superclass through others loses the superclass closing the
     *  circle, noted. */
    private void unite() {
        for (int i = 0; i < classes.size(); i++) {
            ImportedClass c = classes.get(i);
            Set<String> extra = unionSupers.getOrDefault(c.name(), Set.of());
            List<String> supers = new ArrayList<>(c.supers());
            for (String s : extra) {
                if (!s.equals(c.name()) && !supers.contains(s)) {
                    supers.add(s);
                }
            }
            boolean owned = c.owned() && !shared.contains(c.name());
            classes.set(i, new ImportedClass(c.name(), c.doc(), supers, c.properties(), owned, c.deprecated(),
                    c.rules(), c.logic()));
        }
        Map<String, List<String>> graph = new HashMap<>();
        for (ImportedClass c : classes) {
            graph.put(c.name(), new ArrayList<>(c.supers()));
        }
        for (int i = 0; i < classes.size(); i++) {
            ImportedClass c = classes.get(i);
            List<String> path;
            while ((path = cycle(c.name(), c.name(), graph, new ArrayList<>(), new HashSet<>())) != null) {
                // the circle's first step from this class is dropped: the class keeps its other superclasses
                String dropped = path.get(1);
                graph.computeIfAbsent(c.name(), k -> new ArrayList<>()).remove(dropped);
                note(c.name(), "a class that would be its own superclass (" + String.join(" extends ", path)
                        + ") does not extend " + dropped);
            }
            if (graph.getOrDefault(c.name(), List.of()).size() != c.supers().size()) {
                classes.set(i, new ImportedClass(c.name(), c.doc(), graph.getOrDefault(c.name(), List.of()),
                        c.properties(), c.owned(), c.deprecated(), c.rules(), c.logic()));
            }
        }
    }

    private static @com.legend.base.Nullable List<String> cycle(String start, String at,
            Map<String, List<String>> graph,
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

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    /** A property's type, {@link #NEVER} when it can hold no value, or null when it has no Pure form (refused). */
    private @com.legend.base.Nullable Resolved resolve(Json.Node node, String loc, String owner, String property) {
        if (node instanceof Json.Bool b) {
            return b.value() ? new Resolved(Kind.PRIMITIVE, VARIANT, false) : NEVER;
        }
        if (!(node instanceof Json.Obj schema)) {
            note(where(loc), "a schema that is not a JSON object or a boolean is any value: a Variant");
            return new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        keywords(schema, loc);
        if (!directs.contains(loc)) {
            if (schema.has("not")) {
                note(where(loc), "not is not carried (a validation Pure states no constraint for here)");
            }
            if (schema.has("if") && !declaresProperties(schema)) {
                note(where(loc), "a condition (if/then/else) on a value that is no property's own is not carried");
            }
        }
        boolean empty = isTrue(schema, "nullable") || isTrue(schema, "x-nullable");
        String ref = refOf(schema);
        if (ref != null) {
            if (declaresProperties(schema)) {
                // a reference beside properties: a class of its own, the referenced class its superclass
                return new Resolved(Kind.CLASS, classFor(loc, owner + upperFirst(property), true), empty);
            }
            String t = target(loc, ref);
            if (t == null) {
                note(where(loc), "the reference '" + ref + "' names nothing in the documents given: a Variant (give "
                        + "the document it names in 'documents')");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
            Resolved r = resolveAt(t, owner, property);
            if (r == null || r == NEVER) {
                return r;
            }
            return r.orEmpty(empty).withRules(r.kind() == Kind.PRIMITIVE && !r.isVariant()
                    ? rules(schema, r.type(), loc) : List.of());
        }
        if (shapeAt(loc) == Shape.ENUMERATION) {
            return new Resolved(Kind.ENUMERATION, enumFor(loc, owner + upperFirst(property)),
                    empty || mayBeNull(schema));
        }
        List<Json.Node> values = values(schema);
        if (values != null) {
            return valued(schema, values, loc, empty);
        }
        if (declaresProperties(schema)) {
            // properties of its own: a class, whatever its union says (a union of required lists is a validation)
            return new Resolved(Kind.CLASS, classFor(loc, owner + upperFirst(property), true), empty);
        }
        for (String union : List.of("oneOf", "anyOf")) {
            List<Json.Obj> parts = objects(schema, union);
            if (parts.isEmpty()) {
                continue;
            }
            int count = 0;
            int one = -1;
            for (int i = 0; i < parts.size(); i++) {
                if (!isNull(parts.get(i))) {
                    count++;
                    one = i;
                }
            }
            boolean mayBeNull = count < parts.size();
            if (count == 0) {
                note(where(loc), "a value that is always null is not a property");
                return NEVER;
            }
            if (count == 1 && !declaresProperties(schema)) {
                Resolved r = resolve(parts.get(one), child(loc, union, Integer.toString(one)), owner, property);
                return r == null || r == NEVER ? r : r.orEmpty(mayBeNull || empty);
            }
            if (unionOfObjects(schema, loc) != null) {
                return new Resolved(Kind.CLASS, classFor(loc, owner + upperFirst(property), true), mayBeNull || empty);
            }
            String common = commonPrimitive(parts, loc, union);
            if (common != null) {
                return new Resolved(Kind.PRIMITIVE, common, mayBeNull || empty);
            }
            note(where(loc), "a union (" + union + ") of values that are not all objects is a Variant (Pure has no "
                    + "union type)");
            return new Resolved(Kind.PRIMITIVE, VARIANT, mayBeNull || empty);
        }
        if (objectShaped(schema, loc)) {
            return new Resolved(Kind.CLASS, classFor(loc, owner + upperFirst(property), true), empty);
        }
        List<Json.Obj> all = objects(schema, "allOf");
        if (!all.isEmpty()) {
            // an allOf of no object: its one part that says a shape (the others annotations), else the shape they share
            List<Integer> shaped = new ArrayList<>();
            boolean partNull = false;
            for (int i = 0; i < all.size(); i++) {
                partNull |= isTrue(all.get(i), "nullable");
                if (saysShape(all.get(i))) {
                    shaped.add(i);
                }
            }
            if (shaped.size() == 1) {
                int one = shaped.get(0);
                Resolved r = resolve(all.get(one), child(loc, "allOf", Integer.toString(one)), owner, property);
                return r == null || r == NEVER ? r : r.orEmpty(empty || partNull).withRules(
                        r.kind() == Kind.PRIMITIVE && !r.isVariant() ? rules(schema, r.type(), loc) : List.of());
            }
            if (shaped.size() > 1) {
                String common = commonPrimitive(all, loc, "allOf");
                if (common != null) {
                    return new Resolved(Kind.PRIMITIVE, common, empty || partNull);
                }
                note(where(loc), "an allOf of values that are not objects is a Variant");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty || partNull);
            }
        }
        return typed(schema, loc, owner, property, empty);
    }

    /** The type a schema's {@code type} says, or its keywords imply. */
    private @com.legend.base.Nullable Resolved typed(Json.Obj schema, String loc, String owner, String property,
            boolean nullable) {
        boolean empty = nullable;
        List<String> types = types(schema);
        if (types.remove("null")) {
            empty = true;
            if (types.isEmpty()) {
                note(where(loc), "a value that is always null is not a property");
                return NEVER;
            }
        }
        if (types.size() > 1) {
            if (types.size() == 2 && types.contains("integer") && types.contains("number")) {
                return new Resolved(Kind.PRIMITIVE, "Number", empty).withRules(rules(schema, "Number", loc));
            }
            note(where(loc), "a value of several types (" + String.join(", ", types) + ") is a Variant (Pure has no "
                    + "union type)");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        String t = types.isEmpty() ? implied(schema) : types.get(0);
        switch (t) {
            case "array" -> {
                return list(schema, loc, owner, property, empty);
            }
            case "string", "integer", "number", "boolean" -> {
                String primitive = primitive(t, text(schema, "format"));
                return new Resolved(Kind.PRIMITIVE, primitive, empty).withRules(rules(schema, primitive, loc));
            }
            case "object", "" -> {
                // a free-form object, a map, or any value at all: semi-structured, as Pure holds JSON
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
            case "file" -> {
                note(where(loc), "a file (Swagger 2) is a Variant");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
            default -> {
                note(where(loc), "the type '" + t + "' is not JSON Schema's: a Variant");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
        }
    }

    private @com.legend.base.Nullable Resolved list(Json.Obj schema, String loc, String owner, String property,
            boolean empty) {
        Json.Node items = schema.getOr("items", null);
        if (items instanceof Json.Arr || schema.has("prefixItems")) {
            note(where(loc), "a tuple (prefixItems, items as a list) is a Variant, the JSON list it is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        int least = schema.getOr("minItems", null) instanceof Json.Num n ? (int) n.longValue() : 0;
        Integer most = schema.getOr("maxItems", null) instanceof Json.Num n ? Integer.valueOf((int) n.longValue())
                : null;
        if (items == null || (items instanceof Json.Bool b && b.value())) {
            note(where(loc), "a list of any values is one Variant, the JSON list it is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        Resolved item = resolve(items, child(loc, "items"), owner, singular(property));
        if (item == null) {
            return null;
        }
        if (item == NEVER) {
            note(where(loc), "a list that can hold no value is one Variant, the empty JSON list");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        if (item.many() || item.isVariant()) {
            note(where(loc), "a list of " + (item.many() ? "lists" : "Variants") + " is one Variant, the JSON list it "
                    + "is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        if (options.listsAsVariant() && item.kind() != Kind.CLASS) {
            note(where(loc), "a list of " + item.type() + " is one Variant, the JSON list a column holds ('lists': "
                    + "'variant')");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        List<Rule> eachRules = new ArrayList<>();
        for (Rule r : item.rules()) {
            eachRules.add(each(r));
        }
        if (isTrue(schema, "uniqueItems")) {
            if (item.kind() == Kind.CLASS) {
                note(where(loc), "uniqueItems of a list of objects is not carried (Pure tells objects apart by "
                        + "identity, JSON by value)");
            } else {
                eachRules.add(new Unique("", false));
            }
        }
        return new Resolved(item.kind(), item.type(), true, empty, least, most, eachRules);
    }

    /** A place another names, read as what it is made: a class, an enumeration, or what its schema says. */
    private @com.legend.base.Nullable Resolved resolveAt(String loc, String owner, String property) {
        Named n = named.get(loc);
        return switch (shapeAt(loc)) {
            case CLASS -> new Resolved(Kind.CLASS, classFor(loc, null, false), false);
            case ENUMERATION -> new Resolved(Kind.ENUMERATION, enumFor(loc, n != null ? n.name() : nameFor(loc)),
                    at(loc) instanceof Json.Obj o && mayBeNull(o));
            case ALIAS -> alias(loc, owner, n != null ? n.name() : property);
        };
    }

    /** A place that is no element, read as what it is: once, wherever it is named; one reaching itself through
     *  references alone is any value. */
    private @com.legend.base.Nullable Resolved alias(String loc, String owner, String property) {
        if (aliases.containsKey(loc)) {
            return aliases.get(loc);
        }
        if (!resolving.add(loc)) {
            note(where(loc), "a schema that is itself, through references alone, is any value: a Variant");
            return new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        Json.Node node = at(loc);
        Resolved r = resolve(node == null ? Json.bool(true) : node, loc, owner, property);
        resolving.remove(loc);
        aliases.put(loc, r);
        return r;
    }

    /** A {@code const} or an {@code enum} not made an enumeration: the type of its values, which a rule keeps. */
    private Resolved valued(Json.Obj schema, List<Json.Node> values, String loc, boolean nullable) {
        List<Json.Node> present = new ArrayList<>();
        for (Json.Node v : values) {
            if (!(v instanceof Json.Null) && !present.contains(v)) {
                present.add(v);
            }
        }
        boolean empty = nullable || present.size() < values.size();
        if (present.isEmpty()) {
            note(where(loc), values.isEmpty() ? "an enum of no value is not a property"
                    : "a value that is always null is not a property");
            return NEVER;
        }
        String type = null;
        for (Json.Node v : present) {
            String t = v instanceof Json.Str ? "String" : v instanceof Json.Bool ? "Boolean"
                    : v instanceof Json.Num n ? (n.isInteger() ? "Integer" : "Float") : VARIANT;
            type = type == null || type.equals(t) ? t
                    : Set.of(type, t).equals(Set.of("Integer", "Float")) ? "Float" : VARIANT;
        }
        if (VARIANT.equals(type)) {
            note(where(loc), "an enum of values of several types, or not single values, is a Variant");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        // the type the schema declares, when its values are of it (an integer's values numbers, a date's texts)
        List<String> declaredTypes = types(schema);
        declaredTypes.remove("null");
        String one = declaredTypes.size() == 1 ? declaredTypes.get(0) : "";
        String declared = PRIMITIVES.contains(one) ? primitive(one, text(schema, "format")) : null;
        String t = type == null ? "String" : type;
        boolean dated = false;
        if (declared != null) {
            boolean numeric = Set.of("Integer", "Float", "Decimal", "Number").contains(declared)
                    && Set.of("Integer", "Float").contains(t);
            dated = t.equals("String") && Set.of("StrictDate", "DateTime", "StrictTime").contains(declared);
            if (numeric || dated) {
                t = declared;
            }
        }
        List<Rule> rules = new ArrayList<>(rules(schema, t, loc));
        if (!dated) {
            rules.add(new OneOf("", false, present));
        }
        return new Resolved(Kind.PRIMITIVE, t, empty).withRules(rules);
    }

    /** The primitive a union's or allOf's parts share ({@code integer} and {@code number} sharing Number), or null. */
    private @com.legend.base.Nullable String commonPrimitive(List<Json.Obj> parts, String loc, String keyword) {
        String common = null;
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj p = parts.get(i);
            if (isNull(p) || !saysShape(p)) {
                continue;
            }
            if (refOf(p) != null || p.has("enum") || p.has("const") || p.has("oneOf") || p.has("anyOf")
                    || p.has("allOf")) {
                return null;
            }
            List<String> ts = types(p);
            ts.remove("null");
            String t = ts.size() == 1 ? ts.get(0) : ts.isEmpty() ? implied(p) : "";
            if (!PRIMITIVES.contains(t)) {
                return null;
            }
            String prim = primitive(t, text(p, "format"));
            if (common == null || common.equals(prim)) {
                common = prim;
            } else if (Set.of("Integer", "Float", "Decimal", "Number").containsAll(Set.of(common, prim))) {
                common = "Number";
            } else if (Set.of("String", "StrictDate", "DateTime", "StrictTime").containsAll(Set.of(common, prim))) {
                common = "String";
            } else {
                return null;
            }
        }
        return common;
    }

    /** Whether a part of an allOf says something of a shape (beyond documentation and nullability). */
    private static boolean saysShape(Json.Obj part) {
        for (String k : part.fields().keySet()) {
            if (!Set.of("title", "description", "nullable", "x-nullable", "default", "deprecated", "readOnly",
                    "writeOnly", "example", "examples", "$comment", "externalDocs", "xml").contains(k)
                    && !k.startsWith("x-")) {
                return true;
            }
        }
        return false;
    }

    private static List<String> types(Json.Obj schema) {
        List<String> types = new ArrayList<>();
        Json.Node type = schema.getOr("type", null);
        if (type instanceof Json.Str s) {
            types.add(s.value());
        } else if (type instanceof Json.Arr a) {
            for (Json.Node n : a.items()) {
                if (n instanceof Json.Str s && !types.contains(s.value())) {
                    types.add(s.value());
                }
            }
        }
        return types;
    }

    /** The type a schema with no {@code type} implies by its keywords, or "" (any value). */
    private static String implied(Json.Obj schema) {
        if (schema.has("items") || schema.has("prefixItems") || schema.has("minItems") || schema.has("maxItems")) {
            return "array";
        }
        if (schema.has("properties") || schema.has("additionalProperties") || schema.has("patternProperties")) {
            return "object";
        }
        if (schema.has("minLength") || schema.has("maxLength") || schema.has("pattern")
                || (schema.getOr("format", null) instanceof Json.Str f
                    && Set.of("date", "date-time", "time", "email", "uri", "uuid").contains(f.value()))) {
            return "string";
        }
        if (schema.has("minimum") || schema.has("maximum") || schema.has("multipleOf")
                || schema.getOr("exclusiveMinimum", null) instanceof Json.Num
                || schema.getOr("exclusiveMaximum", null) instanceof Json.Num) {
            return "number";
        }
        return "";
    }

    /** A JSON Schema type and format as a Pure primitive. */
    private static String primitive(String type, @com.legend.base.Nullable String format) {
        String f = format == null ? "" : format;
        return switch (type) {
            // a format is an annotation (JSON Schema's own rule): a text of any other format is a String
            case "string" -> STRING_FORMATS.getOrDefault(f, "String");
            case "integer" -> "Integer";
            // a number of any other format (float, double) is a Float
            case "number" -> NUMBER_FORMATS.getOrDefault(f, "Float");
            case "boolean" -> "Boolean";
            default -> throw new IllegalStateException("not a primitive JSON type: " + type);
        };
    }

    /** JSON's primitive types. */
    private static final Set<String> PRIMITIVES = Set.of("string", "integer", "number", "boolean");

    /** The formats of a text that are a Pure type of their own. */
    private static final Map<String, String> STRING_FORMATS = Map.of("date", "StrictDate", "date-time", "DateTime",
            "time", "StrictTime");

    /** The formats of a number that are a Pure type of their own. */
    private static final Map<String, String> NUMBER_FORMATS = Map.of("decimal", "Decimal", "int32", "Integer",
            "int64", "Integer", "integer", "Integer");

    /** The rules a schema's validations make for a value of {@code type}: bounds of a number, lengths and a pattern of
     *  a text, a multiple of an integer. */
    private List<Rule> rules(Json.Obj schema, String type, String loc) {
        List<Rule> out = new ArrayList<>();
        boolean number = Set.of("Integer", "Float", "Decimal", "Number").contains(type);
        if (number) {
            BigDecimal min = num(schema, "minimum");
            BigDecimal max = num(schema, "maximum");
            // draft 4: exclusiveMinimum a boolean beside minimum; draft 6 on: a number of its own
            boolean exMin = isTrue(schema, "exclusiveMinimum");
            boolean exMax = isTrue(schema, "exclusiveMaximum");
            if (min != null) {
                out.add(new Bound("", false, min, true, exMin));
            }
            if (max != null) {
                out.add(new Bound("", false, max, false, exMax));
            }
            BigDecimal xMin = num(schema, "exclusiveMinimum");
            BigDecimal xMax = num(schema, "exclusiveMaximum");
            if (xMin != null) {
                out.add(new Bound("", false, xMin, true, true));
            }
            if (xMax != null) {
                out.add(new Bound("", false, xMax, false, true));
            }
            BigDecimal step = num(schema, "multipleOf");
            if (step != null) {
                if (type.equals("Integer") && step.signum() > 0 && step.stripTrailingZeros().scale() <= 0) {
                    out.add(new MultipleOf("", false, step));
                } else {
                    note(where(loc), "multipleOf of a number that is not a whole number is not carried");
                }
            }
        }
        if (type.equals("String")) {
            BigDecimal min = num(schema, "minLength");
            BigDecimal max = num(schema, "maxLength");
            if (min != null && min.signum() > 0) {
                out.add(new Length("", false, min.intValue(), true));
            }
            if (max != null) {
                out.add(new Length("", false, max.intValue(), false));
            }
            if (schema.getOr("pattern", null) instanceof Json.Str p) {
                out.add(new Pattern("", false, p.value()));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // Logic: a schema's conditions over a whole value (if/then/else, not, a union of requirements, dependentSchemas)
    // as conditions on a class's properties
    // ---------------------------------------------------------------------

    /** A part of a schema's logic Pure's constraint cannot say: why. */
    private static final class Unsaid extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Unsaid(String why) {
            super(why, null, false, false);
        }
    }

    private static final Condition TRUE = new All(List.of());
    private static final Condition FALSE = new Any(List.of());

    /** A logic's condition, its constraint added; one that says what Pure cannot, or that holds of every value or of
     *  none, noted. */
    private void logic(List<Logic> out, String name, String place, String kind,
            java.util.function.Supplier<Condition> condition) {
        Condition c;
        try {
            c = condition.get();
        } catch (Unsaid u) {
            note(where(place), kindName(kind) + " is not carried: Pure's constraint cannot say " + u.getMessage());
            return;
        }
        if (c.equals(FALSE)) {
            note(where(place), kindName(kind) + " that no value keeps is not carried");
        } else if (!c.equals(TRUE)) {
            out.add(new Logic(name, c));
        }
    }

    private static String kindName(String kind) {
        return switch (kind) {
            case "if" -> "a condition (if/then/else)";
            case "not" -> "a not";
            case "dependency" -> "a dependentSchemas";
            case "oneOf", "anyOf" -> "a " + kind + " of requirements";
            default -> throw new IllegalStateException("not a kind of logic: " + kind);
        };
    }

    /** A schema of only some of its keywords. */
    private static Json.Obj only(Json.Obj schema, String... keys) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
        for (String k : keys) {
            if (schema.has(k)) {
                out.put(k, schema.get(k));
            }
        }
        return new Json.Obj(out);
    }

    /** The property has a value: always, when it is required; never, when it can hold none. */
    private static Condition has(ImportedProperty p) {
        return p.lower() >= 1 ? TRUE : never(p) ? FALSE : new Has(p.name());
    }

    private static boolean never(ImportedProperty p) {
        return p.upper() != null && p.upper() == 0;
    }

    /** A property the logic names, by its JSON name. */
    private static ImportedProperty property(Map<String, ImportedProperty> scope, String key) {
        ImportedProperty p = scope.get(key);
        if (p == null) {
            throw new Unsaid("'" + key + "', not a property of the class");
        }
        return p;
    }

    /** What a schema says of a whole value of the class whose properties are {@code scope}. */
    private Condition whole(@com.legend.base.Nullable Json.Node node, String loc, Map<String, ImportedProperty> scope,
            Set<String> following) {
        if (node instanceof Json.Bool b) {
            return b.value() ? TRUE : FALSE;
        }
        if (!(node instanceof Json.Obj o)) {
            throw new Unsaid("a schema that is not a JSON object");
        }
        List<Condition> all = new ArrayList<>();
        for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
            String key = e.getKey();
            Json.Node v = e.getValue();
            String at = child(loc, key);
            switch (key) {
                case "required" -> {
                    for (Json.Node n : v instanceof Json.Arr a ? a.items() : List.<Json.Node>of()) {
                        all.add(has(property(scope, n instanceof Json.Str str ? str.value() : "")));
                    }
                }
                case "properties" -> {
                    for (Map.Entry<String, Json.Node> p : v instanceof Json.Obj props ? props.fields().entrySet()
                            : Map.<String, Json.Node>of().entrySet()) {
                        ImportedProperty ip = property(scope, p.getKey());
                        if (!never(ip)) {
                            all.add(any(List.of(not(has(ip)), value(ip, p.getValue(), child(at, p.getKey()),
                                    following, false))));
                        }
                    }
                }
                case "not" -> all.add(not(whole(v, at, scope, following)));
                case "allOf", "anyOf", "oneOf" -> {
                    List<Condition> parts = new ArrayList<>();
                    List<Json.Node> items = v instanceof Json.Arr a ? a.items() : List.of();
                    for (int i = 0; i < items.size(); i++) {
                        parts.add(whole(items.get(i), child(at, Integer.toString(i)), scope, following));
                    }
                    all.add(combined(key, parts));
                }
                case "if" -> all.add(conditional(whole(v, at, scope, following),
                        o.has("then") ? whole(o.get("then"), child(loc, "then"), scope, following) : TRUE,
                        o.has("else") ? whole(o.get("else"), child(loc, "else"), scope, following) : TRUE));
                case "then", "else" -> {
                    // with their if; without one, nothing
                }
                case "dependentRequired", "dependencies", "dependentSchemas" -> {
                    for (Map.Entry<String, Json.Node> d : v instanceof Json.Obj ds ? ds.fields().entrySet()
                            : Map.<String, Json.Node>of().entrySet()) {
                        ImportedProperty ip = property(scope, d.getKey());
                        Condition then;
                        if (d.getValue() instanceof Json.Arr names) {
                            List<Condition> present = new ArrayList<>();
                            for (Json.Node n : names.items()) {
                                present.add(has(property(scope, n instanceof Json.Str str ? str.value() : "")));
                            }
                            then = all(present);
                        } else {
                            then = whole(d.getValue(), child(at, d.getKey()), scope, following);
                        }
                        all.add(any(List.of(not(has(ip)), then)));
                    }
                }
                case "$ref", "$dynamicRef", "$recursiveRef" -> {
                    String t = followed(loc, v, following);
                    all.add(whole(at(t), t, scope, following));
                    following.remove(t);
                }
                case "type" -> {
                    if (!types(o).contains("object")) {
                        throw new Unsaid("a value that is an object only if it is " + Json.toCompact(v));
                    }
                }
                case "additionalProperties", "unevaluatedProperties" -> {
                    if (!(v instanceof Json.Bool b && b.value())
                            && !(v instanceof Json.Obj vo && vo.fields().isEmpty())) {
                        throw new Unsaid("which properties a value may have beside its own (" + key + ")");
                    }
                }
                default -> {
                    if (!saysNothing(key)) {
                        throw new Unsaid("the keyword '" + key + "'");
                    }
                }
            }
        }
        return all(all);
    }

    /** What a schema says of the value of the property {@code p}, present: of each of its values when {@code each}
     *  (a list's {@code items}). */
    private Condition value(ImportedProperty p, @com.legend.base.Nullable Json.Node node, String loc,
            Set<String> following, boolean each) {
        if (node instanceof Json.Bool b) {
            return b.value() ? TRUE : not(has(p));
        }
        if (!(node instanceof Json.Obj o)) {
            throw new Unsaid("a schema that is not a JSON object");
        }
        boolean list = (p.upper() == null || p.upper() > 1) && !each;
        boolean variant = p.kind() == Kind.PRIMITIVE && p.type().equals(VARIANT);
        List<Condition> all = new ArrayList<>();
        boolean ruled = false;
        // what an object says of its own properties: of an object-typed property's value, by its path
        Json.Obj nested = only(o, "properties", "required", "dependentRequired", "dependencies", "dependentSchemas");
        if (!nested.fields().isEmpty()) {
            if (list || variant || (each && p.kind() == Kind.CLASS)) {
                throw new Unsaid("the properties of " + (variant ? "a Variant" : "each object of a list"));
            }
            if (p.kind() == Kind.CLASS) {
                Map<String, ImportedProperty> inner = keysOf.get(p.type());
                if (inner == null) {
                    throw new Unsaid("the properties of " + p.type() + ", a class holding itself");
                }
                Map<String, ImportedProperty> scope = new LinkedHashMap<>();
                for (Map.Entry<String, ImportedProperty> q : inner.entrySet()) {
                    ImportedProperty ip = q.getValue();
                    scope.put(q.getKey(), never(ip) ? ip : new ImportedProperty(p.name() + "." + ip.name(), ip.doc(),
                            ip.kind(), ip.type(), ip.lower(), ip.upper()));
                }
                all.add(whole(nested, loc, scope, following));
            }
            // of a value that is no object, nothing
        }
        for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
            String key = e.getKey();
            Json.Node v = e.getValue();
            String at = child(loc, key);
            switch (key) {
                case "const" -> all.add(oneOf(p, List.of(v), list, variant, each));
                case "enum" -> all.add(oneOf(p, v instanceof Json.Arr a ? a.items() : List.of(), list, variant, each));
                case "type" -> {
                    List<String> types = types(o);
                    if (types.size() == 1 && "null".equals(types.get(0))) {
                        all.add(not(has(p)));
                    } else {
                        all.add(typeOf(p, types, list, variant));
                    }
                }
                case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength",
                        "maxLength", "pattern" -> {
                    // a number's or a text's keyword says nothing of a list, nor of a value not a number or text
                    if (!list && !ruled && p.kind() == Kind.PRIMITIVE && !variant) {
                        ruled = true;
                        for (Rule r : rules(o, p.type(), loc)) {
                            all.add(new Holds(of(each ? each(r) : r, p.name())));
                        }
                    } else if (!list && (variant || p.kind() != Kind.PRIMITIVE)) {
                        throw new Unsaid("a " + key + " of a value that is " + (variant ? "a Variant"
                                : "no primitive"));
                    }
                }
                case "items" -> {
                    if (list) {
                        all.add(value(p, v, at, following, true));
                    }
                }
                case "uniqueItems" -> {
                    if (list && isTrue(o, "uniqueItems") && p.kind() != Kind.CLASS) {
                        all.add(new Holds(new Unique(p.name(), false)));
                    }
                }
                case "not" -> all.add(not(value(p, v, at, following, each)));
                case "allOf", "anyOf", "oneOf" -> {
                    List<Condition> parts = new ArrayList<>();
                    List<Json.Node> items = v instanceof Json.Arr a ? a.items() : List.of();
                    for (int i = 0; i < items.size(); i++) {
                        parts.add(value(p, items.get(i), child(at, Integer.toString(i)), following, each));
                    }
                    all.add(combined(key, parts));
                }
                case "if" -> all.add(conditional(value(p, v, at, following, each),
                        o.has("then") ? value(p, o.get("then"), child(loc, "then"), following, each) : TRUE,
                        o.has("else") ? value(p, o.get("else"), child(loc, "else"), following, each) : TRUE));
                case "then", "else", "properties", "required", "dependentRequired", "dependencies",
                        "dependentSchemas" -> {
                    // with their if; an object's, above
                }
                case "$ref", "$dynamicRef", "$recursiveRef" -> {
                    String t = followed(loc, v, following);
                    all.add(value(p, at(t), t, following, each));
                    following.remove(t);
                }
                case "additionalProperties", "unevaluatedProperties" -> {
                    if (p.kind() == Kind.CLASS && !(v instanceof Json.Bool b && b.value())) {
                        throw new Unsaid("which properties a value may have beside its own (" + key + ")");
                    }
                }
                default -> {
                    if (!saysNothing(key) && !Set.of("format", "nullable", "x-nullable").contains(key)) {
                        throw new Unsaid("the keyword '" + key + "' of a property's value");
                    }
                }
            }
        }
        return all(all);
    }

    /** The property's value is one of {@code values}. */
    private static Condition oneOf(ImportedProperty p, List<Json.Node> values, boolean list, boolean variant,
            boolean each) {
        if (list || variant || p.kind() == Kind.CLASS) {
            throw new Unsaid("a const or enum of " + (list ? "a list" : variant ? "a Variant" : "an object"));
        }
        return new Holds(new OneOf(p.name(), each, values));
    }

    /** The place a reference in a logic names, now followed; one naming no document given, or itself, unsaid. */
    private String followed(String loc, Json.Node ref, Set<String> following) {
        String t = ref instanceof Json.Str s ? target(loc, s.value()) : null;
        if (t == null) {
            throw new Unsaid("the reference " + Json.toCompact(ref) + ", to nothing in the documents given");
        }
        if (!following.add(t)) {
            throw new Unsaid("a reference to itself");
        }
        return t;
    }

    /** Whether a value of the property's type is of one of JSON's {@code types}: always (true), never (false), or as
     *  it happens (unsaid). */
    private static Condition typeOf(ImportedProperty p, List<String> types, boolean list, boolean variant) {
        if (variant) {
            throw new Unsaid("the type of a Variant");
        }
        boolean depends = false;
        for (String t : types) {
            if (list) {
                if (t.equals("array")) {
                    return TRUE;
                }
                continue;
            }
            String type = p.kind() == Kind.ENUMERATION ? "String" : p.kind() == Kind.CLASS ? "" : p.type();
            switch (t) {
                case "string" -> {
                    if (Set.of("String", "StrictDate", "DateTime", "StrictTime").contains(type)) {
                        return TRUE;
                    }
                }
                case "number" -> {
                    if (Set.of("Integer", "Float", "Decimal", "Number").contains(type)) {
                        return TRUE;
                    }
                }
                case "integer" -> {
                    if (type.equals("Integer")) {
                        return TRUE;
                    }
                    depends |= Set.of("Float", "Decimal", "Number").contains(type);
                }
                case "boolean" -> {
                    if (type.equals("Boolean")) {
                        return TRUE;
                    }
                }
                case "object" -> {
                    if (p.kind() == Kind.CLASS) {
                        return TRUE;
                    }
                }
                default -> {
                    // null, array: a value present is neither
                }
            }
        }
        if (depends) {
            throw new Unsaid("whether a number is an integer");
        }
        return FALSE;
    }

    /** The keywords that say nothing of a value: annotations, other vocabularies', and the definitions held. */
    private static boolean saysNothing(String key) {
        return ANNOTATIONS.contains(key) || VOCABULARIES.contains(key) || key.startsWith("x-")
                || Set.of("title", "description", "default", "deprecated", "$defs", "definitions").contains(key);
    }

    private static Condition combined(String union, List<Condition> parts) {
        return switch (union) {
            case "allOf" -> all(parts);
            case "anyOf" -> any(parts);
            default -> {
                // oneOf: exactly one holds
                if (parts.size() > 6) {
                    throw new Unsaid("a oneOf of more than six alternatives (exactly one of them)");
                }
                List<Condition> one = new ArrayList<>();
                for (int i = 0; i < parts.size(); i++) {
                    List<Condition> only = new ArrayList<>(List.of(parts.get(i)));
                    for (int j = 0; j < parts.size(); j++) {
                        if (j != i) {
                            only.add(not(parts.get(j)));
                        }
                    }
                    one.add(all(only));
                }
                yield any(one);
            }
        };
    }

    /** if/then/else: when the condition holds, then; when it does not, else. */
    private static Condition conditional(Condition when, Condition then, Condition otherwise) {
        return all(List.of(any(List.of(not(when), then)), any(List.of(when, otherwise))));
    }

    private static Condition all(List<Condition> conditions) {
        List<Condition> out = new ArrayList<>();
        for (Condition c : conditions) {
            if (c instanceof All a) {
                for (Condition inner : a.conditions()) {
                    if (!out.contains(inner)) {
                        out.add(inner);
                    }
                }
            } else if (c.equals(FALSE)) {
                return FALSE;
            } else if (!out.contains(c)) {
                out.add(c);
            }
        }
        // a condition beside a disjunction holding its negation: a && (!a || b) is a && b
        for (int i = 0; i < out.size(); i++) {
            if (out.get(i) instanceof Any a) {
                List<Condition> kept = new ArrayList<>();
                for (Condition x : a.conditions()) {
                    if (!out.contains(not(x))) {
                        kept.add(x);
                    }
                }
                if (kept.size() < a.conditions().size()) {
                    out.set(i, any(kept));
                    return all(out);
                }
            }
        }
        return out.size() == 1 ? out.get(0) : new All(guardsFirst(out, false));
    }

    /** The tests of a property's presence first: a value is read ({@code toOne}) only after its presence is known. */
    private static List<Condition> guardsFirst(List<Condition> conditions, boolean absent) {
        List<Condition> out = new ArrayList<>();
        for (Condition c : conditions) {
            if (absent ? c instanceof Not n && n.condition() instanceof Has : c instanceof Has) {
                out.add(c);
            }
        }
        for (Condition c : conditions) {
            if (!out.contains(c)) {
                out.add(c);
            }
        }
        return out;
    }

    private static Condition any(List<Condition> conditions) {
        List<Condition> out = new ArrayList<>();
        for (Condition c : conditions) {
            if (c instanceof Any a) {
                for (Condition inner : a.conditions()) {
                    if (!out.contains(inner)) {
                        out.add(inner);
                    }
                }
            } else if (c.equals(TRUE)) {
                return TRUE;
            } else if (!out.contains(c)) {
                out.add(c);
            }
        }
        // a condition and its negation: always
        for (Condition c : out) {
            if (out.contains(not(c))) {
                return TRUE;
            }
        }
        // a condition beside a conjunction holding its negation: !a || (a && b) is !a || b
        for (int i = 0; i < out.size(); i++) {
            if (out.get(i) instanceof All a) {
                List<Condition> kept = new ArrayList<>();
                for (Condition x : a.conditions()) {
                    if (!out.contains(not(x))) {
                        kept.add(x);
                    }
                }
                if (kept.size() < a.conditions().size()) {
                    out.set(i, all(kept));
                    return any(out);
                }
            }
        }
        return out.size() == 1 ? out.get(0) : new Any(guardsFirst(out, true));
    }

    private static Condition not(Condition c) {
        if (c instanceof Not n) {
            return n.condition();
        }
        return c.equals(TRUE) ? FALSE : c.equals(FALSE) ? TRUE : new Not(c);
    }

    // ---------------------------------------------------------------------
    // Enumerations
    // ---------------------------------------------------------------------

    /** The enumeration at a place: made once; its values the texts of its {@code enum}, or of its union of
     *  {@code const}s, each documented. */
    private String enumFor(String loc, String suggested) {
        String known = enumAt.get(loc);
        if (known != null) {
            return known;
        }
        Named n = named.get(loc);
        String name = n != null ? n.name() : unique(suggested);
        enumAt.put(loc, name);
        Json.Obj schema = schemaAt(loc);
        keywords(schema, loc);
        List<String> texts = new ArrayList<>();
        List<@com.legend.base.Nullable String> docs = new ArrayList<>();
        List<Json.Obj> alternatives = constAlternatives(schema);
        if (alternatives != null) {
            for (Json.Obj a : alternatives) {
                String t = constText(a);
                if (t != null && !texts.contains(t)) {
                    texts.add(t);
                    docs.add(doc(a));
                }
            }
        } else {
            for (Json.Node v : schema.getArr("enum").items()) {
                if (v instanceof Json.Str s && !texts.contains(s.value())) {
                    texts.add(s.value());
                }
            }
            List<String> given = strings(schema, VALUE_DOCS);
            for (int i = 0; i < texts.size(); i++) {
                docs.add(given != null && i < given.size() && !given.get(i).strip().isEmpty() ? given.get(i) : null);
            }
        }
        List<String> givenNames = strings(schema, VALUE_NAMES);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            String text = texts.get(i);
            String raw = givenNames != null && givenNames.size() == texts.size() ? givenNames.get(i) : text;
            String held = isName(raw) ? raw : valueName(raw);
            String value = held;
            for (int k = 2; values.contains(value); k++) {
                value = held + "_" + k;
            }
            if (!value.equals(text)) {
                note(where(loc), "a value whose text Pure cannot hold as a name is renamed ('" + text + "' is " + value
                        + ")");
            }
            values.add(value);
        }
        enumerations.add(new ImportedEnum(name, doc(schema), values, texts, docs));
        return name;
    }

    // ---------------------------------------------------------------------
    // Keywords and notes
    // ---------------------------------------------------------------------

    /** Each keyword of a schema, once: read, an annotation, a validation not carried (noted), or not JSON Schema's
     *  (noted, not read: JSON Schema's own rule for a keyword a reader does not know). */
    private void keywords(Json.Obj schema, String loc) {
        if (!scanned.add(loc)) {
            return;
        }
        for (String key : schema.fields().keySet()) {
            if (READ.contains(key) || ANNOTATIONS.contains(key) || VOCABULARIES.contains(key) || key.startsWith("x-")
                    || (mapRoots.contains(loc) && CARRIED_AT_A_MAP.contains(key))) {
                continue;
            }
            if (VALIDATIONS.contains(key)) {
                note(where(loc), key + " is not carried (a validation Pure states no constraint for here)");
            } else {
                note(where(loc), "the keyword '" + key + "' is not JSON Schema's, and is not read");
            }
        }
    }

    /** A note, said once with where it was first met; met again, counted. */
    private void note(String where, String message) {
        Note n = notes.get(message);
        if (n == null) {
            notes.put(message, new Note(where.isEmpty() ? "#" : where));
        } else {
            n.count++;
        }
    }

    /** A place as a person reads it: the main document's pointer ({@code #/definitions/A}), another's after its
     *  name ({@code common.yaml#/Pet}). */
    private static String where(String loc) {
        return loc;
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
        note(where, "a name Pure cannot hold as it is is camel-cased ('" + raw + "' is " + held + ")");
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

    /** A name Pure holds as it is: an ASCII letter or {@code _}, then letters, digits and {@code _}; no literal's
     *  word. */
    private static boolean isName(String name) {
        if (name.isEmpty() || Character.isDigit(name.charAt(0)) || LITERAL_WORDS.contains(name)) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            if (!alnum(name.charAt(i)) && name.charAt(i) != '_') {
                return false;
            }
        }
        return true;
    }

    /** An ASCII letter or digit. */
    private static boolean alnum(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }

    private static boolean hex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /** Whether a text is one or more ASCII digits: an index into a list. */
    private static boolean digits(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) < '0' || text.charAt(i) > '9') {
                return false;
            }
        }
        return !text.isEmpty();
    }

    /** A text cut at each {@code separator}: every part, the empty ones too. */
    private static List<String> parts(String text, char separator) {
        List<String> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= text.length(); i++) {
            if (i == text.length() || text.charAt(i) == separator) {
                out.add(text.substring(start, i));
                start = i + 1;
            }
        }
        return out;
    }

    /** A document's file name without its extension ({@code common.yaml} is {@code common}). */
    private static String withoutExtension(String file) {
        int dot = file.lastIndexOf('.');
        if (dot <= 0 || dot == file.length() - 1) {
            return file;
        }
        for (int i = dot + 1; i < file.length(); i++) {
            char c = file.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
                return file;
            }
        }
        return file.substring(0, dot);
    }

    /** The runs of ASCII letters and digits of a text: the words a name is made of. */
    private static List<String> words(String raw) {
        List<String> out = new ArrayList<>();
        int start = -1;
        for (int i = 0; i <= raw.length(); i++) {
            boolean in = i < raw.length() && alnum(raw.charAt(i));
            if (in && start < 0) {
                start = i;
            } else if (!in && start >= 0) {
                out.add(raw.substring(start, i));
                start = -1;
            }
        }
        return out;
    }

    /** Upstream's camel case ({@code makeCamelCase}) of the parts between characters a name cannot hold. */
    static String camelCase(String raw, boolean upperFirst) {
        StringBuilder out = new StringBuilder();
        for (String part : words(raw)) {
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
        StringBuilder made = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            made.append(alnum(c) || c == '_' ? c : '_');
        }
        String held = made.toString();
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
        // status, analysis, address: a singular already
        if (plural.endsWith("s") && !plural.endsWith("ss") && !plural.endsWith("us") && !plural.endsWith("is")
                && plural.length() > 1) {
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

    /** An element's documentation: its description (VS Code's markdown one when it has no plain one), else its
     *  title. */
    private static @com.legend.base.Nullable String doc(Json.Obj o) {
        for (String k : List.of("description", "markdownDescription")) {
            String d = text(o, k);
            if (d != null && !d.strip().isEmpty()) {
                return d;
            }
        }
        return text(o, "title");
    }

    /** Whether a schema says it is deprecated: {@code deprecated}, or VS Code's {@code deprecationMessage}. */
    private static boolean deprecated(Json.Obj o) {
        return isTrue(o, "deprecated") || o.getOr("deprecationMessage", null) instanceof Json.Str;
    }

    private static boolean isTrue(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Bool b && b.value();
    }

    private static @com.legend.base.Nullable BigDecimal num(Json.Obj o, String key) {
        if (!(o.getOr(key, null) instanceof Json.Num n)) {
            return null;
        }
        return n.decimalValue() != null ? n.decimalValue() : n.isInteger() ? BigDecimal.valueOf(n.longValue())
                : new BigDecimal(Double.toString(n.doubleValue()));
    }

    private static @com.legend.base.Nullable Json.Obj objectAt(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Obj v ? v : null;
    }

    private static List<Json.Obj> objects(Json.Obj o, String key) {
        List<Json.Obj> out = new ArrayList<>();
        if (o.getOr(key, null) instanceof Json.Arr a) {
            for (Json.Node n : a.items()) {
                // true is any value; false, no value: an enum of none
                out.add(n instanceof Json.Obj item ? item : n instanceof Json.Bool b && b.value() ? Json.obj()
                        : new Json.Obj(new LinkedHashMap<>(Map.of("enum", Json.arr()))));
            }
        }
        return out;
    }

    /** The first of {@code keys} a schema holds as a list of texts. */
    private static @com.legend.base.Nullable List<String> strings(Json.Obj o, List<String> keys) {
        for (String k : keys) {
            if (o.getOr(k, null) instanceof Json.Arr a) {
                List<String> out = new ArrayList<>();
                for (Json.Node n : a.items()) {
                    out.add(n instanceof Json.Str s ? s.value() : "");
                }
                return out;
            }
        }
        return null;
    }

    /** A schema's {@code enum}, or its {@code const} as a list of one; null when it has neither. */
    private static @com.legend.base.Nullable List<Json.Node> values(Json.Obj schema) {
        if (schema.getOr("enum", null) instanceof Json.Arr a) {
            return a.items();
        }
        if (schema.has("const")) {
            return List.of(schema.get("const"));
        }
        return null;
    }

    private static boolean isNull(Json.Obj schema) {
        if (schema.getOr("type", null) instanceof Json.Str s && "null".equals(s.value())) {
            return true;
        }
        List<Json.Node> v = values(schema);
        return v != null && !v.isEmpty() && v.stream().allMatch(n -> n instanceof Json.Null);
    }

    private static boolean mayBeNull(Json.Obj schema) {
        List<Json.Node> v = values(schema);
        if (v != null && v.contains(Json.nil())) {
            return true;
        }
        for (String union : List.of("oneOf", "anyOf")) {
            for (Json.Obj p : objects(schema, union)) {
                if (isNull(p)) {
                    return true;
                }
            }
        }
        return types(schema).contains("null") || isTrue(schema, "nullable");
    }

    /** A property's default, when it is one value of the property's type (an enumeration's by its Pure name). */
    private Json.@com.legend.base.Nullable Node defaultOf(Json.Obj schema, Resolved r, String loc) {
        Json.Node d = schema.getOr("default", null);
        if (d == null || d instanceof Json.Null || r.many()) {
            return null;
        }
        boolean fits = switch (r.kind()) {
            case PRIMITIVE -> switch (r.type()) {
                case "String" -> d instanceof Json.Str;
                case "Integer" -> d instanceof Json.Num n && n.isInteger();
                case "Float", "Decimal", "Number" -> d instanceof Json.Num;
                case "Boolean" -> d instanceof Json.Bool;
                default -> false;
            };
            case ENUMERATION -> d instanceof Json.Str;
            case CLASS -> false;
        };
        if (!fits) {
            note(where(loc), "a default that is not one value of its property's type is not carried");
            return null;
        }
        if (r.kind() == Kind.ENUMERATION && d instanceof Json.Str s) {
            for (ImportedEnum e : enumerations) {
                if (e.name().equals(r.type())) {
                    int i = e.texts().indexOf(s.value());
                    if (i < 0) {
                        note(where(loc), "a default that is not one of its enumeration's values is not carried");
                        return null;
                    }
                    return Json.str(e.values().get(i));
                }
            }
            return null;
        }
        return d;
    }
}
