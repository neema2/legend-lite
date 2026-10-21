// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import static com.legend.protocol.schema.SchemaDocuments.child;
import static com.legend.protocol.schema.SchemaDocuments.digits;
import static com.legend.protocol.schema.SchemaDocuments.docOf;
import static com.legend.protocol.schema.SchemaDocuments.loc;
import static com.legend.protocol.schema.SchemaDocuments.parts;
import static com.legend.protocol.schema.SchemaDocuments.refOf;
import static com.legend.protocol.schema.SchemaNames.camelCase;
import static com.legend.protocol.schema.SchemaNames.inPlaceName;
import static com.legend.protocol.schema.SchemaNames.isName;
import static com.legend.protocol.schema.SchemaNames.pureName;
import static com.legend.protocol.schema.SchemaNames.singular;
import static com.legend.protocol.schema.SchemaNames.valueName;
import static com.legend.protocol.schema.SchemaNames.withoutExtension;

import com.legend.json.Json;
import com.legend.json.Yaml;
import com.legend.protocol.schema.ImportedModel.All;
import com.legend.protocol.schema.ImportedModel.Annotation;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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

    /** legend-engine's profiles for what a JSON Schema says beyond a model's shape
     *  (core_external_format_json/transformation/toBeRefactored/models.pure, 4.145.0), which its schema-to-model
     *  generation writes: an element's name in the schema, its title, an example, a default; readOnly, writeOnly, no
     *  additional properties; a format; OpenAPI's discriminator. */
    private static final String GENERATION = "meta::json::schema::JSONSchemaGeneration";
    private static final String TYPE_EXTENSION = "meta::json::schema::JSONSchemaTypeExtension";
    private static final String OPENAPI_EXTENSION = "meta::json::schema::JSONSchemaOpenAPIExtension";

    /** The formats upstream's type extension names (a property's Pure type says the rest). */
    private static final Set<String> FORMAT_STEREOTYPES = Set.of("int32", "int64", "double", "byte", "binary");

    /** The longest example carried as a tag: a document's examples can be whole documents. */
    private static final int EXAMPLE_LIMIT = 500;

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

    /** The extensions that name an enumeration's values, and that document them. */
    private static final List<String> VALUE_NAMES = List.of("x-enum-varnames", "x-enumNames", "x-enum-names",
            "javaEnumNames", "tsEnumNames");
    private static final List<String> VALUE_DOCS = List.of("x-enum-descriptions", "x-enumDescriptions",
            "enumDescriptions", "markdownEnumDescriptions");

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

    /** A note, said once: its code, where it was first met, and how many times. */
    private static final class Note {
        final String code;
        final String where;
        int count = 1;

        Note(String code, String where) {
            this.code = code;
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
    /** The documents read, and the places and references in them. */
    private final SchemaDocuments docs;
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
    /** The places of the schemas a document names by a key ({@code $defs}, {@code definitions},
     *  {@code components.schemas}): their key is their name in the schema. */
    private final Set<String> keyed = new HashSet<>();
    /** The class or enumeration of each property's schema written in place, by its text and its document: another
     *  place of the same text reuses it ({@link #inPlace}). */
    private final Map<String, String> twins = new HashMap<>();
    /** The unions written in place that are one class for each set of alternatives ({@link #unionFor}), by place:
     *  their superclass says nothing of one property's. */
    private final Set<String> sharedUnions = new HashSet<>();
    /** What a union named after its members names its wrappers after, by its place: its objects' names. */
    private final Map<String, String> wrapperBases = new HashMap<>();
    /** The place of a map at the root, made a class of its entries: none, or one. */
    private final Set<String> mapRoots = new HashSet<>();

    private JsonSchemaReader(Options options, SchemaDocuments docs) {
        this.options = options;
        this.docs = docs;
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
        JsonSchemaReader reader = new JsonSchemaReader(options, SchemaDocuments.of(text, options.documents()));
        reader.collect();
        reader.build();
        reader.unite();
        reader.discriminators();
        if (!reader.refusals.isEmpty()) {
            throw new SchemaImportException(reader.refusals);
        }
        List<ImportedModel.Note> notes = new ArrayList<>();
        for (Map.Entry<String, Note> n : reader.notes.entrySet()) {
            Note note = n.getValue();
            notes.add(new ImportedModel.Note(note.code, note.where, n.getKey(), note.count));
        }
        return new ImportedModel(reader.classes, reader.enumerations, notes);
    }

    // ---------------------------------------------------------------------
    // The documents' named schemas
    // ---------------------------------------------------------------------

    private void collect() {
        Json.Obj main = docs.root("");
        boolean openApi = main.has("openapi") || main.has("swagger");
        for (String doc : docs.names()) {
            Json.Obj d = docs.root(doc);
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
                note("root.unresolved", "#", "the root is the reference '" + ref + "' alone, to a document not given: no class is made "
                        + "of it");
            }
            return;
        }
        if (title == null && docs.at(root) instanceof Json.Obj o) {
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
        } else if (docs.at(root) instanceof Json.Obj rootSchema) {
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
                add(item, pureName(rootName, true) + "Item");
            } else if (rootName != null && map) {
                mapRoots.add(root);
                add(root, pureName(rootName, true) + "Entry");
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
        while (docs.at(place) instanceof Json.Obj o && !declaresProperties(o) && objects(o, "allOf").isEmpty()
                && seen.add(place)) {
            String ref = refOf(o);
            if (ref == null) {
                break;
            }
            String t = docs.target(place, ref);
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
            keyed.add(child(loc(doc, pointer), key));
        }
    }

    /** A named schema: its Pure name, numbered when another's (noted). */
    private void add(String loc, String key) {
        String name = pureName(key, true);
        String held = unique(name);
        if (!held.equals(name)) {
            note("name.numbered", where(loc), "its name '" + name + "' is another schema's, so it is named " + held);
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
                            Json.Node r = docs.at(response);
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
        Json.Node node = docs.at(place);
        String ref = node instanceof Json.Obj o ? refOf(o) : null;
        if (ref != null) {
            String t = docs.target(place, ref);
            if (t == null) {
                return null;
            }
            place = t;
            node = docs.at(place);
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
        if (loc == null || !(docs.at(loc) instanceof Json.Obj s) || refOf(s) != null) {
            return;
        }
        if (shapeAt(loc) == Shape.CLASS) {
            add(loc, name);
        } else if (docs.at(child(loc, "items")) instanceof Json.Obj items && refOf(items) == null
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
        Json.Node node = docs.at(loc);
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
            String t = docs.target(loc, ref);
            return (t != null && shapeAt(t) == Shape.CLASS) || declaresProperties(part);
        }
        return objectShaped(part, loc);
    }

    /**
     * The union keyword ({@code oneOf}, {@code anyOf}) whose alternatives but {@code null} are two or more, one an
     * object at least and each other a value of its own (a primitive, an enumeration, a list), which
     * {@link #alternatives} wraps; a union one of whose alternatives says no value (a {@code required} alone) is
     * logic, and one of no object a {@code Variant} or the primitive its alternatives share.
     */
    private @com.legend.base.Nullable String unionOfObjects(Json.Obj schema, String loc) {
        for (String union : List.of("oneOf", "anyOf")) {
            List<Json.Obj> parts = objects(schema, union);
            int values = 0;
            boolean anObject = false;
            boolean allValues = true;
            for (int i = 0; i < parts.size(); i++) {
                if (isNull(parts.get(i))) {
                    continue;
                }
                values++;
                if (isObject(parts.get(i), child(loc, union, Integer.toString(i)))) {
                    anObject = true;
                } else {
                    allValues &= saysValue(parts.get(i));
                }
            }
            if (values >= 2 && anObject && allValues) {
                return union;
            }
        }
        return null;
    }

    /** Whether a union's alternative says a value of its own -- a type, a reference, an {@code enum} or a
     *  {@code const}, or keywords that imply a type -- rather than a requirement alone. */
    private static boolean saysValue(Json.Obj part) {
        return refOf(part) != null || part.has("type") || part.has("enum") || part.has("const")
                || !implied(part).isEmpty();
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
            refsIn(place, docs.at(place), refs);
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
                String t = docs.target(loc, ref);
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
        buildClass(held, docs.schemaAt(loc), loc, owned && n == null);
        return held;
    }

    /**
     * The class (or, {@code enumeration}, the enumeration) of a property's schema written in place: the one another
     * place of the same text, in the same document, made -- an API's request and response bodies repeat their inline
     * objects and enums, which openapi-generator's inline model resolver also reuses, named after the first -- else
     * its own. A schema a document names by a key is never another's: its name is the author's.
     */
    private String inPlace(String loc, String name, boolean enumeration) {
        if (named.containsKey(loc) || (enumeration ? enumAt : classAt).containsKey(loc)) {
            return enumeration ? enumFor(loc, name) : classFor(loc, name, true);
        }
        String text = (enumeration ? "enumeration " : "class ") + docOf(loc) + " " + Json.toCompact(docs.schemaAt(loc));
        String twin = twins.get(text);
        if (twin != null) {
            (enumeration ? enumAt : classAt).put(loc, twin);
            return twin;
        }
        if (enumeration) {
            String made = enumFor(loc, name);
            twins.put(text, made);
            return made;
        }
        return twinClass(text, loc, unique(name));
    }

    /** A class written in place, of a name already its own and the text it is the class of: the text recorded before
     *  the class is built, so a place of the same text met while building it (through a reference back) is it. */
    private String twinClass(String text, String loc, String held) {
        twins.put(text, held);
        classAt.put(loc, held);
        buildClass(held, docs.schemaAt(loc), loc, true);
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
            gather(docs.schemaAt(g.conditions.get(i)), g.conditions.get(i), g, true);
        }
        if (g.properties.size() > before) {
            note("condition.optional-properties", where(loc), "the properties of a condition (if/then/else, dependentSchemas) are read as optional "
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
            String property = pureName(e.getKey(), false);
            String pureName = property;
            for (int n = 2; !names.add(pureName); n++) {
                pureName = property + n;
            }
            if (!pureName.equals(property)) {
                note("name.numbered", where(at), "its name '" + property + "' is another property's, so it is named " + pureName);
            }
            directs.add(at);
            Resolved r = resolve(ps, at, name, pureName);
            if (r == null) {
                continue;
            }
            if (r == NEVER) {
                note("property.no-value", where(at), "a property that can hold no value is not a property");
                nevers.add(e.getKey());
                continue;
            }
            boolean mustHave = g.required.contains(e.getKey()) && !g.conditional.contains(e.getKey()) && !r.nullable();
            int lower = r.many() ? (mustHave ? r.minItems() : 0) : (mustHave ? 1 : 0);
            Json.Obj pso = ps instanceof Json.Obj o ? o : Json.obj();
            Json.Node carried = defaultOf(pso, r, at);
            List<Annotation> annotations = annotations(pso, e.getKey(), pureName);
            Json.Node given = pso.getOr("default", null);
            if (carried == null && given != null && !(given instanceof Json.Null)) {
                // a default that is no Pure value of the property: kept as the schema says it
                annotations.add(Annotation.tag(GENERATION, "defaultValue", json(given)));
            }
            String format = text(pso, "format");
            if (format != null && FORMAT_STEREOTYPES.contains(format)) {
                annotations.add(Annotation.stereotype(TYPE_EXTENSION, format));
            }
            if (r.nullable()) {
                annotations.add(Annotation.stereotype(TYPE_EXTENSION, "null"));
            }
            out.add(new ImportedProperty(pureName, doc(pso), r.kind(), r.type(), lower, r.many() ? r.maxItems()
                    : Integer.valueOf(1), deprecated(pso), carried, annotations));
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
            Json.Obj at = docs.at(place) instanceof Json.Obj o ? o : Json.obj();
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
                note("rule.not-carried", where(loc), "a dependentRequired naming a property the schema does not declare of its own is not "
                        + "carried");
            } else if (!others.isEmpty()) {
                rules.add(new Requires(property, false, others));
            }
        }
        // a union of objects: its alternatives subclasses; written in place, each a value the owner holds
        String union = unionOfObjects(schema, loc);
        boolean inPlace = union == null || alternatives(name, schema, union, loc);
        Named self = named.get(loc);
        // a union one for its alternatives, wherever written: its documentation is each property's own
        boolean shared = sharedUnions.contains(loc);
        List<Annotation> annotations = shared ? new ArrayList<>() : annotations(schema,
                self != null && keyed.contains(loc) ? self.key() : null, name);
        if (schema.getOr("additionalProperties", null) instanceof Json.Bool b && !b.value()) {
            annotations.add(Annotation.stereotype(GENERATION, "noAdditionalProperties"));
        }
        classes.add(position, new ImportedClass(name, shared ? null : doc(schema), g.supers, out, owned && inPlace,
                deprecated(schema),
                rules, logic, annotations));
    }

    /** A map at the root: a class of its entries, each its key and its value ({@code <Root>Value} when the value is an
     *  object of its own); the key's pattern ({@code patternProperties}' one, {@code propertyNames}') its rules. */
    private void entries(String name, String loc) {
        int position = classes.size();
        Json.Obj schema = docs.schemaAt(loc);
        keywords(schema, loc);
        note("root.map-entries", where(loc), "a map at the root is a class of its entries, each its key and its value");
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
            Json.Node v = docs.at(values.get(0));
            valueSchema = v instanceof Json.Obj o ? o : valueSchema;
            r = resolve(v == null ? Json.bool(true) : v, values.get(0), owner, "value");
        } else {
            note("variant.map-values", where(loc), "a map whose values are of several schemas (patternProperties) holds each a Variant");
            r = new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        if (r != null && r != NEVER) {
            int lower = r.nullable() ? 0 : r.many() ? r.minItems() : 1;
            properties.add(new ImportedProperty("value", doc(valueSchema), r.kind(), r.type(), lower,
                    r.many() ? r.maxItems() : Integer.valueOf(1), deprecated(valueSchema), null,
                    annotations(valueSchema, null, "value")));
            for (Rule rule : r.rules()) {
                rules.add(of(rule, "value"));
            }
        }
        classes.add(position, new ImportedClass(name, doc(schema), List.of(), properties, false, deprecated(schema),
                rules, List.of(), annotations(schema, null, name)));
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
            note("map.not-carried", where(loc), "a map beside the properties (additionalProperties, patternProperties) is not carried: a "
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
        String t = docs.target(at, ref);
        if (t == null) {
            note("reference.unresolved", where(at), "the reference '" + ref + "' names nothing in the documents given: its properties are not "
                    + "read");
        } else if (docs.at(t) instanceof Json.Obj o) {
            gather(o, t, g, true);
        }
    }

    /** A superclass from a reference in an allOf or beside properties: a class's; else noted. */
    private void addSuper(Gathered g, String ref, String from, String at) {
        String t = docs.target(from, ref);
        if (t == null) {
            note("reference.unresolved", where(at), "the reference '" + ref + "' names nothing in the documents given: its properties are not "
                    + "inherited");
            return;
        }
        if (t.equals(g.self)) {
            note("inheritance.self", where(at), "an allOf naming its own schema names no superclass, and is skipped");
            return;
        }
        if (shapeAt(t) == Shape.CLASS) {
            String s = classFor(t, null, false);
            if (!g.supers.contains(s)) {
                g.supers.add(s);
            }
        } else {
            note("inheritance.not-object", where(at), "an allOf part that is not an object (a map, a primitive) is not carried: a class has the "
                    + "properties it names");
        }
    }

    /**
     * A UNION OF OBJECTS read as inheritance (the study's §4.2): each alternative a subclass of {@code superclass} --
     * a referenced class gains it as a superclass, an object written in place is a class of its own, named after the
     * union and its title (or {@code Option1}, {@code Option2}, ...), and a value that is no object a class wrapping
     * it ({@link #wrapper}). An anyOf is read as a oneOf: an instance is one class.
     *
     * @return whether every alternative is written in place (each a value its owner holds)
     */
    private boolean alternatives(String superclass, Json.Obj schema, String union, String loc) {
        if (union.equals("anyOf")) {
            note("inheritance.any-of", where(loc), "anyOf is read as oneOf (an instance is one of its alternatives)");
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
            if (!isObject(part, at)) {
                sub = wrapper(wrapperBases.getOrDefault(loc, superclass), part, at);
                if (sub == null) {
                    continue;
                }
            } else if (ref != null && !declaresProperties(part)) {
                keywords(part, at);
                String t = docs.target(at, ref);
                if (t == null) {
                    continue;
                }
                sub = classFor(t, null, false);
                inPlace = false;
            } else {
                String title = text(part, "title");
                sub = classFor(at, superclass + (title != null ? pureName(title, true) : "Option" + (i + 1)),
                        true);
            }
            unionSupers.computeIfAbsent(sub, k -> new LinkedHashSet<>()).add(superclass);
        }
        return inPlace;
    }

    /**
     * The superclass of a union written in place: ONE for each set of alternatives in a document, wherever it is
     * written -- Stripe writes {@code anyOf: [string, $ref customer, $ref deleted_customer]} on twenty properties,
     * each with its own description, and they are one type -- named after its members when each object among them is
     * a schema the document names, three at most ({@code CustomerOrDeletedCustomerOrString}, its wrapper
     * {@code CustomerOrDeletedCustomerString}); else after its first place, as a twin ({@link #inPlace}). A union
     * that says more than its alternatives (properties, a type, a rule) is its place's own.
     */
    private String unionFor(String loc, Json.Obj schema, String union, String owner, String property) {
        if (named.containsKey(loc) || classAt.containsKey(loc) || saysMoreThan(schema, union)) {
            return inPlace(loc, inPlaceName(owner, property), false);
        }
        String text = "union " + docOf(loc) + " " + union + " " + Json.toCompact(schema.get(union)) + " "
                + (schema.getOr("discriminator", null) instanceof Json.Obj d ? Json.toCompact(d) : "");
        String twin = twins.get(text);
        if (twin != null) {
            classAt.put(loc, twin);
            return twin;
        }
        sharedUnions.add(loc);
        List<String> members = members(schema, union, loc);
        if (members != null) {
            wrapperBases.put(loc, members.get(0));
        }
        return twinClass(text, loc, unique(members != null ? members.get(1) : inPlaceName(owner, property)));
    }

    /** Whether a union's schema says something beyond its alternatives: documentation, nullability, a discriminator,
     *  {@code type: object} and extensions say nothing more. */
    private static boolean saysMoreThan(Json.Obj schema, String union) {
        for (Map.Entry<String, Json.Node> e : schema.fields().entrySet()) {
            String k = e.getKey();
            boolean object = k.equals("type") && e.getValue() instanceof Json.Str t && t.value().equals("object");
            if (!k.equals(union) && !k.equals("discriminator") && !object && saysShape(only(schema, k))) {
                return true;
            }
        }
        return false;
    }

    /** A union's name of its members, and its wrappers' base: its objects' names joined by {@code Or}, then its
     *  values' types ({@code [CustomerOrDeletedCustomer, CustomerOrDeletedCustomerOrString]}); null unless each
     *  object is a schema the document names, three at most. */
    private @com.legend.base.Nullable List<String> members(Json.Obj schema, String union, String loc) {
        List<String> objects = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<Json.Obj> parts = objects(schema, union);
        for (int i = 0; i < parts.size(); i++) {
            Json.Obj part = parts.get(i);
            String at = child(loc, union, Integer.toString(i));
            if (isNull(part)) {
                continue;
            }
            if (!isObject(part, at)) {
                values.add(wrapperSuffix(part, at));
                continue;
            }
            String ref = refOf(part);
            String t = ref == null || declaresProperties(part) ? null : docs.target(at, ref);
            Named n = t == null ? null : named.get(t);
            if (n == null) {
                return null;
            }
            objects.add(n.name());
        }
        if (objects.isEmpty() || objects.size() > 3) {
            return null;
        }
        String base = String.join("Or", objects);
        return List.of(base, values.isEmpty() ? base : base + "Or" + String.join("Or", values));
    }

    /**
     * An alternative of a union that is no object, as a class of its own: its one property {@code value}, the
     * alternative's value -- Stripe's {@code anyOf: [string, $ref card]} is a {@code CustomerDefaultSource} whose
     * subclasses are {@code Card} and {@code CustomerDefaultSourceString {value: String[1]}}. Pure has no union of a
     * primitive and a class; a superclass of classes is one every reader of Pure reads (register S91). Null when the
     * alternative can hold no value.
     */
    private @com.legend.base.Nullable String wrapper(String superclass, Json.Obj part, String at) {
        String base = superclass + wrapperSuffix(part, at);
        Resolved r = resolve(part, at, base, "value");
        if (r == null || r == NEVER) {
            return null;
        }
        String name = unique(base);
        List<Rule> rules = new ArrayList<>();
        for (Rule rule : r.rules()) {
            rules.add(of(rule, "value"));
        }
        ImportedProperty value = new ImportedProperty("value", null, r.kind(), r.type(),
                r.many() ? r.minItems() : (r.nullable() ? 0 : 1), r.many() ? r.maxItems() : Integer.valueOf(1));
        classes.add(new ImportedClass(name, doc(part), List.of(), List.of(value), true, deprecated(part), rules,
                List.of(), annotations(part, null, name)));
        return name;
    }

    /** A wrapper's name after its superclass's: the alternative's title, the name of the schema it references, or
     *  its type ({@code String}, {@code StrictDate}; a list's, {@code StringList}); else {@code Value}. */
    private String wrapperSuffix(Json.Obj part, String at) {
        String title = text(part, "title");
        if (title != null && !title.strip().isEmpty()) {
            return pureName(title, true);
        }
        String ref = refOf(part);
        if (ref != null) {
            String t = docs.target(at, ref);
            Named n = t == null ? null : named.get(t);
            return n != null ? n.name() : t != null ? nameFor(t) : "Value";
        }
        List<String> types = new ArrayList<>(types(part));
        types.remove("null");
        String type = types.size() == 1 ? types.get(0) : types.isEmpty() ? implied(part) : "";
        if (PRIMITIVES.contains(type)) {
            return primitive(type, text(part, "format"));
        }
        if (type.equals("array")) {
            return (part.getOr("items", null) instanceof Json.Obj items && refOf(items) == null
                    ? wrapperSuffix(items, child(at, "items")) : "") + "List";
        }
        return "Value";
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
            classes.set(i, c.with(supers, owned));
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
                note("inheritance.cycle", c.name(), "a class that would be its own superclass (" + String.join(" extends ", path)
                        + ") does not extend " + dropped);
            }
            if (graph.getOrDefault(c.name(), List.of()).size() != c.supers().size()) {
                classes.set(i, c.with(graph.getOrDefault(c.name(), List.of()), c.owned()));
            }
        }
    }

    /**
     * OpenAPI's discriminator, as upstream marks it: the property telling the subclasses apart
     * ({@code <<JSONSchemaOpenAPIExtension.discriminatorProperty>>}), and each subclass's value of it
     * ({@code JSONSchemaOpenAPIExtension.discriminatorName}): its {@code mapping}'s key, else the subclass's name in
     * the schema (OpenAPI's default). A subclass's SQL tells its rows apart by it.
     */
    private void discriminators() {
        Map<String, String> locOf = new HashMap<>();
        for (Map.Entry<String, String> e : classAt.entrySet()) {
            locOf.put(e.getValue(), e.getKey());
        }
        for (int i = 0; i < classes.size(); i++) {
            ImportedClass c = classes.get(i);
            String loc = locOf.get(c.name());
            Json.Obj d = loc == null ? null : objectAt(docs.schemaAt(loc), "discriminator");
            String property = d == null ? null : text(d, "propertyName");
            if (loc == null || d == null || property == null) {
                continue;
            }
            // the subclasses' values: the mapping's, by the place each names
            Map<String, String> valueAt = new HashMap<>();
            if (objectAt(d, "mapping") instanceof Json.Obj mapping) {
                for (Map.Entry<String, Json.Node> m : mapping.fields().entrySet()) {
                    String t = m.getValue() instanceof Json.Str ref ? docs.target(loc, ref.value()) : null;
                    if (t != null) {
                        valueAt.put(t, m.getKey());
                    }
                }
            }
            for (int j = 0; j < classes.size(); j++) {
                ImportedClass sub = classes.get(j);
                String subLoc = locOf.get(sub.name());
                if (!sub.supers().contains(c.name()) || subLoc == null) {
                    continue;
                }
                Named n = named.get(subLoc);
                String value = valueAt.getOrDefault(subLoc, n != null && keyed.contains(subLoc) ? n.key() : null);
                if (value != null) {
                    classes.set(j, sub.with(sub.properties(), List.of(Annotation.tag(OPENAPI_EXTENSION,
                            "discriminatorName", value))));
                }
            }
            List<ImportedProperty> marked = new ArrayList<>();
            for (ImportedProperty p : classes.get(i).properties()) {
                boolean it = p.name().equals(pureName(property, false));
                marked.add(it ? p.annotated(Annotation.stereotype(OPENAPI_EXTENSION, "discriminatorProperty")) : p);
            }
            classes.set(i, classes.get(i).with(marked, List.of()));
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
            note("variant.not-a-schema", where(loc), "a schema that is not a JSON object or a boolean is any value: a Variant");
            return new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        keywords(schema, loc);
        if (!directs.contains(loc)) {
            if (schema.has("not")) {
                note("rule.not-carried", where(loc), "not is not carried (a validation Pure states no constraint for here)");
            }
            if (schema.has("if") && !declaresProperties(schema)) {
                note("logic.not-carried", where(loc), "a condition (if/then/else) on a value that is no property's own is not carried");
            }
        }
        boolean empty = isTrue(schema, "nullable") || isTrue(schema, "x-nullable");
        String ref = refOf(schema);
        if (ref != null) {
            if (declaresProperties(schema)) {
                // a reference beside properties: a class of its own, the referenced class its superclass
                return new Resolved(Kind.CLASS, inPlace(loc, inPlaceName(owner, property), false), empty);
            }
            String t = docs.target(loc, ref);
            if (t == null) {
                note("reference.unresolved", where(loc), "the reference '" + ref + "' names nothing in the documents given: a Variant (give "
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
            return new Resolved(Kind.ENUMERATION, inPlace(loc, inPlaceName(owner, property), true),
                    empty || mayBeNull(schema));
        }
        List<Json.Node> values = values(schema);
        if (values != null) {
            return valued(schema, values, loc, empty);
        }
        if (declaresProperties(schema)) {
            // properties of its own: a class, whatever its union says (a union of required lists is a validation)
            return new Resolved(Kind.CLASS, inPlace(loc, inPlaceName(owner, property), false), empty);
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
                note("property.no-value", where(loc), "a value that is always null is not a property");
                return NEVER;
            }
            if (count == 1 && !declaresProperties(schema)) {
                Resolved r = resolve(parts.get(one), child(loc, union, Integer.toString(one)), owner, property);
                return r == null || r == NEVER ? r : r.orEmpty(mayBeNull || empty);
            }
            String alternatives = unionOfObjects(schema, loc);
            if (alternatives != null) {
                return new Resolved(Kind.CLASS, unionFor(loc, schema, alternatives, owner, property),
                        mayBeNull || empty);
            }
            String common = commonPrimitive(parts, loc, union);
            if (common != null) {
                return new Resolved(Kind.PRIMITIVE, common, mayBeNull || empty);
            }
            note("variant.union", where(loc), "a union (" + union + ") of values that are not all objects is a Variant (Pure has no "
                    + "union type)");
            return new Resolved(Kind.PRIMITIVE, VARIANT, mayBeNull || empty);
        }
        if (objectShaped(schema, loc)) {
            return new Resolved(Kind.CLASS, inPlace(loc, inPlaceName(owner, property), false), empty);
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
                note("variant.union", where(loc), "an allOf of values that are not objects is a Variant");
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
                note("property.no-value", where(loc), "a value that is always null is not a property");
                return NEVER;
            }
        }
        if (types.size() > 1) {
            if (types.size() == 2 && types.contains("integer") && types.contains("number")) {
                return new Resolved(Kind.PRIMITIVE, "Number", empty).withRules(rules(schema, "Number", loc));
            }
            note("variant.types", where(loc), "a value of several types (" + String.join(", ", types) + ") is a Variant (Pure has no "
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
                note("variant.file", where(loc), "a file (Swagger 2) is a Variant");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
            default -> {
                note("variant.unknown-type", where(loc), "the type '" + t + "' is not JSON Schema's: a Variant");
                return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
            }
        }
    }

    private @com.legend.base.Nullable Resolved list(Json.Obj schema, String loc, String owner, String property,
            boolean empty) {
        Json.Node items = schema.getOr("items", null);
        if (items instanceof Json.Arr || schema.has("prefixItems")) {
            note("variant.tuple", where(loc), "a tuple (prefixItems, items as a list) is a Variant, the JSON list it is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        int least = schema.getOr("minItems", null) instanceof Json.Num n ? (int) n.longValue() : 0;
        Integer most = schema.getOr("maxItems", null) instanceof Json.Num n ? Integer.valueOf((int) n.longValue())
                : null;
        if (items == null || (items instanceof Json.Bool b && b.value())) {
            note("variant.list", where(loc), "a list of any values is one Variant, the JSON list it is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        Resolved item = resolve(items, child(loc, "items"), owner, singular(property));
        if (item == null) {
            return null;
        }
        if (item == NEVER) {
            note("variant.list", where(loc), "a list that can hold no value is one Variant, the empty JSON list");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        if (item.many() || item.isVariant()) {
            note("variant.list", where(loc), "a list of " + (item.many() ? "lists" : "Variants") + " is one Variant, the JSON list it "
                    + "is");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        if (options.listsAsVariant() && item.kind() != Kind.CLASS) {
            note("variant.lists-option", where(loc), "a list of " + item.type() + " is one Variant, the JSON list a column holds ('lists': "
                    + "'variant')");
            return new Resolved(Kind.PRIMITIVE, VARIANT, empty);
        }
        List<Rule> eachRules = new ArrayList<>();
        for (Rule r : item.rules()) {
            eachRules.add(each(r));
        }
        if (isTrue(schema, "uniqueItems")) {
            if (item.kind() == Kind.CLASS) {
                note("rule.not-carried", where(loc), "uniqueItems of a list of objects is not carried (Pure tells objects apart by "
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
                    docs.at(loc) instanceof Json.Obj o && mayBeNull(o));
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
            note("variant.self-reference", where(loc), "a schema that is itself, through references alone, is any value: a Variant");
            return new Resolved(Kind.PRIMITIVE, VARIANT, false);
        }
        Json.Node node = docs.at(loc);
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
            note("property.no-value", where(loc), values.isEmpty() ? "an enum of no value is not a property"
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
            note("variant.enum", where(loc), "an enum of values of several types, or not single values, is a Variant");
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
                    note("rule.not-carried", where(loc), "multipleOf of a number that is not a whole number is not carried");
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
            note("logic.not-carried", where(place), kindName(kind) + " is not carried: Pure's constraint cannot say " + u.getMessage());
            return;
        }
        if (c.equals(FALSE)) {
            note("logic.never-holds", where(place), kindName(kind) + " that no value keeps is not carried");
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
                    all.add(whole(docs.at(t), t, scope, following));
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
                    all.add(value(p, docs.at(t), t, following, each));
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
        String t = ref instanceof Json.Str s ? docs.target(loc, s.value()) : null;
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
        Json.Obj schema = docs.schemaAt(loc);
        keywords(schema, loc);
        List<String> texts = new ArrayList<>();
        List<@com.legend.base.Nullable String> docs = new ArrayList<>();
        List<Json.Obj> alternatives = constAlternatives(schema);
        if (alternatives != null) {
            for (Json.Obj a : alternatives) {
                String t = constText(a);
                if (t != null && !texts.contains(t)) {
                    texts.add(t);
                    docs.add(label(a));
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
        List<List<Annotation>> valueAnnotations = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            String text = texts.get(i);
            String raw = givenNames != null && givenNames.size() == texts.size() ? givenNames.get(i) : text;
            String held = isName(raw) ? raw : valueName(raw);
            String value = held;
            for (int k = 2; values.contains(value); k++) {
                value = held + "_" + k;
            }
            values.add(value);
            // a value named otherwise than its text: the text kept, as upstream keeps it (and a mapping reads it)
            valueAnnotations.add(value.equals(text) ? List.of() : List.of(Annotation.tag(GENERATION, "name", text)));
        }
        enumerations.add(new ImportedEnum(name, doc(schema), values, texts, docs,
                annotations(schema, n != null && keyed.contains(loc) ? n.key() : null, name), valueAnnotations));
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
                note("rule.not-carried", where(loc), key + " is not carried (a validation Pure states no constraint for here)");
            } else {
                note("keyword.unknown", where(loc), "the keyword '" + key + "' is not JSON Schema's, and is not read");
            }
        }
    }

    /** A note, said once with where it was first met; met again, counted. */
    private void note(String code, String where, String message) {
        Note n = notes.get(message);
        if (n == null) {
            notes.put(message, new Note(code, where.isEmpty() ? "#" : where));
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

    /** A name not yet a class's or enumeration's: {@code name}, else {@code name2}, {@code name3}, ... */
    private String unique(String name) {
        String out = name;
        for (int n = 2; !taken.add(out); n++) {
            out = name + n;
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // JSON
    // ---------------------------------------------------------------------

    private static @com.legend.base.Nullable String text(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Str s ? s.value() : null;
    }

    /** An element's documentation: its description (VS Code's markdown one when it has no plain one); its title is
     *  a tag of its own ({@link #annotations}), as upstream writes it. */
    private static @com.legend.base.Nullable String doc(Json.Obj o) {
        for (String k : List.of("description", "markdownDescription")) {
            String d = text(o, k);
            if (d != null && !d.strip().isEmpty()) {
                return d;
            }
        }
        return null;
    }

    /** A value's documentation, a {@code const} of a union's: its description, else its title. */
    private static @com.legend.base.Nullable String label(Json.Obj o) {
        String d = doc(o);
        return d != null ? d : text(o, "title");
    }

    /**
     * What a schema says of an element beyond its shape, as upstream's profiles say it: its name in the schema
     * ({@code schemaName}) where Pure's differs, its title where it is neither name, an example, readOnly and
     * writeOnly.
     */
    private static List<Annotation> annotations(Json.Obj schema, @com.legend.base.Nullable String schemaName,
            String pureName) {
        List<Annotation> out = new ArrayList<>();
        if (schemaName != null && !schemaName.equals(pureName)) {
            out.add(Annotation.tag(GENERATION, "name", schemaName));
        }
        String title = text(schema, "title");
        if (title != null && !title.strip().isEmpty() && !title.equals(pureName) && !title.equals(schemaName)) {
            out.add(Annotation.tag(GENERATION, "title", title));
        }
        Json.Node example = schema.getOr("example", null);
        if (example == null && schema.getOr("examples", null) instanceof Json.Arr examples
                && !examples.items().isEmpty()) {
            example = examples.items().get(0);
        }
        if (example != null && !(example instanceof Json.Null) && json(example).length() <= EXAMPLE_LIMIT) {
            out.add(Annotation.tag(GENERATION, "example", json(example)));
        }
        for (String k : List.of("readOnly", "writeOnly")) {
            if (isTrue(schema, k)) {
                out.add(Annotation.stereotype(GENERATION, k));
            }
        }
        return out;
    }

    /** A JSON value as a tag holds it: a text as itself, anything else as its JSON. */
    private static String json(Json.Node value) {
        return value instanceof Json.Str s ? s.value() : Json.toCompact(value);
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
            // kept as the schema says it, a tag (buildClass)
            return null;
        }
        if (r.kind() == Kind.ENUMERATION && d instanceof Json.Str s) {
            for (ImportedEnum e : enumerations) {
                if (e.name().equals(r.type())) {
                    int i = e.texts().indexOf(s.value());
                    if (i < 0) {
                        // not one of its values: kept as the schema says it, a tag (buildClass)
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
