// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import com.legend.json.Json;
import com.legend.json.Yaml;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The documents a schema import reads (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4), and the places in them: a place is
 * a document and a JSON pointer in it, {@code "doc#/a/b"}, the main document's name empty. Each document's
 * {@code $id}s and {@code $anchor}s are indexed once, when it is read; a reference from a place is answered with the
 * place it names ({@link #target}).
 */
final class SchemaDocuments {

    /** A JSON document nests deeper than a model message does. */
    private static final Json.Config DEPTH = new Json.Config(512);

    private final Map<String, Json.Node> documents = new LinkedHashMap<>();
    private final Map<String, Map<String, String>> anchors = new HashMap<>();
    private final Map<String, String> ids = new LinkedHashMap<>();

    private SchemaDocuments() {
    }

    /**
     * The main document, {@code text}, and the {@code others} its references name, each by the name a reference
     * spells it: read, and indexed.
     *
     * @throws SchemaImportException when one is not JSON or YAML, or the main one's root is not a mapping
     */
    static SchemaDocuments of(String text, Map<String, String> others) {
        SchemaDocuments out = new SchemaDocuments();
        out.documents.put("", parse(text, "the schema"));
        if (!(out.documents.get("") instanceof Json.Obj)) {
            throw new SchemaImportException(List.of("the schema is a document whose root is a mapping, not "
                    + Json.toCompact(out.documents.get(""))));
        }
        for (Map.Entry<String, String> d : others.entrySet()) {
            out.documents.put(normalized(d.getKey()), parse(d.getValue(), "the document '" + d.getKey() + "'"));
        }
        for (Map.Entry<String, Json.Node> d : out.documents.entrySet()) {
            // draft 4 and before: a schema's identifier is its "id"
            String dialect = d.getValue() instanceof Json.Obj o && o.getOr("$schema", null) instanceof Json.Str s
                    ? s.value() : null;
            boolean legacy = dialect != null && (dialect.contains("draft-04") || dialect.contains("draft-03"));
            out.index(d.getKey(), "", d.getValue(), legacy);
        }
        return out;
    }

    /** The documents' names, the main one's (empty) first. */
    Set<String> names() {
        return documents.keySet();
    }

    /** A document's root, an empty object when it is none. */
    Json.Obj root(String name) {
        return documents.get(name) instanceof Json.Obj o ? o : Json.obj();
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

    /** The place of a pointer in a document. */
    static String loc(String doc, String pointer) {
        return doc + "#" + pointer;
    }

    /** The document a place is in. */
    static String docOf(String loc) {
        return loc.substring(0, loc.indexOf('#'));
    }

    /** The place below a place by its keys, each escaped as a JSON pointer escapes it. */
    static String child(String loc, String... parts) {
        StringBuilder out = new StringBuilder(loc);
        for (String p : parts) {
            out.append('/').append(p.replace("~", "~0").replace("/", "~1"));
        }
        return out.toString();
    }

    /** The schema at a place known to be one (a JSON object); an empty one if it is not. */
    Json.Obj schemaAt(String loc) {
        return at(loc) instanceof Json.Obj o ? o : Json.obj();
    }

    /** The node at a place, or null. */
    @com.legend.base.Nullable Json.Node at(String loc) {
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
    static @com.legend.base.Nullable String refOf(Json.Obj schema) {
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
    @com.legend.base.Nullable String target(String from, String ref) {
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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
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

    private static boolean hex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /** Whether a text is one or more ASCII digits: an index into a list. */
    static boolean digits(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) < '0' || text.charAt(i) > '9') {
                return false;
            }
        }
        return !text.isEmpty();
    }

    /** A text cut at each {@code separator}: every part, the empty ones too. */
    static List<String> parts(String text, char separator) {
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
}
