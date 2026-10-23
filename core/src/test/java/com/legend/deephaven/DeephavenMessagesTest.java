// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.deephaven.DeephavenMessages.Card;
import com.legend.deephaven.DeephavenMessages.Field;
import com.legend.deephaven.DeephavenMessages.Kind;
import com.legend.deephaven.DeephavenMessages.Message;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lite's Deephaven field table against Deephaven's own definitions (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11): every
 * message and enum the table lists, read from the {@code .proto} files in the pinned server's protocol jar (a test
 * input, never on a classpath), with each field's number, kind, the type it holds and its cardinality as the
 * definition declares them. A row held wrong (the browser check's: a table reference's offset is a zigzag
 * {@code sint32}) fails here, not as a request the server rejects.
 */
class DeephavenMessagesTest {

    private static final String PACKAGE = "io.deephaven.proto.backplane.grpc.";

    /** A field as a definition declares it: its protobuf type as written, label and whether a oneof holds it. */
    private record Declared(String name, int number, String type, boolean repeated, boolean present) {
    }

    @Test
    void everyRowIsAsDeephavensDefinitionsDeclareIt() throws IOException {
        Definitions defs = Definitions.read(Runfile.property("deephaven.proto.jar"));
        int fields = 0;
        for (Message m : DeephavenMessages.all()) {
            Map<String, Declared> declared = defs.messages.get(m.name());
            assertTrue(declared != null, "Deephaven defines no message " + m.name());
            for (Field f : m.fields().values()) {
                Declared d = declared.get(f.json());
                assertTrue(d != null, m.name() + " has no field " + f.json() + " (it has " + declared.keySet() + ")");
                String where = m.name() + "." + f.json();
                assertEquals(d.number(), f.number(), where + ": number");
                assertEquals(d.repeated() ? Card.REPEATED : d.present() ? Card.PRESENT : Card.SINGLE, f.card(),
                        where + ": cardinality");
                Kind scalar = SCALARS.get(d.type());
                if (scalar != null) {
                    assertEquals(scalar, f.kind(), where + ": kind");
                    assertEquals(null, f.type(), where + ": a scalar holds no type");
                } else {
                    String resolved = defs.resolve(m.name(), d.type());
                    assertEquals(defs.enums.containsKey(resolved) ? Kind.ENUM : Kind.MESSAGE, f.kind(), where + ": kind");
                    assertEquals(resolved, f.type(), where + ": the type it holds");
                }
                fields++;
            }
        }
        for (Map.Entry<String, Map<String, Integer>> e : DeephavenMessages.enums().entrySet()) {
            assertEquals(defs.enums.get(e.getKey()), e.getValue(), "enum " + e.getKey());
        }
        assertTrue(fields > 80, "the table lists " + fields + " fields");
    }

    private static final Map<String, Kind> SCALARS = Map.ofEntries(Map.entry("string", Kind.STRING),
            Map.entry("bytes", Kind.BYTES), Map.entry("bool", Kind.BOOL), Map.entry("int32", Kind.INT32),
            Map.entry("int64", Kind.INT64), Map.entry("uint32", Kind.UINT32), Map.entry("uint64", Kind.UINT64),
            Map.entry("sint32", Kind.SINT32), Map.entry("sint64", Kind.SINT64), Map.entry("double", Kind.DOUBLE));

    /** The messages and enums the package's {@code .proto} files define, by name as the table writes it (nested
     *  names joined by dots, the package left off), each message's fields by JSON name. */
    private static final class Definitions {
        final Map<String, Map<String, Declared>> messages = new LinkedHashMap<>();
        final Map<String, Map<String, Integer>> enums = new LinkedHashMap<>();

        static Definitions read(java.nio.file.Path jar) throws IOException {
            Definitions defs = new Definitions();
            try (ZipFile zip = new ZipFile(jar.toFile())) {
                for (ZipEntry entry : java.util.Collections.list(zip.entries())) {
                    if (entry.getName().endsWith(".proto")) {
                        String text = new String(zip.getInputStream(entry).readAllBytes(), StandardCharsets.UTF_8);
                        if (text.contains("package " + PACKAGE.substring(0, PACKAGE.length() - 1) + ";")) {
                            new Parser(tokens(text), defs).file();
                        }
                    }
                }
            }
            assertTrue(defs.messages.containsKey("BatchTableRequest"), "no BatchTableRequest in " + jar);
            return defs;
        }

        /** {@code type} as written inside {@code scope}, resolved as protobuf scopes it: the innermost enclosing
         *  message that defines it, outward to the package. */
        String resolve(String scope, String type) {
            String bare = type.startsWith(".") ? type.substring(1) : type;
            String t = bare.startsWith(PACKAGE) ? bare.substring(PACKAGE.length()) : bare;
            for (String s = scope; ; s = s.contains(".") ? s.substring(0, s.lastIndexOf('.')) : "") {
                String candidate = s.isEmpty() ? t : s + "." + t;
                if (messages.containsKey(candidate) || enums.containsKey(candidate)) {
                    return candidate;
                }
                if (s.isEmpty()) {
                    throw new AssertionError("type " + type + " (in " + scope + ") is not defined");
                }
            }
        }
    }

    private static List<String> tokens(String text) {
        String bare = text.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("//[^\n]*", " ");
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("\"[^\"]*\"|-?[0-9]+|[A-Za-z_][A-Za-z0-9_.]*|[{}\\[\\]()<>=;,]").matcher(bare);
        while (m.find()) {
            out.add(m.group());
        }
        return out;
    }

    /** A reader for the part of protobuf's language these files use: messages, enums and oneofs, nested; fields
     *  with a label and options; and the statements skipped (syntax, package, import, option, reserved, service). */
    private static final class Parser {
        private final List<String> t;
        private final Definitions defs;
        private int at;

        Parser(List<String> tokens, Definitions defs) {
            this.t = tokens;
            this.defs = defs;
        }

        void file() {
            while (at < t.size()) {
                String word = t.get(at);
                switch (word) {
                    case "message" -> message("");
                    case "enum" -> enumeration("");
                    case "service" -> skipBlock();
                    default -> skipStatement();
                }
            }
        }

        private void message(String outer) {
            at++;
            String name = outer.isEmpty() ? t.get(at++) : outer + "." + t.get(at++);
            expect("{");
            Map<String, Declared> fields = new LinkedHashMap<>();
            defs.messages.put(name, fields);
            body(name, fields, false);
        }

        /** A message's or a oneof's statements, to its closing brace. */
        private void body(String name, Map<String, Declared> fields, boolean inOneof) {
            while (!t.get(at).equals("}")) {
                switch (t.get(at)) {
                    case "message" -> message(name);
                    case "enum" -> enumeration(name);
                    case "oneof" -> {
                        at += 2;
                        expect("{");
                        body(name, fields, true);
                    }
                    case "option", "reserved", "extensions" -> skipStatement();
                    default -> field(fields, inOneof);
                }
            }
            at++;
        }

        private void field(Map<String, Declared> fields, boolean inOneof) {
            boolean repeated = t.get(at).equals("repeated");
            boolean optional = t.get(at).equals("optional");
            if (repeated || optional) {
                at++;
            }
            String type = t.get(at++);
            if (type.equals("map")) {
                while (!t.get(at - 1).equals(">")) {
                    at++;
                }
                type = "map";
                repeated = true;
            }
            String name = t.get(at++);
            expect("=");
            int number = Integer.parseInt(t.get(at++));
            skipStatement();
            fields.put(jsonName(name), new Declared(name, number, type, repeated, inOneof || optional));
        }

        private void enumeration(String outer) {
            at++;
            String name = outer.isEmpty() ? t.get(at++) : outer + "." + t.get(at++);
            expect("{");
            Map<String, Integer> values = new LinkedHashMap<>();
            while (!t.get(at).equals("}")) {
                if (t.get(at).equals("option") || t.get(at).equals("reserved")) {
                    skipStatement();
                    continue;
                }
                String value = t.get(at++);
                expect("=");
                values.put(value, Integer.parseInt(t.get(at++)));
                skipStatement();
            }
            at++;
            defs.enums.put(name, values);
        }

        /** To the end of the statement: past its {@code ;}, or past its block. */
        private void skipStatement() {
            while (!t.get(at).equals(";") && !t.get(at).equals("{")) {
                at++;
            }
            if (t.get(at).equals("{")) {
                at--;
                skipBlock();
            } else {
                at++;
            }
        }

        private void skipBlock() {
            while (!t.get(at).equals("{")) {
                at++;
            }
            int depth = 0;
            do {
                if (t.get(at).equals("{")) {
                    depth++;
                } else if (t.get(at).equals("}")) {
                    depth--;
                }
                at++;
            } while (depth > 0);
        }

        private void expect(String token) {
            if (!t.get(at).equals(token)) {
                throw new AssertionError("expected '" + token + "' at token " + at + ", found '" + t.get(at) + "'");
            }
            at++;
        }
    }

    /** protobuf's JSON name of a field: its snake-case name in lower camel case. */
    private static String jsonName(String name) {
        StringBuilder b = new StringBuilder();
        boolean upper = false;
        for (char c : name.toCharArray()) {
            if (c == '_') {
                upper = true;
            } else {
                b.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return b.toString();
    }
}
