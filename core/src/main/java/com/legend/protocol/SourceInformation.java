// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Protocol JSON without its {@code sourceInformation}, as the engine answers {@code returnSourceInformation=false}. */
public final class SourceInformation {

    private SourceInformation() {
    }

    /** {@code json} with every {@code sourceInformation} field removed. A whole model nests deep,
     *  so the parse is not held to a request's depth limit. */
    public static String strip(String json) {
        return Json.toCompact(strip(Json.parse(json, new Json.Config(4096))));
    }

    /** {@link #strip(String)} over a parsed node. */
    public static Object strip(Json.Node n) {
        return strip(n, false);
    }

    /**
     * {@code json} with EVERY span removed: {@code sourceInformation} and the named spans the protocol carries
     * beside it ({@code classSourceInformation}, {@code profileSourceInformation}, ...) -- the JSON of a model
     * parsed without source information, as the engine writes it ({@code returnSourceInformation=false}) and as
     * entity JSON arrives.
     */
    public static String stripAll(String json) {
        return Json.toCompact(strip(Json.parse(json, new Json.Config(4096)), true));
    }

    /**
     * {@code json} without source information, as legend-engine writes a model or lambda parsed with
     * {@code returnSourceInformation=false} (parser-equivalence's CorpusSweepTest, claim 1c; the protocol program's
     * leg 5): every span removed, as {@link #stripAll} removes them, EXCEPT inside the values the engine parses through
     * a span context of its own, which records spans whatever it was asked
     * ({@code new ParseTreeWalkerSourceInformation.Builder(sourceId, lineOffset, columnOffset)}, whose default is to
     * record; legend-engine 4.145.0 builds one in exactly five places, {@link #keptByTheEngine}). And NOTHING else
     * changed: the text is cut, never parsed and written again, so what the emitter wrote stays byte for byte -- a
     * {@code _type} key the engine writes twice included, which a parsed object would keep once. An object's
     * {@code _type} is its first member, where the emitter writes it, so it is known before any span or nested value in
     * the object is met.
     */
    public static String withoutSpans(String json) {
        StringBuilder out = new StringBuilder(json.length());
        java.util.ArrayList<Frame> stack = new java.util.ArrayList<>();
        boolean atKey = false;   // inside an object, where a member's key comes next
        String key = null;       // the member whose value comes next
        int i = 0;
        int n = json.length();
        while (i < n) {
            char c = json.charAt(i);
            if (c == '{' || c == '[') {
                boolean inObject = !stack.isEmpty() && stack.get(stack.size() - 1).object;
                stack.add(new Frame(c == '{', inObject ? key : null));
                atKey = c == '{';
                key = null;
                out.append(c);
                i++;
            } else if (c == '}' || c == ']') {
                stack.remove(stack.size() - 1);
                atKey = false;
                key = null;
                out.append(c);
                i++;
            } else if (c == ',') {
                atKey = !stack.isEmpty() && stack.get(stack.size() - 1).object;
                key = null;
                out.append(c);
                i++;
            } else if (c == ':') {
                out.append(c);
                i++;
            } else if (c == '"') {
                int end = stringEnd(json, i);
                Frame top = stack.isEmpty() ? null : stack.get(stack.size() - 1);
                if (!atKey && "_type".equals(key) && top != null && top.object && top.type == null) {
                    top.type = json.substring(i + 1, end - 1);
                }
                if (atKey && top != null && isSpan(json.substring(i + 1, end - 1)) && !keeps(stack, stack.size() - 1)) {
                    // skip the member: its key, ':', its value; and one of the commas beside it
                    int j = valueEnd(json, skipSpace(json, skipSpace(json, end) + 1));
                    int last = lastNonSpace(out);
                    if (last >= 0 && out.charAt(last) == ',') {
                        out.setLength(last);              // a member before it: drop the comma between them
                        atKey = false;
                    } else {
                        j = skipSpace(json, j);
                        if (j < n && json.charAt(j) == ',') {
                            j++;                          // the object's first member: drop the comma after it
                        }
                        atKey = true;
                    }
                    i = j;
                } else {
                    if (atKey) {
                        key = json.substring(i + 1, end - 1);
                    }
                    out.append(json, i, end);
                    atKey = false;
                    i = end;
                }
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** An open object or array: the member it is the value of (none for an array's element), its {@code _type} (an
     *  object's, once read), and whether the spans inside it stay (decided when first asked, its type read by then). */
    private static final class Frame {
        final boolean object;
        final @com.legend.base.Nullable String key;
        @com.legend.base.Nullable String type;
        @com.legend.base.Nullable Boolean keep;

        Frame(boolean object, @com.legend.base.Nullable String key) {
            this.object = object;
            this.key = key;
        }
    }

    /** Whether the spans inside the frame at {@code depth} stay: it, or a frame around it, is a value the engine
     *  parses with a span context of its own. */
    private static boolean keeps(java.util.List<Frame> stack, int depth) {
        Frame f = stack.get(depth);
        if (f.keep == null) {
            f.keep = depth > 0 && (keeps(stack, depth - 1) || keptByTheEngine(stack, depth));
        }
        return f.keep;
    }

    /**
     * Whether the frame at {@code depth} (not the root) is one of the values legend-engine parses with a span context
     * that records whatever it was asked -- the five places it builds one:
     * <ul>
     *   <li>an {@code equalTo}'s {@code expected} (EqualToGrammarParser, every test suite's EqualTo; and
     *       DomainParseTreeWalker.visitTestAssertion, a function test's);</li>
     *   <li>a function or service test's {@code parameters[].value} (DomainParseTreeWalker.visitFunctionTestParameter;
     *       ServiceParseTreeWalker.visitTestParameter, through visitServiceTestParameter);</li>
     *   <li>a legacy service test's {@code parametersValues[]}, each value; of a list, the values and not the list
     *       around them (ServiceParseTreeWalker.visitTestParameter, through visitParam);</li>
     *   <li>a persistence context's {@code serviceParameters[].value.primitiveType}
     *       (PersistenceContextParseTreeWalker.visitPrimitiveValue).</li>
     * </ul>
     */
    private static boolean keptByTheEngine(java.util.List<Frame> stack, int depth) {
        Frame f = stack.get(depth);
        Frame parent = stack.get(depth - 1);
        if ("expected".equals(f.key) && "equalTo".equals(parent.type)) {
            return true;
        }
        if ("value".equals(f.key) && elementOf(stack, depth - 1, "parameters")
                && (typed(stack, depth - 3, "functionTest") || typed(stack, depth - 3, "serviceTest"))) {
            return true;
        }
        if (elementOf(stack, depth, "parametersValues")) {
            return !"classInstance".equals(f.type);
        }
        if ("values".equals(f.key) && "value".equals(parent.key) && typed(stack, depth - 2, "classInstance")
                && elementOf(stack, depth - 2, "parametersValues")) {
            return true;
        }
        return "primitiveType".equals(f.key) && "primitiveTypeValue".equals(parent.type) && "value".equals(parent.key)
                && elementOf(stack, depth - 2, "serviceParameters") && typed(stack, depth - 4, "persistenceContext");
    }

    /** Whether the frame at {@code depth} is an element of the array that is member {@code array}'s value. */
    private static boolean elementOf(java.util.List<Frame> stack, int depth, String array) {
        return depth >= 1 && !stack.get(depth - 1).object && array.equals(stack.get(depth - 1).key);
    }

    private static boolean typed(java.util.List<Frame> stack, int depth, String type) {
        return depth >= 0 && type.equals(stack.get(depth).type);
    }

    private static boolean isSpan(String key) {
        return key.equals("sourceInformation") || key.endsWith("SourceInformation");
    }

    /** The index after the string literal that opens at {@code start}. */
    private static int stringEnd(String json, int start) {
        int j = start + 1;
        while (json.charAt(j) != '"') {
            j += json.charAt(j) == '\\' ? 2 : 1;
        }
        return j + 1;
    }

    /** The index after the JSON value that starts at {@code start}. */
    private static int valueEnd(String json, int start) {
        char c = json.charAt(start);
        if (c == '"') {
            return stringEnd(json, start);
        }
        if (c == '{' || c == '[') {
            int depth = 0;
            int j = start;
            while (true) {
                char d = json.charAt(j);
                if (d == '"') {
                    j = stringEnd(json, j);
                    continue;
                }
                if (d == '{' || d == '[') {
                    depth++;
                } else if (d == '}' || d == ']') {
                    depth--;
                    if (depth == 0) {
                        return j + 1;
                    }
                }
                j++;
            }
        }
        int j = start;
        while (j < json.length() && ",}] \t\r\n".indexOf(json.charAt(j)) < 0) {
            j++;
        }
        return j;
    }

    private static int skipSpace(String json, int from) {
        int j = from;
        while (j < json.length() && " \t\r\n".indexOf(json.charAt(j)) >= 0) {
            j++;
        }
        return j;
    }

    private static int lastNonSpace(StringBuilder out) {
        int k = out.length() - 1;
        while (k >= 0 && " \t\r\n".indexOf(out.charAt(k)) >= 0) {
            k--;
        }
        return k;
    }

    private static Object strip(Json.Node n, boolean named) {
        if (n instanceof Json.Obj o) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                String k = e.getKey();
                if (!k.equals("sourceInformation") && !(named && k.endsWith("SourceInformation"))) {
                    out.put(k, strip(e.getValue(), named));
                }
            }
            return out;
        }
        if (n instanceof Json.Arr a) {
            List<Object> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(strip(x, named));
            }
            return out;
        }
        return n;
    }
}
