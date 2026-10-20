// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A YAML DOCUMENT READ AS JSON (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4.1): the YAML 1.2 a schema, an OpenAPI
 * document or a project file is written in, read to the same {@link Json.Node} tree {@link Json#parse} reads, so
 * whatever reads JSON reads YAML. Zero dependencies, like the JSON reader beside it.
 *
 * <p><b>Read:</b> block mappings and sequences (a sequence under a key at the key's own indentation too); flow
 * mappings and sequences, across lines; plain, single- and double-quoted scalars, multi-line, folded as YAML folds
 * them; literal ({@code |}) and folded ({@code >}) block scalars with their chomping and indentation indicators;
 * comments; anchors, aliases and merge keys ({@code <<}); the core schema's tags ({@code !!str}, {@code !!int}, ...);
 * a document's {@code ---} and {@code ...} markers. A plain scalar resolves as YAML 1.2's core schema resolves it: a
 * null ({@code null}, {@code ~}, nothing), a boolean ({@code true}, {@code false}), an integer (decimal, {@code 0x},
 * {@code 0o}), a float, else text -- {@code yes}, {@code on} and {@code 012} are YAML 1.1's, not 1.2's, and read as
 * text, {@code 12}. A key is text, as JSON's are.
 *
 * <p><b>Refused, with the line and column</b> (never read otherwise): a second document; a complex key ({@code ?}),
 * or a collection as a key; a key twice in one mapping; a tag other than the core schema's; an alias to no anchor;
 * a tab in indentation; a number JSON cannot hold ({@code .inf}, {@code .nan}); and anything the grammar does not
 * read.
 */
public final class Yaml {

    private Yaml() {
    }

    /**
     * The document {@code text} holds, as JSON.
     *
     * @throws IllegalArgumentException naming the line and column of what is not read
     */
    public static Json.Node parse(String text) {
        String src = text.replace("\r\n", "\n").replace('\r', '\n');
        if (src.startsWith("\uFEFF")) {
            src = src.substring(1);
        }
        return new Reader(src).document();
    }

    /** The deepest nesting read, as deep as any schema nests and far short of the stack's limit. */
    private static final int MAX_DEPTH = 512;

    private static final class Reader {

        private final String src;
        private int pos;
        private final Map<String, Json.Node> anchors = new HashMap<>();
        /** How deep the nodes being read nest: refused past {@link #MAX_DEPTH}, as the JSON reader refuses. */
        private int depth;

        Reader(String src) {
            this.src = src;
        }

        // ---------------------------------------------------------------
        // The document
        // ---------------------------------------------------------------

        Json.Node document() {
            skipBlank();
            while (pos < src.length() && col() == 0 && peek() == '%') {
                // a directive (%YAML 1.2): its line
                skipToLineEnd();
                skipBlank();
            }
            boolean started = false;
            if (atMarker("---")) {
                pos += 3;
                started = true;
            }
            Json.Node root;
            skipSpaces();
            if (atLineEndOrComment()) {
                skipBlank();
                root = pos >= src.length() || atMarker("---") || atMarker("...") ? Json.nil()
                        : blockNodeHere(-1);
            } else if (started) {
                root = inlineNode(-1);
            } else {
                root = blockNodeHere(-1);
            }
            skipBlank();
            if (atMarker("...")) {
                pos += 3;
                skipBlank();
            }
            if (atMarker("---")) {
                throw error("a second document: one document is read");
            }
            if (pos < src.length()) {
                throw error("content after the document's root node (bad indentation?)");
            }
            return root;
        }

        // ---------------------------------------------------------------
        // Block nodes
        // ---------------------------------------------------------------

        /** A node whose content starts at the current position, the start of a line's content. */
        private Json.Node blockNodeHere(int parentIndent) {
            if (col() <= parentIndent) {
                return Json.nil();
            }
            return inlineNode(parentIndent);
        }

        /**
         * A value after a key's {@code :} or a sequence's {@code -}: on this line, or the more indented lines after it
         * (a sequence under a key may sit at the key's own indentation: {@code compact}), or nothing.
         */
        private Json.Node valueAfter(int parentIndent, boolean compact) {
            skipSpaces();
            if (!atLineEndOrComment()) {
                return inlineNode(parentIndent);
            }
            skipBlank();
            if (pos >= src.length() || atMarker("---") || atMarker("...")) {
                return Json.nil();
            }
            int c = col();
            if (c > parentIndent) {
                return inlineNode(parentIndent);
            }
            if (compact && c == parentIndent && atSequenceEntry()) {
                return sequence(c);
            }
            return Json.nil();
        }

        /** A node starting at the current position, its own column its indentation; {@code parentIndent} its
         *  parent's. */
        private Json.Node inlineNode(int parentIndent) {
            if (++depth > MAX_DEPTH) {
                throw error("nesting deeper than " + MAX_DEPTH);
            }
            try {
                return inlineNodeWithin(parentIndent);
            } finally {
                depth--;
            }
        }

        private Json.Node inlineNodeWithin(int parentIndent) {
            int start = pos;
            String anchor = null;
            String tag = null;
            while (peek() == '&' || peek() == '!') {
                if (peek() == '&') {
                    if (anchor != null) {
                        throw error("two anchors on one node");
                    }
                    pos++;
                    anchor = name();
                } else {
                    if (tag != null) {
                        throw error("two tags on one node");
                    }
                    tag = tag();
                }
                skipSpaces();
            }
            Json.Node node;
            if (anchor != null || tag != null) {
                if (atLineEndOrComment()) {
                    node = valueAfter(parentIndent, false);
                } else {
                    node = inlineNode(parentIndent);
                }
            } else {
                node = bare(parentIndent, start);
            }
            if (tag != null) {
                node = tagged(tag, node);
            }
            if (anchor != null) {
                anchors.put(anchor, node);
            }
            return node;
        }

        private Json.Node bare(int parentIndent, int start) {
            char c = peek();
            if (c == '*') {
                pos++;
                String name = name();
                Json.Node aliased = anchors.get(name);
                if (aliased == null) {
                    throw error("an alias to no anchor: *" + name);
                }
                return keyAfter(aliased, start);
            }
            if (c == '|' || c == '>') {
                return blockScalar(parentIndent);
            }
            if (c == '-' && spaceOrEnd(pos + 1)) {
                return sequence(col());
            }
            if (c == '?' && spaceOrEnd(pos + 1)) {
                throw error("a complex key ('?') is not read");
            }
            if (c == '[' || c == '{') {
                Json.Node flow = flowNode();
                skipSpaces();
                if (peek() == ':' && spaceOrEnd(pos + 1)) {
                    throw error("a collection as a key is not read");
                }
                return flow;
            }
            int indent = col();
            if (c == '"' || c == '\'') {
                String text = c == '"' ? doubleQuoted() : singleQuoted();
                skipSpaces();
                if (peek() == ':' && spaceOrEnd(pos + 1)) {
                    return mapping(indent, text);
                }
                return new Json.Str(text);
            }
            String plain = plainLine(false, false);
            if (peek() == ':' && spaceOrEnd(pos + 1)) {
                return mapping(indent, plain);
            }
            String text = plain + continuation(parentIndent);
            return resolve(text);
        }

        /** An alias as a node, or as a mapping's first key ({@code *a : value}). */
        private Json.Node keyAfter(Json.Node aliased, int start) {
            skipSpaces();
            if (peek() == ':' && spaceOrEnd(pos + 1)) {
                pos = start;
                throw error("an alias as a key is not read");
            }
            return aliased;
        }

        /** A block mapping at column {@code indent}, its first key read. */
        private Json.Node mapping(int indent, String firstKey) {
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            List<Json.Node> merges = new ArrayList<>();
            String key = firstKey;
            boolean mergeKey = "<<".equals(firstKey);
            while (true) {
                int keyAt = pos;
                pos++;
                Json.Node value = valueAfter(indent, true);
                endOfLine(keyAt);
                if (mergeKey) {
                    merges.add(value);
                } else if (out.containsKey(key)) {
                    pos = keyAt;
                    throw error("the key '" + key + "' twice in one mapping");
                } else {
                    out.put(key, value);
                }
                skipBlank();
                if (pos >= src.length() || col() < indent || atMarker("---") || atMarker("...")) {
                    break;
                }
                if (col() > indent) {
                    throw error("bad indentation of a mapping entry");
                }
                if (atSequenceEntry()) {
                    throw error("a sequence entry where a key of the mapping is expected");
                }
                char c = peek();
                mergeKey = false;
                if (c == '"') {
                    key = doubleQuoted();
                } else if (c == '\'') {
                    key = singleQuoted();
                } else if (c == '?' && spaceOrEnd(pos + 1)) {
                    throw error("a complex key ('?') is not read");
                } else if (c == '[' || c == '{' || c == '&' || c == '!' || c == '*' || c == '|' || c == '>') {
                    throw error("a key is a scalar on one line");
                } else {
                    key = plainLine(false, false);
                    mergeKey = "<<".equals(key);
                }
                skipSpaces();
                if (!(peek() == ':' && spaceOrEnd(pos + 1))) {
                    throw error("a mapping key without ':' (a key is a scalar on one line)");
                }
            }
            merge(out, merges);
            return new Json.Obj(out);
        }

        /** The merge keys' mappings, under the mapping's own keys: the first merged wins over the later. */
        private void merge(LinkedHashMap<String, Json.Node> into, List<Json.Node> merges) {
            for (Json.Node m : merges) {
                List<Json.Node> sources = m instanceof Json.Arr a ? a.items() : List.of(m);
                for (Json.Node s : sources) {
                    if (!(s instanceof Json.Obj o)) {
                        throw error("a merge key ('<<') merges a mapping, or a sequence of them");
                    }
                    for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                        into.putIfAbsent(e.getKey(), e.getValue());
                    }
                }
            }
        }

        /** A block sequence at column {@code indent}, at its first {@code -}. */
        private Json.Node sequence(int indent) {
            List<Json.Node> out = new ArrayList<>();
            while (true) {
                int entryAt = pos;
                pos++;
                out.add(valueAfter(indent, false));
                endOfLine(entryAt);
                skipBlank();
                if (pos >= src.length() || col() < indent || atMarker("---") || atMarker("...")) {
                    break;
                }
                if (col() > indent) {
                    throw error("bad indentation of a sequence entry");
                }
                if (!atSequenceEntry()) {
                    break;
                }
            }
            return new Json.Arr(out);
        }

        // ---------------------------------------------------------------
        // Scalars
        // ---------------------------------------------------------------

        /** A plain scalar's text on this line: up to a comment, a {@code ": "} or the line's end (in a flow, a flow
         *  indicator too). Only its first line cannot start with an indicator; a further line can. */
        private String plainLine(boolean flow, boolean further) {
            int start = pos;
            char first = peek();
            if (!further && ("#,[]{}&*!|>'\"%@`".indexOf(first) >= 0
                    || ((first == '-' || first == '?' || first == ':') && spaceOrEnd(pos + 1)))) {
                throw error("a plain scalar cannot start with '" + first + "'");
            }
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == '\n') {
                    break;
                }
                if (c == ':' && (spaceOrEnd(pos + 1) || (flow && flowIndicator(pos + 1)))) {
                    break;
                }
                if (c == '#' && pos > start && (src.charAt(pos - 1) == ' ' || src.charAt(pos - 1) == '\t')) {
                    break;
                }
                if (flow && (c == ',' || c == '[' || c == ']' || c == '{' || c == '}')) {
                    break;
                }
                pos++;
            }
            // the spaces before a comment or the line's end are not the scalar's
            return src.substring(start, pos).strip();
        }

        /** A plain scalar's further lines, each more indented than its parent, folded: one space for a line break,
         *  a line break for each empty line. A comment, or a line not more indented, ends it. */
        private String continuation(int parentIndent) {
            StringBuilder out = new StringBuilder();
            while (true) {
                int save = pos;
                skipSpaces();
                if (peek() != '\n') {
                    pos = save;
                    return out.toString();
                }
                int breaks = 0;
                int at = pos;
                int content;
                int indentHere;
                while (true) {
                    int lineStart = at + 1;
                    int i = lineStart;
                    while (i < src.length() && src.charAt(i) == ' ') {
                        i++;
                    }
                    int j = i;
                    while (j < src.length() && src.charAt(j) == '\t') {
                        j++;
                    }
                    if (j < src.length() && src.charAt(j) == '\n') {
                        breaks++;
                        at = j;
                        continue;
                    }
                    content = j;
                    indentHere = i - lineStart;
                    break;
                }
                if (content >= src.length() || indentHere <= parentIndent || src.charAt(content) == '#'
                        || (indentHere == 0 && (src.startsWith("---", content) || src.startsWith("...", content)))) {
                    pos = save;
                    return out.toString();
                }
                pos = content;
                String more = plainLine(false, true);
                if (peek() == ':' && spaceOrEnd(pos + 1)) {
                    throw error("bad indentation: a key more indented than its mapping, under a scalar value");
                }
                if (breaks == 0) {
                    out.append(' ');
                } else {
                    for (int k = 0; k < breaks; k++) {
                        out.append('\n');
                    }
                }
                out.append(more);
            }
        }

        private String singleQuoted() {
            int open = pos;
            pos++;
            StringBuilder out = new StringBuilder();
            while (true) {
                if (pos >= src.length()) {
                    pos = open;
                    throw error("a single-quoted scalar is not closed");
                }
                char c = src.charAt(pos);
                if (c == '\'') {
                    if (pos + 1 < src.length() && src.charAt(pos + 1) == '\'') {
                        out.append('\'');
                        pos += 2;
                        continue;
                    }
                    pos++;
                    return out.toString();
                }
                if (c == '\n') {
                    fold(out, 0);
                    continue;
                }
                out.append(c);
                pos++;
            }
        }

        private String doubleQuoted() {
            int open = pos;
            pos++;
            StringBuilder out = new StringBuilder();
            // the text an escape wrote ends here: a line break trims the spaces after it, never it
            int escaped = 0;
            while (true) {
                if (pos >= src.length()) {
                    pos = open;
                    throw error("a double-quoted scalar is not closed");
                }
                char c = src.charAt(pos);
                if (c == '"') {
                    pos++;
                    return out.toString();
                }
                if (c == '\n') {
                    fold(out, escaped);
                    continue;
                }
                if (c != '\\') {
                    out.append(c);
                    pos++;
                    continue;
                }
                pos++;
                if (pos >= src.length()) {
                    throw error("an escape at the end of the document");
                }
                char e = src.charAt(pos++);
                switch (e) {
                    case '0' -> out.append('\0');
                    case 'a' -> out.append('\u0007');
                    case 'b' -> out.append('\b');
                    case 't', '\t' -> out.append('\t');
                    case 'n' -> out.append('\n');
                    case 'v' -> out.append('\u000B');
                    case 'f' -> out.append('\f');
                    case 'r' -> out.append('\r');
                    case 'e' -> out.append('\u001B');
                    case ' ' -> out.append(' ');
                    case '"' -> out.append('"');
                    case '/' -> out.append('/');
                    case '\\' -> out.append('\\');
                    case 'N' -> out.append('\u0085');
                    case '_' -> out.append('\u00A0');
                    case 'L' -> out.append('\u2028');
                    case 'P' -> out.append('\u2029');
                    case 'x' -> codePoint(out, hex(2));
                    case 'u' -> codePoint(out, hex(4));
                    case 'U' -> codePoint(out, hex(8));
                    case '\n' -> {
                        // an escaped line break: the lines joined, the next line's leading spaces dropped
                        while (pos < src.length() && (src.charAt(pos) == ' ' || src.charAt(pos) == '\t')) {
                            pos++;
                        }
                    }
                    default -> {
                        pos -= 2;
                        throw error("the escape '\\" + e + "' is not YAML's");
                    }
                }
                escaped = out.length();
            }
        }

        /** A line break inside a quoted scalar: the trailing spaces before it dropped (back to {@code floor}, where an
         *  escape's text ends), the next line's leading ones too; one space for it, a line break for each empty line
         *  after it. */
        private void fold(StringBuilder out, int floor) {
            int end = out.length();
            while (end > floor && (out.charAt(end - 1) == ' ' || out.charAt(end - 1) == '\t')) {
                end--;
            }
            out.setLength(end);
            int breaks = 0;
            while (pos < src.length() && src.charAt(pos) == '\n') {
                pos++;
                while (pos < src.length() && (src.charAt(pos) == ' ' || src.charAt(pos) == '\t')) {
                    pos++;
                }
                if (pos < src.length() && src.charAt(pos) == '\n') {
                    breaks++;
                }
            }
            if (breaks == 0) {
                out.append(' ');
            } else {
                for (int i = 0; i < breaks; i++) {
                    out.append('\n');
                }
            }
        }

        private int hex(int digits) {
            if (pos + digits > src.length()) {
                throw error("a short escape");
            }
            String h = src.substring(pos, pos + digits);
            if (!h.matches("[0-9a-fA-F]+")) {
                throw error("an escape's hexadecimal digits: '" + h + "'");
            }
            pos += digits;
            return (int) Long.parseLong(h, 16);
        }

        private static void codePoint(StringBuilder out, int cp) {
            if (cp < 0x10000) {
                out.append((char) cp);
            } else {
                int v = cp - 0x10000;
                out.append((char) (0xD800 + (v >> 10))).append((char) (0xDC00 + (v & 0x3FF)));
            }
        }

        /** A literal ({@code |}) or folded ({@code >}) block scalar: its header, then its lines. */
        private Json.Node blockScalar(int parentIndent) {
            boolean literal = src.charAt(pos) == '|';
            pos++;
            char chomp = 'c';
            int indicator = 0;
            for (int i = 0; i < 2 && pos < src.length(); i++) {
                char h = src.charAt(pos);
                if ((h == '-' || h == '+') && chomp == 'c') {
                    chomp = h;
                    pos++;
                } else if (h >= '1' && h <= '9' && indicator == 0) {
                    indicator = h - '0';
                    pos++;
                }
            }
            skipSpaces();
            if (!atLineEndOrComment()) {
                throw error("a block scalar's header is followed by its line's end");
            }
            skipToLineEnd();
            int indent = indicator > 0 ? Math.max(parentIndent, 0) + indicator : -1;
            List<String> lines = new ArrayList<>();
            int trailing = 0;
            while (pos < src.length()) {
                // at a line's '\n'
                int lineStart = pos + 1;
                int spaces = 0;
                while (lineStart + spaces < src.length() && src.charAt(lineStart + spaces) == ' ') {
                    spaces++;
                }
                int content = lineStart + spaces;
                // a line of spaces alone is empty; of more than the indentation, its text the spaces beyond it
                boolean empty = (content >= src.length() || src.charAt(content) == '\n')
                        && (indent < 0 || spaces <= indent);
                if (empty) {
                    if (lineStart >= src.length()) {
                        pos = src.length();
                        break;
                    }
                    lines.add("");
                    trailing++;
                    pos = content < src.length() ? content : src.length();
                    continue;
                }
                if (indent < 0) {
                    if (spaces <= parentIndent) {
                        break;
                    }
                    indent = spaces;
                }
                if (spaces < indent) {
                    break;
                }
                int end = src.indexOf('\n', lineStart);
                end = end < 0 ? src.length() : end;
                lines.add(src.substring(lineStart + indent, end));
                trailing = 0;
                pos = end;
            }
            // the trailing empty lines are the chomping's; the lines before them the scalar's
            int kept = lines.size() - trailing;
            List<String> body = lines.subList(0, kept);
            StringBuilder out = new StringBuilder();
            if (literal) {
                for (int i = 0; i < body.size(); i++) {
                    if (i > 0) {
                        out.append('\n');
                    }
                    out.append(body.get(i));
                }
            } else {
                folded(body, out);
            }
            if (!body.isEmpty()) {
                if (chomp == '+') {
                    for (int i = 0; i <= trailing; i++) {
                        out.append('\n');
                    }
                } else if (chomp == 'c') {
                    out.append('\n');
                }
            } else if (chomp == '+') {
                for (int i = 0; i < trailing; i++) {
                    out.append('\n');
                }
            }
            return new Json.Str(out.toString());
        }

        /** Folded lines: a line break between two lines of text a space; an empty line a line break; a more
         *  indented line, and the breaks around it, kept. */
        private static void folded(List<String> lines, StringBuilder out) {
            boolean previousText = false;
            int empties = 0;
            boolean any = false;
            for (String line : lines) {
                if (line.isEmpty()) {
                    empties++;
                    continue;
                }
                boolean more = line.charAt(0) == ' ' || line.charAt(0) == '\t';
                if (any) {
                    if (empties == 0) {
                        out.append(previousText && !more ? " " : "\n");
                    } else {
                        int breaks = empties + (previousText && !more ? 0 : 1);
                        for (int i = 0; i < breaks; i++) {
                            out.append('\n');
                        }
                    }
                } else {
                    for (int i = 0; i < empties; i++) {
                        out.append('\n');
                    }
                }
                out.append(line);
                any = true;
                previousText = !more;
                empties = 0;
            }
        }

        /** A plain scalar resolved by YAML 1.2's core schema. */
        private Json.Node resolve(String text) {
            switch (text) {
                case "", "~", "null", "Null", "NULL" -> {
                    return Json.nil();
                }
                case "true", "True", "TRUE" -> {
                    return Json.bool(true);
                }
                case "false", "False", "FALSE" -> {
                    return Json.bool(false);
                }
                case ".inf", ".Inf", ".INF", "+.inf", "+.Inf", "+.INF", "-.inf", "-.Inf", "-.INF", ".nan", ".NaN",
                        ".NAN" -> throw error("the number " + text + ", which JSON cannot hold");
                default -> {
                    // text below
                }
            }
            if (text.matches("[-+]?[0-9]+")) {
                return integer(text.startsWith("+") ? text.substring(1) : text, 10);
            }
            if (text.matches("0o[0-7]+")) {
                return integer(text.substring(2), 8);
            }
            if (text.matches("0x[0-9a-fA-F]+")) {
                return integer(text.substring(2), 16);
            }
            if (text.matches("[-+]?(\\.[0-9]+|[0-9]+(\\.[0-9]*)?)([eE][-+]?[0-9]+)?")) {
                // BigDecimal reads every spelling the pattern allows (".5", "1.", "+1e3")
                return Json.Num.ofDecimal(new BigDecimal(text));
            }
            return new Json.Str(text);
        }

        private static Json.Node integer(String digits, int radix) {
            BigInteger v = new BigInteger(digits, radix);
            return v.bitLength() <= 63 ? Json.Num.ofLong(v.longValue()) : Json.Num.ofDecimal(new BigDecimal(v));
        }

        /** The core schema's tags: a node read as the tag says. */
        private Json.Node tagged(String tag, Json.Node node) {
            switch (tag) {
                case "!!str" -> {
                    if (node instanceof Json.Obj || node instanceof Json.Arr) {
                        throw error("a collection tagged !!str");
                    }
                    return node instanceof Json.Str ? node : new Json.Str(scalarText(node));
                }
                case "!!int", "!!float", "!!bool", "!!null" -> {
                    Json.Node r = node instanceof Json.Str s ? resolve(s.value()) : node;
                    boolean fits = switch (tag) {
                        case "!!int" -> r instanceof Json.Num n && n.isInteger();
                        case "!!float" -> r instanceof Json.Num;
                        case "!!bool" -> r instanceof Json.Bool;
                        default -> r instanceof Json.Null;
                    };
                    if (!fits) {
                        throw error("a node tagged " + tag + " that is not one: " + scalarText(node));
                    }
                    return r;
                }
                case "!!map", "!!seq" -> {
                    return node;
                }
                default -> throw error("the tag " + tag + " is not the core schema's");
            }
        }

        private static String scalarText(Json.Node node) {
            return switch (node) {
                case Json.Str s -> s.value();
                case Json.Null n -> "null";
                case Json.Bool b -> Boolean.toString(b.value());
                case Json.Num n -> n.decimalValue() != null ? n.decimalValue().toPlainString()
                        : Long.toString(n.longValue());
                case Json.Obj o -> "{...}";
                case Json.Arr a -> "[...]";
            };
        }

        // ---------------------------------------------------------------
        // Flow nodes
        // ---------------------------------------------------------------

        private Json.Node flowNode() {
            if (++depth > MAX_DEPTH) {
                throw error("nesting deeper than " + MAX_DEPTH);
            }
            try {
                return flowNodeWithin();
            } finally {
                depth--;
            }
        }

        private Json.Node flowNodeWithin() {
            skipFlowSpace();
            String anchor = null;
            String tag = null;
            while (peek() == '&' || peek() == '!') {
                if (peek() == '&') {
                    pos++;
                    anchor = name();
                } else {
                    tag = tag();
                }
                skipFlowSpace();
            }
            Json.Node node;
            char c = peek();
            if (c == '[') {
                node = flowSequence();
            } else if (c == '{') {
                node = flowMapping();
            } else if (c == '*') {
                pos++;
                String name = name();
                Json.Node aliased = anchors.get(name);
                if (aliased == null) {
                    throw error("an alias to no anchor: *" + name);
                }
                node = aliased;
            } else if (c == '"') {
                node = new Json.Str(doubleQuoted());
            } else if (c == '\'') {
                node = new Json.Str(singleQuoted());
            } else {
                node = resolve(flowPlain());
            }
            if (tag != null) {
                node = tagged(tag, node);
            }
            if (anchor != null) {
                anchors.put(anchor, node);
            }
            return node;
        }

        /** A plain scalar in a flow: across lines, folded, up to a flow indicator, a {@code ": "} or a comment. */
        private String flowPlain() {
            StringBuilder out = new StringBuilder();
            while (true) {
                String part = plainLine(true, out.length() > 0);
                if (out.length() > 0 && !part.isEmpty()) {
                    out.append(' ');
                }
                out.append(part);
                if (peek() != '\n') {
                    return out.toString();
                }
                int save = pos;
                skipFlowSpace();
                char c = peek();
                if (c == ',' || c == ']' || c == '}' || c == ':' || pos >= src.length()) {
                    pos = save;
                    skipFlowSpace();
                    return out.toString();
                }
            }
        }

        private Json.Node flowSequence() {
            pos++;
            List<Json.Node> out = new ArrayList<>();
            while (true) {
                skipFlowSpace();
                if (peek() == ']') {
                    pos++;
                    return new Json.Arr(out);
                }
                if (peek() == '?' && spaceOrEnd(pos + 1)) {
                    throw error("a complex key ('?') is not read");
                }
                Json.Node item = flowNode();
                skipFlowSpace();
                if (peek() == ':' && (spaceOrEnd(pos + 1) || flowIndicator(pos + 1))) {
                    // a single pair: [a: 1]
                    pos++;
                    skipFlowSpace();
                    Json.Node value = peek() == ',' || peek() == ']' ? Json.nil() : flowNode();
                    LinkedHashMap<String, Json.Node> pair = new LinkedHashMap<>();
                    pair.put(keyText(item), value);
                    item = new Json.Obj(pair);
                    skipFlowSpace();
                }
                out.add(item);
                if (peek() == ',') {
                    pos++;
                } else if (peek() != ']') {
                    throw error("',' or ']' in a flow sequence");
                }
            }
        }

        private Json.Node flowMapping() {
            pos++;
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            while (true) {
                skipFlowSpace();
                if (peek() == '}') {
                    pos++;
                    return new Json.Obj(out);
                }
                if (peek() == '?' && spaceOrEnd(pos + 1)) {
                    throw error("a complex key ('?') is not read");
                }
                int keyAt = pos;
                boolean quoted = peek() == '"' || peek() == '\'';
                Json.Node keyNode = flowNode();
                if (keyNode instanceof Json.Obj || keyNode instanceof Json.Arr) {
                    pos = keyAt;
                    throw error("a collection as a key is not read");
                }
                String key = keyText(keyNode);
                skipFlowSpace();
                Json.Node value = Json.nil();
                // a quoted key's ':' needs no space after it (JSON's own spelling)
                if (peek() == ':' && (quoted || spaceOrEnd(pos + 1) || flowIndicator(pos + 1))) {
                    pos++;
                    skipFlowSpace();
                    if (peek() != ',' && peek() != '}') {
                        value = flowNode();
                    }
                }
                if (out.containsKey(key)) {
                    pos = keyAt;
                    throw error("the key '" + key + "' twice in one mapping");
                }
                out.put(key, value);
                skipFlowSpace();
                if (peek() == ',') {
                    pos++;
                } else if (peek() != '}') {
                    throw error("',' or '}' in a flow mapping");
                }
            }
        }

        private String keyText(Json.Node key) {
            if (key instanceof Json.Obj || key instanceof Json.Arr) {
                throw error("a collection as a key is not read");
            }
            return scalarText(key);
        }

        private void skipFlowSpace() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n') {
                    pos++;
                } else if (c == '#' && (pos == 0 || " \t\n".indexOf(src.charAt(pos - 1)) >= 0)) {
                    skipToLineEnd();
                } else {
                    break;
                }
            }
        }

        private boolean flowIndicator(int at) {
            return at < src.length() && ",[]{}".indexOf(src.charAt(at)) >= 0;
        }

        // ---------------------------------------------------------------
        // Lexical
        // ---------------------------------------------------------------

        /** An anchor's or alias's name: up to a space, a line's end or a flow indicator. */
        private String name() {
            int start = pos;
            while (pos < src.length() && " \t\n,[]{}".indexOf(src.charAt(pos)) < 0) {
                pos++;
            }
            if (pos == start) {
                throw error("an anchor or alias with no name");
            }
            return src.substring(start, pos);
        }

        /** A tag, its {@code !} included. */
        private String tag() {
            int start = pos;
            while (pos < src.length() && " \t\n,[]{}".indexOf(src.charAt(pos)) < 0) {
                pos++;
            }
            return src.substring(start, pos);
        }

        private char peek() {
            return pos < src.length() ? src.charAt(pos) : '\0';
        }

        private boolean spaceOrEnd(int at) {
            return at >= src.length() || src.charAt(at) == ' ' || src.charAt(at) == '\t' || src.charAt(at) == '\n';
        }

        /** The column of the current position: its distance from its line's start. */
        private int col() {
            int lineStart = src.lastIndexOf('\n', pos - 1) + 1;
            return pos - lineStart;
        }

        /** After a value on the line of its key or entry ({@code from}): nothing but a comment. */
        private void endOfLine(int from) {
            int lineEnd = src.indexOf('\n', from);
            if (lineEnd >= 0 && pos > lineEnd) {
                return;
            }
            skipSpaces();
            if (!atLineEndOrComment()) {
                throw error("more after a value on its line");
            }
        }

        private boolean atSequenceEntry() {
            return peek() == '-' && spaceOrEnd(pos + 1);
        }

        /** A document marker at a line's start: {@code ---} or {@code ...}, then a space or the line's end. */
        private boolean atMarker(String marker) {
            return col() == 0 && src.startsWith(marker, pos) && spaceOrEnd(pos + 3);
        }

        private void skipSpaces() {
            while (pos < src.length() && (src.charAt(pos) == ' ' || src.charAt(pos) == '\t')) {
                pos++;
            }
        }

        private boolean atLineEndOrComment() {
            return pos >= src.length() || peek() == '\n' || peek() == '#';
        }

        private void skipToLineEnd() {
            while (pos < src.length() && src.charAt(pos) != '\n') {
                pos++;
            }
        }

        /** To the next line's content: past spaces, comments and empty lines; a tab in indentation refused. */
        private void skipBlank() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == ' ') {
                    pos++;
                } else if (c == '\t') {
                    if (onlySpacesBefore()) {
                        // a tab on a line of nothing else is whitespace; in indentation before content it is refused
                        int save = pos;
                        while (pos < src.length() && (src.charAt(pos) == ' ' || src.charAt(pos) == '\t')) {
                            pos++;
                        }
                        if (pos < src.length() && src.charAt(pos) != '\n' && src.charAt(pos) != '#') {
                            pos = save;
                            throw error("a tab in indentation");
                        }
                    } else {
                        pos++;
                    }
                } else if (c == '\n') {
                    pos++;
                } else if (c == '#') {
                    skipToLineEnd();
                } else {
                    break;
                }
            }
        }

        private boolean onlySpacesBefore() {
            int i = pos - 1;
            while (i >= 0 && src.charAt(i) != '\n') {
                if (src.charAt(i) != ' ' && src.charAt(i) != '\t') {
                    return false;
                }
                i--;
            }
            return true;
        }

        private IllegalArgumentException error(String message) {
            int line = 1;
            for (int i = 0; i < pos && i < src.length(); i++) {
                if (src.charAt(i) == '\n') {
                    line++;
                }
            }
            return new IllegalArgumentException("YAML line " + line + ", column " + (col() + 1) + ": " + message);
        }
    }
}
