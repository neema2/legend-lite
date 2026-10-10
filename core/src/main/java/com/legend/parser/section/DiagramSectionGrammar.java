// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * THE {@code ###Diagram} grammar — the RAW built-in: diagram content
 * (color literals like {@code #FFFFCC}) is unlexable Pure, so the shared
 * lexer skips the section and this grammar walks the CHARACTERS with its
 * own line/column tracking (file-absolute via the section's
 * {@code startLine}). Views parse structurally; wire shapes probed
 * (ZTailProbe "diagram") — hide flags only when spelled, class paths
 * unquoted on the wire, property spans covering the class portion only.
 */
public final class DiagramSectionGrammar implements RawSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final DiagramSectionGrammar INSTANCE =
            new DiagramSectionGrammar();

    private DiagramSectionGrammar() {
    }

    @Override
    public String name() {
        return "Diagram";
    }

    /** The SPI feed — same raw parse, protocol JSON out; the drop-in
     *  overlay hosts the ENGINE grammar level. */
    @Override
    public void parse(com.legend.spi.SectionSource src,
            com.legend.spi.ElementSink out) {
        for (var pe : parseRaw(src,
                com.legend.parser.Dialect.LEGEND_ENGINE).elements()) {
            Protocol.PDiagram d = (Protocol.PDiagram) pe.protocol();
            out.accept(d.qualifiedName(),
                    com.legend.protocol.ProtocolEmitter.emitElement(d));
        }
    }

    @Override
    public LexableSectionGrammar.ParsedSection parseRaw(
            com.legend.spi.SectionSource src) {
        // dialect-less callers get the PRODUCT surface; the internal
        // pipeline passes its dialect explicitly (ElementParser)
        return parseRaw(src, com.legend.parser.Dialect.LEGEND_LITE);
    }

    @Override
    public LexableSectionGrammar.ParsedSection parseRaw(
            com.legend.spi.SectionSource src,
            com.legend.parser.Dialect dialect) {
        Raw r = new Raw(src.text(), src.startLine());
        List<LexableSectionGrammar.ParsedElement> elements = new ArrayList<>();
        List<String> imports = new ArrayList<>();
        while (true) {
            r.skipWs();
            if (r.atEnd()) {
                break;
            }
            if (r.startsWith("import ")) {
                // EXACTLY `import pkg(::pkg)*::*;` — a dotted separator
                // refuses like the engine (MutationFuzzTest package-dot)
                String imp = r.toSemicolon().substring(7).trim();
                boolean ok = imp.length() > 3
                        && imp.charAt(imp.length() - 1) == '*'
                        && imp.charAt(imp.length() - 2) == ':'
                        && imp.charAt(imp.length() - 3) == ':';
                boolean segEmpty = true;
                for (int ci = 0; ok && ci < imp.length() - 3; ci++) {
                    char ch = imp.charAt(ci);
                    if (ch == ':') {
                        if (segEmpty || ci + 1 >= imp.length() - 3
                                || imp.charAt(ci + 1) != ':') {
                            ok = false;
                        } else {
                            ci++;
                            segEmpty = true;
                        }
                    } else if (Character.isLetterOrDigit(ch) || ch == '_'
                            || ch == '$') {
                        segEmpty = false;
                    } else {
                        ok = false;
                    }
                }
                if (!ok || segEmpty) {
                    throw r.fail("malformed import '" + imp + "'");
                }
                imports.add(imp);
                continue;
            }
            int elOffset = r.i;
            elements.add(new LexableSectionGrammar.ParsedElement(
                    parseDiagram(r, dialect), elOffset));
        }
        return new LexableSectionGrammar.ParsedSection(elements, imports);
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        Protocol.PDiagram d = (Protocol.PDiagram) element;
        return new com.legend.model.GenericSectionElementDefinition("Diagram",
                "Diagram", d.qualifiedName(), java.util.Map.of(), null);
    }

    private static Protocol.PDiagram parseDiagram(Raw r,
            com.legend.parser.Dialect dialect) {
        int[] start = r.mark();
        r.expectWord("Diagram");
        r.skipWs();
        String qn = r.path();
        r.skipWs();
        if (r.peek() == '(') {
            r.skipBalanced('(', ')');
            r.skipWs();
        }
        r.expect('{');
        List<Protocol.PClassView> classViews = new ArrayList<>();
        List<Protocol.PPropertyView> propertyViews = new ArrayList<>();
        List<Protocol.PGeneralizationView> generalizationViews =
                new ArrayList<>();
        while (true) {
            r.skipWs();
            if (r.peek() == '}') {
                break;
            }
            int[] viewStart = r.mark();
            String kw = r.word();
            switch (kw) {
                case "classView" -> classViews.add(parseClassView(r, viewStart));
                case "propertyView" ->
                        propertyViews.add(parsePropertyView(r, viewStart));
                case "generalizationView" -> generalizationViews.add(
                        parseGeneralizationView(r, viewStart));
                // the M2 dialect (legend-pure fixtures): capitalized view
                // kinds with paren attribute bodies. Parse-ACCEPTED, not
                // modeled — a diagram is presentation metadata nothing in
                // lite opens, and the modern oracle rejects the dialect so
                // there is no wire shape to claim (corpus setup files only
                // need acceptance)
                case "TypeView", "AssociationView", "PropertyView",
                        "GeneralizationView" -> {
                    if (dialect.refusesPlatformDialect()) {
                        // dialect quarantine: the engine grammar has no m2
                        // view arms (a PLATFORM construct)
                        throw r.fail("Unexpected token '" + kw + "'");
                    }
                    r.skipWs();
                    r.segment();
                    r.skipWs();
                    r.skipBalanced('(', ')');
                }
                default -> throw r.fail("unknown diagram view '" + kw + "'");
            }
        }
        r.expect('}');
        SourceInfo span = r.spanFrom(start);
        int cut = qn.lastIndexOf("::");
        return new Protocol.PDiagram(cut < 0 ? "" : qn.substring(0, cut),
                cut < 0 ? qn : qn.substring(cut + 2), classViews,
                propertyViews, generalizationViews, span);
    }

    private static Protocol.PClassView parseClassView(Raw r, int[] start) {
        r.skipWs();
        String id = r.word();
        r.skipWs();
        r.expect('{');
        String classPath = null;
        SourceInfo classSpan = null;
        Boolean hideProperties = null;
        Boolean hideStereotypes = null;
        Boolean hideTaggedValues = null;
        double[] position = null;
        double[] rectangle = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (true) {
            r.skipWs();
            if (r.peek() == '}') {
                break;
            }
            String key = r.word();
            if (!seenKeys.add(key)) {
                // engine-verbatim (sectioned negative pins, 2026-08-13)
                throw r.failAt(start, "Field '" + key + "' should be specified only once");
            }
            r.skipWs();
            r.expect(':');
            r.skipWs();
            switch (key) {
                case "class" -> {
                    int[] ps = r.mark();
                    classPath = r.path();
                    classSpan = r.spanFrom(ps);
                }
                case "position" -> position = r.pair();
                case "rectangle" -> rectangle = r.pair();
                // FALSE is indistinguishable from absent on the wire: the
                // walker sets each flag only when parseBoolean is true
                // (sibling fixture diagram-full, sweep 2026-08-14)
                case "hideProperties" -> hideProperties = r.bool() ? true : null;
                case "hideTaggedValue" -> hideTaggedValues = r.bool() ? true : null;
                case "hideStereotype" -> hideStereotypes = r.bool() ? true : null;
                default -> throw r.fail("unknown classView key '" + key + "'");
            }
            r.skipWs();
            r.expect(';');
        }
        r.expect('}');
        if (classPath == null || classSpan == null || position == null
                || rectangle == null) {
            throw r.failAt(start, "classView '" + id + "' needs class, position and"
                    + " rectangle");
        }
        return new Protocol.PClassView(id, classPath, classSpan,
                hideProperties, hideStereotypes, hideTaggedValues,
                position[0], position[1], rectangle[0], rectangle[1],
                r.spanFrom(start));
    }

    private static Protocol.PPropertyView parsePropertyView(Raw r,
            int[] start) {
        r.skipWs();
        r.expect('{');
        String propClass = null;
        String propName = null;
        SourceInfo propSpan = null;
        String sourceView = null;
        SourceInfo sourceViewSpan = null;
        String targetView = null;
        SourceInfo targetViewSpan = null;
        List<Protocol.PDiagramPoint> points = null;
        java.util.Set<String> seenKeys2 = new java.util.HashSet<>();
        while (true) {
            r.skipWs();
            if (r.peek() == '}') {
                break;
            }
            String key = r.word();
            if (!seenKeys2.add(key)) {
                // engine-verbatim (sectioned negative pins, 2026-08-13)
                throw r.failAt(start, "Field '" + key + "' should be specified only once");
            }
            r.skipWs();
            r.expect(':');
            r.skipWs();
            switch (key) {
                case "property" -> {
                    // the wire span covers the CLASS portion only
                    int[] ps = r.mark();
                    propClass = r.path();
                    propSpan = r.spanFrom(ps);
                    r.expect('.');
                    propName = r.segment();
                }
                case "source" -> {
                    int[] ps = r.mark();
                    sourceView = r.word();
                    sourceViewSpan = r.spanFrom(ps);
                }
                case "target" -> {
                    int[] ps = r.mark();
                    targetView = r.word();
                    targetViewSpan = r.spanFrom(ps);
                }
                case "points" -> points = r.points();
                default -> throw r.fail("unknown propertyView key '" + key
                        + "'");
            }
            r.skipWs();
            r.expect(';');
        }
        r.expect('}');
        if (propClass == null || propName == null || sourceView == null
                || targetView == null || points == null) {
            throw r.failAt(start, "propertyView needs property, source, target and"
                    + " points");
        }
        return new Protocol.PPropertyView(propClass, propName,
                java.util.Objects.requireNonNull(propSpan), sourceView,
                java.util.Objects.requireNonNull(sourceViewSpan), targetView,
                java.util.Objects.requireNonNull(targetViewSpan), points,
                r.spanFrom(start));
    }

    private static Protocol.PGeneralizationView parseGeneralizationView(
            Raw r, int[] start) {
        r.skipWs();
        r.expect('{');
        String sourceView = null;
        SourceInfo sourceViewSpan = null;
        String targetView = null;
        SourceInfo targetViewSpan = null;
        List<Protocol.PDiagramPoint> points = null;
        java.util.Set<String> seenKeys3 = new java.util.HashSet<>();
        while (true) {
            r.skipWs();
            if (r.peek() == '}') {
                break;
            }
            String key = r.word();
            if (!seenKeys3.add(key)) {
                // engine-verbatim (sectioned negative pins, 2026-08-13)
                throw r.failAt(start, "Field '" + key + "' should be specified only once");
            }
            r.skipWs();
            r.expect(':');
            r.skipWs();
            switch (key) {
                case "source" -> {
                    int[] ps = r.mark();
                    sourceView = r.word();
                    sourceViewSpan = r.spanFrom(ps);
                }
                case "target" -> {
                    int[] ps = r.mark();
                    targetView = r.word();
                    targetViewSpan = r.spanFrom(ps);
                }
                case "points" -> points = r.points();
                default -> throw r.fail("unknown generalizationView key '"
                        + key + "'");
            }
            r.skipWs();
            r.expect(';');
        }
        r.expect('}');
        if (sourceView == null || targetView == null || points == null) {
            throw r.failAt(start, "generalizationView needs source, target and"
                    + " points");
        }
        return new Protocol.PGeneralizationView(sourceView,
                java.util.Objects.requireNonNull(sourceViewSpan), targetView,
                java.util.Objects.requireNonNull(targetViewSpan), points,
                r.spanFrom(start));
    }

    /** THE character walker: tracks 1-based file-absolute line/column. */
    private static final class Raw {

        private final String t;
        private int i;
        private int line;
        private int col;

        Raw(String t, int startLine) {
            this.t = t;
            this.line = startLine;
            this.col = 1;
        }

        boolean atEnd() {
            return i >= t.length();
        }

        char peek() {
            if (atEnd()) {
                throw fail("unexpected end of ###Diagram section");
            }
            return t.charAt(i);
        }

        boolean startsWith(String s) {
            return t.startsWith(s, i);
        }

        private void step() {
            if (t.charAt(i) == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
            i++;
        }

        void skipWs() {
            while (!atEnd()) {
                char c = t.charAt(i);
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                    step();
                } else if (startsWith("//")) {
                    while (!atEnd() && t.charAt(i) != '\n') {
                        step();
                    }
                } else if (startsWith("/*")) {
                    while (!atEnd() && !startsWith("*/")) {
                        step();
                    }
                    if (!atEnd()) {
                        step();
                        step();
                    }
                } else {
                    break;
                }
            }
        }

        /** {@code [i, line, col]} for span starts. */
        int[] mark() {
            return new int[]{i, line, col};
        }

        /** File-absolute span from {@code mark} to the LAST consumed char. */
        SourceInfo spanFrom(int[] mark) {
            return new SourceInfo("", mark[1], mark[2], line,
                    col == 1 ? 1 : col - 1);
        }

        String word() {
            int s = i;
            while (!atEnd()) {
                char c = t.charAt(i);
                if (Character.isLetterOrDigit(c) || c == '_' || c == '$'
                        || c == '-') {
                    step();
                } else {
                    break;
                }
            }
            if (i == s) {
                throw fail("expected an identifier");
            }
            return t.substring(s, i);
        }

        /** One path segment, UNQUOTED at the source: a word, or a quoted
         *  string with its quotes stripped here — never re-split later
         *  (a quoted segment may itself contain {@code ::}). */
        String segment() {
            if (peek() == '\'') {
                step();
                int s = i;
                while (!atEnd() && t.charAt(i) != '\'') {
                    step();
                }
                String seg = t.substring(s, i);
                expect('\'');
                return seg;
            }
            return word();
        }

        /** {@code seg(::seg)*}, assembled from already-unquoted segments —
         *  the wire form directly. */
        String path() {
            StringBuilder b = new StringBuilder(segment());
            while (startsWith("::")) {
                step();
                step();
                b.append("::").append(segment());
            }
            return b.toString();
        }

        void expect(char c) {
            if (atEnd() || t.charAt(i) != c) {
                throw fail("expected '" + c + "'");
            }
            step();
        }

        void expectWord(String w) {
            if (!startsWith(w)) {
                throw fail("expected '" + w + "'");
            }
            for (int k = 0; k < w.length(); k++) {
                step();
            }
        }

        boolean bool() {
            if (startsWith("true")) {
                expectWord("true");
                return true;
            }
            expectWord("false");
            return false;
        }

        double number() {
            int s = i;
            while (!atEnd()) {
                char c = t.charAt(i);
                if (Character.isDigit(c) || c == '-' || c == '+' || c == '.'
                        || c == 'E' || c == 'e') {
                    step();
                } else {
                    break;
                }
            }
            if (i == s) {
                throw fail("expected a number");
            }
            return Double.parseDouble(t.substring(s, i));
        }

        /** {@code (x,y)}. */
        double[] pair() {
            expect('(');
            skipWs();
            double x = number();
            skipWs();
            expect(',');
            skipWs();
            double y = number();
            skipWs();
            expect(')');
            return new double[]{x, y};
        }

        /** {@code [(x,y),...]}. */
        List<Protocol.PDiagramPoint> points() {
            List<Protocol.PDiagramPoint> out = new ArrayList<>();
            expect('[');
            skipWs();
            while (peek() != ']') {
                double[] p = pair();
                out.add(new Protocol.PDiagramPoint(p[0], p[1]));
                skipWs();
                if (peek() == ',') {
                    step();
                    skipWs();
                }
            }
            expect(']');
            return out;
        }

        String toSemicolon() {
            int s = i;
            while (!atEnd() && t.charAt(i) != ';') {
                step();
            }
            expect(';');
            return t.substring(s, i - 1);
        }

        /** Balanced skip, blind to bracket chars inside '...' strings
         *  (m2 attribute values may carry parens). */
        void skipBalanced(char open, char close) {
            int depth = 0;
            do {
                char c = peek();
                if (c == '\'') {
                    step();
                    while (!atEnd() && t.charAt(i) != '\'') {
                        step();
                    }
                    expect('\'');
                    continue;
                }
                if (c == open) {
                    depth++;
                } else if (c == close) {
                    depth--;
                }
                step();
            } while (!atEnd() && depth > 0);
            if (depth > 0) {
                throw fail("unbalanced '" + open + "'");
            }
        }

        RuntimeException fail(String message) {
            return new com.legend.parser.ParseException(message, line, col);
        }

        /** {@link #fail} ANCHORED at a saved {@link #mark()} — the engine
         *  walker reports view-body validations at the VIEW keyword
         *  (position-exactness lane: all 20 Diagram diverges sat at the
         *  cursor instead of 4:3-style view starts). */
        RuntimeException failAt(int[] mark, String message) {
            return new com.legend.parser.ParseException(message, mark[1], mark[2]);
        }
    }
}
