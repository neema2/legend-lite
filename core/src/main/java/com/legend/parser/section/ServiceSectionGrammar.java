// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.SpecParser;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * THE {@code ###Service} grammar — third built-in behind the
 * {@link com.legend.spi.SectionGrammar} seam (SECTION_PROGRAM_HANDOFF.md
 * §2.6). Owns {@code Service} and {@code ExecutionEnvironment} elements to
 * the corpus-censused scope: envelope decorations, pattern / owners /
 * documentation / autoActivateUpdates, Single AND Multi executions (the
 * retired straight-to-model twin refused Multi and the legacy {@code test:}
 * block outright), and RAW-captured legacy-test / testSuites payloads.
 *
 * <p>No engine WIRE shape is claimed yet: {@code ProtocolEmitter} walls on
 * {@code PService}, and the byte-parity harness keeps Service files
 * OUT_OF_SCOPE. This grammar's job today is the parse/transform seam — the
 * drop-in surface accepting exactly the section content the engine accepts.
 */
public final class ServiceSectionGrammar
        implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final ServiceSectionGrammar INSTANCE =
            new ServiceSectionGrammar();

    private ServiceSectionGrammar() {
    }

    @Override
    public String name() {
        return "Service";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return switch (e) {
            case Protocol.PService s -> s.qualifiedName();
            case Protocol.PExecutionEnvironment ee -> ee.qualifiedName();
            default -> throw new IllegalStateException(
                    "not a service-section element: " + e.getClass());
        };
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        return parseElement(c);
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.FromProtocol.toServiceSectionElement(element);
    }

    /** One element at the cursor: {@code Service} or
     *  {@code ExecutionEnvironment}. */
    public static Protocol.Element parseElement(TokenStreamCursor c) {
        if (c.peek() == TokenType.SERVICE) {
            return parseService(c);
        }
        if (c.isIdentifierToken(c.peek())
                && "ExecutionEnvironment".equals(c.safeText())) {
            return parseExecutionEnvironment(c);
        }
        throw c.error("unsupported ###Service element: " + c.safeText());
    }

    // ============================================================
    // Service
    // ============================================================

    /** The walker's mcpServer shape gate, spelled without a regex (the
     *  drop-in surface freezes regex-family sites): {@code
     *  [a-zA-Z_][a-zA-Z0-9_]*}. */
    private static boolean isPlainIdentifier(String s) {
        if (s.isEmpty()) {
            return false;
        }
        char h = s.charAt(0);
        if (!(Character.isLetter(h) && h < 128) && h != '_') {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (!((Character.isLetterOrDigit(ch) && ch < 128)
                    || ch == '_')) {
                return false;
            }
        }
        return true;
    }

    private static Protocol.PService parseService(TokenStreamCursor c) {
        int declStart = c.pos();
        c.expect(TokenType.SERVICE);
        TokenStreamCursor.Decorations dec = c.parseDecorations();
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);

        String pattern = null;
        String title = null;
        List<String> owners = new ArrayList<>();
        String ownershipKind = null;
        String ownershipId = null;
        List<String> ownershipUsers = null;
        String mcpServer = null;
        String documentation = null;
        Boolean autoActivate = null;
        Protocol.PServiceExecution execution = null;
        Protocol.PLegacyServiceTest test = null;
        List<Protocol.PServiceTestSuite> testSuites = null;
        List<Protocol.PPostValidation> postValidations = null;

        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seenKeys.add(key)) {
                // every service field validates at most once
                // (ServiceParseTreeWalker validateAndExtract*Field);
                // anchored at the ELEMENT like the walker's ctx
                throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                        "Field '" + key + "' should be specified only once");
            }
            switch (key) {
                case "pattern" -> {
                    pattern = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "title" -> {
                    // engine ServiceParserGrammar.g4: serviceTitle
                    title = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "ownership" -> {
                    // ownership: DID { identifier: '...' } |
                    //            UserList { users: [...] }
                    // — the kind PAIRS with its body key in the .g4
                    // (sibling negative neg-service-ownership-did-with-
                    // users: DID takes ONLY identifier)
                    ownershipKind = c.parseIdentifier();
                    if (!"DID".equals(ownershipKind)
                            && !"UserList".equals(ownershipKind)) {
                        throw c.error("unknown ownership kind: "
                                + ownershipKind);
                    }
                    c.expect(TokenType.BRACE_OPEN);
                    String ik = c.parseIdentifier();
                    c.expect(TokenType.COLON);
                    if ("identifier".equals(ik)
                            && "DID".equals(ownershipKind)) {
                        ownershipId = SectionParse.stringValue(c);
                    } else if ("users".equals(ik)
                            && "UserList".equals(ownershipKind)) {
                        ownershipUsers = new ArrayList<>();
                        c.expect(TokenType.BRACKET_OPEN);
                        while (c.peek() != TokenType.BRACKET_CLOSE
                                && !c.atEnd()) {
                            ownershipUsers.add(SectionParse.stringValue(c));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                    } else {
                        throw c.error("unknown ownership key: " + ik);
                    }
                    c.match(TokenType.SEMI_COLON);
                    c.expect(TokenType.BRACE_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "mcpServer" -> {
                    // wire: plain string field (harvest
                    // testServiceWithMcpServer); the walker re-validates
                    // the IDENTIFIER shape (quoted names parse but refuse)
                    mcpServer = c.parseIdentifier();
                    if (!isPlainIdentifier(mcpServer)) {
                        throw c.error("Service mcpServer should be a valid"
                                + " identifier ([a-zA-Z_][a-zA-Z0-9_]*)");
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                case "postValidations" -> {
                    // [ { description: '..'; params: [..];
                    //     assertions: [ id: <lambda>; ... ]; }, ... ]
                    // (harvest testServiceWithPostValidation; probe pv2)
                    postValidations = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        postValidations.add(parsePostValidation(c));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.match(TokenType.SEMI_COLON);
                }
                case "documentation" -> {
                    documentation = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "autoActivateUpdates" -> {
                    autoActivate = SectionParse.booleanValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "owners" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        owners.add(SectionParse.stringValue(c));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "execution" -> execution = parseExecution(c, qn);
                case "test" -> test = parseLegacyTest(c);
                case "testSuites" -> {
                    testSuites = parseTestSuites(c);
                    c.match(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown key '" + key
                        + "' inside Service '" + qn + "'");
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (execution == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                    "Field 'execution' is required");
        }
        // ENGINE-VERBATIM required fields (ServiceParserGrammar), BOTH
        // surfaces: the old lenient default (pattern -> "/") had no
        // reference in either grammar and nothing depended on it —
        // conformed away in the own-corpus decision review (2026-08-11)
        if (pattern == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                    "Field 'pattern' is required");
        }
        if (documentation == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                    "Field 'documentation' is required");
        }
        return new Protocol.PService(pkg, name, dec.stereotypes(),
                dec.taggedValues(), pattern, title, owners, ownershipKind,
                ownershipId, ownershipUsers, mcpServer,
                documentation, autoActivate, execution, test,
                testSuites, postValidations,
                c.spanOf(declStart, c.pos() - 1));
    }


    /** {@code name = toBytes('...')} — the byte-array parameter literal:
     *  wire {@code {"_type":"byteArray","value":"<base64>"}} spanning the
     *  whole call (harvest testBindingServices, probe-verified). Null when
     *  the value is not a toBytes literal. */
    private static Protocol.PServiceTestSuite.@com.legend.Nullable PSuiteParam
            byteArrayParam(TokenStreamCursor c, String name) {
        if (!(c.peek() == TokenType.VALID_STRING
                && "toBytes".equals(c.safeText()))) {
            return null;
        }
        int vs = c.pos();
        c.advance();
        c.expect(TokenType.PAREN_OPEN);
        String quoted = c.text();
        c.expect(TokenType.STRING);
        String decoded = TokenStreamCursor.unquoteAndUnescape(quoted, c);
        c.expect(TokenType.PAREN_CLOSE);
        String b64 = java.util.Base64.getEncoder().encodeToString(
                decoded.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new Protocol.PServiceTestSuite.PSuiteParam(name,
                new com.legend.protocol.spec.CByteArray(b64,
                        c.spanOf(vs, c.pos() - 1)));
    }


    /** One legacy-test parameter VALUE — the engine's primitiveValue
     *  grammar, which differs from general expressions in three probed
     *  ways (harvest testServiceTestParameters): signed literals FOLD
     *  (-54 is integer -54, not minus(54)); Enum.value is an enumValue
     *  node; a [list] wires as classInstance/listInstance (wrapped at
     *  the emitter over the PureCollection this returns). */
    private static com.legend.protocol.spec.ValueSpecification
            legacyParamValue(TokenStreamCursor c, int from) {
        var ts = c.tokens();
        int n = c.pos() - from;
        // -NUMBER → folded negative literal
        if (n == 2 && ts.type(from) == TokenType.MINUS) {
            TokenType lt = ts.type(from + 1);
            String txt = ts.text(from + 1);
            var span = c.spanOf(from, from + 1);
            if (lt == TokenType.INTEGER) {
                return new com.legend.protocol.spec.CInteger(
                        Long.parseLong("-" + txt), span);
            }
            if (lt == TokenType.FLOAT) {
                return new com.legend.protocol.spec.CFloat(
                        Double.parseDouble("-" + txt), span);
            }
        }
        // path DOT ident → enumValue
        int dot = -1;
        for (int i = from; i < c.pos(); i++) {
            if (ts.type(i) == TokenType.DOT) {
                dot = i;
            }
        }
        if (dot > from && dot == c.pos() - 2
                && c.isIdentifierToken(ts.type(c.pos() - 1))) {
            boolean pathish = true;
            for (int i = from; i < dot; i++) {
                if (!(c.isIdentifierToken(ts.type(i))
                        || ts.type(i) == TokenType.PATH_SEPARATOR)) {
                    pathish = false;
                    break;
                }
            }
            if (pathish) {
                StringBuilder path = new StringBuilder();
                for (int i = from; i < dot; i++) {
                    path.append(ts.text(i));
                }
                return new com.legend.protocol.spec.EnumValue(
                        Protocol.unquotePath(path.toString()),
                        ts.text(c.pos() - 1), null,
                        c.spanOf(from, c.pos() - 1));
            }
        }
        // list([...]) → classInstance/listInstance wire; the span is the
        // WHOLE call (probed 9..23 over list(['param'])) — build the
        // marker AppliedFunction with that span so the emitter can wrap
        if (c.isIdentifierToken(ts.type(from)) && "list".equals(ts.text(from))
                && from + 1 < c.pos()
                && ts.type(from + 1) == TokenType.PAREN_OPEN
                && ts.type(c.pos() - 1) == TokenType.PAREN_CLOSE) {
            com.legend.protocol.spec.ValueSpecification inner =
                    com.legend.parser.SpecParser.parse(c.tokens().slice(from + 2, c.pos() - 1), c.dialect());
            return new com.legend.protocol.spec.AppliedFunction("list",
                    java.util.List.of(foldSignedLiterals(inner)),
                    java.util.List.of(),
                    c.spanOf(from, c.pos() - 1));
        }
        return foldSignedLiterals(com.legend.parser.SpecParser.parse(c.tokens().slice(from, c.pos()), c.dialect()));
    }

    /** The folded literal's span runs the '-' through the digits (harvest
     *  testServiceTestParameters [1.8, 2, -3] → cols 65-66). */
    private static com.legend.protocol.@com.legend.Nullable SourceInfo
            signedSpan(
            com.legend.protocol.@com.legend.Nullable SourceInfo minus,
            com.legend.protocol.@com.legend.Nullable SourceInfo lit) {
        if (minus == null || lit == null) {
            return minus;
        }
        return new com.legend.protocol.SourceInfo(minus.sourceId(),
                minus.startLine(), minus.startColumn(),
                lit.endLine(), lit.endColumn());
    }

    /** The engine's primitiveValue grammar folds SIGNED literals (-3 is
     *  integer -3, not minus(3)) at every depth of a parameter value —
     *  rewrite unary-minus-over-literal recursively (harvest
     *  testServiceTestParameters list([1,2,-3])). */
    private static com.legend.protocol.spec.ValueSpecification
            foldSignedLiterals(com.legend.protocol.spec.ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.AppliedFunction af
                && "minus".equals(af.function())
                && af.parameters().size() == 1) {
            var arg = af.parameters().get(0);
            if (arg instanceof com.legend.protocol.spec.CInteger ci) {
                return new com.legend.protocol.spec.CInteger(
                        -ci.value().longValue(),
                        signedSpan(af.pos(), ci.pos()));
            }
            if (arg instanceof com.legend.protocol.spec.CFloat cf) {
                return new com.legend.protocol.spec.CFloat(-cf.value(),
                        signedSpan(af.pos(), cf.pos()));
            }
        }
        java.util.List<com.legend.protocol.spec.ValueSpecification> cs =
                v.children();
        if (cs.isEmpty()) {
            return v;
        }
        java.util.List<com.legend.protocol.spec.ValueSpecification> out =
                new ArrayList<>(cs.size());
        boolean changed = false;
        for (var child : cs) {
            var f = foldSignedLiterals(child);
            changed |= f != child;
            out.add(f);
        }
        return changed ? v.withChildren(out) : v;
    }


    /** One {@code postValidations} entry — description/params/assertions
     *  (harvest testServiceWithPostValidation). Assertion lambdas parse
     *  through SpecParser (typed-lambda spelling). */
    private static Protocol.PPostValidation parsePostValidation(
            TokenStreamCursor c) {
        int eS = c.pos();
        c.expect(TokenType.BRACE_OPEN);
        String description = null;
        List<com.legend.protocol.spec.ValueSpecification> params =
                new ArrayList<>();
        List<Protocol.PPostValidationAssertion> assertions =
                new ArrayList<>();
        java.util.Set<String> seenPvKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seenPvKeys.add(key)) {
                throw c.error("Field '" + key
                        + "' should be specified only once");
            }
            switch (key) {
                case "description" -> {
                    description = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "params" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        int pS = c.pos();
                        int d = 0;
                        while (!c.atEnd()) {
                            TokenType tk = c.peek();
                            if (d == 0 && (tk == TokenType.COMMA
                                    || tk == TokenType.BRACKET_CLOSE)) {
                                break;
                            }
                            if (tk == TokenType.PAREN_OPEN
                                    || tk == TokenType.BRACE_OPEN
                                    || tk == TokenType.BRACKET_OPEN) {
                                d++;
                            } else if (tk == TokenType.PAREN_CLOSE
                                    || tk == TokenType.BRACE_CLOSE
                                    || tk == TokenType.BRACKET_CLOSE) {
                                d--;
                            }
                            c.advance();
                        }
                        params.add(com.legend.parser.SpecParser.parse(c.tokens().slice(pS, c.pos()), c.dialect()));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "assertions" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        int aS = c.pos();
                        String id = c.parseIdentifier();
                        c.expect(TokenType.COLON);
                        int ls = c.pos();
                        int d = 0;
                        while (!c.atEnd()) {
                            TokenType tk = c.peek();
                            if (d == 0 && (tk == TokenType.SEMI_COLON
                                    || tk == TokenType.BRACKET_CLOSE)) {
                                break;
                            }
                            if (tk == TokenType.PAREN_OPEN
                                    || tk == TokenType.BRACE_OPEN
                                    || tk == TokenType.BRACKET_OPEN) {
                                d++;
                            } else if (tk == TokenType.PAREN_CLOSE
                                    || tk == TokenType.BRACE_CLOSE
                                    || tk == TokenType.BRACKET_CLOSE) {
                                d--;
                            }
                            c.advance();
                        }
                        com.legend.protocol.spec.ValueSpecification lambda =
                                com.legend.parser.SpecParser.parse(c.tokens().slice(ls, c.pos()), c.dialect());
                        c.match(TokenType.SEMI_COLON);
                        // ALL assertion spans include the trailing ';' —
                        // the engine's walker parses `<lambda>;` as one
                        // slice, so the LAMBDA's own span extends through
                        // it too (probed: endColumn 221 vs 220)
                        var extended = c.spanOf(aS, c.pos() - 1);
                        if (lambda instanceof
                                com.legend.protocol.spec.LambdaFunction lf
                                && lf.pos() != null) {
                            var lp = lf.pos();
                            lambda = new com.legend.protocol.spec
                                    .LambdaFunction(lf.parameters(),
                                            lf.body(),
                                            new com.legend.protocol.SourceInfo(
                                                    lp.sourceId(),
                                                    lp.startLine(),
                                                    lp.startColumn(),
                                                    extended.endLine(),
                                                    extended.endColumn()));
                        }
                        assertions.add(
                                new Protocol.PPostValidationAssertion(id,
                                        lambda, extended));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown postValidation key: "
                        + key);
            }
        }
        int close = c.pos();
        c.expect(TokenType.BRACE_CLOSE);
        // visitPostValidation: description, params, assertions ALL required
        if (description == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), eS,
                    "Field 'description' is required");
        }
        if (!seenPvKeys.contains("params")) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), eS,
                    "Field 'params' is required");
        }
        if (!seenPvKeys.contains("assertions")) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), eS,
                    "Field 'assertions' is required");
        }
        return new Protocol.PPostValidation(description, params,
                assertions, c.spanOf(eS, close));
    }

    /** The legacy test BODY loop ({@code data:} / {@code asserts:}),
     *  shared by Single and Multi keyed entries; returns data. */
    private static @com.legend.Nullable String parseLegacyBody(
            TokenStreamCursor c,
            List<Protocol.PLegacyServiceTest.PLegacyAssert> asserts,
            int entryAnchor) {
        String data = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seenKeys.add(key)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), entryAnchor,
                        "Field '" + key + "' should be specified only once");
            }
            switch (key) {
                case "data" -> {
                    data = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "asserts" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        int as = c.pos();
                        c.expect(TokenType.BRACE_OPEN);
                        c.expect(TokenType.BRACKET_OPEN);
                        // parametersValues — spec values on the wire
                        // (harvest testServiceTestParameters)
                        List<com.legend.protocol.spec.ValueSpecification>
                                paramValues = new ArrayList<>();
                        while (c.peek() != TokenType.BRACKET_CLOSE
                                && !c.atEnd()) {
                            int pvS = c.pos();
                            int pd = 0;
                            while (!c.atEnd()) {
                                TokenType tk = c.peek();
                                if (pd == 0 && (tk == TokenType.COMMA
                                        || tk == TokenType.BRACKET_CLOSE)) {
                                    break;
                                }
                                if (tk == TokenType.PAREN_OPEN
                                        || tk == TokenType.BRACE_OPEN
                                        || tk == TokenType.BRACKET_OPEN) {
                                    pd++;
                                } else if (tk == TokenType.PAREN_CLOSE
                                        || tk == TokenType.BRACE_CLOSE
                                        || tk == TokenType.BRACKET_CLOSE) {
                                    pd--;
                                }
                                c.advance();
                            }
                            paramValues.add(legacyParamValue(c, pvS));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.expect(TokenType.COMMA);
                        int ls = c.pos();
                        int d = 0;
                        while (!c.atEnd()) {
                            TokenType tk = c.peek();
                            switch (tk) {
                                case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN ->
                                        d++;
                                case PAREN_CLOSE, BRACKET_CLOSE -> d--;
                                case BRACE_CLOSE -> {
                                    d--;
                                }
                                default -> { }
                            }
                            if (tk == TokenType.BRACE_CLOSE && d < 0) {
                                break;
                            }
                            c.advance();
                        }
                        com.legend.protocol.spec.ValueSpecification lambda =
                                com.legend.parser.SpecParser.parse(c.tokens().slice(ls, c.pos()), c.dialect());
                        c.expect(TokenType.BRACE_CLOSE);
                        asserts.add(new Protocol.PLegacyServiceTest
                                .PLegacyAssert(paramValues, lambda,
                                        c.spanOf(as, c.pos() - 1)));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown legacy test key '" + key
                        + "'");
            }
        }
        return data;
    }

    /** {@code testSuites: [ id: { data?; tests } ]} — the wire spans
     *  anchor at ID tokens; connection data and asserts ride THE
     *  embedded-data / test-assertion machinery (probed
     *  service-suites2). */
    private static List<Protocol.PServiceTestSuite> parseTestSuites(
            TokenStreamCursor c) {
        List<Protocol.PServiceTestSuite> out = new ArrayList<>();
        c.expect(TokenType.BRACKET_OPEN);
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            int ss = c.pos();
            String id = c.parseIdentifier();
            // 4.138 COMPACT form: id 'doc'? ( ... ) — v1 keeps id: { ... }
            if (c.peek() == TokenType.STRING
                    || c.peek() == TokenType.PAREN_OPEN) {
                out.add(parseCompactSuite(c, ss, id));
                c.match(TokenType.COMMA);
                continue;
            }
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACE_OPEN);
            Protocol.PServiceTestSuite.PSuiteData data = null;
            List<Protocol.PServiceTestSuite.PSuiteTest> tests =
                    new ArrayList<>();
            boolean testsSpelled = false;
            java.util.Set<String> seenSuiteKeys = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String key = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if (!seenSuiteKeys.add(key)) {
                    throw c.error("Field '" + key
                            + "' should be specified only once");
                }
                switch (key) {
                    case "data" -> data = parseSuiteData(c, keyStart);
                    case "tests" -> {
                        testsSpelled = true;
                        parseSuiteTests(c, tests);
                    }
                    default -> throw c.error("unknown testSuite key '"
                            + key + "'");
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (!testsSpelled) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), ss,
                        "Field 'tests' is required");
            }
            out.add(new Protocol.PServiceTestSuite(id, null, data, tests,
                    c.spanOf(ss, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return out;
    }

    /** The 4.138 COMPACT suite (ZServiceV2Probe): {@code id 'doc'? (
     *  (path; | path: Kind #{...}#;)* (testId 'doc'? => <expected>;)* )}
     *  — {@code path;} is a referenceDataResolver, {@code path: ...} a
     *  baseDataResolver, and each test carries ONE "default"
     *  equalTo-assertion. The testData wire span anchors at the suite's
     *  DOC string. */
    private static Protocol.PServiceTestSuite parseCompactSuite(
            TokenStreamCursor c, int ss, String id) {
        String doc = null;
        int docTok = c.pos();
        if (c.peek() == TokenType.STRING) {
            doc = SectionParse.stringValue(c);
        }
        c.expect(TokenType.PAREN_OPEN);
        List<Protocol.PServiceTestSuite.PResolverData> resolvers =
                new ArrayList<>();
        List<Protocol.PServiceTestSuite.PSuiteTest> tests = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            int es = c.pos();
            String name = Protocol.unquotePath(c.parseQualifiedName());
            int nameEnd = c.pos() - 1;
            if (c.peek() == TokenType.SEMI_COLON) {
                c.advance();                        // reference resolver
                // resolver spans END BEFORE the ';' (G8-adjudicated)
                resolvers.add(new Protocol.PServiceTestSuite.PResolverData(
                        null, name, c.spanOf(es, nameEnd),
                        c.spanOf(es, c.pos() - 2)));
            } else if (c.peek() == TokenType.COLON
                    && !(c.isIdentifierToken(c.peek(1))
                            && c.peek(2) == TokenType.EQUAL)) {
                // `path: Kind #{...}#;` is a baseDataResolver; `id :
                // PURE_TDSOBJECT => ...` is an atomic test spelling its
                // serializationFormat — the arrow after the identifier
                // tells them apart (serviceAtomicTest, 4.145.0)
                c.advance();                        // base resolver
                Protocol.PEmbeddedDataValue v = com.legend.parser
                        .MappingProtocolParser.parseEmbeddedValueAt(c);
                c.expect(TokenType.SEMI_COLON);
                resolvers.add(new Protocol.PServiceTestSuite.PResolverData(
                        v, name, c.spanOf(es, nameEnd),
                        c.spanOf(es, c.pos() - 2)));
            } else {
                String tdoc = null;                 // a test entry
                if (c.peek() == TokenType.STRING) {
                    tdoc = SectionParse.stringValue(c);
                }
                List<Protocol.PServiceTestSuite.PSuiteParam> parameters =
                        null;
                if (c.peek() == TokenType.PAREN_OPEN) {
                    c.advance();                    // ( name = value, ... )
                    parameters = new ArrayList<>();
                    while (c.peek() != TokenType.PAREN_CLOSE) {
                        String pn = c.parseIdentifier();
                        c.expect(TokenType.EQUAL);
                        Protocol.PServiceTestSuite.PSuiteParam tb =
                                byteArrayParam(c, pn);
                        if (tb != null) {
                            parameters.add(tb);
                            c.match(TokenType.COMMA);
                            continue;
                        }
                        int vs = c.pos();
                        int d = 0;
                        while (!c.atEnd()) {
                            TokenType tk = c.peek();
                            switch (tk) {
                                case PAREN_OPEN, BRACE_OPEN,
                                        BRACKET_OPEN -> d++;
                                case PAREN_CLOSE, BRACE_CLOSE,
                                        BRACKET_CLOSE -> d--;
                                default -> { }
                            }
                            if ((tk == TokenType.COMMA && d <= 0)
                                    || (tk == TokenType.PAREN_CLOSE
                                            && d < 0)) {
                                break;
                            }
                            c.advance();
                        }
                        parameters.add(new Protocol.PServiceTestSuite
                                .PSuiteParam(pn, SpecParser.parse(c.tokens().slice(vs, c.pos()), c.dialect())));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.PAREN_CLOSE);
                }
                List<String> keys = new ArrayList<>();
                if (c.peek() == TokenType.BRACKET_OPEN) {
                    c.advance();                    // [ 'KEY_A', ... ]
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        keys.add(SectionParse.stringValue(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                String fmt = null;
                if (c.match(TokenType.COLON)) {     // : PURE_TDSOBJECT
                    fmt = c.parseIdentifier();
                }
                c.expect(TokenType.EQUAL);
                c.expect(TokenType.GREATER_THAN);
                Protocol.PTestAssertion a = com.legend.parser
                        .MappingProtocolParser.parseDefaultAssertionAt(c);
                c.match(TokenType.SEMI_COLON);
                tests.add(new Protocol.PServiceTestSuite.PSuiteTest(name,
                        tdoc, fmt, keys, parameters, List.of(a),
                        c.spanOf(es, c.pos() - 1)));
            }
        }
        int close = c.pos();
        c.expect(TokenType.PAREN_CLOSE);
        Protocol.PServiceTestSuite.PSuiteData data = resolvers.isEmpty()
                ? null
                : new Protocol.PServiceTestSuite.PSuiteData(List.of(),
                        resolvers, c.spanOf(docTok, close));
        return new Protocol.PServiceTestSuite(id, doc, data, tests,
                c.spanOf(ss, close));
    }

    private static Protocol.PServiceTestSuite.PSuiteData parseSuiteData(
            TokenStreamCursor c, int keyStart) {
        c.expect(TokenType.BRACKET_OPEN);
        List<Protocol.PServiceTestSuite.PSuiteConnData> conns =
                new ArrayList<>();
        boolean connectionsSpelled = false;
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            String k = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!"connections".equals(k)) {
                throw c.error("unknown suite data key '" + k + "'");
            }
            if (connectionsSpelled) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), keyStart,
                        "Field 'connections' should be specified only once");
            }
            connectionsSpelled = true;
            c.expect(TokenType.BRACKET_OPEN);
            while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
                int cs = c.pos();
                // connection ids may be QUALIFIED paths
                String cid = Protocol.unquotePath(c.parseQualifiedName());
                c.expect(TokenType.COLON);
                Protocol.PEmbeddedDataValue value = com.legend.parser
                        .MappingProtocolParser.parseEmbeddedValueAt(c);
                conns.add(new Protocol.PServiceTestSuite.PSuiteConnData(cid,
                        value, c.spanOf(cs, c.pos() - 1)));
                c.match(TokenType.COMMA);
            }
            c.expect(TokenType.BRACKET_CLOSE);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return new Protocol.PServiceTestSuite.PSuiteData(conns, null,
                c.spanOf(keyStart, c.pos() - 1));
    }

    private static void parseSuiteTests(TokenStreamCursor c,
            List<Protocol.PServiceTestSuite.PSuiteTest> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            int ts = c.pos();
            String id = c.parseIdentifier();
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACE_OPEN);
            String serializationFormat = null;
            List<String> keys = new ArrayList<>();
            List<Protocol.PServiceTestSuite.PSuiteParam> parameters = null;
            List<Protocol.PTestAssertion> asserts = new ArrayList<>();
            boolean assertsSpelled = false;
            java.util.Set<String> seenTestKeys = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                String key = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if (!seenTestKeys.add(key)) {
                    throw c.error("Field '" + key
                            + "' should be specified only once");
                }
                if ("asserts".equals(key)) {
                    assertsSpelled = true;
                }
                switch (key) {
                    case "serializationFormat" -> {
                        serializationFormat = c.parseIdentifier();
                        c.expect(TokenType.SEMI_COLON);
                    }
                    case "keys" -> {
                        c.expect(TokenType.BRACKET_OPEN);
                        if (c.peek() == TokenType.BRACKET_CLOSE) {
                            // the engine's keys list requires at least ONE
                            // entry (negative fixture engine-fixture#d4927241e998)
                            throw c.error("Unexpected token ']'");
                        }
                        while (c.peek() != TokenType.BRACKET_CLOSE) {
                            keys.add(SectionParse.stringValue(c));
                            if (!c.match(TokenType.COMMA)) {
                                break;
                            }
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.match(TokenType.SEMI_COLON);
                    }
                    case "parameters" -> {
                        // [ name = value, ... ] — values ride the spec
                        // wire (probed service-test-params)
                        parameters = new ArrayList<>();
                        c.expect(TokenType.BRACKET_OPEN);
                        while (c.peek() != TokenType.BRACKET_CLOSE) {
                            String pn = c.parseIdentifier();
                            c.expect(TokenType.EQUAL);
                            Protocol.PServiceTestSuite.PSuiteParam tb2 =
                                    byteArrayParam(c, pn);
                            if (tb2 != null) {
                                parameters.add(tb2);
                                c.match(TokenType.COMMA);
                                continue;
                            }
                            int vs2 = c.pos();
                            int d2 = 0;
                            while (!c.atEnd()) {
                                TokenType tk = c.peek();
                                switch (tk) {
                                    case PAREN_OPEN, BRACE_OPEN,
                                            BRACKET_OPEN -> d2++;
                                    case PAREN_CLOSE, BRACE_CLOSE,
                                            BRACKET_CLOSE -> d2--;
                                    default -> { }
                                }
                                if ((tk == TokenType.COMMA && d2 <= 0)
                                        || (tk == TokenType.BRACKET_CLOSE
                                                && d2 < 0)) {
                                    break;
                                }
                                c.advance();
                            }
                            parameters.add(new Protocol.PServiceTestSuite
                                    .PSuiteParam(pn, SpecParser.parse(c.tokens().slice(vs2,
                                                    c.pos()), c.dialect())));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.match(TokenType.SEMI_COLON);
                    }
                    case "asserts" -> {
                        c.expect(TokenType.BRACKET_OPEN);
                        while (!c.atEnd()
                                && c.peek() != TokenType.BRACKET_CLOSE) {
                            asserts.add(com.legend.parser
                                    .MappingProtocolParser
                                    .parseTestAssertionAt(c));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                    }
                    default -> throw c.error("unknown suite test key '"
                            + key + "'");
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (!assertsSpelled) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), ts,
                        "Field 'asserts' is required");
            }
            out.add(new Protocol.PServiceTestSuite.PSuiteTest(id, null,
                    serializationFormat, keys, parameters, asserts,
                    c.spanOf(ts, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }

    /** {@code test: Single { data: '...'; asserts: [ { [params], lambda }
     *  ] }} — wire {@code singleExecutionTest}; the engine REFUSES an
     *  empty asserts list (sentinel parity). */
    private static Protocol.PLegacyServiceTest parseLegacyTest(
            TokenStreamCursor c) {
        int start = c.pos();
        String kind = c.parseIdentifier();
        if ("Multi".equals(kind)) {
            // Multi tests: tests['KEY']: { data; asserts } entries — the
            // entry span starts at the tests keyword (probed
            // service-multi-test); empty bodies legal
            c.expect(TokenType.BRACE_OPEN);
            List<Protocol.PLegacyServiceTest.PKeyedLegacyTest> keyed =
                    new ArrayList<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int es = c.pos();
                String tk = c.parseIdentifier();
                if (!"tests".equals(tk)) {
                    throw c.error("unknown Multi test key '" + tk + "'");
                }
                c.expect(TokenType.BRACKET_OPEN);
                String keyValue = SectionParse.stringValue(c);
                c.expect(TokenType.BRACKET_CLOSE);
                c.expect(TokenType.COLON);
                c.expect(TokenType.BRACE_OPEN);
                List<Protocol.PLegacyServiceTest.PLegacyAssert> ka =
                        new ArrayList<>();
                String kd = parseLegacyBody(c, ka, es);
                c.expect(TokenType.BRACE_CLOSE);
                c.match(TokenType.SEMI_COLON);
                if (kd == null) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), es,
                            "Multi test '" + keyValue + "' needs data");
                }
                keyed.add(new Protocol.PLegacyServiceTest.PKeyedLegacyTest(
                        keyValue, kd, ka, c.spanOf(es, c.pos() - 1)));
                c.match(TokenType.COMMA);
            }
            c.expect(TokenType.BRACE_CLOSE);
            c.match(TokenType.SEMI_COLON);
            return new Protocol.PLegacyServiceTest("Multi", null,
                    java.util.List.of(), keyed,
                    c.spanOf(start, c.pos() - 1));
        }
        if (!"Single".equals(kind)) {
            throw c.error("unsupported legacy test kind: " + kind);
        }
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PLegacyServiceTest.PLegacyAssert> asserts =
                new ArrayList<>();
        String data = parseLegacyBody(c, asserts, start);
        c.expect(TokenType.BRACE_CLOSE);
        c.match(TokenType.SEMI_COLON);
        if (data == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                    "legacy test needs data");
        }
        return new Protocol.PLegacyServiceTest("Single", data, asserts,
                java.util.List.of(), c.spanOf(start, c.pos() - 1));
    }

    private static Protocol.PServiceExecution parseExecution(
            TokenStreamCursor c, String qn) {
        int execStart = c.pos();
        String kind = c.parseIdentifier();
        return switch (kind) {
            case "Single" -> {
                c.expect(TokenType.BRACE_OPEN);
                com.legend.protocol.spec.ValueSpecification query = null;
                String mapping = null;
                SourceInfo mappingSpan = null;
                String runtime = null;
                SourceInfo runtimeSpan = null;
                Protocol.PEmbeddedRuntime embedded = null;
                // NOTE (reprobe C11, tried & reverted): the engine's
                // fixed key SEQUENCE here is raw ANTLR expectation
                // mechanics — its error position depends on where the
                // parse stops (sometimes mid-value), and a rank-order
                // check broke four agreeing pins. Order-free stays, as
                // allowlisted justified leniency.
                java.util.Set<String> seenExecKeys = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    c.expect(TokenType.COLON);
                    if (!seenExecKeys.add(key)) {
                        throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field '" + key
                                + "' should be specified only once");
                    }
                    switch (key) {
                        case "query" -> query = parseQuery(c, qn);
                        case "mapping" -> {
                            int ms = c.pos();
                            mapping = Protocol.unquotePath(c.parseQualifiedName());
                            mappingSpan = c.spanOf(ms, c.pos() - 1);
                            c.expect(TokenType.SEMI_COLON);
                        }
                        case "runtime" -> {
                            if (c.peek() == TokenType.ISLAND_OPEN) {
                                embedded = parseEmbeddedRuntime(c);
                            } else {
                                int rs = c.pos();
                                runtime = Protocol.unquotePath(
                                        c.parseQualifiedName());
                                runtimeSpan = c.spanOf(rs, c.pos() - 1);
                            }
                            c.expect(TokenType.SEMI_COLON);
                        }
                        default -> throw c.error("unknown key '" + key
                                + "' inside Service.execution");
                    }
                }
                c.expect(TokenType.BRACE_CLOSE);
                if (query == null) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field 'query' is required");
                }
                // mapping and runtime come TOGETHER or not at all — the
                // walker names whichever is missing (visitExecution)
                boolean hasMapping = mapping != null;
                boolean hasRuntime = runtime != null || embedded != null;
                if (hasMapping && !hasRuntime) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field 'runtime' is required");
                }
                if (hasRuntime && !hasMapping) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field 'mapping' is required");
                }
                yield new Protocol.PSingleExecution(query, mapping,
                        mappingSpan, runtime, runtimeSpan, embedded,
                        c.spanOf(execStart, c.pos() - 1));
            }
            case "Multi" -> {
                c.expect(TokenType.BRACE_OPEN);
                com.legend.protocol.spec.ValueSpecification query = null;
                String executionKey = null;
                List<Protocol.PKeyedExecution> executions = null;
                java.util.Set<String> seenMultiKeys = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    int keyTok = c.pos();
                    String key = c.parseIdentifier();
                    if (!"executions".equals(key)
                            && !seenMultiKeys.add(key)) {
                        throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field '" + key
                                + "' should be specified only once");
                    }
                    if ("executions".equals(key)) {
                        // executions['QA']: { mapping: ...; runtime: ...; }
                        // — the wire span starts at the KEY token (probed)
                        c.expect(TokenType.BRACKET_OPEN);
                        String keyValue = SectionParse.stringValue(c);
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.expect(TokenType.COLON);
                        if (executions == null) {
                            executions = new ArrayList<>();
                        }
                        executions.add(parseKeyedBody(c, keyValue, keyTok, true));
                        c.match(TokenType.COMMA);
                        continue;
                    }
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "query" -> query = parseQuery(c, qn);
                        case "key" -> {
                            executionKey = SectionParse.stringValue(c);
                            c.expect(TokenType.SEMI_COLON);
                        }
                        default -> throw c.error("unknown key '" + key
                                + "' inside Service.execution Multi");
                    }
                }
                c.expect(TokenType.BRACE_CLOSE);
                if (query == null) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field 'query' is required");
                }
                if (executions != null && !executions.isEmpty()
                        && executionKey == null) {
                    // key is REQUIRED once keyed executions appear
                    // (visitExecution multiExec)
                    throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), execStart, "Field 'key' is required");
                }
                yield new Protocol.PMultiExecution(query, executionKey,
                        executions, c.spanOf(execStart, c.pos() - 1));
            }
            default -> throw c.error("unsupported execution kind: " + kind
                    + " (expected Single or Multi)");
        };
    }

    /** {@code { mapping: ...; runtime: ...; }} for one keyed environment.
     *  {@code requireRuntime}: a Multi service's keyed execution REQUIRES
     *  runtime (visitKeyedExecutionParameter); an ExecutionEnvironment
     *  single does not (visitSingleExecutionParameters). */
    private static Protocol.PKeyedExecution parseKeyedBody(
            TokenStreamCursor c, String keyValue, int start,
            boolean requireRuntime) {
        if (keyValue.contains(" ")) {
            // ServiceParseTreeWalker:787, engine-verbatim (sibling
            // negative neg-service-execenv-key-with-space)
            throw TokenStreamCursor.throwAt(c.tokens(), start,
                    "Execution param key cannot contain spaces."
                    + " Invalid Key: " + keyValue);
        }
        c.expect(TokenType.BRACE_OPEN);
        String mapping = null;
        SourceInfo mappingSpan = null;
        String runtime = null;
        SourceInfo runtimeSpan = null;
        Protocol.PEmbeddedRuntime embedded = null;
        Protocol.PRuntimeComponents runtimeComponents = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seenKeys.add(key)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                        "Field '" + key + "' should be specified only once");
            }
            switch (key) {
                case "mapping" -> {
                    if (runtime != null || embedded != null) {
                        // the .g4 fixes mapping BEFORE runtime (mutant
                        // swap-siblings probe: engine refuses the swap)
                        throw c.error("Unexpected token 'mapping'");
                    }
                    int ms = c.pos();
                    mapping = Protocol.unquotePath(c.parseQualifiedName());
                    mappingSpan = c.spanOf(ms, c.pos() - 1);
                }
                case "runtime" -> {
                    if (c.peek() == TokenType.ISLAND_OPEN) {
                        embedded = parseEmbeddedRuntime(c);
                    } else {
                        int rs = c.pos();
                        runtime = Protocol.unquotePath(c.parseQualifiedName());
                        runtimeSpan = c.spanOf(rs, c.pos() - 1);
                    }
                }
                case "runtimeComponents" -> {
                    // { class; binding; runtime; } — wire binding/clazz
                    // pointers + runtimePointer (harvest
                    // testExecutionEnvironment; probe ee3)
                    c.expect(TokenType.BRACE_OPEN);
                    Protocol.PPointer clazz = null;
                    Protocol.PPointer binding = null;
                    String rcRuntime = null;
                    com.legend.protocol.SourceInfo rcRuntimeSpan = null;
                    while (!c.atEnd()
                            && c.peek() != TokenType.BRACE_CLOSE) {
                        int rkS = c.pos();
                        String rk = c.parseIdentifier();
                        c.expect(TokenType.COLON);
                        String path = Protocol.unquotePath(
                                c.parseQualifiedName());
                        c.expect(TokenType.SEMI_COLON);
                        // spans run KEY through ';' (probed 12..31)
                        var span2 = c.spanOf(rkS, c.pos() - 1);
                        switch (rk) {
                            case "class" -> clazz = new Protocol.PPointer(
                                    "CLASS", path, span2);
                            case "binding" -> binding = new Protocol.PPointer(
                                    "BINDING", path, span2);
                            case "runtime" -> {
                                rcRuntime = path;
                                rcRuntimeSpan = span2;
                            }
                            default -> throw c.error(
                                    "unknown runtimeComponents key: " + rk);
                        }
                    }
                    c.expect(TokenType.BRACE_CLOSE);
                    // engine's required-field refusal shape, positioned —
                    // a bare requireNonNull threw an unlocated NPE here
                    // (adversarial audit #6)
                    if (binding == null) {
                        throw c.error("Field 'binding' is required");
                    }
                    if (clazz == null) {
                        throw c.error("Field 'class' is required");
                    }
                    if (rcRuntime == null || rcRuntimeSpan == null) {
                        throw c.error("Field 'runtime' is required");
                    }
                    runtimeComponents = new Protocol.PRuntimeComponents(
                            binding, clazz, rcRuntime, rcRuntimeSpan);
                    continue;
                }
                default -> throw c.error(
                        "unknown key '" + key + "' inside keyed execution");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (mapping == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                    "Field 'mapping' is required");
        }
        if (requireRuntime && runtime == null && embedded == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                    "Field 'runtime' is required");
        }
        if (runtime == null && embedded == null && runtimeComponents == null) {
            // the EE GRAMMAR itself requires runtime OR runtimeComponents
            // — a mapping-only single is a parse error (negative fixture
            // engine-fixture#6ab8e975f069, refusal at the '}')
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), c.pos() - 1, "Unexpected token '}'");
        }
        return new Protocol.PKeyedExecution(keyValue, mapping, mappingSpan,
                runtime, runtimeSpan, embedded, runtimeComponents,
                c.spanOf(start, c.pos() - 1));
    }

    /** An embedded anonymous runtime island — re-lexed through THE runtime
     *  grammar's body loop; the wire span is CONTENT-anchored (first..last
     *  content token, probed service-single). */
    private static Protocol.PEmbeddedRuntime parseEmbeddedRuntime(
            TokenStreamCursor c) {
        c.advance();                                // ISLAND_OPEN
        int embStart = c.pos();
        int depth = 0;
        while (!c.atEnd()) {
            TokenType t = c.peek();
            if (t == TokenType.ISLAND_START) {
                depth++;
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    break;
                }
                depth--;
            }
            c.advance();
        }
        String emb = c.reconstructText(embStart, c.pos());
        Protocol.PEmbeddedRuntime value = RuntimeSectionGrammar
                .parseEmbeddedBody(emb, c.tokens().startLine(embStart),
                        c.tokens().startColumn(embStart), c.dialect());
        c.expect(TokenType.ISLAND_END);
        return value;
    }

    /** {@code query: |expr...;} — scan to the top-level {@code ;} (paren
     *  depth aware) and spec-parse the slice, exactly as the retired twin
     *  did; the LEADING '|' stays in the slice so the wire query is the
     *  full lambda. */
    private static com.legend.protocol.spec.ValueSpecification parseQuery(
            TokenStreamCursor c, String qn) {
        int bs = c.pos();
        int d = 0;
        while (!c.atEnd()) {
            TokenType tk = c.peek();
            switch (tk) {
                // ALL bracket kinds count — a {|let x; expr;} brace-lambda
                // query carries ';' INSIDE braces (powerbi dataspaces)
                case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN -> d++;
                case PAREN_CLOSE, BRACE_CLOSE, BRACKET_CLOSE -> d--;
                default -> { }
            }
            if (tk == TokenType.SEMI_COLON && d <= 0) {
                break;
            }
            c.advance();
        }
        if (c.pos() == bs) {
            throw c.error("empty query expression in Service '" + qn + "'");
        }

        com.legend.protocol.spec.ValueSpecification v =
                SpecParser.parse(c.tokens().slice(bs, c.pos()), c.dialect());
        c.expect(TokenType.SEMI_COLON);
        return v;
    }

    // ============================================================
    // ExecutionEnvironment
    // ============================================================

    static Protocol.PExecutionEnvironment parseExecutionEnvironment(
            TokenStreamCursor c) {
        int declStart = c.pos();
        c.advance();                        // 'ExecutionEnvironment'
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PExecutionParameters> executions = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!"executions".equals(key)) {
                throw c.error("unknown key '" + key
                        + "' inside ExecutionEnvironment '" + qn + "'");
            }
            c.expect(TokenType.BRACKET_OPEN);
            while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                // KEY: { mapping; runtime; } — bare identifier key here,
                // unlike Multi's quoted executions['KEY']; the wire span
                // starts at the KEY token (probed exec-env)
                int keyTok2 = c.pos();
                String keyValue = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if (c.peek() == TokenType.BRACKET_OPEN) {
                    // KEY: [ subKey: {..}, .. ] — a MULTI entry (harvest
                    // testExecutionEnvironmentInMultiExecService)
                    c.advance();
                    List<Protocol.PKeyedExecution> singles =
                            new ArrayList<>();
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        int subTok = c.pos();
                        String subKey = c.parseIdentifier();
                        c.expect(TokenType.COLON);
                        singles.add(parseKeyedBody(c, subKey, subTok, false));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    executions.add(new Protocol.PMultiKeyedExecution(
                            keyValue, singles));
                } else {
                    executions.add(parseKeyedBody(c, keyValue, keyTok2, false));
                }
                c.match(TokenType.COMMA);
            }
            c.expect(TokenType.BRACKET_CLOSE);
            c.match(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        return new Protocol.PExecutionEnvironment(pkg, name, executions,
                c.spanOf(declStart, c.pos() - 1));
    }

    // ============================================================
    // Small shared helpers
    // ============================================================



}
