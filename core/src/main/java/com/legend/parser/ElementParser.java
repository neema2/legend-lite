package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationDefinition.AssociationEndDefinition;
import com.legend.parser.element.AuthenticationSpec;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassDefinition.ConstraintDefinition;
import com.legend.parser.element.ClassDefinition.DerivedPropertyDefinition;
import com.legend.parser.element.ClassDefinition.ParameterDefinition;
import com.legend.parser.element.ConnectionDefinition;
import com.legend.parser.element.ConnectionSpecification;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JsonModelConnection;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.ProfileDefinition;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.element.RuntimeDefinition;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.element.StereotypeApplication;
import com.legend.parser.element.TaggedValue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hand-rolled recursive-descent parser for Pure top-level element
 * declarations &mdash; the driver for step B of the pipeline.
 *
 * <h2>Usage</h2>
 * <pre>
 *   ParsedModel model = ElementParser.parse(pureSource);    // text overload
 *   ParsedModel model = ElementParser.parse(tokenStream);   // pre-lexed overload
 * </pre>
 *
 * <h2>Status (Phase B.4a)</h2>
 * Handles:
 * <ul>
 *   <li>{@code import} statements (wildcard and specific).</li>
 *   <li>{@code Class} (regular and {@code native}) with stereotypes, tagged
 *       values, generic type parameters, superclasses, properties,
 *       derived properties (body as raw text), and class-level constraints.</li>
 *   <li>{@code Association} &mdash; exactly two ends.</li>
 *   <li>{@code Enum} &mdash; one or more value names.</li>
 *   <li>{@code Profile} &mdash; stereotypes and tags.</li>
 *   <li>{@code function} &mdash; signature plus body as raw text
 *       (lazy compilation in Phase G).</li>
 *   <li>{@code Service} &mdash; pattern, doc, execution (query/mapping/runtime),
 *       and {@code testSuites} block captured as raw text (decision D-3).</li>
 *   <li>{@code Runtime} (and {@code SingleConnectionRuntime}) &mdash; mappings,
 *       connection bindings, embedded {@code JsonModelConnection} islands.</li>
 *   <li>{@code RelationalDatabaseConnection} &mdash; store, type, specification,
 *       authentication.</li>
 * </ul>
 *
 * <p>B.4a (this slice) adds {@code Database} with the full relational
 * expression sub-AST &mdash; column refs, target columns, literals,
 * function calls, comparisons, boolean combinations, null tests, groups,
 * array literals, and join-navigation chains. Used by {@code Join},
 * {@code Filter}, {@code MultiGrainFilter}, and view column expressions.
 *
 * <p>NOT yet supported (sub-slices B.4b-f):
 * <ul>
 *   <li>{@code Mapping} (next: B.4b Relational class mappings, then
 *       Association / Enum / M2M class mappings / test-suites).</li>
 * </ul>
 * Encountering an unsupported keyword raises a {@link ParseException}.
 *
 * <h2>Deliberate divergences from engine</h2>
 * <ul>
 *   <li>{@link FunctionDefinition} drops engine's compiler-cache fields
 *       ({@code resolvedBody}, {@code parsedReturnType}, parameter
 *       {@code functionType}/{@code parsedType}) &mdash; those bridge parser
 *       output and compiler output via {@code withResolvedBody}, which is
 *       the F-must-not-trigger-G violation AGENTS.md invariant 1 forbids.
 *       Compile-side output lives on {@code compiler.element.TypedFunction}
 *       (Phase F, future).</li>
 *   <li>{@link ServiceDefinition} drops engine's {@code toRegexPattern()},
 *       {@code extractPathParams()}, and convenience factories &mdash;
 *       those are REST-runtime concerns, not parser data.</li>
 *   <li><strong>Unknown keys inside {@code Runtime} / {@code Connection} /
 *       {@code Service} bodies throw</strong> (engine silently
 *       {@code skipToSemicolon}'s them). Matches AGENTS.md invariant 4
 *       (no fallbacks). May fire on forward-compat Pure syntax &mdash;
 *       tracked as decision D-2 in core's README.</li>
 * </ul>
 *
 * <h2>Stateful only during parse</h2>
 * A parser instance is allocated per call to {@link #parse(TokenStream)},
 * driven to completion, then discarded. The resulting {@link ParsedModel}
 * is the only object that survives.
 *
 * <p>Ported from engine's {@code com.gs.legend.parser.PureModelParser},
 * keeping the same recursive-descent shape and identifier-token set
 * for parity testing.
 */
public final class ElementParser {

    // ============================================================
    // Keywords that may appear as identifiers (engine-parity set)
    // ============================================================

    private static final Set<TokenType> IDENTIFIER_TOKENS;

    static {
        IDENTIFIER_TOKENS = EnumSet.of(
                TokenType.VALID_STRING, TokenType.STRING,
                // M3
                TokenType.ALL, TokenType.LET, TokenType.ALL_VERSIONS, TokenType.ALL_VERSIONS_IN_RANGE,
                TokenType.COMPARATOR, TokenType.TO_BYTES_FUNCTION,
                // Domain
                TokenType.IMPORT, TokenType.CLASS, TokenType.FUNCTION, TokenType.PROFILE,
                TokenType.ASSOCIATION, TokenType.ENUM, TokenType.MEASURE, TokenType.EXTENDS,
                TokenType.STEREOTYPES, TokenType.TAGS, TokenType.NATIVE, TokenType.PROJECTS, TokenType.AS,
                TokenType.CONSTRAINT_ENFORCEMENT_LEVEL_ERROR, TokenType.CONSTRAINT_ENFORCEMENT_LEVEL_WARN,
                TokenType.AGGREGATION_TYPE_COMPOSITE, TokenType.AGGREGATION_TYPE_SHARED, TokenType.AGGREGATION_TYPE_NONE,
                // Mapping
                TokenType.MAPPING, TokenType.INCLUDE, TokenType.TESTS,
                TokenType.MAPPING_TESTABLE_DOC, TokenType.MAPPING_TESTABLE_DATA, TokenType.MAPPING_TESTABLE_ASSERT,
                TokenType.MAPPING_TESTABLE_SUITES, TokenType.MAPPING_TEST_ASSERTS, TokenType.MAPPING_TESTS,
                TokenType.MAPPING_TESTS_QUERY,
                // Runtime
                TokenType.RUNTIME, TokenType.SINGLE_CONNECTION_RUNTIME, TokenType.MAPPINGS,
                TokenType.CONNECTIONS, TokenType.CONNECTION, TokenType.CONNECTIONSTORES,
                // Relational / Database
                TokenType.DATABASE, TokenType.TABLE, TokenType.SCHEMA, TokenType.VIEW,
                TokenType.TABULAR_FUNC, TokenType.FILTER, TokenType.MULTIGRAIN_FILTER, TokenType.JOIN,
                TokenType.RELATIONAL_AND, TokenType.RELATIONAL_OR,
                TokenType.MILESTONING, TokenType.BUSINESS_MILESTONING,
                TokenType.BUSINESS_MILESTONING_FROM, TokenType.BUSINESS_MILESTONING_THRU,
                TokenType.THRU_IS_INCLUSIVE, TokenType.BUS_SNAPSHOT_DATE,
                TokenType.PROCESSING_MILESTONING, TokenType.PROCESSING_MILESTONING_IN,
                TokenType.PROCESSING_MILESTONING_OUT, TokenType.OUT_IS_INCLUSIVE,
                TokenType.INFINITY_DATE, TokenType.PROCESSING_SNAPSHOT_DATE,
                TokenType.ASSOCIATION_MAPPING, TokenType.ENUMERATION_MAPPING,
                TokenType.OTHERWISE, TokenType.INLINE, TokenType.BINDING, TokenType.SCOPE,
                TokenType.PURE_MAPPING, TokenType.RELATIONAL,
                // Connection
                TokenType.STORE, TokenType.TYPE, TokenType.MODE,
                TokenType.RELATIONAL_DATASOURCE_SPEC, TokenType.RELATIONAL_AUTH_STRATEGY,
                TokenType.DB_TIMEZONE, TokenType.QUOTE_IDENTIFIERS, TokenType.QUERY_GENERATION_CONFIGS,
                TokenType.DUCKDB, TokenType.SQLITE, TokenType.POSTGRES, TokenType.H2, TokenType.SNOWFLAKE,
                TokenType.LOCALDUCKDB, TokenType.INMEMORY, TokenType.NOAUTH,
                // Service
                TokenType.SERVICE, TokenType.SERVICE_PATTERN, TokenType.SERVICE_OWNERS,
                TokenType.SERVICE_DOCUMENTATION, TokenType.SERVICE_AUTO_ACTIVATE_UPDATES,
                TokenType.SERVICE_EXEC, TokenType.SERVICE_SINGLE, TokenType.SERVICE_MULTI,
                TokenType.SERVICE_MAPPING, TokenType.SERVICE_RUNTIME,
                // Boolean literals
                TokenType.TRUE, TokenType.FALSE,
                // Additional
                TokenType.RELATIONAL_DATABASE_CONNECTION,
                TokenType.RELATIONAL_POST_PROCESSORS, TokenType.QUERY_TIMEOUT
        );
    }

    // ============================================================
    // Instance state (transient during parse)
    // ============================================================

    private final TokenStream tokens;
    private final int count;
    private int pos;

    private ElementParser(TokenStream tokens) {
        this.tokens = tokens;
        this.count = tokens.count();
        this.pos = 0;
    }

    // ============================================================
    // Public API
    // ============================================================

    /** Tokenize and parse a Pure source string into a {@link ParsedModel}. */
    public static ParsedModel parse(String source) {
        return parse(Lexer.tokenize(source));
    }

    /** Parse a pre-lexed token stream into a {@link ParsedModel}. */
    public static ParsedModel parse(TokenStream tokens) {
        ElementParser parser = new ElementParser(tokens);
        return parser.parseModel();
    }

    // ============================================================
    // Top-level
    // ============================================================

    private ParsedModel parseModel() {
        List<PackageableElement> elements = new ArrayList<>();
        ImportScope.Builder imports = new ImportScope.Builder();

        while (!atEnd()) {
            TokenType t = peek();
            switch (t) {
                case IMPORT -> imports.add(parseImportStatement());
                case CLASS -> elements.add(parseClassDefinition(false));
                case NATIVE -> {
                    advance(); // consume 'native'
                    if (peek() != TokenType.CLASS) {
                        error("expected 'Class' after 'native', got " + peek() + " ('" + safeText() + "')");
                    }
                    elements.add(parseClassDefinition(true));
                }
                case ASSOCIATION -> elements.add(parseAssociation());
                case ENUM -> elements.add(parseEnumDefinition());
                case PROFILE -> elements.add(parseProfile());
                case FUNCTION -> elements.add(parseFunctionDefinition());
                case SERVICE -> elements.add(parseServiceDefinition());
                case RUNTIME -> elements.add(parseRuntime());
                case SINGLE_CONNECTION_RUNTIME -> elements.add(parseSingleConnectionRuntime());
                case RELATIONAL_DATABASE_CONNECTION -> elements.add(parseConnection());
                case DATABASE -> elements.add(parseDatabase());
                default -> error("unsupported top-level keyword in Phase B.4a: "
                        + t + " ('" + safeText() + "')");
            }
        }

        return new ParsedModel(elements, imports.build());
    }

    // ============================================================
    // Import statement
    // ============================================================

    /** {@code import packagePath::* ;} or {@code import packagePath::Type ;} */
    private String parseImportStatement() {
        expect(TokenType.IMPORT);
        StringBuilder sb = new StringBuilder();
        sb.append(parseIdentifier());
        while (match(TokenType.PATH_SEPARATOR)) {
            sb.append("::");
            if (match(TokenType.STAR)) {
                sb.append("*");
                break;
            }
            sb.append(parseIdentifier());
        }
        expect(TokenType.SEMI_COLON);
        return sb.toString();
    }

    // ============================================================
    // Class declaration
    // ============================================================

    private ClassDefinition parseClassDefinition(boolean isNative) {
        expect(TokenType.CLASS);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();

        List<String> typeParams = parseClassTypeParams();

        List<String> superClasses = new ArrayList<>();
        if (match(TokenType.EXTENDS)) {
            superClasses.add(parseType());
            while (match(TokenType.COMMA)) {
                superClasses.add(parseType());
            }
        }

        List<ConstraintDefinition> constraints = peek() == TokenType.BRACKET_OPEN
                ? parseConstraints()
                : List.of();

        expect(TokenType.BRACE_OPEN);

        List<ClassDefinition.PropertyDefinition> properties = new ArrayList<>();
        List<DerivedPropertyDefinition> derivedProperties = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            if (isDerivedPropertyStart()) {
                derivedProperties.add(parseDerivedProperty());
            } else {
                properties.add(parseProperty());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        return new ClassDefinition(
                qualifiedName,
                typeParams,
                superClasses,
                properties,
                derivedProperties,
                constraints,
                stereotypes,
                taggedValues,
                isNative);
    }

    /** Optional generic type parameters: {@code <T>}, {@code <U, V>}, ... */
    private List<String> parseClassTypeParams() {
        if (peek() != TokenType.LESS_THAN) return List.of();
        advance(); // consume <
        List<String> params = new ArrayList<>();
        params.add(parseIdentifier());
        while (match(TokenType.COMMA)) {
            params.add(parseIdentifier());
        }
        expect(TokenType.GREATER_THAN);
        return params;
    }

    /**
     * Property starts as {@code identifier (':' ...)}; derived property starts
     * as {@code identifier '(' ...}. Distinguish by lookahead at offset +1
     * across allowed stereotype/tag annotations.
     */
    private boolean isDerivedPropertyStart() {
        int saved = pos;
        // skip optional stereotypes <<...>>
        if (peek() == TokenType.LESS_THAN && peek(1) == TokenType.LESS_THAN) {
            int depth = 2;
            pos += 2;
            while (pos < count && depth > 0) {
                if (peek() == TokenType.GREATER_THAN) depth--;
                else if (peek() == TokenType.LESS_THAN) depth++;
                pos++;
            }
        }
        // skip optional tagged values { ... }
        if (peek() == TokenType.BRACE_OPEN) {
            int depth = 1;
            pos++;
            while (pos < count && depth > 0) {
                if (peek() == TokenType.BRACE_OPEN) depth++;
                else if (peek() == TokenType.BRACE_CLOSE) depth--;
                pos++;
            }
        }
        boolean derived = IDENTIFIER_TOKENS.contains(peek()) && peek(1) == TokenType.PAREN_OPEN;
        pos = saved;
        return derived;
    }

    /**
     * Parse a derived (computed) property:
     * {@code <<stereos>> {tags} name(params) { body }: Type[mult];}.
     * The body between {@code &lcub;...&rcub;} is captured as raw source text
     * &mdash; it is parsed and type-checked lazily by {@code SpecCompiler}
     * in Phase G (lazy expression-body invariant).
     *
     * <p>NOTE: stereotypes and tagged values on derived properties are
     * parsed-and-discarded for engine parity (engine drops them too).
     * If we want to preserve them later, this is the place.
     */
    private DerivedPropertyDefinition parseDerivedProperty() {
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String name = parseIdentifier();

        expect(TokenType.PAREN_OPEN);
        List<ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseDerivedPropertyParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseDerivedPropertyParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        String body = "";
        if (peek() == TokenType.BRACE_OPEN) {
            advance(); // consume {
            int bodyStart = pos;
            int depth = 1;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.BRACE_OPEN) depth++;
                else if (t == TokenType.BRACE_CLOSE) depth--;
                if (depth > 0) advance();
            }
            body = reconstructText(bodyStart, pos);
            expect(TokenType.BRACE_CLOSE);
        }

        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        expect(TokenType.SEMI_COLON);

        return new DerivedPropertyDefinition(
                name, params, body, type, mult[0],
                mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1]));
    }

    private ParameterDefinition parseDerivedPropertyParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        return new ParameterDefinition(
                name, type, mult[0],
                mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1]));
    }

    // ============================================================
    // Constraint declarations (class-level)
    // ============================================================

    /**
     * {@code [ constraint (, constraint)* ]} after the class header. Each
     * constraint may be {@code name: expression} or just {@code expression}
     * (named {@code "unnamed"} for parity with engine). The expression is
     * captured as raw source text and parsed lazily by {@code SpecCompiler}.
     */
    private List<ConstraintDefinition> parseConstraints() {
        expect(TokenType.BRACKET_OPEN);
        List<ConstraintDefinition> result = new ArrayList<>();
        if (peek() != TokenType.BRACKET_CLOSE) {
            result.add(parseConstraint());
            while (match(TokenType.COMMA)) {
                result.add(parseConstraint());
            }
        }
        expect(TokenType.BRACKET_CLOSE);
        return result;
    }

    private ConstraintDefinition parseConstraint() {
        String name = "unnamed";
        if (IDENTIFIER_TOKENS.contains(peek()) && peek(1) == TokenType.COLON) {
            name = parseIdentifier();
            advance(); // consume :
        }

        // Scan until top-level ',' or matching ']' &mdash; balance brackets,
        // parens, and braces so embedded expressions don't fool us.
        int bodyStart = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE) {
                if (depth == 0) break;
                depth--;
            } else if (t == TokenType.COMMA && depth == 0) {
                break;
            }
            advance();
        }
        String expression = reconstructText(bodyStart, pos);
        return new ConstraintDefinition(name, expression);
    }

    // ============================================================
    // Association
    // ============================================================

    /** {@code Association <<stereos>> {tags} qualifiedName { end1; end2; }} */
    private AssociationDefinition parseAssociation() {
        expect(TokenType.ASSOCIATION);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<AssociationEndDefinition> ends = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String name = parseIdentifier();
            expect(TokenType.COLON);
            String type = parseType();
            int[] mult = parseMultiplicity();
            expect(TokenType.SEMI_COLON);
            ends.add(new AssociationEndDefinition(
                    name, type, mult[0],
                    mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1])));
        }
        expect(TokenType.BRACE_CLOSE);

        if (ends.size() != 2) {
            error("Association must have exactly 2 properties, found: " + ends.size());
        }
        return new AssociationDefinition(qualifiedName, ends.get(0), ends.get(1));
    }

    // ============================================================
    // Enum
    // ============================================================

    /** {@code Enum <<stereos>> {tags} qualifiedName { VAL (, VAL)* }} */
    private EnumDefinition parseEnumDefinition() {
        expect(TokenType.ENUM);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<String> values = new ArrayList<>();
        if (peek() != TokenType.BRACE_CLOSE) {
            parseStereotypes();   // per-value, dropped
            parseTaggedValues();  // per-value, dropped
            values.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                parseStereotypes();
                parseTaggedValues();
                values.add(parseIdentifier());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        if (values.isEmpty()) {
            error("Enum '" + qualifiedName + "' must have at least one value");
        }
        return new EnumDefinition(qualifiedName, values);
    }

    // ============================================================
    // Profile
    // ============================================================

    /**
     * {@code Profile qualifiedName { (stereotypes: [...]; | tags: [...];)* }}
     */
    private ProfileDefinition parseProfile() {
        expect(TokenType.PROFILE);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            if (peek() == TokenType.STEREOTYPES) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                if (peek() != TokenType.BRACKET_CLOSE) {
                    stereotypes.add(parseIdentifier());
                    while (match(TokenType.COMMA)) {
                        stereotypes.add(parseIdentifier());
                    }
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
            } else if (peek() == TokenType.TAGS) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                if (peek() != TokenType.BRACKET_CLOSE) {
                    tags.add(parseIdentifier());
                    while (match(TokenType.COMMA)) {
                        tags.add(parseIdentifier());
                    }
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
            } else {
                error("expected 'stereotypes' or 'tags' inside Profile, found " + peek()
                        + " ('" + safeText() + "')");
            }
        }

        expect(TokenType.BRACE_CLOSE);
        return new ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    // ============================================================
    // Function declaration
    // ============================================================

    /**
     * {@code function <<stereos>> {tags} qualifiedName(params) : returnType[mult]
     * [optional-constraints] { body }}.
     * Body captured as raw source text per decision D-1 (lazy parsing in Phase G).
     */
    private FunctionDefinition parseFunctionDefinition() {
        expect(TokenType.FUNCTION);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();

        expect(TokenType.PAREN_OPEN);
        List<FunctionDefinition.ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseFunctionParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseFunctionParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        expect(TokenType.COLON);
        String returnType = parseType();
        int[] mult = parseMultiplicity();

        // Optional constraints block — engine accepts then discards.
        // We accept and discard for parity; future work may attach these.
        if (peek() == TokenType.BRACKET_OPEN) {
            parseConstraints();
        }

        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) advance();
        }
        String body = reconstructText(bodyStart, pos);
        expect(TokenType.BRACE_CLOSE);

        return new FunctionDefinition(
                qualifiedName, params, returnType,
                mult[0], mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1]),
                body, stereotypes, taggedValues);
    }

    private FunctionDefinition.ParameterDefinition parseFunctionParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        return new FunctionDefinition.ParameterDefinition(
                name, type, mult[0],
                mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1]));
    }

    // ============================================================
    // Service declaration
    // ============================================================

    /**
     * {@code Service qualifiedName { pattern: ...; documentation: ...;
     * execution: Single { query: |...; mapping: ...; runtime: ...; }
     * testSuites { ... } }}.
     *
     * <p>Unknown top-level keys throw (no silent skip). Query body and the
     * full {@code testSuites} block are captured as raw text (D-1, D-3).
     */
    private ServiceDefinition parseServiceDefinition() {
        expect(TokenType.SERVICE);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String pattern = null;
        String documentation = null;
        String functionBody = null;
        String mappingRef = null;
        String runtimeRef = null;
        String testSuitesSource = null;

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            switch (key) {
                case "pattern" -> {
                    pattern = unquoteString(consume(TokenType.STRING));
                    expect(TokenType.SEMI_COLON);
                }
                case "documentation" -> {
                    documentation = unquoteString(consume(TokenType.STRING));
                    expect(TokenType.SEMI_COLON);
                }
                case "autoActivateUpdates" -> {
                    advance(); // boolean literal
                    expect(TokenType.SEMI_COLON);
                }
                case "owners" -> {
                    expect(TokenType.BRACKET_OPEN);
                    if (peek() != TokenType.BRACKET_CLOSE) {
                        consume(TokenType.STRING);
                        while (match(TokenType.COMMA)) consume(TokenType.STRING);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    expect(TokenType.SEMI_COLON);
                }
                case "execution" -> {
                    parseIdentifier(); // "Single" (only flavor supported in B.3)
                    expect(TokenType.BRACE_OPEN);
                    while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
                        String execKey = parseIdentifier();
                        expect(TokenType.COLON);
                        switch (execKey) {
                            case "query" -> {
                                match(TokenType.PIPE); // optional leading '|'
                                int bs = pos;
                                int d = 0;
                                while (!atEnd()) {
                                    TokenType tk = peek();
                                    if (tk == TokenType.PAREN_OPEN) d++;
                                    else if (tk == TokenType.PAREN_CLOSE) d--;
                                    else if (tk == TokenType.SEMI_COLON && d <= 0) break;
                                    advance();
                                }
                                functionBody = reconstructText(bs, pos);
                                expect(TokenType.SEMI_COLON);
                            }
                            case "mapping" -> {
                                mappingRef = parseQualifiedName();
                                expect(TokenType.SEMI_COLON);
                            }
                            case "runtime" -> {
                                runtimeRef = parseQualifiedName();
                                expect(TokenType.SEMI_COLON);
                            }
                            default -> error("unknown key '" + execKey
                                    + "' inside Service.execution (Phase B.3 strict mode; see D-2)");
                        }
                    }
                    expect(TokenType.BRACE_CLOSE);
                }
                case "testSuites" -> {
                    // Capture the entire balanced block as raw text — D-3.
                    // testSuites may be followed by '{' or '['; both balance the same way.
                    TokenType opener = peek();
                    if (opener != TokenType.BRACE_OPEN && opener != TokenType.BRACKET_OPEN) {
                        error("expected '{' or '[' after testSuites:, got " + opener);
                    }
                    int bs = pos;
                    skipBalancedContent(opener,
                            opener == TokenType.BRACE_OPEN ? TokenType.BRACE_CLOSE : TokenType.BRACKET_CLOSE);
                    testSuitesSource = reconstructText(bs, pos);
                    match(TokenType.SEMI_COLON);
                }
                default -> error("unknown key '" + key + "' inside Service '"
                        + qualifiedName + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);

        return new ServiceDefinition(
                qualifiedName,
                pattern != null ? pattern : "/",
                functionBody != null ? functionBody : "",
                documentation,
                mappingRef,
                runtimeRef,
                testSuitesSource);
    }

    // ============================================================
    // Runtime declaration
    // ============================================================

    /**
     * {@code Runtime qualifiedName { mappings: [...]; connections: [...]; }}.
     * Embedded {@code JsonModelConnection} islands ({@code #{ ... }#}) are
     * captured and parsed via regex (engine parity).
     */
    private RuntimeDefinition parseRuntime() {
        expect(TokenType.RUNTIME);
        String qualifiedName = parseQualifiedName();
        return parseRuntimeBody(qualifiedName);
    }

    /**
     * {@code SingleConnectionRuntime qualifiedName { ... }} &mdash; engine's
     * implementation skips the body and returns an empty runtime. We match
     * that here pending real grammar support.
     */
    private RuntimeDefinition parseSingleConnectionRuntime() {
        expect(TokenType.SINGLE_CONNECTION_RUNTIME);
        String qualifiedName = parseQualifiedName();
        // skipBalancedContent consumes the opening '{' itself, then closing '}'.
        skipBalancedContent(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, List.of(), Map.of(), List.of());
    }

    private RuntimeDefinition parseRuntimeBody(String qualifiedName) {
        expect(TokenType.BRACE_OPEN);
        List<String> mappings = new ArrayList<>();
        // HashMap (not LinkedHashMap) — bindings are looked up by store name;
        // iteration order is never observed semantically. Saves the per-put
        // linked-list bookkeeping cost.
        Map<String, String> connectionBindings = new HashMap<>();
        List<JsonModelConnection> jsonConnections = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            switch (key) {
                case "mappings" -> {
                    expect(TokenType.BRACKET_OPEN);
                    if (peek() != TokenType.BRACKET_CLOSE) {
                        mappings.add(parseQualifiedName());
                        while (match(TokenType.COMMA)) mappings.add(parseQualifiedName());
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.SEMI_COLON);
                }
                case "connections" -> parseRuntimeConnections(connectionBindings, jsonConnections);
                default -> error("unknown key '" + key + "' inside Runtime '"
                        + qualifiedName + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, mappings, connectionBindings, jsonConnections);
    }

    private void parseRuntimeConnections(Map<String, String> bindings,
                                         List<JsonModelConnection> jsonConnections) {
        expect(TokenType.BRACKET_OPEN);
        while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
            String storeName = parseQualifiedName();
            expect(TokenType.COLON);
            expect(TokenType.BRACKET_OPEN);
            while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                parseIdentifier();      // tag (e.g. "id", "json") — engine doesn't keep this
                expect(TokenType.COLON);
                if (peek() == TokenType.ISLAND_OPEN) {
                    advance(); // consume ISLAND_OPEN ('#{' or '#name{')
                    int embStart = pos;
                    while (peek() != TokenType.ISLAND_END && !atEnd()) advance();
                    String embText = reconstructText(embStart, pos);
                    if (peek() == TokenType.ISLAND_END) advance();
                    JsonModelConnection jmc = parseEmbeddedJsonModelConnection(embText);
                    if (jmc != null) jsonConnections.add(jmc);
                } else {
                    bindings.put(storeName, parseQualifiedName());
                }
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACKET_CLOSE);
            match(TokenType.COMMA);
        }
        expect(TokenType.BRACKET_CLOSE);
        match(TokenType.SEMI_COLON);
    }

    // Compiled once, used by parseEmbeddedJsonModelConnection.
    private static final Pattern JMC_CLASS_PATTERN =
            Pattern.compile("class\\s*:\\s*([\\w:]+)\\s*;");
    private static final Pattern JMC_URL_PATTERN =
            Pattern.compile("url\\s*:\\s*'([^']*)'\\s*;");

    /**
     * Parse an embedded {@code JsonModelConnection { class: ...; url: '...'; }}
     * block via regex against its raw source text. Returns {@code null} if the
     * block doesn't match (engine parity).
     */
    private JsonModelConnection parseEmbeddedJsonModelConnection(String raw) {
        raw = raw.trim();
        if (!raw.startsWith("JsonModelConnection")) return null;
        Matcher cm = JMC_CLASS_PATTERN.matcher(raw);
        Matcher um = JMC_URL_PATTERN.matcher(raw);
        if (!cm.find() || !um.find()) return null;
        return new JsonModelConnection(cm.group(1), um.group(1));
    }

    // ============================================================
    // RelationalDatabaseConnection
    // ============================================================

    /**
     * {@code RelationalDatabaseConnection qualifiedName { store: ...; type: ...;
     * specification: ...; auth: ...; }}.
     * Unknown keys throw (strict mode; D-2).
     */
    private ConnectionDefinition parseConnection() {
        expect(TokenType.RELATIONAL_DATABASE_CONNECTION);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String storeName = null;
        ConnectionDefinition.DatabaseType dbType = null;
        ConnectionSpecification specification = null;
        AuthenticationSpec authentication = null;

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            switch (key) {
                case "store" -> {
                    storeName = parseQualifiedName();
                    expect(TokenType.SEMI_COLON);
                }
                case "type" -> {
                    String typeStr = parseIdentifier();
                    try {
                        dbType = ConnectionDefinition.DatabaseType.valueOf(typeStr);
                    } catch (IllegalArgumentException ex) {
                        error("unknown database type '" + typeStr + "' (expected one of "
                                + java.util.Arrays.toString(ConnectionDefinition.DatabaseType.values()) + ")");
                    }
                    expect(TokenType.SEMI_COLON);
                }
                case "specification" -> {
                    String specType = parseIdentifier();
                    expect(TokenType.BRACE_OPEN);
                    Map<String, String> props = parseKeyValueBlock();
                    expect(TokenType.BRACE_CLOSE);
                    specification = switch (specType) {
                        case "InMemory" -> new ConnectionSpecification.InMemory();
                        case "LocalFile" -> new ConnectionSpecification.LocalFile(props.get("path"));
                        case "Static" -> new ConnectionSpecification.StaticDatasource(
                                props.get("host"),
                                props.containsKey("port") ? Integer.parseInt(props.get("port")) : 0,
                                props.get("database"));
                        default -> {
                            error("unknown specification flavor '" + specType
                                    + "' (expected InMemory / LocalFile / Static)");
                            yield null; // unreachable
                        }
                    };
                    expect(TokenType.SEMI_COLON);
                }
                case "auth" -> {
                    String authType = parseIdentifier();
                    Map<String, String> props = Map.of();
                    if (match(TokenType.BRACE_OPEN)) {
                        props = parseKeyValueBlock();
                        expect(TokenType.BRACE_CLOSE);
                    }
                    authentication = switch (authType) {
                        case "NoAuth" -> new AuthenticationSpec.NoAuth();
                        case "UsernamePassword" -> new AuthenticationSpec.UsernamePassword(
                                props.get("username"), props.get("passwordVaultRef"));
                        default -> {
                            error("unknown auth flavor '" + authType
                                    + "' (expected NoAuth / UsernamePassword)");
                            yield null; // unreachable
                        }
                    };
                    expect(TokenType.SEMI_COLON);
                }
                default -> error("unknown key '" + key
                        + "' inside RelationalDatabaseConnection '" + qualifiedName
                        + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);

        if (dbType == null) error("RelationalDatabaseConnection '" + qualifiedName + "' missing required 'type:' key");
        if (specification == null) error("RelationalDatabaseConnection '" + qualifiedName + "' missing required 'specification:' key");
        if (authentication == null) error("RelationalDatabaseConnection '" + qualifiedName + "' missing required 'auth:' key");

        return new ConnectionDefinition(qualifiedName, storeName, dbType, specification, authentication);
    }

    // ============================================================
    // Shared key/value-block + balanced-content helpers
    // ============================================================

    /**
     * Parse a sequence of {@code key: value;} pairs inside an already-opened
     * brace block, stopping at (but not consuming) the closing {@code }}.
     * Values are either {@code STRING}, {@code INTEGER}, identifiers, or
     * qualified names; all are stored as their raw text.
     */
    private Map<String, String> parseKeyValueBlock() {
        // HashMap: connection properties are looked up by key, never iterated
        // positionally; JDBC URL construction doesn't depend on order either.
        Map<String, String> props = new HashMap<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            String value;
            if (peek() == TokenType.STRING) {
                value = unquoteString(consume(TokenType.STRING));
            } else if (peek() == TokenType.INTEGER) {
                value = consume(TokenType.INTEGER);
            } else {
                value = parseQualifiedName();
            }
            props.put(key, value);
            expect(TokenType.SEMI_COLON);
        }
        return props;
    }

    /**
     * Skip over a balanced {@code open..close} region. The opener at the
     * current position is consumed; advance until the matching close is
     * consumed too. Handles arbitrary nesting of the same open/close pair.
     */
    private void skipBalancedContent(TokenType open, TokenType close) {
        expect(open);
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == open) depth++;
            else if (t == close) depth--;
            advance();
            if (depth == 0) return;
        }
    }

    /** Strip the leading and trailing {@code '} from a {@code STRING} token's raw text. */
    private static String unquoteString(String raw) {
        if (raw == null || raw.length() < 2) return raw;
        if (raw.charAt(0) == '\'' && raw.charAt(raw.length() - 1) == '\'') {
            return raw.substring(1, raw.length() - 1);
        }
        return raw;
    }

    // ============================================================
    // Database declaration
    // ============================================================

    /**
     * {@code Database <<stereos>> {tags} qualifiedName ( includes? (Schema | Table | View | Join | Filter | MultiGrainFilter)* )}.
     */
    private DatabaseDefinition parseDatabase() {
        expect(TokenType.DATABASE);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        String dbScope = qualifiedName;
        expect(TokenType.PAREN_OPEN);

        List<String> includes = new ArrayList<>();
        List<DatabaseDefinition.SchemaDefinition> schemas = new ArrayList<>();
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> filters = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> multiGrainFilters = new ArrayList<>();

        while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
            if (textEquals("include")) {
                advance();
                includes.add(parseQualifiedName());
            } else if (textEquals("Schema")) {
                schemas.add(parseDbSchema(dbScope, tables, views));
            } else if (textEquals("Table")) {
                tables.add(parseDbTable());
            } else if (textEquals("View")) {
                views.add(parseDbView(dbScope));
            } else if (textEquals("Join")) {
                joins.add(parseDbJoin(dbScope));
            } else if (textEquals("Filter")) {
                filters.add(parseDbFilter(dbScope));
            } else if (textEquals("MultiGrainFilter")) {
                multiGrainFilters.add(parseDbMultiGrainFilter(dbScope));
            } else {
                error("unknown Database element '" + safeText()
                        + "' in '" + qualifiedName + "' (expected include / Schema / Table / View / Join / Filter / MultiGrainFilter)");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition(qualifiedName, includes, schemas, tables, views,
                joins, filters, multiGrainFilters);
    }

    /**
     * {@code Schema name ( (Table | View)* )}. Schema tables and views are
     * also mirrored into the database-level flat lists for callers that want
     * a flat view of all tables &mdash; matches engine.
     */
    private DatabaseDefinition.SchemaDefinition parseDbSchema(
            String dbScope,
            List<DatabaseDefinition.TableDefinition> flatTables,
            List<DatabaseDefinition.ViewDefinition> flatViews) {
        advance(); // "Schema"
        String schemaName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        List<DatabaseDefinition.TableDefinition> schemaTables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> schemaViews = new ArrayList<>();
        while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
            if (textEquals("Table")) {
                DatabaseDefinition.TableDefinition t = parseDbTable();
                schemaTables.add(t);
                flatTables.add(t);
            } else if (textEquals("View")) {
                DatabaseDefinition.ViewDefinition v = parseDbView(dbScope);
                schemaViews.add(v);
                flatViews.add(v);
            } else {
                error("unknown Schema element '" + safeText()
                        + "' inside '" + schemaName + "' (expected Table or View)");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.SchemaDefinition(schemaName, schemaTables, schemaViews);
    }

    private DatabaseDefinition.TableDefinition parseDbTable() {
        advance(); // "Table"
        String tableName = parseRelationalIdentifier();
        expect(TokenType.PAREN_OPEN);
        List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            columns.add(parseColumnDefinition());
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) break;
                columns.add(parseColumnDefinition());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.TableDefinition(tableName, columns);
    }

    private DatabaseDefinition.ColumnDefinition parseColumnDefinition() {
        String columnName = parseRelationalIdentifier();
        String dataType = parseIdentifier();
        // Optional size / precision: VARCHAR(100), DECIMAL(10,2)
        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            String size = consume(TokenType.INTEGER);
            dataType = dataType + "(" + size;
            if (match(TokenType.COMMA)) {
                dataType += "," + consume(TokenType.INTEGER);
            }
            expect(TokenType.PAREN_CLOSE);
            dataType += ")";
        }
        boolean primaryKey = match(TokenType.PRIMARY_KEY);
        // PRIMARY KEY implies NOT NULL (engine parity).
        boolean notNull = primaryKey || match(TokenType.NOT_NULL);
        return new DatabaseDefinition.ColumnDefinition(columnName, dataType, primaryKey, notNull);
    }

    private DatabaseDefinition.JoinDefinition parseDbJoin(String dbScope) {
        advance(); // "Join"
        String joinName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation operation = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.JoinDefinition(joinName, operation);
    }

    private DatabaseDefinition.FilterDefinition parseDbFilter(String dbScope) {
        advance(); // "Filter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.FilterDefinition parseDbMultiGrainFilter(String dbScope) {
        advance(); // "MultiGrainFilter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.ViewDefinition parseDbView(String dbScope) {
        advance(); // "View"
        String viewName = parseRelationalIdentifier();
        expect(TokenType.PAREN_OPEN);

        FilterMapping filter = null;
        List<RelationalOperation> groupBy = new ArrayList<>();
        boolean distinct = false;

        while (peek() == TokenType.FILTER_CMD
                || peek() == TokenType.GROUP_BY_CMD
                || peek() == TokenType.DISTINCT_CMD) {
            TokenType cmd = peek();
            advance();
            if (cmd == TokenType.FILTER_CMD) {
                filter = parseViewFilterClause(dbScope);
            } else if (cmd == TokenType.GROUP_BY_CMD) {
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseDbOperation(dbScope));
                    while (match(TokenType.COMMA)) groupBy.add(parseDbOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
            } else { // DISTINCT_CMD
                distinct = true;
            }
        }

        List<DatabaseDefinition.ViewDefinition.ViewColumnMapping> columnMappings = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            columnMappings.add(parseViewColumnMapping(dbScope));
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) break;
                columnMappings.add(parseViewColumnMapping(dbScope));
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.ViewDefinition(viewName, filter, groupBy, distinct, columnMappings);
    }

    /**
     * Parse the body of a view's {@code ~filter ...} clause. Caller has
     * already consumed the {@code ~filter} command token. Four forms:
     * <pre>
     *   ~filter F                                  → Direct(Local("F"))
     *   ~filter [DB] F                             → Direct(Cross("DB", "F"))
     *   ~filter [DB1] @J1 > @J2 | F                → JoinMediated("DB1", [..], Local("F"))
     *   ~filter [DB1] @J1 > @J2 | [DB2] F          → JoinMediated("DB1", [..], Cross("DB2", "F"))
     * </pre>
     *
     * <p>The fork between Direct and JoinMediated happens after seeing the
     * first {@code [DB]} (or the absence thereof): if the next token is
     * {@code @}, we're on the join-mediated path; otherwise it's a direct
     * filter reference.
     */
    private FilterMapping parseViewFilterClause(String dbScope) {
        String firstDb = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            firstDb = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        if (peek() == TokenType.AT) {
            // Join-mediated form: firstDb is the source db (required by grammar)
            if (firstDb == null) {
                error("Join-mediated filter must start with a [DB] qualifier");
            }
            List<JoinChainElement> joins = parseJoinChain(firstDb);
            expect(TokenType.PIPE);
            FilterPointer target = parseFilterPointer();
            return new FilterMapping.JoinMediated(firstDb, joins, target);
        }
        // Direct form: firstDb (possibly null) qualifies the filter name itself.
        String name = parseIdentifier();
        FilterPointer pointer = firstDb == null
                ? new FilterPointer.Local(name)
                : new FilterPointer.Cross(firstDb, name);
        return new FilterMapping.Direct(pointer);
    }

    /**
     * Parse a {@code [DB]?} filter-name pair into a {@link FilterPointer}.
     * Used for the target side of a join-mediated filter (post-pipe).
     */
    private FilterPointer parseFilterPointer() {
        String db = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            db = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String name = parseIdentifier();
        return db == null ? new FilterPointer.Local(name) : new FilterPointer.Cross(db, name);
    }

    /**
     * Parse {@code @J1 > @J2 > ... > @Jn} into a list of {@link JoinChainElement}.
     * Caller has consumed any leading {@code [DB]}. The {@code initialDb}
     * argument seeds the chain's source database when no per-hop {@code [DB]}
     * is specified.
     */
    private List<JoinChainElement> parseJoinChain(String initialDb) {
        List<JoinChainElement> chain = new ArrayList<>();
        expect(TokenType.AT);
        chain.add(new JoinChainElement(parseIdentifier(), null, initialDb, false));
        while (peek() == TokenType.GREATER_THAN) {
            advance();
            String hopDb = initialDb;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                hopDb = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.AT);
            chain.add(new JoinChainElement(parseIdentifier(), null, hopDb, false));
        }
        return chain;
    }

    private DatabaseDefinition.ViewDefinition.ViewColumnMapping parseViewColumnMapping(String dbScope) {
        String colName = parseIdentifier();
        String targetSetId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            targetSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);
        RelationalOperation expression = parseDbOperation(dbScope);
        boolean pk = match(TokenType.PRIMARY_KEY);
        return new DatabaseDefinition.ViewDefinition.ViewColumnMapping(
                colName, targetSetId, expression, pk);
    }

    // ============================================================
    // Relational expression sub-grammar
    // ============================================================

    /** {@code dbOperation: dbJoinOperation | dbBooleanOperation}. */
    private RelationalOperation parseDbOperation(String dbScope) {
        if (peek() == TokenType.AT
                || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            return parseDbJoinOperation(dbScope);
        }
        return parseDbBooleanOperation(dbScope);
    }

    /** {@code dbBooleanOperation: dbAtomicOperation (("and" | "or") dbOperation)?}. */
    private RelationalOperation parseDbBooleanOperation(String dbScope) {
        RelationalOperation left = parseDbAtomicOperation(dbScope);
        if (!atEnd() && IDENTIFIER_TOKENS.contains(peek())
                && (textEquals("and") || textEquals("or"))) {
            String op = text();
            advance();
            RelationalOperation right = parseDbOperation(dbScope);
            return new RelationalOperation.BooleanOp(left, op, right);
        }
        return left;
    }

    /**
     * {@code dbAtomicOperation: group | functionCall | columnRef | joinNav
     *                         | targetColumn | literal
     *                         (comparison | IS_NULL | IS_NOT_NULL)?}.
     */
    private RelationalOperation parseDbAtomicOperation(String dbScope) {
        RelationalOperation expr;

        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            expr = new RelationalOperation.Group(parseDbOperation(dbScope));
            expect(TokenType.PAREN_CLOSE);
        } else if (peek() == TokenType.AT
                || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            expr = parseDbJoinOperation(dbScope);
        } else if (peek() == TokenType.STRING) {
            expr = RelationalOperation.Literal.string(unquoteString(text()));
            advance();
        } else if (peek() == TokenType.INTEGER) {
            expr = RelationalOperation.Literal.integer(Long.parseLong(text()));
            advance();
        } else if (peek() == TokenType.FLOAT || peek() == TokenType.DECIMAL) {
            expr = RelationalOperation.Literal.decimal(Double.parseDouble(text()));
            advance();
        } else if (peek() == TokenType.TARGET) {
            advance();
            expect(TokenType.DOT);
            expr = new RelationalOperation.TargetColumnRef(parseRelationalIdentifier());
        } else {
            String firstId = parseRelationalIdentifier();
            if (peek() == TokenType.PAREN_OPEN && !firstId.contains(".")) {
                advance(); // '('
                List<RelationalOperation> args = new ArrayList<>();
                if (peek() != TokenType.PAREN_CLOSE) {
                    args.add(parseDbFunctionArg(dbScope));
                    while (match(TokenType.COMMA)) args.add(parseDbFunctionArg(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                expr = new RelationalOperation.FunctionCall(firstId, args);
            } else if (peek() == TokenType.DOT) {
                advance();
                String second = parseRelationalIdentifier();
                if (peek() == TokenType.DOT) {
                    advance();
                    String third = parseRelationalIdentifier();
                    expr = new RelationalOperation.ColumnRef(null, firstId + "." + second, third);
                } else {
                    expr = new RelationalOperation.ColumnRef(null, firstId, second);
                }
            } else {
                // Bare identifier in a Database-context expression
                // (Filter / Join / MultiGrainFilter / view filter). FINOS
                // engine rejects this at parse time with this exact message
                // (RelationalParseTreeWalker.generateTableAlias). Match that
                // behavior: no implicit-table column refs in the AST.
                //
                // Mapping-context bare identifiers (B.4b) are handled by
                // resolving them eagerly to the class-mapping's main table at
                // parse time, also matching engine.
                error("Missing table or alias for column '" + firstId + "'");
                throw new IllegalStateException("unreachable");
            }
        }

        // Optional right side: comparison or null-test.
        if (!atEnd()) {
            TokenType next = peek();
            if (next == TokenType.EQUAL || next == TokenType.TEST_EQUAL
                    || next == TokenType.TEST_NOT_EQUAL || next == TokenType.LESS_THAN
                    || next == TokenType.GREATER_THAN || next == TokenType.LESS_OR_EQUAL
                    || next == TokenType.GREATER_OR_EQUAL || next == TokenType.NOT_EQUAL) {
                String op = text();
                advance();
                RelationalOperation right = parseDbAtomicOperation(dbScope);
                expr = new RelationalOperation.Comparison(expr, op, right);
            } else if (next == TokenType.IS_NULL) {
                advance();
                expr = new RelationalOperation.IsNull(expr);
            } else if (next == TokenType.IS_NOT_NULL) {
                advance();
                expr = new RelationalOperation.IsNotNull(expr);
            }
        }
        return expr;
    }

    private RelationalOperation parseDbFunctionArg(String dbScope) {
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            List<RelationalOperation> elements = new ArrayList<>();
            if (peek() != TokenType.BRACKET_CLOSE) {
                elements.add(parseDbFunctionArg(dbScope));
                while (match(TokenType.COMMA)) elements.add(parseDbFunctionArg(dbScope));
            }
            expect(TokenType.BRACKET_CLOSE);
            return new RelationalOperation.ArrayLiteral(elements);
        }
        return parseDbOperation(dbScope);
    }

    /**
     * {@code dbJoinOperation: ([DB])? @joinName ( '>' ( '(' joinType ')' )? ([DB])? @joinName )* ( '|' dbBooleanOperation )?}.
     */
    private RelationalOperation parseDbJoinOperation(String dbScope) {
        String dbName = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            dbName = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String db = dbName != null ? dbName : dbScope;

        List<JoinChainElement> chain = new ArrayList<>();
        expect(TokenType.AT);
        chain.add(new JoinChainElement(parseIdentifier(), null, db, false));
        while (match(TokenType.GREATER_THAN)) {
            String joinType = null;
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                joinType = parseIdentifier();
                expect(TokenType.PAREN_CLOSE);
            }
            String hopDb = db;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                hopDb = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.AT);
            chain.add(new JoinChainElement(parseIdentifier(), joinType, hopDb, false));
        }

        RelationalOperation terminal = null;
        if (match(TokenType.PIPE)) {
            terminal = parseDbBooleanOperation(dbScope);
        }
        return new RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    /**
     * Look ahead past a {@code [...]} bracket region and report whether the
     * very next token is {@code @} (i.e. this bracket region is a database
     * qualifier on a join chain, not a generic indexing operator).
     */
    private boolean lookAheadIsJoin() {
        int saved = pos;
        advance(); // skip '['
        while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) advance();
        if (!atEnd()) advance(); // skip ']'
        boolean isJoin = !atEnd() && peek() == TokenType.AT;
        pos = saved;
        return isJoin;
    }

    /**
     * Parse a relational identifier &mdash; either a quoted identifier
     * {@code "T_PERSON"} (returned without the surrounding quotes) or a bare
     * {@link #parseIdentifier()}. Quoted identifiers carry case-sensitive
     * names in source-dialect-style relational grammars.
     */
    private String parseRelationalIdentifier() {
        if (peek() == TokenType.QUOTED_STRING) {
            String quoted = text();
            advance();
            return quoted.length() >= 2 ? quoted.substring(1, quoted.length() - 1) : quoted;
        }
        return parseIdentifier();
    }

    // ============================================================
    // Source-text reconstruction (for lazy expression bodies)
    // ============================================================

    /**
     * Reconstruct the verbatim source text spanning tokens
     * {@code [startToken, endToken)}. Returns empty string if the range is
     * empty. Used to capture derived-property bodies and constraint
     * expressions for lazy compilation in Phase G.
     */
    private String reconstructText(int startToken, int endToken) {
        if (startToken >= endToken) return "";
        int charStart = tokens.start(startToken);
        int charEnd = tokens.end(endToken - 1);
        return tokens.source().substring(charStart, charEnd);
    }

    // ============================================================
    // Property declaration (regular)
    // ============================================================

    private ClassDefinition.PropertyDefinition parseProperty() {
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String name = parseIdentifier();
        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        expect(TokenType.SEMI_COLON);
        return new ClassDefinition.PropertyDefinition(
                name, type, mult[0],
                mult[1] == Integer.MIN_VALUE ? null : Integer.valueOf(mult[1]),
                stereotypes, taggedValues);
    }

    // ============================================================
    // Shared helpers (engine-parity)
    // ============================================================

    /** Parse a Pure identifier &mdash; accepts {@code VALID_STRING} or any keyword-as-identifier. */
    private String parseIdentifier() {
        TokenType t = peek();
        if (IDENTIFIER_TOKENS.contains(t)) {
            String id = text();
            advance();
            return id;
        }
        error("expected identifier but found " + t + " ('" + safeText() + "')");
        return null; // unreachable
    }

    /** Parse {@code identifier (:: identifier)*}; returns the joined {@code "a::b::c"}. */
    private String parseQualifiedName() {
        StringBuilder sb = new StringBuilder();
        sb.append(parseIdentifier());
        while (peek() == TokenType.PATH_SEPARATOR) {
            advance();
            sb.append("::");
            sb.append(parseIdentifier());
        }
        return sb.toString();
    }

    /** Parse a (possibly qualified) type reference &mdash; just a qualified name in B.1. */
    private String parseType() {
        return parseQualifiedName();
    }

    /**
     * Parse a multiplicity annotation: {@code [1]}, {@code [*]},
     * {@code [0..1]}, {@code [1..*]}. Returns {@code [lower, upper]} with
     * {@code upper == Integer.MIN_VALUE} sentinel for unbounded {@code *}.
     */
    private int[] parseMultiplicity() {
        expect(TokenType.BRACKET_OPEN);
        int lower;
        int upper;
        if (match(TokenType.STAR)) {
            lower = 0;
            upper = Integer.MIN_VALUE; // sentinel: unbounded
        } else {
            lower = parseInt(consume(TokenType.INTEGER));
            if (match(TokenType.DOT_DOT)) {
                if (match(TokenType.STAR)) {
                    upper = Integer.MIN_VALUE;
                } else {
                    upper = parseInt(consume(TokenType.INTEGER));
                }
            } else {
                upper = lower;
            }
        }
        expect(TokenType.BRACKET_CLOSE);
        return new int[]{lower, upper};
    }

    private static int parseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new ParseException("invalid integer literal '" + s + "'");
        }
    }

    /** {@code <<profile.stereotype, ...>>}; returns empty list if absent. */
    private List<StereotypeApplication> parseStereotypes() {
        if (peek() != TokenType.LESS_THAN || peek(1) != TokenType.LESS_THAN) {
            return List.of();
        }
        advance();
        advance();
        List<StereotypeApplication> result = new ArrayList<>();
        result.add(parseStereotype());
        while (match(TokenType.COMMA)) {
            result.add(parseStereotype());
        }
        expect(TokenType.GREATER_THAN);
        expect(TokenType.GREATER_THAN);
        return result;
    }

    private StereotypeApplication parseStereotype() {
        String profile = parseQualifiedName();
        expect(TokenType.DOT);
        String name = parseIdentifier();
        return new StereotypeApplication(profile, name);
    }

    /** {@code { profile.tag = 'value', ... }}; returns empty list if not a tag block. */
    private List<TaggedValue> parseTaggedValues() {
        if (peek() != TokenType.BRACE_OPEN) return List.of();
        if (!isTaggedValueStart()) return List.of();

        advance(); // skip {
        List<TaggedValue> result = new ArrayList<>();
        result.add(parseTaggedValue());
        while (match(TokenType.COMMA)) {
            result.add(parseTaggedValue());
        }
        expect(TokenType.BRACE_CLOSE);
        return result;
    }

    /** Lookahead for {@code { QN . id = STRING ...}} pattern; restores cursor on miss. */
    private boolean isTaggedValueStart() {
        int saved = pos;
        if (peek() != TokenType.BRACE_OPEN) return false;
        pos++;
        if (!IDENTIFIER_TOKENS.contains(peek())) { pos = saved; return false; }
        while (IDENTIFIER_TOKENS.contains(peek())) {
            pos++;
            if (pos >= count) { pos = saved; return false; }
            if (peek() == TokenType.PATH_SEPARATOR) pos++;
            else break;
        }
        if (pos >= count || peek() != TokenType.DOT) { pos = saved; return false; }
        pos++;
        if (pos >= count || !IDENTIFIER_TOKENS.contains(peek())) { pos = saved; return false; }
        pos++;
        if (pos >= count || peek() != TokenType.EQUAL) { pos = saved; return false; }
        pos = saved;
        return true;
    }

    private TaggedValue parseTaggedValue() {
        String profile = parseQualifiedName();
        expect(TokenType.DOT);
        String tag = parseIdentifier();
        expect(TokenType.EQUAL);
        String rawValue = consume(TokenType.STRING);
        String value = rawValue;
        if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
            value = value.substring(1, value.length() - 1);
        }
        return new TaggedValue(profile, tag, value);
    }

    // ============================================================
    // Token cursor
    // ============================================================

    private TokenType peek() {
        return pos < count ? tokens.type(pos) : TokenType.EOF;
    }

    private TokenType peek(int offset) {
        int idx = pos + offset;
        return idx < count ? tokens.type(idx) : TokenType.EOF;
    }

    private String text() {
        return tokens.text(pos);
    }

    private void advance() {
        pos++;
    }

    private boolean atEnd() {
        return pos >= count;
    }

    private void expect(TokenType type) {
        if (peek() != type) {
            error("expected " + type + " but found " + peek() + " ('" + safeText() + "')");
        }
        advance();
    }

    private boolean match(TokenType type) {
        if (peek() == type) { advance(); return true; }
        return false;
    }

    private String consume(TokenType type) {
        if (peek() != type) {
            error("expected " + type + " but found " + peek() + " ('" + safeText() + "')");
        }
        String t = text();
        advance();
        return t;
    }

    private String safeText() {
        return pos < count ? text() : "<EOF>";
    }

    /** Whether the current token's source text equals {@code s} exactly (case-sensitive). */
    private boolean textEquals(String s) {
        return pos < count && s.equals(tokens.text(pos));
    }

    private void error(String message) {
        int charPos;
        if (pos < count) {
            charPos = tokens.start(pos);
        } else if (count > 0) {
            charPos = tokens.end(count - 1);
        } else {
            throw new ParseException(message);
        }
        int line = 1, col = 0;
        String src = tokens.source();
        for (int i = 0; i < charPos && i < src.length(); i++) {
            if (src.charAt(i) == '\n') { line++; col = 0; }
            else col++;
        }
        throw new ParseException(message, line, col);
    }
}
