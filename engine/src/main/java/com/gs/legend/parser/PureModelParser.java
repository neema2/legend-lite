package com.gs.legend.parser;

import com.gs.legend.antlr.PackageableElementBuilder;
import com.gs.legend.compiler.Mult;
import com.gs.legend.compiler.PType;
import com.gs.legend.model.def.*;
import com.gs.legend.model.def.AssociationDefinition.AssociationEndDefinition;
import com.gs.legend.model.def.ClassDefinition.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Hand-rolled recursive descent parser for Pure model definitions.
 *
 * Reads tokens from {@link PureLexer2} and builds domain objects directly —
 * no parse tree, no visitor, no ANTLR.
 *
 * Entry point: {@link #parseDefinition()} — replaces
 * {@link PackageableElementBuilder#extractAllDefinitionsWithImports}.
 */
public final class PureModelParser {

    private final PureLexer2 lexer;
    private final int tokenCount;
    private int pos; // current token index

    // Identifier tokens: keywords that can also be used as identifiers
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

    public PureModelParser(PureLexer2 lexer) {
        this.lexer = lexer;
        this.tokenCount = lexer.tokenCount();
        this.pos = 0;
    }

    // ==================== Token Access ====================

    private TokenType peek() {
        return pos < tokenCount ? lexer.tokenType(pos) : TokenType.EOF;
    }

    private TokenType peek(int offset) {
        int idx = pos + offset;
        return idx < tokenCount ? lexer.tokenType(idx) : TokenType.EOF;
    }

    private String text() {
        return lexer.tokenText(pos);
    }

    /** Zero-allocation string comparison against current token. */
    private boolean textEquals(String expected) {
        return pos < tokenCount && lexer.tokenEquals(pos, expected);
    }


    private void advance() {
        pos++;
    }

    private boolean atEnd() {
        return pos >= tokenCount;
    }

    private void expect(TokenType type) {
        if (peek() != type) {
            error("missing " + type + " but found " + peek() + " ('" + safeText() + "')");
        }
        advance();
    }

    private boolean match(TokenType type) {
        if (peek() == type) {
            advance();
            return true;
        }
        return false;
    }

    private String consume(TokenType type) {
        if (peek() != type) {
            error("missing " + type + " but found " + peek() + " ('" + safeText() + "')");
        }
        String t = text();
        advance();
        return t;
    }

    private String safeText() {
        return pos < tokenCount ? text() : "<EOF>";
    }

    private void error(String message) {
        if (pos < tokenCount) {
            int charPos = lexer.tokenStart(pos);
            int line = 1, col = 0;
            String src = lexer.source();
            for (int i = 0; i < charPos && i < src.length(); i++) {
                if (src.charAt(i) == '\n') { line++; col = 0; }
                else col++;
            }
            throw new PureParseException(message, line, col);
        }
        // EOF — report last token position
        if (tokenCount > 0) {
            int charPos = lexer.tokenEnd(tokenCount - 1);
            int line = 1, col = 0;
            String src = lexer.source();
            for (int i = 0; i < charPos && i < src.length(); i++) {
                if (src.charAt(i) == '\n') { line++; col = 0; }
                else col++;
            }
            throw new PureParseException(message, line, col);
        }
        throw new PureParseException(message);
    }

    // ==================== Top-Level Entry Point ====================

    /**
     * Parse a full Pure source — imports + element definitions.
     * Replaces {@link PackageableElementBuilder#extractAllDefinitionsWithImports}.
     */
    public PackageableElementBuilder.ParseResult parseDefinition() {
        List<PackageableElement> definitions = new ArrayList<>();
        ImportScope imports = new ImportScope();

        while (!atEnd()) {
            TokenType t = peek();
            switch (t) {
                case IMPORT -> {
                    String importPath = parseImportStatement();
                    imports.addImport(importPath);
                }
                case CLASS -> definitions.add(parseClassDefinition());
                case ENUM -> definitions.add(parseEnumDefinition());
                case PROFILE -> definitions.add(parseProfile());
                case ASSOCIATION -> definitions.add(parseAssociation());
                case FUNCTION -> definitions.add(parseFunctionDefinition());
                case MAPPING -> definitions.add(parseMapping());
                case DATABASE -> definitions.add(parseDatabase());
                case RELATIONAL_DATABASE_CONNECTION -> definitions.add(parseConnection());
                case RUNTIME -> definitions.add(parseRuntime());
                case SINGLE_CONNECTION_RUNTIME -> definitions.add(parseSingleConnectionRuntime());
                case SERVICE -> definitions.add(parseServiceDefinition());
                default -> error("Unknown definition keyword: " + t + " ('" + safeText() + "')");
            }
        }

        return new PackageableElementBuilder.ParseResult(definitions, imports);
    }

    // ==================== Import ====================

    /**
     * import packagePath :: * ;
     */
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

    // ==================== Shared Helpers ====================

    /**
     * Parse an identifier — accepts VALID_STRING or any keyword-as-identifier token.
     */
    private String parseIdentifier() {
        TokenType t = peek();
        if (IDENTIFIER_TOKENS.contains(t)) {
            String id = text();
            advance();
            return id;
        }
        error("Expected identifier but found " + t + " ('" + safeText() + "')");
        return null; // unreachable
    }

    /**
     * Parse a qualified name: identifier (:: identifier)*
     * Returns the concatenated text, e.g. "test::model::Person"
     */
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

    /**
     * Parse multiplicity: [ multContent ]
     * Returns int[2] where [1] is -1 for unbounded (*)
     */
    private int[] parseMultiplicity() {
        expect(TokenType.BRACKET_OPEN);
        int lower;
        int upper;
        if (match(TokenType.STAR)) {
            lower = 0;
            upper = -1;
        } else {
            lower = Integer.parseInt(consume(TokenType.INTEGER));
            if (match(TokenType.DOT_DOT)) {
                if (match(TokenType.STAR)) {
                    upper = -1;
                } else {
                    upper = Integer.parseInt(consume(TokenType.INTEGER));
                }
            } else {
                upper = lower;
            }
        }
        expect(TokenType.BRACKET_CLOSE);
        return new int[]{lower, upper};
    }

    /**
     * Parse a type reference: qualifiedName (with optional generics we skip for now).
     * Returns the raw text of the type.
     */
    private String parseType() {
        int start = pos;
        if (peek() == TokenType.BRACE_OPEN) {
            // Bare function type: {Type[mult]->Type[mult]}
            advance(); // skip {
            int depth = 1;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.BRACE_OPEN) depth++;
                else if (t == TokenType.BRACE_CLOSE) depth--;
                if (depth > 0) advance();
            }
            if (!atEnd()) advance(); // skip final }
        } else {
            parseQualifiedName(); // consume the base type
            // Handle generic types: Type<...>
            if (peek() == TokenType.LESS_THAN) {
                int depth = 1;
                advance(); // skip <
                while (!atEnd() && depth > 0) {
                    TokenType t = peek();
                    if (t == TokenType.LESS_THAN) depth++;
                    else if (t == TokenType.GREATER_THAN) depth--;
                    if (depth > 0) advance();
                }
                if (!atEnd()) advance(); // skip final >
            }
        }
        // Reconstruct text from token span
        return reconstructText(start, pos);
    }

    /**
     * Reconstruct source text from a span of tokens.
     */
    private String reconstructText(int startToken, int endToken) {
        if (startToken >= endToken) return "";
        int charStart = lexer.tokenStart(startToken);
        int charEnd = lexer.tokenEnd(endToken - 1);
        return lexer.source().substring(charStart, charEnd);
    }

    /**
     * Parse stereotypes: << stereotype (, stereotype)* >>
     * stereotype: qualifiedName . identifier
     */
    private List<StereotypeApplication> parseStereotypes() {
        // << is LESS_THAN LESS_THAN
        if (peek() != TokenType.LESS_THAN || peek(1) != TokenType.LESS_THAN) {
            return List.of();
        }
        advance(); advance(); // skip <<

        List<StereotypeApplication> result = new ArrayList<>();
        result.add(parseStereotype());
        while (match(TokenType.COMMA)) {
            result.add(parseStereotype());
        }

        // >> is GREATER_THAN GREATER_THAN
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

    /**
     * Parse tagged values: { taggedValue (, taggedValue)* }
     * taggedValue: qualifiedName . identifier = STRING
     *
     * Only parsed when preceded by stereotypes context or when the pattern
     * matches { QN . id = 'value' }.
     */
    private List<TaggedValue> parseTaggedValues() {
        if (peek() != TokenType.BRACE_OPEN) return List.of();
        // Lookahead: { identifier (:: identifier)* . identifier =
        // We need to check this is a tagged value block, not a class body
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

    private boolean isTaggedValueStart() {
        // Lookahead: { QN . id = STRING }
        // Minimum: { id . id = 'x' , ... }
        int saved = pos;
        if (peek() != TokenType.BRACE_OPEN) return false;
        pos++;
        // Try to read qualifiedName . identifier =
        if (!IDENTIFIER_TOKENS.contains(peek())) { pos = saved; return false; }
        while (IDENTIFIER_TOKENS.contains(peek())) {
            pos++;
            if (pos >= tokenCount) { pos = saved; return false; }
            if (peek() == TokenType.PATH_SEPARATOR) pos++;
            else break;
        }
        if (pos >= tokenCount || peek() != TokenType.DOT) { pos = saved; return false; }
        pos++; // skip .
        if (pos >= tokenCount || !IDENTIFIER_TOKENS.contains(peek())) { pos = saved; return false; }
        pos++; // skip tag name
        if (pos >= tokenCount || peek() != TokenType.EQUAL) { pos = saved; return false; }
        pos = saved;
        return true;
    }

    private TaggedValue parseTaggedValue() {
        String profile = parseQualifiedName();
        expect(TokenType.DOT);
        String tag = parseIdentifier();
        expect(TokenType.EQUAL);
        String rawValue = consume(TokenType.STRING);
        // Strip quotes
        String value = rawValue;
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }
        return new TaggedValue(profile, tag, value);
    }

    /**
     * Parse a property return type: : type [ multiplicity ]
     * Returns String[3]: [type, lowerBound, upperBound]
     */
    private String[] parsePropertyReturnType() {
        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        return new String[]{type, String.valueOf(mult[0]), String.valueOf(mult[1])};
    }

    // ==================== Enum ====================

    /**
     * Enum qualifiedName { value1, value2, ... }
     */
    private EnumDefinition parseEnumDefinition() {
        expect(TokenType.ENUM);
        // stereotypes? taggedValues? — skip for now (same as ANTLR visitor)
        parseStereotypes();
        parseTaggedValues();
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);
        List<String> values = new ArrayList<>();
        if (peek() != TokenType.BRACE_CLOSE) {
            // Each enum value: stereotypes? taggedValues? identifier
            parseStereotypes(); // skip per-value stereotypes
            parseTaggedValues(); // skip per-value tagged values
            values.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                parseStereotypes();
                parseTaggedValues();
                values.add(parseIdentifier());
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return EnumDefinition.of(qualifiedName, values);
    }

    // ==================== Profile ====================

    /**
     * Profile qualifiedName { (stereotypes: [...]; | tags: [...];)* }
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
                error("Expected 'stereotypes' or 'tags' in profile, found: " + peek());
            }
        }

        expect(TokenType.BRACE_CLOSE);
        return new ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    // ==================== Association ====================

    /**
     * Association stereotypes? taggedValues? qualifiedName { property; property; }
     */
    private AssociationDefinition parseAssociation() {
        expect(TokenType.ASSOCIATION);
        parseStereotypes();
        parseTaggedValues();
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<AssociationEndDefinition> ends = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String propName = parseIdentifier();
            String[] ret = parsePropertyReturnType();
            String propType = ret[0];
            int lower = Integer.parseInt(ret[1]);
            int upper = Integer.parseInt(ret[2]);
            expect(TokenType.SEMI_COLON);
            ends.add(new AssociationEndDefinition(propName, propType, lower, upper == -1 ? null : upper));
        }

        expect(TokenType.BRACE_CLOSE);
        if (ends.size() != 2) {
            error("Association must have exactly 2 properties, found: " + ends.size());
        }
        return new AssociationDefinition(qualifiedName, ends.get(0), ends.get(1));
    }

    // ==================== Class ====================

    /**
     * Class stereotypes? taggedValues? qualifiedName (extends type (, type)*)?
     *   constraints? { properties }
     */
    private ClassDefinition parseClassDefinition() {
        expect(TokenType.CLASS);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();

        // Superclasses
        List<String> superClasses = new ArrayList<>();
        if (match(TokenType.EXTENDS)) {
            superClasses.add(parseType());
            while (match(TokenType.COMMA)) {
                superClasses.add(parseType());
            }
        }

        // Constraints before class body
        List<ConstraintDefinition> constraints = new ArrayList<>();
        if (peek() == TokenType.BRACKET_OPEN) {
            constraints = parseConstraints();
        }

        expect(TokenType.BRACE_OPEN);

        List<PropertyDefinition> properties = new ArrayList<>();
        List<DerivedPropertyDefinition> derivedProperties = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            // Derived property starts with identifier() PAREN_OPEN — it's functionName()
            // Simple property starts with identifier COLON
            // We need lookahead to distinguish
            if (isDerivedProperty()) {
                derivedProperties.add(parseDerivedProperty());
            } else {
                properties.add(parseProperty());
            }
        }

        expect(TokenType.BRACE_CLOSE);
        return new ClassDefinition(qualifiedName, superClasses, properties, derivedProperties,
                constraints, stereotypes, taggedValues);
    }

    private boolean isDerivedProperty() {
        // Derived: stereotypes? taggedValues? identifier ( ... )
        // Simple: stereotypes? taggedValues? identifier : Type[mult]
        // Skip past optional stereotypes and tagged values to find the identifier
        int probe = pos;
        // Skip << ... >>
        if (probe < tokenCount && lexer.tokenType(probe) == TokenType.LESS_THAN
                && probe + 1 < tokenCount && lexer.tokenType(probe + 1) == TokenType.LESS_THAN) {
            probe += 2;
            int depth = 2;
            while (probe < tokenCount && depth > 0) {
                if (lexer.tokenType(probe) == TokenType.GREATER_THAN) depth--;
                else if (lexer.tokenType(probe) == TokenType.LESS_THAN) depth++;
                probe++;
            }
        }
        // Skip { ... } tagged values
        if (probe < tokenCount && lexer.tokenType(probe) == TokenType.BRACE_OPEN) {
            probe++;
            int depth = 1;
            while (probe < tokenCount && depth > 0) {
                if (lexer.tokenType(probe) == TokenType.BRACE_OPEN) depth++;
                else if (lexer.tokenType(probe) == TokenType.BRACE_CLOSE) depth--;
                probe++;
            }
        }
        // Now should be at identifier
        if (probe < tokenCount && IDENTIFIER_TOKENS.contains(lexer.tokenType(probe))) {
            probe++;
            // Check what follows the identifier
            if (probe < tokenCount && lexer.tokenType(probe) == TokenType.PAREN_OPEN) {
                return true; // derived property
            }
        }
        return false;
    }

    private PropertyDefinition parseProperty() {
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String propName = parseIdentifier();
        String[] ret = parsePropertyReturnType();
        String propType = ret[0];
        int lower = Integer.parseInt(ret[1]);
        int upper = Integer.parseInt(ret[2]);
        expect(TokenType.SEMI_COLON);
        return new PropertyDefinition(propName, propType, lower, upper == -1 ? null : upper,
                stereotypes, taggedValues);
    }

    private DerivedPropertyDefinition parseDerivedProperty() {
        parseStereotypes();
        parseTaggedValues();
        String propName = parseIdentifier();

        // Parameters: ( paramDef, ... )
        expect(TokenType.PAREN_OPEN);
        // DerivedPropertyDefinition uses ClassDefinition.ParameterDefinition, not FunctionDefinition's
        List<ClassDefinition.ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseDerivedPropertyParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseDerivedPropertyParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        // Body: { expression } before the return type
        String body = "";
        if (peek() == TokenType.BRACE_OPEN) {
            advance(); // skip {
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

        // Return type: : Type[mult]
        String[] ret = parsePropertyReturnType();
        String returnType = ret[0];
        int lower = Integer.parseInt(ret[1]);
        int upper = Integer.parseInt(ret[2]);

        expect(TokenType.SEMI_COLON);

        // DerivedPropertyDefinition(name, params, expression, type, lower, upper)
        return new DerivedPropertyDefinition(propName, params, body, returnType,
                lower, upper == -1 ? null : upper);
    }

    private ClassDefinition.ParameterDefinition parseDerivedPropertyParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        String type = parseType();
        int[] mult = parseMultiplicity();
        return new ClassDefinition.ParameterDefinition(name, type,
                mult[0], mult[1] == -1 ? null : mult[1]);
    }

    private List<ConstraintDefinition> parseConstraints() {
        expect(TokenType.BRACKET_OPEN);
        List<ConstraintDefinition> constraints = new ArrayList<>();
        if (peek() != TokenType.BRACKET_CLOSE) {
            constraints.add(parseConstraint());
            while (match(TokenType.COMMA)) {
                constraints.add(parseConstraint());
            }
        }
        expect(TokenType.BRACKET_CLOSE);
        return constraints;
    }

    private ConstraintDefinition parseConstraint() {
        // Simple constraint: constraintId? combinedExpression
        // constraintId: identifier COLON (but not property — lookahead for ~)
        String name = "unnamed";

        // Check for name: before constraint body. Tricky because we might see
        // ~owner, ~function, ~message etc after the name:
        if (IDENTIFIER_TOKENS.contains(peek()) && peek(1) == TokenType.COLON) {
            name = parseIdentifier();
            advance(); // skip :
        }

        // Parse constraint expression — skip tilde commands if present
        // For now, scan to comma or bracket close, reconstructing text
        int bodyStart = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE) {
                if (depth == 0) break;
                depth--;
            } else if (t == TokenType.COMMA && depth == 0) break;
            advance();
        }
        String expression = reconstructText(bodyStart, pos);
        return new ConstraintDefinition(name, expression);
    }

    // ==================== Function ====================

    /**
     * function stereotypes? taggedValues? qualifiedName
     *   ( params ) : returnType [mult]
     *   { codeBlock }
     */
    private FunctionDefinition parseFunctionDefinition() {
        expect(TokenType.FUNCTION);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();

        // Type parameters (generic) — skip for now
        // typeAndMultiplicityParameters: LESS_THAN ... GREATER_THAN

        // Function signature: ( params ) : Type [ mult ]
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
        PType parsedReturnType = parsePureType(returnType);
        int[] returnMult = parseMultiplicity();

        // Constraints (optional)
        if (peek() == TokenType.BRACKET_OPEN) {
            parseConstraints(); // discard for now
        }

        // Function body: { codeBlock }
        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) advance();
        }
        // bodyStart..pos is the body content (exclusive of closing brace)
        String body = reconstructText(bodyStart, pos);
        expect(TokenType.BRACE_CLOSE);

        return new FunctionDefinition(qualifiedName, params, returnType,
                returnMult[0], returnMult[1] == -1 ? null : returnMult[1], body,
                stereotypes, taggedValues, null, parsedReturnType);
    }

    private FunctionDefinition.ParameterDefinition parseFunctionParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        String type = parseType();
        PType parsedType = parsePureType(type);
        int[] mult = parseMultiplicity();
        // Extract function type if it's a Function<{...}> parameter
        PType.FunctionType fnType = (parsedType instanceof PType.FunctionType ft) ? ft
                : (parsedType instanceof PType.Parameterized p
                        && "Function".equals(p.rawType())
                        && !p.typeArgs().isEmpty()
                        && p.typeArgs().get(0) instanceof PType.FunctionType ft2) ? ft2
                : null;
        return new FunctionDefinition.ParameterDefinition(name, type,
                mult[0], mult[1] == -1 ? null : mult[1], fnType, parsedType);
    }

    /**
     * Convert a type string to PType.
     * Handles: simple types, Function&lt;{...}&gt;, bare {T[m]->R[m]}, Relation&lt;(...)&gt;.
     */
    private PType parsePureType(String typeText) {
        if (typeText == null || typeText.isEmpty()) return null;

        // Bare function type: {T[m]->R[m]}
        if (typeText.startsWith("{") && typeText.endsWith("}")) {
            return parseFunctionTypeLiteral(typeText.substring(1, typeText.length() - 1));
        }

        // Generic/Parameterized: Name<...>
        int ltIdx = typeText.indexOf('<');
        if (ltIdx > 0 && typeText.endsWith(">")) {
            String rawType = typeText.substring(0, ltIdx);
            String inner = typeText.substring(ltIdx + 1, typeText.length() - 1);

            // Function<{...}> — extract function type from inner braces
            if ("Function".equals(rawType) || "FunctionDefinition".equals(rawType)) {
                if (inner.startsWith("{") && inner.endsWith("}")) {
                    PType fnType = parseFunctionTypeLiteral(inner.substring(1, inner.length() - 1));
                    return new PType.Parameterized(rawType, List.of(fnType));
                }
            }

            // Relation<(col:Type, ...)> — parse inline column spec
            if ("Relation".equals(rawType)) {
                if (inner.startsWith("(") && inner.endsWith(")")) {
                    String colSpec = inner.substring(1, inner.length() - 1);
                    List<PType.RelationTypeVar.Column> cols = parseRelationColumns(colSpec);
                    return new PType.Parameterized(rawType,
                            List.of(new PType.RelationTypeVar(cols)));
                }
                // Relation<T> — type variable
                return new PType.Parameterized(rawType,
                        List.of(new PType.TypeVar(inner)));
            }

            // Other generics: just split type args
            return new PType.Parameterized(rawType,
                    splitTypeArgs(inner).stream().map(this::parsePureType).toList());
        }

        // Simple named type
        return new PType.Concrete(typeText);
    }

    /** Parse function type body: "T[m],U[m]->R[m]" */
    private PType.FunctionType parseFunctionTypeLiteral(String body) {
        // Find the last -> to split params from return
        int arrowIdx = body.lastIndexOf("->");
        if (arrowIdx < 0) return null;
        String paramsStr = body.substring(0, arrowIdx);
        String returnStr = body.substring(arrowIdx + 2);

        List<PType.Param> params = new ArrayList<>();
        if (!paramsStr.isEmpty()) {
            for (String paramStr : splitTypeArgs(paramsStr)) {
                paramStr = paramStr.trim();
                // Format: Type[mult] or name:Type[mult]
                int bracketIdx = paramStr.lastIndexOf('[');
                if (bracketIdx > 0) {
                    String typePart = paramStr.substring(0, bracketIdx);
                    String multStr = paramStr.substring(bracketIdx + 1,
                            paramStr.endsWith("]") ? paramStr.length() - 1 : paramStr.length());
                    PType type = parsePureType(typePart);
                    Mult mult = Mult.parse(multStr);
                    params.add(new PType.Param(null, type, mult));
                }
            }
        }
        // Parse return: Type[mult]
        returnStr = returnStr.trim();
        int bracketIdx = returnStr.lastIndexOf('[');
        PType retType = new PType.Concrete("Any");
        Mult retMult = Mult.ONE;
        if (bracketIdx > 0) {
            retType = parsePureType(returnStr.substring(0, bracketIdx));
            String multStr = returnStr.substring(bracketIdx + 1,
                    returnStr.endsWith("]") ? returnStr.length() - 1 : returnStr.length());
            retMult = Mult.parse(multStr);
        }
        return new PType.FunctionType(params, retType, retMult);
    }

    /** Parse relation column specs: "NAME:Type,AGE:Type" */
    private List<PType.RelationTypeVar.Column> parseRelationColumns(String colSpec) {
        List<PType.RelationTypeVar.Column> cols = new ArrayList<>();
        for (String part : splitTypeArgs(colSpec)) {
            part = part.trim();
            int colonIdx = part.indexOf(':');
            if (colonIdx > 0) {
                String colName = part.substring(0, colonIdx).trim();
                String typeStr = part.substring(colonIdx + 1).trim();
                cols.add(new PType.RelationTypeVar.Column(colName, parsePureType(typeStr), Mult.ONE));
            }
        }
        return cols;
    }

    /** Split comma-separated type args respecting nesting depth. */
    private List<String> splitTypeArgs(String s) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<' || c == '(' || c == '{') depth++;
            else if (c == '>' || c == ')' || c == '}') depth--;
            else if (c == ',' && depth == 0) {
                result.add(s.substring(start, i));
                start = i + 1;
            }
        }
        if (start < s.length()) result.add(s.substring(start));
        return result;
    }


    // ==================== Runtime ====================

    private RuntimeDefinition parseRuntime() {
        expect(TokenType.RUNTIME);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<String> mappings = new ArrayList<>();
        Map<String, String> connectionBindings = new HashMap<>();
        List<JsonModelConnection> jsonConnections = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            if ("mappings".equals(key)) {
                expect(TokenType.BRACKET_OPEN);
                if (peek() != TokenType.BRACKET_CLOSE) {
                    mappings.add(parseQualifiedName());
                    while (match(TokenType.COMMA)) {
                        mappings.add(parseQualifiedName());
                    }
                }
                expect(TokenType.BRACKET_CLOSE);
                match(TokenType.SEMI_COLON);
            } else if ("connections".equals(key)) {
                expect(TokenType.BRACKET_OPEN);
                while (peek() != TokenType.BRACKET_CLOSE) {
                    // storeName : [ identifier : connectionRef ]
                    String storeName = parseQualifiedName();
                    expect(TokenType.COLON);
                    expect(TokenType.BRACKET_OPEN);
                    while (peek() != TokenType.BRACKET_CLOSE) {
                        parseIdentifier();
                        expect(TokenType.COLON);
                        if (peek() == TokenType.ISLAND_OPEN) {
                            // Embedded connection: #{ JsonModelConnection { class: ...; url: '...'; } }#
                            advance(); // skip #{
                            int embStart = pos;
                            while (peek() != TokenType.ISLAND_END && !atEnd()) advance();
                            String embText = reconstructText(embStart, pos);
                            advance(); // skip }#
                            // Parse embedded JsonModelConnection
                            JsonModelConnection jmc = parseEmbeddedJsonModelConnection(embText);
                            if (jmc != null) jsonConnections.add(jmc);
                        } else {
                            String connectionRef = parseQualifiedName();
                            connectionBindings.put(storeName, connectionRef);
                        }
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                match(TokenType.SEMI_COLON);
            } else {
                // Skip unknown key-value
                skipToSemicolon();
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, mappings, connectionBindings, jsonConnections);
    }

    private RuntimeDefinition parseSingleConnectionRuntime() {
        // SingleConnectionRuntime is rare — skip its body and build a minimal RuntimeDefinition
        expect(TokenType.SINGLE_CONNECTION_RUNTIME);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);
        skipBalancedContent(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
        expect(TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, List.of(), new HashMap<>(), List.of());
    }

    private static final Pattern JMC_CLASS_PATTERN =
            Pattern.compile("class\\s*:\\s*([\\w:]+)\\s*;");
    private static final Pattern JMC_URL_PATTERN =
            Pattern.compile("url\\s*:\\s*'([^']*)'\\s*;");
    private static final Pattern CONTENT_TYPE_PATTERN =
            Pattern.compile("contentType:\\s*'([^']*)'" );
    private static final Pattern DATA_PATTERN =
            Pattern.compile("data:\\s*'([^']*)'" );

    private JsonModelConnection parseEmbeddedJsonModelConnection(String raw) {
        raw = raw.trim();
        if (!raw.startsWith("JsonModelConnection")) return null;
        var classMatcher = JMC_CLASS_PATTERN.matcher(raw);
        var urlMatcher = JMC_URL_PATTERN.matcher(raw);
        if (!classMatcher.find() || !urlMatcher.find()) return null;
        return new JsonModelConnection(classMatcher.group(1), urlMatcher.group(1));
    }

    // ==================== Connection ====================

    private ConnectionDefinition parseConnection() {
        expect(TokenType.RELATIONAL_DATABASE_CONNECTION);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String storeName = null;
        ConnectionDefinition.DatabaseType dbType = ConnectionDefinition.DatabaseType.DuckDB;
        ConnectionSpecification specification = new ConnectionSpecification.InMemory();
        AuthenticationSpec authentication = new AuthenticationSpec.NoAuth();

        while (peek() != TokenType.BRACE_CLOSE) {
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
                    } catch (IllegalArgumentException ignored) { }
                    expect(TokenType.SEMI_COLON);
                }
                case "specification" -> {
                    String specType = parseIdentifier();
                    expect(TokenType.BRACE_OPEN);
                    Map<String, String> specProps = parseKeyValueBlock();
                    specification = switch (specType) {
                        case "InMemory" -> new ConnectionSpecification.InMemory();
                        case "LocalFile" -> new ConnectionSpecification.LocalFile(
                                specProps.get("path"));
                        case "Static" -> new ConnectionSpecification.StaticDatasource(
                                specProps.get("host"),
                                specProps.containsKey("port")
                                        ? Integer.parseInt(specProps.get("port")) : 0,
                                specProps.get("database"));
                        default -> new ConnectionSpecification.InMemory();
                    };
                    expect(TokenType.SEMI_COLON);
                }
                case "auth" -> {
                    String authType = parseIdentifier();
                    expect(TokenType.BRACE_OPEN);
                    Map<String, String> authProps = parseKeyValueBlock();
                    authentication = switch (authType) {
                        case "NoAuth" -> new AuthenticationSpec.NoAuth();
                        case "UsernamePassword" -> new AuthenticationSpec.UsernamePassword(
                                authProps.get("username"),
                                authProps.get("passwordVaultRef"));
                        default -> new AuthenticationSpec.NoAuth();
                    };
                    expect(TokenType.SEMI_COLON);
                }
                default -> skipToSemicolon();
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new ConnectionDefinition(qualifiedName, storeName, dbType, specification, authentication);
    }

    // ==================== Service ====================

    private ServiceDefinition parseServiceDefinition() {
        expect(TokenType.SERVICE);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String pattern = null;
        String documentation = null;
        String functionBody = null;
        String mappingRef = null;
        String runtimeRef = null;

        while (peek() != TokenType.BRACE_CLOSE) {
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
                case "execution" -> {
                    // execution: Single { query: |...; mapping: ...; runtime: ...; }
                    parseIdentifier(); // "Single"
                    expect(TokenType.BRACE_OPEN);
                    while (peek() != TokenType.BRACE_CLOSE) {
                        String execKey = parseIdentifier();
                        expect(TokenType.COLON);
                        switch (execKey) {
                            case "query" -> {
                                // Skip the '|' (PIPE token)
                                match(TokenType.PIPE);
                                int bodyStart = pos;
                                // Scan forward to ';' at the same depth
                                int depth = 0;
                                while (!atEnd()) {
                                    TokenType tk = peek();
                                    if (tk == TokenType.PAREN_OPEN) depth++;
                                    else if (tk == TokenType.PAREN_CLOSE) depth--;
                                    else if (tk == TokenType.SEMI_COLON && depth <= 0) break;
                                    advance();
                                }
                                functionBody = reconstructText(bodyStart, pos);
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
                            default -> skipToSemicolon();
                        }
                    }
                    expect(TokenType.BRACE_CLOSE);
                }
                case "autoActivateUpdates" -> {
                    // Skip boolean value
                    advance(); // true/false
                    expect(TokenType.SEMI_COLON);
                }
                case "testSuites" -> {
                    // Skip test suites block for now
                    skipBalancedContent(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
                    // After the balanced braces, the next token may or may not be ;
                    match(TokenType.SEMI_COLON);
                }
                default -> skipToSemicolon();
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return ServiceDefinition.of(
                qualifiedName,
                pattern != null ? pattern : "/",
                functionBody != null ? functionBody : "",
                documentation,
                mappingRef, runtimeRef,
                List.of());
    }

    // ==================== Database ====================

    private DatabaseDefinition parseDatabase() {
        expect(TokenType.DATABASE);
        // Optional stereotypes/taggedValues (ignored)
        parseStereotypes();
        parseTaggedValues();
        String qualifiedName = parseQualifiedName();
        String dbScope = qualifiedName;
        expect(TokenType.PAREN_OPEN);

        List<String> includes = new ArrayList<>();
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> filters = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> multiGrainFilters = new ArrayList<>();
        List<DatabaseDefinition.SchemaDefinition> schemas = new ArrayList<>();

        while (peek() != TokenType.PAREN_CLOSE) {
            if (textEquals("include")) {
                advance();
                includes.add(parseQualifiedName());
            } else if (textEquals("Schema")) {
                    advance();
                    String schemaName = parseIdentifier();
                    expect(TokenType.PAREN_OPEN);
                    List<DatabaseDefinition.TableDefinition> schemaTables = new ArrayList<>();
                    List<DatabaseDefinition.ViewDefinition> schemaViews = new ArrayList<>();
                    while (peek() != TokenType.PAREN_CLOSE) {
                        if (textEquals("Table")) {
                            schemaTables.add(parseDbTable());
                            tables.add(schemaTables.get(schemaTables.size() - 1));
                        } else if (textEquals("View")) {
                            DatabaseDefinition.ViewDefinition v = parseDbView(dbScope);
                            schemaViews.add(v);
                            views.add(v);
                        } else {
                            error("Unknown schema element: '" + safeText() + "'");
                        }
                    }
                    expect(TokenType.PAREN_CLOSE);
                    schemas.add(new DatabaseDefinition.SchemaDefinition(schemaName, schemaTables, schemaViews));
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
                error("Unknown database element: '" + safeText() + "'");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition(qualifiedName, includes, schemas, tables, views, joins, filters, multiGrainFilters);
    }

    private DatabaseDefinition.TableDefinition parseDbTable() {
        advance(); // skip "Table"
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
        // Check for size spec like VARCHAR(100) or DECIMAL(10,2)
        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            String size = consume(TokenType.INTEGER);
            dataType = dataType + "(" + size;
            if (match(TokenType.COMMA)) {
                String scale = consume(TokenType.INTEGER);
                dataType += "," + scale;
            }
            expect(TokenType.PAREN_CLOSE);
            dataType += ")";
        }
        boolean primaryKey = match(TokenType.PRIMARY_KEY);
        boolean notNull = primaryKey || match(TokenType.NOT_NULL);
        return new DatabaseDefinition.ColumnDefinition(columnName, dataType, primaryKey, notNull);
    }

    private DatabaseDefinition.JoinDefinition parseDbJoin(String dbScope) {
        advance(); // skip "Join"
        String joinName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation operation = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.JoinDefinition(joinName, operation);
    }

    private DatabaseDefinition.FilterDefinition parseDbFilter(String dbScope) {
        advance(); // skip "Filter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.FilterDefinition parseDbMultiGrainFilter(String dbScope) {
        advance(); // skip "MultiGrainFilter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.ViewDefinition parseDbView(String dbScope) {
        advance(); // skip "View"
        String viewName = parseRelationalIdentifier();
        expect(TokenType.PAREN_OPEN);

        RelationalOperation filterMapping = null;
        List<RelationalOperation> groupBy = new ArrayList<>();
        boolean distinct = false;
        List<DatabaseDefinition.ViewDefinition.ViewColumnMapping> columnMappings = new ArrayList<>();

        // Optional ~filter, ~groupBy, ~distinct
        while (peek() == TokenType.FILTER_CMD || peek() == TokenType.GROUP_BY_CMD
                || peek() == TokenType.DISTINCT_CMD) {
            TokenType cmd = peek();
            advance();
            if (cmd == TokenType.FILTER_CMD) {
                String filterRef = parseIdentifier();
                filterMapping = RelationalOperation.Literal.string("~filter:" + filterRef);
            } else if (cmd == TokenType.GROUP_BY_CMD) {
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseDbOperation(dbScope));
                    while (match(TokenType.COMMA)) groupBy.add(parseDbOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
            } else if (cmd == TokenType.DISTINCT_CMD) {
                distinct = true;
            }
        }

        // Column mappings
        if (peek() != TokenType.PAREN_CLOSE) {
            columnMappings.add(parseViewColumnMapping(dbScope));
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) break;
                columnMappings.add(parseViewColumnMapping(dbScope));
            }
        }

        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.ViewDefinition(viewName, filterMapping, groupBy, distinct, columnMappings);
    }

    private DatabaseDefinition.ViewDefinition.ViewColumnMapping parseViewColumnMapping(String dbScope) {
        String colName = parseIdentifier();
        // Optional target set ID: [id]
        String targetSetId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            targetSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);
        RelationalOperation expression = parseDbOperation(dbScope);
        boolean pk = match(TokenType.PRIMARY_KEY);
        return new DatabaseDefinition.ViewDefinition.ViewColumnMapping(colName, targetSetId, expression, pk);
    }

    // ==================== DbOperation AST Walker ====================

    private RelationalOperation parseDbOperation(String dbScope) {
        // dbOperation: dbJoinOperation | dbBooleanOperation
        if (peek() == TokenType.AT || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            return parseDbJoinOperation(dbScope);
        }
        return parseDbBooleanOperation(dbScope);
    }

    private RelationalOperation parseDbBooleanOperation(String dbScope) {
        RelationalOperation left = parseDbAtomicOperation(dbScope);
        // Check for boolean right: "and" | "or"
        if (!atEnd() && IDENTIFIER_TOKENS.contains(peek())) {
            if (textEquals("and") || textEquals("or")) {
                String op = text();
                advance();
                RelationalOperation right = parseDbOperation(dbScope);
                return new RelationalOperation.BooleanOp(left, op, right);
            }
        }
        return left;
    }

    private RelationalOperation parseDbAtomicOperation(String dbScope) {
        RelationalOperation expr;

        if (peek() == TokenType.PAREN_OPEN) {
            // Group: ( dbOperation )
            advance();
            expr = new RelationalOperation.Group(parseDbOperation(dbScope));
            expect(TokenType.PAREN_CLOSE);
        } else if (peek() == TokenType.AT || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
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
            // {target}.COLUMN in self-join definitions
            advance();
            expect(TokenType.DOT);
            String col = parseRelationalIdentifier();
            expr = new RelationalOperation.TargetColumnRef(col);
        } else {
            // Column or function: identifier...
            // Check if it's a function call: identifier(...)
            String firstId = parseRelationalIdentifier();
            if (peek() == TokenType.PAREN_OPEN && !firstId.contains(".")) {
                // Function call: funcName(args)
                advance(); // skip (
                List<RelationalOperation> args = new ArrayList<>();
                if (peek() != TokenType.PAREN_CLOSE) {
                    args.add(parseDbFunctionArg(dbScope));
                    while (match(TokenType.COMMA)) args.add(parseDbFunctionArg(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                expr = new RelationalOperation.FunctionCall(firstId, args);
            } else {
                // Column reference: TABLE.COLUMN or bare identifier
                if (peek() == TokenType.DOT) {
                    advance();
                    String secondId = parseRelationalIdentifier();
                    if (peek() == TokenType.DOT) {
                        advance();
                        String thirdId = parseRelationalIdentifier();
                        expr = RelationalOperation.ColumnRef.of(null, firstId + "." + secondId, thirdId);
                    } else {
                        expr = RelationalOperation.ColumnRef.of(null, firstId, secondId);
                    }
                } else {
                    expr = RelationalOperation.ColumnRef.of(null, firstId, firstId);
                }
            }
        }

        // Handle optional right side: comparison or self-operator
        if (!atEnd()) {
            TokenType next = peek();
            if (next == TokenType.EQUAL || next == TokenType.TEST_EQUAL || next == TokenType.TEST_NOT_EQUAL
                    || next == TokenType.LESS_THAN || next == TokenType.GREATER_THAN
                    || next == TokenType.LESS_OR_EQUAL || next == TokenType.GREATER_OR_EQUAL
                    || next == TokenType.NOT_EQUAL) {
                String op = text();
                advance();
                RelationalOperation right = parseDbAtomicOperation(dbScope);
                expr = new RelationalOperation.Comparison(expr, op, right);
            } else if (isDbSelfOperator()) {
                expr = parseDbSelfOperator(expr);
            }
        }
        return expr;
    }

    private RelationalOperation parseDbFunctionArg(String dbScope) {
        if (peek() == TokenType.BRACKET_OPEN) {
            // Array: [args...]
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

    private RelationalOperation parseDbJoinOperation(String dbScope) {
        // Optional [DB] database pointer
        String dbName = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            dbName = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }

        // @JoinName chains: @J1 > @J2 > ...
        // Database context: navigation-level [DB] overrides the enclosing database
        String db = dbName != null ? dbName : dbScope;
        List<JoinChainElement> chain = new ArrayList<>();
        expect(TokenType.AT);
        chain.add(new JoinChainElement(parseIdentifier(), null, db, false));
        while (match(TokenType.GREATER_THAN)) {
            // Optional (joinType)
            String joinType = null;
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                joinType = parseIdentifier();
                expect(TokenType.PAREN_CLOSE);
            }
            // Optional [DB] per-hop override
            String hopDb = db;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                hopDb = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.AT);
            chain.add(new JoinChainElement(parseIdentifier(), joinType, hopDb, false));
        }

        // Optional terminal: | expression
        RelationalOperation terminal = null;
        if (match(TokenType.PIPE)) {
            terminal = parseDbBooleanOperation(dbScope);
        }

        return new RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    private boolean isDbSelfOperator() {
        if (!IDENTIFIER_TOKENS.contains(peek())) return false;
        return textEquals("is");
    }

    private RelationalOperation parseDbSelfOperator(RelationalOperation expr) {
        advance(); // "is"
        if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("not")) {
            advance(); // "not"
            advance(); // "null"
            return new RelationalOperation.IsNotNull(expr);
        }
        advance(); // "null"
        return new RelationalOperation.IsNull(expr);
    }

    private boolean lookAheadIsJoin() {
        // Check if [qualifiedName] is followed by @ (join) or is a database pointer
        int saved = pos;
        advance(); // skip [
        while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) advance();
        if (!atEnd()) advance(); // skip ]
        boolean isJoin = !atEnd() && peek() == TokenType.AT;
        pos = saved;
        return isJoin;
    }

    private String parseRelationalIdentifier() {
        if (peek() == TokenType.QUOTED_STRING) {
            String quoted = text();
            advance();
            return quoted.length() >= 2 ? quoted.substring(1, quoted.length() - 1) : quoted;
        }
        return parseIdentifier();
    }

    // ==================== Mapping ====================

    private MappingDefinition parseMapping() {
        expect(TokenType.MAPPING);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.PAREN_OPEN);

        List<MappingInclude> includes = new ArrayList<>();
        List<MappingDefinition.ClassMappingDefinition> classMappings = new ArrayList<>();
        List<AssociationMappingDefinition> associationMappings = new ArrayList<>();
        List<MappingDefinition.EnumerationMappingDefinition> enumerationMappings = new ArrayList<>();
        List<MappingDefinition.TestSuiteDefinition> testSuites = new ArrayList<>();

        while (peek() != TokenType.PAREN_CLOSE) {
            // Check for include
            if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("include")) {
                advance();
                includes.add(parseMappingInclude());
                continue;
            }
            // Check for testSuites at bottom
            if (peek() == TokenType.MAPPING_TESTABLE_SUITES) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (peek() != TokenType.BRACKET_CLOSE) {
                    testSuites.add(parseMappingTestSuite());
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                continue;
            }
            // Otherwise: className or qualifiedName followed by mapping type
            parseMappingElement(classMappings, associationMappings, enumerationMappings);
        }
        expect(TokenType.PAREN_CLOSE);
        return new MappingDefinition(qualifiedName, includes, classMappings,
                associationMappings, enumerationMappings, testSuites);
    }

    private MappingInclude parseMappingInclude() {
        String includedPath = parseQualifiedName();
        List<MappingInclude.StoreSubstitution> subs = new ArrayList<>();
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            while (peek() != TokenType.BRACKET_CLOSE) {
                String source = parseQualifiedName();
                expect(TokenType.ARROW);
                String target = parseQualifiedName();
                subs.add(new MappingInclude.StoreSubstitution(source, target));
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACKET_CLOSE);
        }
        return new MappingInclude(includedPath, subs);
    }

    private void parseMappingElement(
            List<MappingDefinition.ClassMappingDefinition> classMappings,
            List<AssociationMappingDefinition> associationMappings,
            List<MappingDefinition.EnumerationMappingDefinition> enumerationMappings) {
        // Optional root marker * before class name
        boolean isRoot = match(TokenType.STAR);

        // Parse the class/assoc/enum name
        String name = parseQualifiedName();
        String setId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            setId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }

        // Optional extends [parentId]
        String extendsSetId = null;
        if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("extends")) {
            advance();
            expect(TokenType.BRACKET_OPEN);
            extendsSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }

        expect(TokenType.COLON);

        // Mapping type keyword
        String mappingType = parseIdentifier();

        if ("EnumerationMapping".equals(mappingType)) {
            // Enumeration mapping: optional ID then { valueMappings }
            String enumMappingId = null;
            if (IDENTIFIER_TOKENS.contains(peek()) && peek() != TokenType.BRACE_OPEN) {
                enumMappingId = parseIdentifier();
            }
            expect(TokenType.BRACE_OPEN);
            Map<String, List<Object>> valueMappings = new LinkedHashMap<>();
            while (peek() != TokenType.BRACE_CLOSE) {
                String enumValue = parseIdentifier();
                expect(TokenType.COLON);
                List<Object> sourceValues = new ArrayList<>();
                if (peek() == TokenType.BRACKET_OPEN) {
                    advance();
                    while (peek() != TokenType.BRACKET_CLOSE) {
                        sourceValues.add(parseEnumSourceValue());
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                } else {
                    sourceValues.add(parseEnumSourceValue());
                }
                valueMappings.put(enumValue, sourceValues);
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACE_CLOSE);
            enumerationMappings.add(new MappingDefinition.EnumerationMappingDefinition(
                    name, enumMappingId, valueMappings));
        } else if ("Relational".equals(mappingType)) {
            expect(TokenType.BRACE_OPEN);
            // Legend-engine syntax: AssocName: Relational { AssociationMapping (...) }
            if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("AssociationMapping")) {
                advance(); // consume "AssociationMapping"
                associationMappings.add(parseAssociationMappingBody(name));
            } else {
                classMappings.add(parseRelationalClassMappingBody(name, setId, isRoot, extendsSetId));
            }
            expect(TokenType.BRACE_CLOSE);
        } else if ("Pure".equals(mappingType)) {
            expect(TokenType.BRACE_OPEN);
            classMappings.add(parsePureM2MClassMappingBody(name));
            expect(TokenType.BRACE_CLOSE);
        } else {
            // Unknown mapping type — skip body
            if (peek() == TokenType.BRACE_OPEN) {
                advance();
                skipBalancedContent(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
                expect(TokenType.BRACE_CLOSE);
            } else if (peek() == TokenType.PAREN_OPEN) {
                advance();
                skipBalancedContent(TokenType.PAREN_OPEN, TokenType.PAREN_CLOSE);
                expect(TokenType.PAREN_CLOSE);
            }
            // Add as a raw class mapping to avoid losing it
            classMappings.add(new MappingDefinition.ClassMappingDefinition(
                    name, mappingType, setId, isRoot, extendsSetId,
                    null, null, false, null, null, List.of(), null, null, null));
        }
    }

    private Object parseEnumSourceValue() {
        if (peek() == TokenType.STRING) {
            String s = unquoteString(text());
            advance();
            return s;
        } else if (peek() == TokenType.INTEGER) {
            int val = Integer.parseInt(text());
            advance();
            return val;
        } else {
            // Enum reference: EnumType.VALUE
            int start = pos;
            while (!atEnd() && peek() != TokenType.COMMA && peek() != TokenType.BRACKET_CLOSE
                    && peek() != TokenType.BRACE_CLOSE) {
                advance();
            }
            return reconstructText(start, pos);
        }
    }

    /**
     * Parses the body of an association mapping inside a Relational block.
     * Legend-engine syntax: {@code AssocName: Relational { AssociationMapping ( prop: [db]@Join, ... ) }}
     * The "AssociationMapping" keyword has already been consumed by the caller.
     */
    private AssociationMappingDefinition parseAssociationMappingBody(String associationName) {
        expect(TokenType.PAREN_OPEN);
        List<AssociationMappingDefinition.AssociationPropertyMapping> props = new ArrayList<>();
        while (peek() != TokenType.PAREN_CLOSE) {
            String propName = parseIdentifier();
            // Optional [source,target]
            String sourceSetId = null;
            String targetSetId = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                sourceSetId = parseIdentifier();
                if (match(TokenType.COMMA)) {
                    targetSetId = parseIdentifier();
                }
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.COLON);
            // Parse join chain: [DB]@JoinName
            String dbName = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                dbName = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            List<JoinChainElement> joinChain = parseMappingJoinChain(dbName);
            props.add(new AssociationMappingDefinition.AssociationPropertyMapping(
                    propName, sourceSetId, targetSetId, joinChain, null));
            match(TokenType.COMMA);
        }
        expect(TokenType.PAREN_CLOSE);
        return new AssociationMappingDefinition(associationName, "Relational", props);
    }

    private MappingDefinition.ClassMappingDefinition parseRelationalClassMappingBody(
            String className, String setId, boolean isRoot, String extendsSetId) {

        MappingDefinition.MappingFilter filter = null;
        boolean distinct = false;
        List<RelationalOperation> groupBy = new ArrayList<>();
        List<RelationalOperation> primaryKey = new ArrayList<>();
        MappingDefinition.TableReference mainTable = null;
        String dbScope = null; // set from ~mainTable, threaded to all child parsers
        List<MappingDefinition.PropertyMappingDefinition> propertyMappings = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE) {
            // Check for tilde commands (dedicated token types)
            TokenType tildeToken = peek();
            if (tildeToken == TokenType.MAIN_TABLE_CMD) {
                advance();
                mainTable = parseMappingMainTable();
                dbScope = mainTable.databaseName();
                continue;
            } else if (tildeToken == TokenType.FILTER_CMD) {
                advance();
                filter = parseMappingFilter();
                continue;
            } else if (tildeToken == TokenType.DISTINCT_CMD) {
                advance();
                distinct = true;
                continue;
            } else if (tildeToken == TokenType.GROUP_BY_CMD) {
                advance();
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseMappingOperation(dbScope));
                    while (match(TokenType.COMMA)) groupBy.add(parseMappingOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                continue;
            } else if (tildeToken == TokenType.PRIMARY_KEY_CMD) {
                advance();
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    primaryKey.add(parseMappingOperation(dbScope));
                    while (match(TokenType.COMMA)) primaryKey.add(parseMappingOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                continue;
            }

            // Property mapping: identifier : ... OR identifier( ... ) for embedded
            String propName = parseIdentifier();

            // Embedded property mapping without colon: propName( subMappings )
            // Or inline: propName() Inline[setId] or propName(setId)
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                if (peek() == TokenType.PAREN_CLOSE) {
                    // Empty parens: propName() Inline[setId]
                    advance(); // skip )
                    if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("Inline")) {
                        advance(); // skip Inline
                        expect(TokenType.BRACKET_OPEN);
                        String targetSetId = parseIdentifier();
                        expect(TokenType.BRACKET_CLOSE);
                        propertyMappings.add(MappingDefinition.PropertyMappingDefinition.inline(
                                propName, targetSetId));
                    } else {
                        // Empty embedded mapping
                        propertyMappings.add(MappingDefinition.PropertyMappingDefinition.embedded(
                                propName, new ArrayList<>()));
                    }
                } else if (IDENTIFIER_TOKENS.contains(peek()) && peek() != TokenType.BRACKET_OPEN
                        && peek() != TokenType.TILDE) {
                    // Could be inline (setId) or embedded property mappings
                    int saved = pos;
                    String firstToken = text();
                    advance();
                    if (peek() == TokenType.PAREN_CLOSE) {
                        // inline: ( setId )
                        expect(TokenType.PAREN_CLOSE);
                        propertyMappings.add(MappingDefinition.PropertyMappingDefinition.inline(
                                propName, firstToken));
                    } else {
                        pos = saved;
                        propertyMappings.add(parseEmbeddedPropertyMapping(propName, dbScope));
                    }
                } else {
                    propertyMappings.add(parseEmbeddedPropertyMapping(propName, dbScope));
                }
            } else {
                // Regular property mapping: colon then expression
                expect(TokenType.COLON);

                // Check for EnumerationMapping prefix
                String enumMappingId = null;
                if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("EnumerationMapping")) {
                    advance();
                    if (IDENTIFIER_TOKENS.contains(peek()) && peek() != TokenType.COLON) {
                        enumMappingId = parseIdentifier();
                    } else {
                        enumMappingId = "";
                    }
                    expect(TokenType.COLON);
                }

                propertyMappings.add(parseRegularPropertyMapping(propName, enumMappingId, dbScope));
            }

            match(TokenType.COMMA);
        }

        return new MappingDefinition.ClassMappingDefinition(
                className, "Relational", setId, isRoot, extendsSetId,
                mainTable, filter, distinct, groupBy, primaryKey,
                propertyMappings, null, null, null);
    }

    private MappingDefinition.PropertyMappingDefinition parseEmbeddedPropertyMapping(String propName, String dbScope) {
        List<MappingDefinition.PropertyMappingDefinition> subMappings = new ArrayList<>();
        while (peek() != TokenType.PAREN_CLOSE) {
            String subPropName = parseIdentifier();
            expect(TokenType.COLON);
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                subMappings.add(parseEmbeddedPropertyMapping(subPropName, dbScope));
            } else {
                subMappings.add(parseRegularPropertyMapping(subPropName, null, dbScope));
            }
            match(TokenType.COMMA);
        }
        expect(TokenType.PAREN_CLOSE);
        // Check for Otherwise after the closing paren
        if (IDENTIFIER_TOKENS.contains(peek()) && textEquals("Otherwise")) {
            advance();
            expect(TokenType.PAREN_OPEN);
            expect(TokenType.BRACKET_OPEN);
            String fallbackSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
            expect(TokenType.COLON);
            String dbName = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                dbName = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            String db = dbName != null ? dbName : dbScope;
            List<JoinChainElement> joinChain = parseMappingJoinChain(db);
            expect(TokenType.PAREN_CLOSE);
            var fallbackJoin = new PropertyMappingValue.JoinMapping(db, joinChain, null);
            return MappingDefinition.PropertyMappingDefinition.otherwise(
                    propName, subMappings, fallbackSetId, fallbackJoin);
        }
        return MappingDefinition.PropertyMappingDefinition.embedded(propName, subMappings);
    }

    private MappingDefinition.PropertyMappingDefinition parseRegularPropertyMapping(
            String propName, String enumMappingId, String dbScope) {
        // Check for join operation: [DB]@JoinName
        int exprStart = pos; // save for raw expression capture
        if (peek() == TokenType.BRACKET_OPEN || peek() == TokenType.AT) {
            String dbName = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                dbName = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            if (peek() == TokenType.AT) {
                // Join mapping — explicit [DB] overrides scope
                String db = dbName != null ? dbName : dbScope;
                List<JoinChainElement> chain = parseMappingJoinChain(db);
                RelationalOperation terminal = null;
                if (match(TokenType.PIPE)) {
                    terminal = parseMappingAtomicOperation(dbScope);
                }
                return MappingDefinition.PropertyMappingDefinition.join(propName,
                        new MappingDefinition.JoinReference(dbName, chain, terminal));
            }
            // Column reference: [DB] TABLE.COLUMN (may be followed by ->func())
            String firstId = parseRelationalIdentifier();
            if (peek() == TokenType.DOT) {
                advance();
                String secondId = parseRelationalIdentifier();
                if (peek() == TokenType.DOT) {
                    advance();
                    String thirdId = parseRelationalIdentifier();
                    // SCHEMA.TABLE.COLUMN — check for arrow chain (variant access)
                    if (peek() == TokenType.ARROW) {
                        return arrowChainToRawExpr(propName, exprStart);
                    }
                    return columnPropertyMapping(propName, dbName, firstId + "." + secondId, thirdId, enumMappingId);
                }
                // TABLE.COLUMN — check for arrow chain (variant access)
                if (peek() == TokenType.ARROW) {
                    return arrowChainToRawExpr(propName, exprStart);
                }
                return columnPropertyMapping(propName, dbName, firstId, secondId, enumMappingId);
            }
            // Bare column
            return columnPropertyMapping(propName, dbName, firstId, firstId, enumMappingId);
        }

        // DynaFunction expression: parse as RelationalOperation tree
        RelationalOperation expr = parseMappingOperation(dbScope);
        return MappingDefinition.PropertyMappingDefinition.mappingExpression(propName, expr);
    }

    /**
     * When a column reference is followed by ->func(args), capture the entire
     * expression as raw text (matching ANTLR behavior for variant access).
     * exprStart is the token position where the expression began (before [DB]).
     */
    private MappingDefinition.PropertyMappingDefinition arrowChainToRawExpr(
            String propName, int exprStart) {
        // Skip the arrow chain: ->func(args) possibly chained
        while (!atEnd() && peek() == TokenType.ARROW) {
            advance(); // skip ->
            parseIdentifier(); // funcName
            expect(TokenType.PAREN_OPEN);
            int depth = 1;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.PAREN_OPEN) depth++;
                else if (t == TokenType.PAREN_CLOSE) depth--;
                if (depth > 0) advance();
            }
            if (!atEnd()) advance(); // skip final )
        }
        String rawExpr = reconstructText(exprStart, pos);
        return MappingDefinition.PropertyMappingDefinition.expression(propName, rawExpr, null);
    }

    private MappingDefinition.PropertyMappingDefinition columnPropertyMapping(
            String propName, String dbName, String tableName, String columnName, String enumMappingId) {
        var colRef = new MappingDefinition.ColumnReference(dbName, tableName, columnName);
        if (enumMappingId != null) {
            return MappingDefinition.PropertyMappingDefinition.columnWithEnumMapping(
                    propName, colRef, enumMappingId);
        }
        return MappingDefinition.PropertyMappingDefinition.column(propName, colRef);
    }

    private MappingDefinition.TableReference parseMappingMainTable() {
        // [DB] TABLE_NAME or [DB] SCHEMA.TABLE
        expect(TokenType.BRACKET_OPEN);
        String dbName = parseQualifiedName();
        expect(TokenType.BRACKET_CLOSE);
        String tableName = parseRelationalIdentifier();
        if (peek() == TokenType.DOT) {
            advance();
            tableName = tableName + "." + parseRelationalIdentifier();
        }
        return new MappingDefinition.TableReference(dbName, tableName);
    }

    private MappingDefinition.MappingFilter parseMappingFilter() {
        // ~filter [DB]@J1 > @J2 | [DB] filterName
        // or ~filter [DB] filterName
        String dbName = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            dbName = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        // Check for join chain before filter name
        List<JoinChainElement> joinPath = new ArrayList<>();
        if (peek() == TokenType.AT) {
            joinPath = parseMappingJoinChain(dbName);
            // After join chain, expect PIPE then [DB] filterName
            expect(TokenType.PIPE);
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                dbName = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
        }
        String filterName = parseIdentifier();
        return new MappingDefinition.MappingFilter(dbName, joinPath, filterName);
    }

    private List<JoinChainElement> parseMappingJoinChain(String db) {
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
            // Per-hop [DB] override
            String hopDb = db;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                hopDb = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.AT);
            chain.add(new JoinChainElement(parseIdentifier(), joinType, hopDb, false));
        }
        return chain;
    }

    private MappingDefinition.ClassMappingDefinition parsePureM2MClassMappingBody(String className) {
        String sourceClassName = null;
        String filterExpression = null;
        Map<String, String> m2mPropertyExpressions = new LinkedHashMap<>();

        while (peek() != TokenType.BRACE_CLOSE) {
            // ~src
            if (peek() == TokenType.SRC_CMD) {
                advance();
                sourceClassName = parseQualifiedName();
                continue;
            }
            // ~filter
            if (peek() == TokenType.FILTER_CMD) {
                advance();
                int bodyStart = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN) depth++;
                    else if (t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE) {
                        if (depth <= 0) break;
                        depth--;
                    }
                    else if (t == TokenType.COMMA && depth <= 0) break;
                    // Stop if next is identifier:colon (start of property mapping)
                    else if (depth == 0 && IDENTIFIER_TOKENS.contains(t)
                            && pos + 1 < tokenCount
                            && lexer.tokenType(pos + 1) == TokenType.COLON) break;
                    advance();
                }
                filterExpression = reconstructText(bodyStart, pos);
                match(TokenType.COMMA);
                continue;
            }
            // Property mapping: name : expression
            String propName = parseIdentifier();
            expect(TokenType.COLON);
            int exprStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN || t == TokenType.BRACKET_OPEN) depth++;
                else if (t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE || t == TokenType.BRACKET_CLOSE) {
                    if (depth <= 0) break;
                    depth--;
                }
                else if (t == TokenType.COMMA && depth <= 0) break;
                advance();
            }
            m2mPropertyExpressions.put(propName, reconstructText(exprStart, pos));
            match(TokenType.COMMA);
        }

        return MappingDefinition.ClassMappingDefinition.pure(
                className, sourceClassName, filterExpression, m2mPropertyExpressions);
    }

    // ==================== Mapping Operation Walker ====================

    private RelationalOperation parseMappingOperation(String dbScope) {
        var left = parseMappingAtomicOperation(dbScope);
        if (!atEnd() && IDENTIFIER_TOKENS.contains(peek())) {
            if (textEquals("and") || textEquals("or")) {
                String op = text();
                advance();
                var right = parseMappingOperation(dbScope);
                return new RelationalOperation.BooleanOp(left, op, right);
            }
        }
        return left;
    }

    private RelationalOperation parseMappingAtomicOperation(String dbScope) {
        RelationalOperation expr;

        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            expr = new RelationalOperation.Group(parseMappingOperation(dbScope));
            expect(TokenType.PAREN_CLOSE);
        } else if (peek() == TokenType.AT) {
            // Could be @JoinName (join) or @TypeName (cast annotation)
            int saved = pos;
            advance(); // skip @
            String name = parseIdentifier();
            // Check if this looks like a join (followed by > or |)
            if (peek() == TokenType.GREATER_THAN || peek() == TokenType.PIPE
                    || peek() == TokenType.BRACKET_OPEN) {
                pos = saved;
                expr = parseMappingJoinOp(dbScope);
            } else {
                // Type annotation: @Integer, @String, etc.
                expr = RelationalOperation.Literal.string("@" + name);
            }
        } else if (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin()) {
            expr = parseMappingJoinOp(dbScope);
        } else if (peek() == TokenType.STRING) {
            expr = RelationalOperation.Literal.string(unquoteString(text()));
            advance();
        } else if (peek() == TokenType.INTEGER) {
            expr = RelationalOperation.Literal.integer(Long.parseLong(text()));
            advance();
        } else if (peek() == TokenType.FLOAT || peek() == TokenType.DECIMAL) {
            expr = RelationalOperation.Literal.decimal(Double.parseDouble(text()));
            advance();
        } else {
            // Column or function
            String dbName = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                int saved = pos;
                advance();
                dbName = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
                if (peek() == TokenType.AT) {
                    pos = saved;
                    return parseMappingJoinOp(dbScope);
                }
            }
            String firstId = parseRelationalIdentifier();
            if (peek() == TokenType.PAREN_OPEN && !firstId.contains(".")) {
                advance();
                List<RelationalOperation> args = new ArrayList<>();
                if (peek() != TokenType.PAREN_CLOSE) {
                    args.add(parseMappingOperation(dbScope));
                    while (match(TokenType.COMMA)) args.add(parseMappingOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                expr = new RelationalOperation.FunctionCall(firstId, args);
            } else if (peek() == TokenType.DOT) {
                advance();
                String secondId = parseRelationalIdentifier();
                if (peek() == TokenType.DOT) {
                    advance();
                    String thirdId = parseRelationalIdentifier();
                    expr = RelationalOperation.ColumnRef.of(dbName, firstId + "." + secondId, thirdId);
                } else {
                    expr = RelationalOperation.ColumnRef.of(dbName, firstId, secondId);
                }
            } else {
                expr = RelationalOperation.ColumnRef.of(dbName, firstId, firstId);
            }
        }

        // Handle arrow chain: expr->funcName(args)
        while (!atEnd() && peek() == TokenType.ARROW) {
            advance(); // skip ->
            String funcName = parseIdentifier();
            expect(TokenType.PAREN_OPEN);
            List<RelationalOperation> args = new ArrayList<>();
            args.add(expr); // receiver is first arg
            if (peek() != TokenType.PAREN_CLOSE) {
                args.add(parseMappingOperation(dbScope));
                while (match(TokenType.COMMA)) args.add(parseMappingOperation(dbScope));
            }
            expect(TokenType.PAREN_CLOSE);
            expr = new RelationalOperation.FunctionCall(funcName, args);
        }

        // Handle optional right side
        if (!atEnd()) {
            TokenType next = peek();
            if (next == TokenType.EQUAL || next == TokenType.TEST_EQUAL || next == TokenType.TEST_NOT_EQUAL
                    || next == TokenType.LESS_THAN || next == TokenType.GREATER_THAN
                    || next == TokenType.LESS_OR_EQUAL || next == TokenType.GREATER_OR_EQUAL
                    || next == TokenType.NOT_EQUAL) {
                String op = text();
                advance();
                var right = parseMappingAtomicOperation(dbScope);
                expr = new RelationalOperation.Comparison(expr, op, right);
            } else if (isDbSelfOperator()) {
                expr = parseDbSelfOperator(expr);
            }
        }
        return expr;
    }

    private RelationalOperation.JoinNavigation parseMappingJoinOp(String dbScope) {
        String dbName = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            dbName = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String db = dbName != null ? dbName : dbScope;
        List<JoinChainElement> chain = parseMappingJoinChain(db);
        RelationalOperation terminal = null;
        if (match(TokenType.PIPE)) {
            terminal = parseMappingAtomicOperation(dbScope);
        }
        return new RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    private MappingDefinition.TestSuiteDefinition parseMappingTestSuite() {
        // suiteName: { function: |...; tests: [...] }
        String suiteName = parseIdentifier();
        expect(TokenType.COLON);
        expect(TokenType.BRACE_OPEN);
        String functionBody = null;
        List<MappingDefinition.TestDefinition> tests = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            switch (key) {
                case "function" -> {
                    match(TokenType.PIPE);
                    int bodyStart = pos;
                    int depth = 0;
                    while (!atEnd()) {
                        TokenType t = peek();
                        if (t == TokenType.PAREN_OPEN) depth++;
                        else if (t == TokenType.PAREN_CLOSE) depth--;
                        else if (t == TokenType.SEMI_COLON && depth <= 0) break;
                        advance();
                    }
                    functionBody = reconstructText(bodyStart, pos);
                    expect(TokenType.SEMI_COLON);
                }
                case "tests" -> {
                    expect(TokenType.BRACKET_OPEN);
                    while (peek() != TokenType.BRACKET_CLOSE) {
                        tests.add(parseMappingTest());
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.SEMI_COLON);
                }
                default -> skipToSemicolon();
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new MappingDefinition.TestSuiteDefinition(suiteName, functionBody, tests);
    }

    private MappingDefinition.TestDefinition parseMappingTest() {
        String testName = parseIdentifier();
        expect(TokenType.COLON);
        expect(TokenType.BRACE_OPEN);
        String documentation = null;
        List<MappingDefinition.TestData> inputData = new ArrayList<>();
        List<MappingDefinition.TestAssertion> asserts = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            switch (key) {
                case "doc" -> {
                    documentation = unquoteString(consume(TokenType.STRING));
                    expect(TokenType.SEMI_COLON);
                }
                case "data" -> {
                    expect(TokenType.BRACKET_OPEN);
                    while (peek() != TokenType.BRACKET_CLOSE) {
                        inputData.add(parseMappingTestData());
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.SEMI_COLON);
                }
                case "asserts" -> {
                    expect(TokenType.BRACKET_OPEN);
                    while (peek() != TokenType.BRACKET_CLOSE) {
                        asserts.add(parseMappingTestAssert());
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.SEMI_COLON);
                }
                default -> skipToSemicolon();
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new MappingDefinition.TestDefinition(testName, documentation, inputData, asserts);
    }

    private MappingDefinition.TestData parseMappingTestData() {
        String storeName = parseQualifiedName();
        expect(TokenType.COLON);
        String format = parseIdentifier();
        // Island: #{ content }#
        expect(TokenType.ISLAND_OPEN);
        int contentStart = pos;
        int islandDepth = 0;
        while (!atEnd()) {
            if (peek() == TokenType.ISLAND_START) islandDepth++;
            else if (peek() == TokenType.ISLAND_END) {
                if (islandDepth <= 0) break;
                islandDepth--;
            }
            advance();
        }
        String content = reconstructText(contentStart, pos).trim();
        expect(TokenType.ISLAND_END);

        if ("Reference".equals(format)) {
            String refPath = content;
            if (refPath.endsWith("}#")) refPath = refPath.substring(0, refPath.length() - 2).trim();
            return new MappingDefinition.TestData(storeName, null, refPath, true);
        }

        String contentType = null;
        String data = null;
        var ctMatcher = CONTENT_TYPE_PATTERN.matcher(content);
        if (ctMatcher.find()) contentType = ctMatcher.group(1);
        var dMatcher = DATA_PATTERN.matcher(content);
        if (dMatcher.find()) data = dMatcher.group(1);
        return new MappingDefinition.TestData(storeName, contentType, data, false);
    }

    private MappingDefinition.TestAssertion parseMappingTestAssert() {
        String assertName = parseIdentifier();
        expect(TokenType.COLON);
        String assertType = parseIdentifier();
        expect(TokenType.ISLAND_OPEN);
        int contentStart = pos;
        int islandDepth = 0;
        while (!atEnd()) {
            if (peek() == TokenType.ISLAND_START) islandDepth++;
            else if (peek() == TokenType.ISLAND_END) {
                if (islandDepth <= 0) break;
                islandDepth--;
            }
            advance();
        }
        String content = reconstructText(contentStart, pos).trim();
        expect(TokenType.ISLAND_END);

        String expectedData = null;
        var dMatcher = DATA_PATTERN.matcher(content);
        if (dMatcher.find()) expectedData = dMatcher.group(1);
        return new MappingDefinition.TestAssertion(assertName, assertType, null, expectedData);
    }

    // ==================== Utility ====================

    private void skipBalancedContent(TokenType open, TokenType close) {
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == open) depth++;
            else if (t == close) {
                if (depth <= 0) return;
                depth--;
            }
            advance();
        }
    }


    private void skipToSemicolon() {
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN || t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE || t == TokenType.PAREN_CLOSE || t == TokenType.BRACKET_CLOSE) depth--;
            else if (t == TokenType.SEMI_COLON && depth <= 0) {
                advance();
                return;
            }
            advance();
        }
    }

    /**
     * Parse a { key: value; ... } block into a map of key-value string pairs.
     * Consumes up to and including the closing brace.
     * Values can be quoted strings, integers, or identifiers.
     */
    private Map<String, String> parseKeyValueBlock() {
        Map<String, String> props = new LinkedHashMap<>();
        while (peek() != TokenType.BRACE_CLOSE) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            String value;
            if (peek() == TokenType.STRING || peek() == TokenType.QUOTED_STRING) {
                value = unquoteString(text());
                advance();
            } else if (peek() == TokenType.INTEGER) {
                value = text();
                advance();
            } else {
                value = parseIdentifier();
            }
            props.put(key, value);
            match(TokenType.SEMI_COLON);
        }
        expect(TokenType.BRACE_CLOSE);
        return props;
    }

    private String unquoteString(String s) {
        if (s == null || s.length() < 2) return s;
        if ((s.startsWith("'") && s.endsWith("'")) ||
                (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
