package com.gs.legend.test;

import com.gs.legend.antlr.PackageableElementBuilder;
import com.gs.legend.antlr.PureLexer;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.parser.PureLexer2;
import com.gs.legend.parser.PureModelParser;
import com.gs.legend.parser.PureParser;
import org.antlr.v4.runtime.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case test harness for the hand-rolled parser rewrite.
 *
 * Phase 1: Validates ANTLR tokenization of tricky Pure snippets (baseline).
 *          When PureLexer2 is ready, add assertTokensMatch() to compare both.
 *
 * Phase 2: Add assertParseResultsMatch() to compare ANTLR vs hand-rolled parse results.
 *
 * Rule: whenever a bug is found during implementation, add the triggering
 * snippet to this corpus BEFORE fixing the bug.
 */
@DisplayName("Parser Edge Cases")
class ParserEdgeCaseTest {

    // ==================== Harness Helpers ====================

    /**
     * Lex source with ANTLR and return token (type, text) pairs.
     * Excludes EOF token.
     */
    private static List<int[]> antlrTokenTypes(String source) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(source));
        lexer.removeErrorListeners();
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<int[]> result = new ArrayList<>();
        for (Token t : tokens.getTokens()) {
            if (t.getType() == Token.EOF) break;
            result.add(new int[]{t.getType(), t.getStartIndex(), t.getStopIndex()});
        }
        return result;
    }

    private static List<String> antlrTokenTexts(String source) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(source));
        lexer.removeErrorListeners();
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<String> result = new ArrayList<>();
        for (Token t : tokens.getTokens()) {
            if (t.getType() == Token.EOF) break;
            result.add(t.getText());
        }
        return result;
    }

    /**
     * Assert that ANTLR lexes the source without errors and produces tokens.
     * This establishes the baseline. When PureLexer2 exists, this becomes
     * assertTokensMatch() comparing both lexers.
     */
    private void assertLexes(String label, String source) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(source));
        List<String> errors = new ArrayList<>();
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> r, Object sym, int line, int col, String msg, RecognitionException e) {
                errors.add(line + ":" + col + " " + msg);
            }
        });
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        assertTrue(errors.isEmpty(), label + ": ANTLR lex errors: " + errors);
        assertTrue(tokens.getTokens().size() > 1, label + ": expected tokens but got only EOF");
    }

    /**
     * Assert that a specific token text appears in the token stream.
     */
    private void assertHasToken(String label, String source, String expectedText) {
        List<String> texts = antlrTokenTexts(source);
        assertTrue(texts.contains(expectedText),
                label + ": expected token '" + expectedText + "' not found in " + texts);
    }

    /**
     * Assert that a specific token text appears at the given index.
     */
    private void assertTokenAt(String label, String source, int index, String expectedText) {
        List<String> texts = antlrTokenTexts(source);
        assertTrue(index < texts.size(), label + ": index " + index + " out of bounds (size=" + texts.size() + ")");
        assertEquals(expectedText, texts.get(index), label + ": token[" + index + "]");
    }

    // ==================== PureLexer2 Token Parity ====================

    /**
     * Compare ANTLR and PureLexer2 token streams by text.
     * Token types may differ (different enum systems), but the text of
     * each non-skip token must match exactly in sequence.
     */
    private void assertTokenParity(String label, String source) {
        // ANTLR
        List<String> antlrTexts = antlrTokenTexts(source);
        // Hand-rolled
        PureLexer2 lexer2 = new PureLexer2(source);
        lexer2.tokenize();
        List<String> hrTexts = new ArrayList<>();
        for (int i = 0; i < lexer2.tokenCount(); i++) {
            hrTexts.add(lexer2.tokenText(i));
        }
        assertEquals(antlrTexts.size(), hrTexts.size(),
                label + ": token count mismatch\n  ANTLR:  " + antlrTexts + "\n  HR:     " + hrTexts);
        for (int i = 0; i < antlrTexts.size(); i++) {
            assertEquals(antlrTexts.get(i), hrTexts.get(i),
                    label + ": token[" + i + "] text mismatch\n  ANTLR:  " + antlrTexts + "\n  HR:     " + hrTexts);
        }
    }

    @Nested
    @DisplayName("PureLexer2 Parity")
    class LexerParityTests {
        @Test void enumDef() {
            assertTokenParity("enum", "Enum test::Color { RED, GREEN, BLUE }");
        }
        @Test void classDef() {
            assertTokenParity("class", "Class test::Person { name: String[1]; }");
        }
        @Test void functionDef() {
            assertTokenParity("function", "function test::greet():String[1] { 'hello' }");
        }
        @Test void importStmt() {
            assertTokenParity("import", "import runtime::connections::*;");
        }
        @Test void primaryKey() {
            assertTokenParity("PRIMARY_KEY", "Database db(Table t(col INTEGER PRIMARY KEY))");
        }
        @Test void notNull() {
            assertTokenParity("NOT_NULL", "Database db(Table t(col VARCHAR(200) NOT NULL))");
        }
        @Test void isNull() {
            assertTokenParity("IS_NULL", "Database db(Filter f(col is null))");
        }
        @Test void isNotNull() {
            assertTokenParity("IS_NOT_NULL", "Database db(Filter f(col is not null))");
        }
        @Test void tildeFilter() {
            assertTokenParity("tilde_filter", "~filter");
        }
        @Test void latestDate() {
            assertTokenParity("LATEST_DATE", "function f():Any[*]{ %latest }");
        }
        @Test void dateLiteral() {
            assertTokenParity("DATE", "function f():Any[*]{ %2024-01-15 }");
        }
        @Test void graphFetch() {
            assertTokenParity("graphFetch", "#{test::Person{name}}#");
        }
        @Test void graphFetchArrow() {
            assertTokenParity("graphFetch_arrow",
                    "function f():Any[*]{ test::Person.all()->graphFetch(#{test::Person{name}}#)->serialize(#{test::Person{name}}#) }");
        }
        @Test void target() {
            assertTokenParity("TARGET", "Mapping m(test::A[a]: Relational { prop: [db]{target}.col })");
        }
        @Test void subTypeStart() {
            assertTokenParity("SUBTYPE_START", "function f():Any[*]{ $x->subType(@test::B) }");
        }
        @Test void keywordPackage() {
            assertTokenParity("KW_package", "Class mapping::filter::Person { name: String[1]; }");
        }
        @Test void numericLiterals() {
            assertTokenParity("integer", "function f():Any[*]{ 42 }");
            assertTokenParity("float", "function f():Any[*]{ 3.14 }");
        }
        @Test void emptyString() {
            assertTokenParity("empty_string", "function f():Any[*]{ '' }");
        }
        @Test void quotedString() {
            assertTokenParity("quoted_string", "Database db(Table t(\"My Column\" INTEGER))");
        }
        @Test void lineComment() {
            assertTokenParity("comment", "// skip this\nClass test::A { name: String[1]; }");
        }
        @Test void blockComment() {
            assertTokenParity("block_comment", "/* skip */\nClass test::A { name: String[1]; }");
        }
        @Test void sectionHeader() {
            assertTokenParity("section_header", "###Pure\nClass test::A { name: String[1]; }");
        }
        @Test void connection() {
            assertTokenParity("connection",
                    "RelationalDatabaseConnection test::C { store: test::DB; type: DuckDB; specification: LocalDuckDB{}; auth: NoAuth{}; }");
        }
        @Test void service() {
            assertTokenParity("service",
                    "Service test::S { pattern: '/test'; documentation: 'test'; execution: Single { mapping: test::M; runtime: test::R; } }");
        }
    }

    // ==================== Model Parser Dual-Parse Parity ====================

    /**
     * Compare ANTLR and PureModelParser definition outputs.
     * Parses with both, compares definitions list by equals().
     */
    private void assertModelParity(String label, String source) {
        // ANTLR path
        PackageableElementBuilder.ParseResult antlrResult = PureParser.parseModelWithImports(source);
        // Hand-rolled path
        PureLexer2 lexer2 = new PureLexer2(source);
        lexer2.tokenize();
        PureModelParser parser2 = new PureModelParser(lexer2);
        PackageableElementBuilder.ParseResult hrResult = parser2.parseDefinition();

        // Compare imports
        assertEquals(antlrResult.imports().getWildcardImports(),
                hrResult.imports().getWildcardImports(),
                label + ": wildcard imports mismatch");

        // Compare definitions
        List<PackageableElement> antlrDefs = antlrResult.definitions();
        List<PackageableElement> hrDefs = hrResult.definitions();
        assertEquals(antlrDefs.size(), hrDefs.size(),
                label + ": definition count mismatch\n  ANTLR: " + antlrDefs + "\n  HR:    " + hrDefs);
        for (int i = 0; i < antlrDefs.size(); i++) {
            assertEquals(antlrDefs.get(i), hrDefs.get(i),
                    label + ": definition[" + i + "] mismatch\n  ANTLR: " + antlrDefs.get(i) + "\n  HR:    " + hrDefs.get(i));
        }
    }

    @Nested
    @DisplayName("Model Parser Parity")
    class ModelParserParityTests {
        @Test void enumSimple() {
            assertModelParity("enum", "Enum test::Color { RED, GREEN, BLUE }");
        }
        @Test void enumSingleValue() {
            assertModelParity("enum_single", "Enum test::X { ONLY }");
        }
        @Test void profile() {
            assertModelParity("profile",
                    "Profile doc::Documentation { stereotypes: [legacy, deprecated]; tags: [author, since]; }");
        }
        @Test void profileEmpty() {
            assertModelParity("profile_empty", "Profile test::Empty { }");
        }
        @Test void association() {
            assertModelParity("association",
                    "Association test::PersonAddress { person: Person[1]; addresses: Address[*]; }");
        }
        @Test void classSimple() {
            assertModelParity("class_simple",
                    "Class test::Person { firstName: String[1]; lastName: String[1]; age: Integer[0..1]; }");
        }
        @Test void classWithSuperclass() {
            assertModelParity("class_extends",
                    "Class test::Employee extends test::Person { employeeId: String[1]; }");
        }
        @Test void classWithDerived() {
            assertModelParity("class_derived",
                    "Class test::Person { firstName: String[1]; lastName: String[1]; fullName() {$this.firstName + ' ' + $this.lastName}: String[1]; }");
        }
        @Test void functionSimple() {
            assertModelParity("function",
                    "function test::greet(name: String[1]):String[1] { $name + ' hello' }");
        }
        @Test void functionNoParams() {
            assertModelParity("function_no_params",
                    "function test::hello():String[1] { 'hello' }");
        }
        @Test void importAndClass() {
            assertModelParity("import_class",
                    "import test::model::*;\nClass test::Person { name: String[1]; }");
        }
        @Test void multipleDefinitions() {
            assertModelParity("multi_defs",
                    "Enum test::Color { RED, GREEN }\nClass test::Person { name: String[1]; color: test::Color[1]; }");
        }

        // ---- Connection ----
        @Test void connectionInMemory() {
            assertModelParity("conn_inmemory",
                    "RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }");
        }

        // ---- Runtime ----
        @Test void runtimeSimple() {
            assertModelParity("runtime_simple",
                    "Runtime test::RT { mappings: [ test::MyMapping ]; connections: [ store::DB: [ env: store::Conn ] ]; }");
        }

        // ---- Database ----
        @Test void databaseSimple() {
            assertModelParity("db_simple",
                    "Database store::DB ( Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), AGE INTEGER) )");
        }
        @Test void databaseWithJoin() {
            assertModelParity("db_join",
                    "Database store::DB ( Table PERSON (ID INTEGER PRIMARY KEY) Table ADDRESS (PERSON_ID INTEGER) Join PersonAddress(PERSON.ID = ADDRESS.PERSON_ID) )");
        }

        // ---- Mapping ----
        @Test void mappingRelational() {
            assertModelParity("mapping_relational",
                    "Mapping test::M ( test::Person: Relational { ~mainTable [store::DB] PERSON name: [store::DB] PERSON.NAME, age: [store::DB] PERSON.AGE } )");
        }
        @Test void mappingM2M() {
            assertModelParity("mapping_m2m",
                    "Mapping test::M ( test::Target: Pure { ~src test::Source name: $src.firstName + ' ' + $src.lastName } )");
        }

        // ---- Service ----
        @Test void serviceSimple() {
            assertModelParity("service_simple",
                    "Service test::Svc { pattern: '/api/test'; documentation: 'A test service'; execution: Single { query: |test::Person.all()->project(~[name]); mapping: test::M; runtime: test::RT; } }");
        }

        // ---- Stress file parity ----
        @Test void stressFileParity_runtime() throws Exception {
            String source = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get("src/test/resources/stress/91-runtime.pure")));
            assertModelParity("stress_runtime", source);
        }
        @Test void stressFileParity_productStore() throws Exception {
            String source = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get("src/test/resources/stress/31-products-store.pure")));
            assertModelParity("stress_product_store", source);
        }
        @Test void stressFileParity_services() throws Exception {
            String source = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get("src/test/resources/stress/92-services.pure")));
            assertModelParity("stress_services", source);
        }
        @Test void stressFileParity_allStoreFiles() throws Exception {
            java.nio.file.Path stressDir = java.nio.file.Paths.get("src/test/resources/stress");
            java.util.List<java.nio.file.Path> storeFiles = java.nio.file.Files.list(stressDir)
                    .filter(p -> p.getFileName().toString().endsWith("-store.pure"))
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
            for (java.nio.file.Path f : storeFiles) {
                String source = new String(java.nio.file.Files.readAllBytes(f));
                assertModelParity("stress:" + f.getFileName(), source);
            }
        }
        @Test void stressFileParity_allModelFiles() throws Exception {
            java.nio.file.Path stressDir = java.nio.file.Paths.get("src/test/resources/stress");
            java.util.List<java.nio.file.Path> modelFiles = java.nio.file.Files.list(stressDir)
                    .filter(p -> p.getFileName().toString().endsWith(".pure")
                            && !p.getFileName().toString().contains("-store"))
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
            for (java.nio.file.Path f : modelFiles) {
                String source = new String(java.nio.file.Files.readAllBytes(f));
                assertModelParity("stress:" + f.getFileName(), source);
            }
        }
    }

    // ==================== Composite Tokens ====================

    @Test
    @DisplayName("Composite: PRIMARY KEY")
    void compositeTokenPrimaryKey() {
        String src = "Database db(Table t(col INTEGER PRIMARY KEY))";
        assertLexes("PRIMARY_KEY", src);
        assertHasToken("PRIMARY_KEY", src, "PRIMARY KEY");
    }

    @Test
    @DisplayName("Composite: NOT NULL")
    void compositeTokenNotNull() {
        String src = "Database db(Table t(col VARCHAR(200) NOT NULL))";
        assertLexes("NOT_NULL", src);
        assertHasToken("NOT_NULL", src, "NOT NULL");
    }

    @Test
    @DisplayName("Composite: is null")
    void compositeTokenIsNull() {
        String src = "Database db(Filter f(col is null))";
        assertLexes("IS_NULL", src);
        assertHasToken("IS_NULL", src, "is null");
    }

    @Test
    @DisplayName("Composite: is not null")
    void compositeTokenIsNotNull() {
        String src = "Database db(Filter f(col is not null))";
        assertLexes("IS_NOT_NULL", src);
        assertHasToken("IS_NOT_NULL", src, "is not null");
    }

    @Test
    @DisplayName("Composite: {target}")
    void compositeTokenTarget() {
        String src = "Mapping m(test::A[a]: Relational { prop: [db]{target}.col })";
        assertLexes("TARGET", src);
        assertHasToken("TARGET", src, "{target}");
    }

    @Test
    @DisplayName("Composite: ->subType(@")
    void compositeTokenSubType() {
        String src = "function f():Any[*]{ $x->subType(@test::B) }";
        assertLexes("SUBTYPE_START", src);
        assertHasToken("SUBTYPE_START", src, "->subType(@");
    }

    // ==================== Percent Disambiguation ====================

    @Test
    @DisplayName("Percent: %latest")
    void percentLatestDate() {
        String src = "function f():Any[*]{ %latest }";
        assertLexes("LATEST_DATE", src);
        assertHasToken("LATEST_DATE", src, "%latest");
    }

    @Test
    @DisplayName("Percent: date literal")
    void percentDate() {
        String src = "function f():Any[*]{ %2024-01-15 }";
        assertLexes("DATE", src);
        assertHasToken("DATE", src, "%2024-01-15");
    }

    @Test
    @DisplayName("Percent: datetime literal")
    void percentDatetime() {
        String src = "function f():Any[*]{ %2024-01-15T10:30:00 }";
        assertLexes("DATETIME", src);
        assertHasToken("DATETIME", src, "%2024-01-15T10:30:00");
    }

    @Test
    @DisplayName("Percent: strict time literal")
    void percentStrictTime() {
        String src = "function f():Any[*]{ %10:30:45 }";
        assertLexes("STRICTTIME", src);
        // StrictTime tokens start with %
        List<String> texts = antlrTokenTexts(src);
        assertTrue(texts.stream().anyMatch(t -> t.startsWith("%") && t.contains(":")),
                "STRICTTIME: no time token found in " + texts);
    }

    // ==================== Keyword-as-Identifier ====================

    @Test
    @DisplayName("KW-as-ident: keyword package names")
    void keywordAsPackageName() {
        String src = "Class mapping::filter::Person { name: String[1]; }";
        assertLexes("KW_package", src);
        assertHasToken("KW_package", src, "mapping");
        assertHasToken("KW_package", src, "filter");
        assertHasToken("KW_package", src, "Person");
    }

    @Test
    @DisplayName("KW-as-ident: keyword property names")
    void keywordAsPropertyName() {
        String src = "Class test::X { service: String[1]; runtime: Integer[1]; }";
        assertLexes("KW_property", src);
        assertHasToken("KW_property", src, "service");
        assertHasToken("KW_property", src, "runtime");
    }

    @Test
    @DisplayName("KW-as-ident: keyword import path")
    void keywordAsImportPath() {
        String src = "import runtime::connections::*;";
        assertLexes("KW_import_path", src);
        assertHasToken("KW_import_path", src, "runtime");
        assertHasToken("KW_import_path", src, "connections");
    }

    // ==================== Island Mode ====================

    @Test
    @DisplayName("Island: simple graph fetch")
    void islandSimpleGraphFetch() {
        String src = "#{test::Person{name}}#";
        assertLexes("graphFetch_simple", src);
    }

    @Test
    @DisplayName("Island: nested graph fetch")
    void islandNestedGraphFetch() {
        String src = "#{test::Person{name,address{street,city}}}#";
        assertLexes("graphFetch_nested", src);
    }

    @Test
    @DisplayName("Island: graph fetch with arrow exit")
    void islandGraphFetchArrow() {
        // The }#-> sequence is the tricky ISLAND_ARROW_EXIT case
        String src = "function f():Any[*]{ test::Person.all()->graphFetch(#{test::Person{name}}#)->serialize(#{test::Person{name}}#) }";
        assertLexes("graphFetch_arrow", src);
    }

    @Test
    @DisplayName("Island: deeply nested braces")
    void islandDeeplyNestedBraces() {
        String src = "#{test::A{b{c{d}}}}#";
        assertLexes("island_nested_braces", src);
    }

    // ==================== Tilde Commands ====================

    @Test
    @DisplayName("Tilde: mapping commands")
    void tildeCommands() {
        // These are tokenized as single tokens by the ANTLR lexer
        for (String cmd : List.of("~filter", "~distinct", "~groupBy", "~mainTable", "~primaryKey", "~src")) {
            assertLexes("tilde:" + cmd, cmd);
            assertHasToken("tilde:" + cmd, cmd, cmd);
        }
    }

    @Test
    @DisplayName("Tilde: constraint commands")
    void tildeConstraintCommands() {
        for (String cmd : List.of("~owner", "~externalId", "~function", "~message", "~enforcementLevel")) {
            assertLexes("tilde:" + cmd, cmd);
            assertHasToken("tilde:" + cmd, cmd, cmd);
        }
    }

    @Test
    @DisplayName("Tilde: column spec (not a command)")
    void tildeColumnSpec() {
        // ~colName should be TILDE + VALID_STRING, not a single tilde-command token
        String src = "~colName";
        assertLexes("tilde_bare", src);
        List<String> texts = antlrTokenTexts(src);
        // Should be two tokens: ~ and colName
        assertTrue(texts.size() >= 2, "tilde_bare: expected >=2 tokens, got " + texts);
    }

    // ==================== Relational Identifiers ====================

    @Test
    @DisplayName("Relational: quoted column name")
    void relationalQuotedColumn() {
        String src = "Database db(Table t(\"My Column\" INTEGER))";
        assertLexes("quoted_column", src);
        assertHasToken("quoted_column", src, "\"My Column\"");
    }

    @Test
    @DisplayName("Relational: schema + table")
    void relationalSchemaTable() {
        String src = "Database db(Schema s(Table t(col INTEGER)))";
        assertLexes("schema_table", src);
        assertHasToken("schema_table", src, "Schema");
        assertHasToken("schema_table", src, "Table");
    }

    // ==================== String/Escape Edge Cases ====================

    @Test
    @DisplayName("String: escaped single quote")
    void stringEscapedQuote() {
        String src = "function f():Any[*]{ 'it\\'s' }";
        assertLexes("escape_quote", src);
        assertHasToken("escape_quote", src, "'it\\'s'");
    }

    @Test
    @DisplayName("String: escaped newline")
    void stringEscapedNewline() {
        String src = "function f():Any[*]{ 'line1\\nline2' }";
        assertLexes("escape_newline", src);
    }

    @Test
    @DisplayName("String: empty")
    void stringEmpty() {
        String src = "function f():Any[*]{ '' }";
        assertLexes("empty_string", src);
        assertHasToken("empty_string", src, "''");
    }

    // ==================== Definition Dispatch ====================

    @Test
    @DisplayName("Dispatch: Enum")
    void dispatchEnum() {
        assertLexes("enum", "Enum test::Color { RED, GREEN, BLUE }");
    }

    @Test
    @DisplayName("Dispatch: Profile")
    void dispatchProfile() {
        assertLexes("profile", "Profile test::MyProfile { stereotypes: [st1]; tags: [tag1]; }");
    }

    @Test
    @DisplayName("Dispatch: Class")
    void dispatchClass() {
        assertLexes("class", "Class test::Person { name: String[1]; }");
    }

    @Test
    @DisplayName("Dispatch: Association")
    void dispatchAssociation() {
        assertLexes("association", "Association test::AB { a: test::A[1]; b: test::B[1]; }");
    }

    @Test
    @DisplayName("Dispatch: Function")
    void dispatchFunction() {
        assertLexes("function", "function test::greet():String[1] { 'hello' }");
    }

    @Test
    @DisplayName("Dispatch: Runtime")
    void dispatchRuntime() {
        assertLexes("runtime", "Runtime test::R { mappings: [test::M]; }");
    }

    @Test
    @DisplayName("Dispatch: Service")
    void dispatchService() {
        assertLexes("service", "Service test::S { pattern: '/test'; documentation: 'test'; execution: Single { mapping: test::M; runtime: test::R; } }");
    }

    @Test
    @DisplayName("Dispatch: Database")
    void dispatchDatabase() {
        assertLexes("database", "Database test::DB(Table personTable(id INTEGER PRIMARY KEY, name VARCHAR(200)))");
    }

    @Test
    @DisplayName("Dispatch: Connection")
    void dispatchConnection() {
        assertLexes("connection", "RelationalDatabaseConnection test::C { store: test::DB; type: DuckDB; specification: LocalDuckDB{}; auth: NoAuth{}; }");
    }

    // ==================== Numeric Edge Cases ====================

    @Test
    @DisplayName("Numeric: integer")
    void numericInteger() {
        String src = "function f():Any[*]{ 42 }";
        assertLexes("integer", src);
        assertHasToken("integer", src, "42");
    }

    @Test
    @DisplayName("Numeric: float")
    void numericFloat() {
        String src = "function f():Any[*]{ 3.14 }";
        assertLexes("float", src);
        assertHasToken("float", src, "3.14");
    }

    @Test
    @DisplayName("Numeric: float with exponent")
    void numericFloatExp() {
        String src = "function f():Any[*]{ 1.5e10 }";
        assertLexes("float_exp", src);
    }

    @Test
    @DisplayName("Numeric: decimal")
    void numericDecimal() {
        String src = "function f():Any[*]{ 3.14d }";
        assertLexes("decimal", src);
    }

    @Test
    @DisplayName("Numeric: negative")
    void numericNegative() {
        String src = "function f():Any[*]{ -1 }";
        assertLexes("negative", src);
    }

    // ==================== Comments ====================

    @Test
    @DisplayName("Comment: line comment")
    void commentLine() {
        String src = "// this is a comment\nClass test::A { name: String[1]; }";
        assertLexes("line_comment", src);
        // The comment should be skipped — first meaningful token should be 'Class'
        assertTokenAt("line_comment", src, 0, "Class");
    }

    @Test
    @DisplayName("Comment: block comment")
    void commentBlock() {
        String src = "/* multi\n   line\n   comment */\nClass test::A { name: String[1]; }";
        assertLexes("block_comment", src);
        assertTokenAt("block_comment", src, 0, "Class");
    }

    @Test
    @DisplayName("Comment: section header")
    void commentSectionHeader() {
        String src = "###Pure\nClass test::A { name: String[1]; }";
        assertLexes("section_header", src);
    }

    // ==================== Stress Parity ====================

    private static String loadResource(String path) {
        try (InputStream is = ParserEdgeCaseTest.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) return null;
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @DisplayName("Stress File Parity")
    class StressFileParityTests {

        private static final String[] STRESS_FILES = {
                "stress/01-products.pure",
                "stress/02-refdata.pure",
                "stress/03-counterparty.pure",
                "stress/06-trading.pure",
                "stress/08-risk.pure",
                "stress/31-products-store.pure",
                "stress/36-trading-store.pure",
                "stress/90-cross-domain-associations.pure",
                "stress/91-runtime.pure",
                "stress/92-services.pure",
        };

        @Test
        @DisplayName("All stress .pure files produce identical tokens")
        void stressFileParity() {
            for (String path : STRESS_FILES) {
                String source = loadResource(path);
                assertNotNull(source, "Resource not found: " + path);
                assertTokenParity(path, source);
            }
        }

        @Test
        @DisplayName("Concatenated stress files: full model parity")
        void fullModelParity() {
            StringBuilder sb = new StringBuilder();
            for (String path : STRESS_FILES) {
                String source = loadResource(path);
                if (source != null) {
                    sb.append(source).append('\n');
                }
            }
            String fullModel = sb.toString();
            assertTokenParity("full_stress_model (" + (fullModel.length() / 1024) + " KB)", fullModel);
        }
    }

    // ==================== Lexer Benchmark ====================

    @Nested
    @DisplayName("Lexer Benchmark")
    class LexerBenchmarkTests {

        @Test
        @DisplayName("PureLexer2 vs ANTLR: speed comparison on stress files")
        void benchmarkComparison() {
            // Load all stress files
            StringBuilder sb = new StringBuilder();
            for (String path : StressFileParityTests.STRESS_FILES) {
                String source = loadResource(path);
                if (source != null) sb.append(source).append('\n');
            }
            String model = sb.toString();
            int warmup = 50;
            int iterations = 200;

            // Warmup
            for (int i = 0; i < warmup; i++) {
                antlrLex(model);
                hrLex(model);
            }

            // Benchmark ANTLR
            long t0 = System.nanoTime();
            int antlrTokens = 0;
            for (int i = 0; i < iterations; i++) {
                antlrTokens = antlrLex(model);
            }
            long antlrNs = System.nanoTime() - t0;

            // Benchmark PureLexer2
            long t1 = System.nanoTime();
            int hrTokens = 0;
            for (int i = 0; i < iterations; i++) {
                hrTokens = hrLex(model);
            }
            long hrNs = System.nanoTime() - t1;

            double antlrMs = antlrNs / 1_000_000.0 / iterations;
            double hrMs = hrNs / 1_000_000.0 / iterations;
            double speedup = antlrMs / hrMs;

            System.out.printf("=== LEXER BENCHMARK (%d KB, %d iterations) ===%n", model.length() / 1024, iterations);
            System.out.printf("  ANTLR:     %.3f ms/iter  (%d tokens)%n", antlrMs, antlrTokens);
            System.out.printf("  PureLexer2: %.3f ms/iter  (%d tokens)%n", hrMs, hrTokens);
            System.out.printf("  Speedup:   %.1fx%n", speedup);

            assertEquals(antlrTokens, hrTokens, "Token count mismatch");
            assertTrue(speedup > 1.0, "PureLexer2 should be faster than ANTLR, but was " + speedup + "x");
        }

        private int antlrLex(String source) {
            PureLexer lexer = new PureLexer(CharStreams.fromString(source));
            lexer.removeErrorListeners();
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            tokens.fill();
            return tokens.getTokens().size() - 1; // exclude EOF
        }

        private int hrLex(String source) {
            PureLexer2 lexer = new PureLexer2(source);
            lexer.tokenize();
            return lexer.tokenCount();
        }
    }
}
