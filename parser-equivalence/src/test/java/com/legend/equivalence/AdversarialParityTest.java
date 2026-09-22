// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE GENERATIVE DIFFERENTIAL GATE (2026-08-12 adversarial audit): inputs
 * the harvested corpus structurally CANNOT contain — engine/pure repos only
 * spell what their own tests spell — verdict-adjudicated against the live
 * 4.138.2 oracle in this JVM. Every family below found a real divergence on
 * its first run; each fixed row stays here as a regression pin, and the
 * whole suite runs in ~5s.
 *
 * <p>Three verdict classes are asserted: ACCEPT-parity (both accept or both
 * refuse), no INTERNAL exceptions on lite's side (a refusal must be a
 * {@code ParseException}, never a raw JDK exception or an
 * {@code UnsupportedOperationException} invariant trip on engine-legal
 * input), and a DIVERGENCE RATCHET for the named still-open rows (shrink
 * only; a new divergence in a green family is a hard failure).
 *
 * <p>This gate deliberately does NOT compare bytes — {@code CorpusSweepTest}
 * owns byte parity. Verdict asymmetry is the blind spot this class exists
 * to close (deep-audit H1: "byte parity is blind where the oracle refuses";
 * GATES.md: "a corpus sweep structurally cannot find a disagreement about a
 * form the corpus never contains").
 */
class AdversarialParityTest {

    private enum Verdict { ACCEPTS, REFUSES, LITE_INTERNAL_ERROR }

    private record Parse(Verdict verdict, @com.legend.base.Nullable String json) {
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            ObjectMapperFactory
                    .getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    private static Parse engine(String src) {
        try {
            return new Parse(Verdict.ACCEPTS, MAPPER.writeValueAsString(
                    PureGrammarParser.newInstance().parseModel(src)));
        } catch (Throwable t) {
            return new Parse(Verdict.REFUSES, null);
        }
    }

    private static Parse lite(String src) {
        try {
            return new Parse(Verdict.ACCEPTS,
                    com.legend.parser.PmcdParser.parseDocument(src));
        } catch (com.legend.parser.ParseException e) {
            return new Parse(Verdict.REFUSES, null);
        } catch (Throwable t) {
            return new Parse(Verdict.LITE_INTERNAL_ERROR, null);
        }
    }

    private record Row(String label, String src) {
    }

    private static void runFamily(String family, List<Row> rows,
            int maxDivergent) {
        List<String> divergent = new ArrayList<>();
        List<String> internal = new ArrayList<>();
        List<String> byteDiffs = new ArrayList<>();
        for (Row r : rows) {
            Parse e = engine(r.src());
            Parse l = lite(r.src());
            if (l.verdict() == Verdict.LITE_INTERNAL_ERROR) {
                internal.add(r.label());
            } else if (e.verdict() != l.verdict()) {
                divergent.add(r.label() + " (engine " + e.verdict()
                        + ", lite " + l.verdict() + ")");
            } else if (e.verdict() == Verdict.ACCEPTS
                    && !Comparators.sameBytes(e.json(), l.json())) {
                // verdict parity is HALF the claim: an adversarial row both
                // sides accept must ALSO byte-match — the fixed colspec
                // spans, escaped aliases, and text blocks were only
                // verdict-verified until this arm existed
                byteDiffs.add(r.label());
            }
        }
        assertEquals(List.of(), internal,
                family + ": lite threw a NON-ParseException — internal "
                        + "errors on user input are always bugs");
        if (divergent.size() > maxDivergent) {
            assertEquals(List.of(), divergent, family
                    + ": verdict divergences grew past the ratchet ("
                    + maxDivergent + ")");
        }
        assertEquals(List.of(), byteDiffs,
                family + ": BYTE diffs on both-accept adversarial rows");
    }

    // ------------------------------------------------------------------
    // Families. Every keyword the lexer fuses or classifies specially is
    // exercised in every identifier position; the section/comment/escape
    // families each reproduce a divergence found (and fixed) 2026-08-12.
    // ------------------------------------------------------------------

    private static final String[] KEYWORDS = {"all", "let", "import",
            "extends", "native", "as", "include", "query", "pattern",
            "owners", "documentation", "execution", "mapping", "runtime",
            "connection", "connections", "mappings", "store", "type",
            "specification", "auth", "filter", "distinct", "groupBy",
            "mainTable", "primaryKey", "src", "and", "or", "true", "false",
            "testSuites", "comparator", "allVersions"};

    @Test
    void keywordAsIdentifierEverywhere() {
        List<Row> rows = new ArrayList<>();
        for (String kw : KEYWORDS) {
            rows.add(new Row("class " + kw,
                    "###Pure\nClass a::" + kw + " { x: String[1]; }\n"));
            rows.add(new Row("prop " + kw,
                    "###Pure\nClass a::A { " + kw + ": String[1]; }\n"));
            rows.add(new Row("var " + kw, "###Pure\nfunction f::f(): Integer[1]\n{\n  let "
                    + kw + " = 1;\n  $" + kw + " + 1;\n}\n"));
            rows.add(new Row("param " + kw, "###Pure\nfunction f::f(" + kw
                    + ": Integer[1]): Integer[1]\n{\n  $" + kw + ";\n}\n"));
            rows.add(new Row("enumval " + kw,
                    "###Pure\nEnum a::E { " + kw + " }\n"));
            rows.add(new Row("colspec " + kw,
                    "###Pure\nfunction f::f(): Any[*]\n{\n  #>{a::db.t}#->select(~"
                            + kw + ")\n}\n"));
        }
        runFamily("keyword-as-identifier", rows, 0);
    }

    @Test
    void esAuthKinds() {
        // the auth-module island beyond UserPassword (probed auth-wire
        // 2026-08-14): apiKey / kerberos / encryptedPrivateKey
        String h = "###Connection\nElasticsearch7ClusterConnection a::C\n{\n"
                + "  store: t::S;\n"
                + "  clusterDetails: # URL { http://u.com:1 }#;\n"
                + "  authentication: ";
        runFamily("es-auth-kinds", List.of(
                new Row("apiKey", h + "# ApiKey {\n    location: 'header';\n    keyName: 'key1';\n    value: SystemPropertiesSecret\n    {\n      systemPropertyName: 's';\n    };\n  }#;\n}\n"),
                new Row("kerberos empty", h + "# Kerberos {\n  }#;\n}\n"),
                new Row("aws secret default creds", h + "# UserPassword {\n    username: 'u';\n    password: AWSSecretsManagerSecret\n    {\n      secretId: 's1';\n      versionId: 'v1';\n      versionStage: 'st1';\n      awsCredentials: Default\n      {\n      }\n    };\n  }#;\n}\n"),
                new Row("aws secret static creds", h + "# UserPassword {\n    username: 'u';\n    password: AWSSecretsManagerSecret\n    {\n      secretId: 's1';\n      versionId: 'v1';\n      versionStage: 'st1';\n      awsCredentials: Static\n      {\n        accessKeyId: PropertiesFileSecret\n        {\n          propertyName: 'p1';\n        };\n        secretAccessKey: PropertiesFileSecret\n        {\n          propertyName: 'p2';\n        };\n      }\n    };\n  }#;\n}\n"),
                new Row("environment secret", h + "# UserPassword {\n    username: 'u';\n    password: EnvironmentSecret\n    {\n      envVariableName: 'E';\n    };\n  }#;\n}\n"),
                new Row("encryptedPrivateKey", h + "# EncryptedPrivateKey {\n    userName: 'alice';\n    privateKey: PropertiesFileSecret\n    {\n      propertyName: 'p1';\n    };\n    passphrase: PropertiesFileSecret\n    {\n      propertyName: 'p2';\n    };\n  }#;\n}\n")),
                0);
    }

    @Test
    void esValidationCluster() {
        // deep-audit #2 "ES validation gaps" + handoff PSK — every row
        // probed live 2026-08-14 (EsProbe session): required/once fields,
        // non-empty arrays, scalar-vs-complex body keys, PSK's missing
        // EOF (trailing island tokens IGNORED), and ORDER_MAP_ENTRIES_BY_KEYS
        // on the nested fields/properties maps.
        String ch = "###Connection\nElasticsearch7ClusterConnection c::C\n{\n"
                + "  store: s::S;\n"
                + "  clusterDetails: # URL { http://localhost:9200 }#;\n"
                + "  authentication: ";
        String ct = ";\n}\n";
        String cs = "###Elasticsearch\nElasticsearch7Cluster s::S\n{\n"
                + "  indices: [ i1: { properties: [ p: Keyword ]; } ];\n}\n";
        String h = "###Elasticsearch\nElasticsearch7Cluster s::S\n{\n";
        runFamily("es-validation", List.of(
                new Row("psk auth", ch + "# PSK { psk: 'abc'; }#" + ct + cs),
                new Row("psk trailing ignored", ch + "# PSK { psk: 'abc'; psk: 'd'; }#" + ct + cs),
                new Row("apikey bad location", ch + "# ApiKey { location: 'body'; keyName: 'k'; value: PropertiesFileSecret { propertyName: 'p'; }; }#" + ct + cs),
                new Row("apikey cookie", ch + "# ApiKey { location: 'cookie'; keyName: 'k'; value: PropertiesFileSecret { propertyName: 'p'; }; }#" + ct + cs),
                new Row("apikey dup keyName", ch + "# ApiKey { location: 'header'; keyName: 'k'; keyName: 'j'; value: PropertiesFileSecret { propertyName: 'p'; }; }#" + ct + cs),
                new Row("conn missing semi", "###Connection\nElasticsearch7ClusterConnection c::C\n{\n  store: s::S\n  clusterDetails: # URL { http://localhost:9200 }#;\n  authentication: # Kerberos {}#;\n}\n" + cs),
                new Row("userpass dup username", ch + "# UserPassword { username: 'u'; username: 'v'; password: PropertiesFileSecret { propertyName: 'p'; }; }#" + ct + cs),
                new Row("store no indices", h + "}\n"),
                new Row("index no properties", h + "  indices: [ i1: { } ];\n}\n"),
                new Row("empty properties array", h + "  indices: [ i1: { properties: [ ]; } ];\n}\n"),
                new Row("trailing comma", h + "  indices: [ i1: { properties: [ p: Keyword, ]; } ];\n}\n"),
                new Row("scalar with properties key", h + "  indices: [ i1: { properties: [ p: Keyword { properties: [ q: Text ]; } ]; } ];\n}\n"),
                new Row("complex with fields key", h + "  indices: [ i1: { properties: [ p: Object { fields: [ q: Text ]; } ]; } ];\n}\n"),
                new Row("object no body", h + "  indices: [ i1: { properties: [ p: Object ]; } ];\n}\n"),
                new Row("object empty body", h + "  indices: [ i1: { properties: [ p: Object { } ]; } ];\n}\n"),
                new Row("scalar empty body", h + "  indices: [ i1: { properties: [ p: Keyword { } ]; } ];\n}\n"),
                new Row("nested dup names", h + "  indices: [ i1: { properties: [ p: Object { properties: [ q: Text, q: Keyword ]; } ]; } ];\n}\n"),
                new Row("top-level dup names", h + "  indices: [ i1: { properties: [ p: Keyword, p: Text ]; } ];\n}\n"),
                new Row("nested multi order", h + "  indices: [ i1: { properties: [ p: Object { properties: [ zz: Text, aa: Keyword, mm: Long ]; } ]; } ];\n}\n"),
                new Row("fields multi order", h + "  indices: [ i1: { properties: [ p: Text { fields: [ zz: Keyword, aa: Keyword ]; } ]; } ];\n}\n"),
                new Row("dup indices blocks", h + "  indices: [ i1: { properties: [ p: Keyword ]; } ];\n  indices: [ i2: { properties: [ p: Keyword ]; } ];\n}\n"),
                new Row("dup properties blocks", h + "  indices: [ i1: { properties: [ p: Keyword ]; properties: [ q: Text ]; } ];\n}\n")),
                0);
    }

    @Test
    void booleanColumnTypes() {
        // test-corpus audit F3: the engine refuses BOOLEAN/BOOL columns;
        // lite keeps them as a DECLARED extension on its own dialect, so
        // the drop-in surface must refuse engine-verbatim
        runFamily("boolean-columns", List.of(
                new Row("BOOLEAN column",
                        "###Relational\nDatabase d::DB\n(\n  Table T (ID INTEGER PRIMARY KEY, FLAG BOOLEAN)\n)\n"),
                new Row("BOOL column",
                        "###Relational\nDatabase d::DB\n(\n  Table T (ID INTEGER PRIMARY KEY, FLAG BOOL)\n)\n"),
                new Row("BIT column (engine-legal)",
                        "###Relational\nDatabase d::DB\n(\n  Table T (ID INTEGER PRIMARY KEY, FLAG BIT)\n)\n")),
                0);
    }

    @Test
    void newInstanceDuplicateKeys() {
        // deep audit #2 finding 1b: engine keeps EVERY ^new binding
        // including duplicate keys (two keyExpressions on the wire);
        // the old Map carrier silently collapsed them to one.
        String h = "function f::q(): Any[*]\n{\n  ";
        String t = ";\n}\n";
        runFamily("newinstance-duplicate-keys", List.of(
                new Row("dup assign", h + "^a::A(x = 1, x = 2)" + t),
                new Row("dup add", h + "^a::A(xs += 1, xs += 2)" + t),
                new Row("dup mixed with other key", h + "^a::A(x = 1, y = 'a', x = 2)" + t),
                new Row("triple dup", h + "^a::A(x = 1, x = 2, x = 3)" + t)),
                0);
    }

    @Test
    void gqlIslands() {
        // #GQL{...}# — the graphQL embedded-Pure extension (census row
        // retired 2026-08-14). Every accepted form byte-pinned against
        // the live oracle on first implementation; inline fragments are
        // refused by BOTH grammars.
        String h = "function f::q(): Any[*]\n{\n  ";
        String t = "\n}\n";
        runFamily("gql-islands", List.of(
                new Row("basic query", h + "#GQL{ query { hero { name } } }#" + t),
                new Row("named query", h + "#GQL{ query Hero { hero } }#" + t),
                new Row("all value kinds", h + "#GQL{ query { hero(id: 5, name: \"luke\", f: 1.5, b: true, n: null, e: JEDI, l: [1, 2], o: {a: 1}) { name } } }#" + t),
                new Row("alias + fragment", h + "#GQL{ query { h: hero { ...heroFields } } fragment heroFields on Hero { name } }#" + t),
                new Row("variables + directives", h + "#GQL{ query Q($id: Int!, $s: [String] = [\"x\"]) { hero(id: $id) @include(if: true) { name } } }#" + t),
                new Row("mutation", h + "#GQL{ mutation { add(x: 1) { ok } } }#" + t),
                new Row("sdl object type", h + "#GQL{ type Foo { id: ID! names: [String] } }#" + t),
                new Row("bare selection", h + "#GQL{ { hero { name } } }#" + t),
                new Row("inline fragment refused", h + "#GQL{ query { hero { ...on Droid { fn } } } }#" + t),
                new Row("inline fragment refused 2", h + "#GQL{ query { hero { name ... on Droid { fn } } } }#" + t)),
                0);
    }

    @Test
    void sectionBoundaryShapes() {
        runFamily("section-boundaries", List.of(
                new Row("mid-line ###",
                        "###Pure\nClass a::A { x: String[1]; } ###Mapping\nMapping m::M ()\n"),
                new Row("leading-space ###",
                        "###Pure\nClass a::A { x: String[1]; }\n ###Mapping\nMapping m::M ()\n"),
                new Row("### in block comment",
                        "###Pure\nClass a::A { x: String[1]; }\n/* c\n###Mapping\n*/\nClass a::B { y: String[1]; }\n"),
                new Row("### in string",
                        "###Pure\nfunction f::f(): String[1]\n{\n  'a\n###Mapping\nb'\n}\n"),
                new Row("unterminated block comment",
                        "###Pure\nClass a::A { x: String[1]; } /* never closed"),
                new Row("plain two sections",
                        "###Pure\nClass a::A { x: String[1]; }\n###Mapping\nMapping m::M ()\n")),
                0);
    }

    @Test
    void escapesAndLiterals() {
        runFamily("escapes-and-literals", List.of(
                new Row("escaped quote", "###Pure\nfunction f::f(): String[1]\n{\n  'it\\'s'\n}\n"),
                new Row("unicode escape", "###Pure\nfunction f::f(): String[1]\n{\n  'a\\u0041b'\n}\n"),
                new Row("octal escape", "###Pure\nfunction f::f(): String[1]\n{\n  'a\\101b'\n}\n"),
                new Row("unknown escape", "###Pure\nfunction f::f(): String[1]\n{\n  'a\\qb'\n}\n"),
                new Row("quoted class name unicode escape",
                        "###Pure\nClass a::'x\\u0041y' { z: String[1]; }\n"),
                new Row("quoted class name octal escape",
                        "###Pure\nClass a::'x\\101y' { z: String[1]; }\n"),
                new Row("quoted property name unicode escape",
                        "###Pure\nClass a::A { 'x\\u0041y': String[1]; }\n"),
                new Row("backslash at EOF", "###Pure\nfunction f::f(): String[1]\n{\n  'abc\\"),
                new Row("graphfetch escaped quote",
                        "###Pure\nClass a::A { name: String[1]; }\nfunction f::f(): Any[*]\n{\n  a::A.all()->graphFetch(#{a::A{name('it\\'s')}}#)\n}\n"),
                new Row("decimal leading zeros", "###Pure\nfunction f::f(): Any[*]\n{\n  007d;\n}\n"),
                new Row("bare fraction decimal", "###Pure\nfunction f::f(): Any[*]\n{\n  [.5d]\n}\n"),
                new Row("overflowing integer", "###Pure\nfunction f::f(): Any[*]\n{\n  99999999999999999999\n}\n"),
                new Row("overflow multiplicity", "###Pure\nClass a::A { x: String[1..9999999999]; }\n")),
                0);
    }

    @Test
    void duplicateFieldsRefuseOnceOnly() {
        runFamily("duplicate-fields", List.of(
                new Row("dataspace title", "###DataSpace\nDataSpace a::DS\n{\n  executionContexts: [];\n  defaultExecutionContext: '';\n  title: 'one';\n  title: 'two';\n}\n"),
                new Row("dataspace description", "###DataSpace\nDataSpace a::DS\n{\n  executionContexts: [];\n  defaultExecutionContext: '';\n  description: 'a';\n  description: 'b';\n}\n"),
                new Row("connection class", "###Connection\nJsonModelConnection a::c\n{\n  class: a::A;\n  class: a::A;\n  url: 'data:x';\n}\n"),
                new Row("connection type", "###Connection\nRelationalDatabaseConnection a::c\n{\n  store: a::db;\n  type: H2;\n  type: H2;\n  specification: LocalH2 {};\n  auth: DefaultH2;\n}\n"),
                new Row("runtime mappings", "###Runtime\nRuntime a::r\n{\n  mappings: [a::m];\n  mappings: [a::m];\n}\n"),
                new Row("profile tags", "###Pure\nProfile a::p { tags: [a]; tags: [b]; }\n"),
                new Row("missing Email address", "###DataSpace\nDataSpace a::DS\n{\n  executionContexts: [];\n  defaultExecutionContext: '';\n  supportInfo: Email {\n  };\n}\n"),
                new Row("bad port", "###Connection\nRelationalDatabaseConnection a::c\n{\n  store: a::db;\n  type: H2;\n  specification: Static { name: 'n'; host: 'h'; port: xyz; };\n  auth: DefaultH2;\n}\n")),
                0);
    }

    @Test
    void expressionEnvelope() {
        runFamily("expression-envelope", List.of(
                new Row("milestoning non-literal", "###Pure\nClass a::A { name: String[1]; }\nfunction f::f(): Any[*]\n{\n  a::A.all(now())\n}\n"),
                new Row("enum value named all", "###Pure\nEnum a::E { all, none }\nfunction f::i(): a::E[1]\n{\n  a::E.all\n}\n"),
                new Row("bracket index", "###Pure\nfunction f::h(x: String[*]): String[1]\n{\n  $x[0]\n}\n"),
                new Row("not-equal <>", "###Pure\nfunction f::f(a: Integer[1], b: Integer[1]): Boolean[1]\n{\n  $a <> $b;\n}\n"),
                new Row("arith in collection", "###Pure\nfunction f::f(): Any[*]\n{\n  [1 + 2, 3]\n}\n"),
                new Row("empty function body", "###Pure\nfunction f::f(): Integer[1]\n{\n}\n"),
                new Row("projection class", "###Pure\nClass a::P projects a::A { +[name] }\n"),
                new Row("Z-suffix datetime", "###Pure\nfunction f::f(): Any[*]\n{\n  %2024-01-01T10:00:00Z\n}\n"),
                new Row("date feb-30", "###Pure\nfunction f::f(): Any[*]\n{\n  %2024-02-30\n}\n"),
                new Row("exponent decimal", "###Pure\nfunction f::f(): Any[*]\n{\n  1e3d;\n}\n"),
                new Row("multi-stmt missing final semi", "###Pure\nfunction f::f(x: Integer[1]): Any[*]\n{\n  let y = $x;\n  $y + 1\n}\n"),
                new Row("single stmt no semi", "###Pure\nfunction f::f(x: Integer[1]): Any[*]\n{\n  $x + 1\n}\n"),
                new Row("single-line triple-quote tv", "###Pure\nProfile a::p { tags: [doc]; }\nClass {a::p.doc = '''hello'''} a::A { x: String[1]; }\n"),
                new Row("comparator expr", "###Pure\nfunction f::f(): Any[*]\n{\n  [1,2]->contains(1, comparator(a: Integer[1], b: Integer[1]): Boolean[1] { $a == $b })\n}\n"),
                new Row("quoted path segment with ::", "###Pure\nClass a::'b::c' { x: String[1]; }\n")),
                0);
    }
    @Test
    void mappingBodies() {
        String pre = "###Pure\nClass a::A { name: String[1]; qty: Integer[1]; }\n"
                + "Class a::S { src: String[1]; }\n";
        String db = "###Relational\nDatabase a::db (Table t (name VARCHAR(20), qty INT))\n";
        runFamily("mapping-bodies", List.of(
                new Row("pure mapping basic", pre
                        + "###Mapping\nMapping a::m\n(\n  a::A: Pure\n  {\n    ~src a::S\n    name: $src.src\n  }\n)\n"),
                new Row("relational mapping", pre + db
                        + "###Mapping\nMapping a::m\n(\n  a::A: Relational\n  {\n    name: [a::db]t.name\n  }\n)\n"),
                new Row("dup property mapping", pre + db
                        + "###Mapping\nMapping a::m\n(\n  a::A: Relational\n  {\n    name: [a::db]t.name,\n    name: [a::db]t.name\n  }\n)\n"),
                new Row("keyword-named property", "###Pure\nClass a::A { filter: String[1]; }\nClass a::S { src: String[1]; }\n"
                        + "###Mapping\nMapping a::m\n(\n  a::A: Pure\n  {\n    ~src a::S\n    filter: $src.src\n  }\n)\n"),
                new Row("quoted mapping id", pre + db
                        + "###Mapping\nMapping a::m\n(\n  a::A[id1]: Relational\n  {\n    name: [a::db]t.name\n  }\n)\n"),
                new Row("escaped string in pure pm", pre
                        + "###Mapping\nMapping a::m\n(\n  a::A: Pure\n  {\n    ~src a::S\n    name: 'it\\'s'\n  }\n)\n"),
                new Row("empty mapping", "###Mapping\nMapping a::m\n(\n)\n"),
                new Row("mapping missing close", "###Mapping\nMapping a::m\n(\n")),
                0);
    }

    @Test
    void databaseDdl() {
        runFamily("database-ddl", List.of(
                new Row("quoted column", "###Relational\nDatabase a::db (Table t (\"my col\" VARCHAR(20)))\n"),
                new Row("column named Table", "###Relational\nDatabase a::db (Table t (Table VARCHAR(20)))\n"),
                new Row("dup table", "###Relational\nDatabase a::db (Table t (c INT) Table t (c INT))\n"),
                new Row("join", "###Relational\nDatabase a::db (Table t (id INT) Table u (id INT)\n  Join j (t.id = u.id))\n"),
                new Row("self join {target}", "###Relational\nDatabase a::db (Table t (id INT, pid INT)\n  Join j (t.pid = {target}.id))\n"),
                new Row("filter", "###Relational\nDatabase a::db (Table t (id INT)\n  Filter f (t.id > 0))\n"),
                new Row("view", "###Relational\nDatabase a::db (Table t (id INT)\n  View v (vid: t.id))\n"),
                new Row("bad column type", "###Relational\nDatabase a::db (Table t (c NOTATYPE(3)))\n"),
                new Row("negative default-ish int", "###Relational\nDatabase a::db (Table t (c DECIMAL(10, 2)))\n"),
                new Row("schema dot", "###Relational\nDatabase a::db (Schema s (Table t (c INT)))\n")),
                0);
    }

    @Test
    void runtimeAndConnectionIslands() {
        String pre = "###Pure\nClass a::A { name: String[1]; }\n"
                + "###Relational\nDatabase a::db (Table t (name VARCHAR(20)))\n"
                + "###Mapping\nMapping a::m\n(\n  a::A: Relational\n  {\n    name: [a::db]t.name\n  }\n)\n";
        runFamily("runtime-connection", List.of(
                new Row("embedded connection island", pre
                        + "###Runtime\nRuntime a::r\n{\n  mappings: [a::m];\n  connections:\n  [\n    a::db:\n    [\n      c1: #{\n        RelationalDatabaseConnection\n        {\n          store: a::db;\n          type: H2;\n          specification: LocalH2 {};\n          auth: DefaultH2;\n        }\n      }#\n    ]\n  ];\n}\n"),
                new Row("named connection ref", pre
                        + "###Connection\nRelationalDatabaseConnection a::c\n{\n  store: a::db;\n  type: H2;\n  specification: LocalH2 {};\n  auth: DefaultH2;\n}\n"
                        + "###Runtime\nRuntime a::r\n{\n  mappings: [a::m];\n  connections:\n  [\n    a::db: [c1: a::c]\n  ];\n}\n"),
                new Row("runtime empty mappings", "###Runtime\nRuntime a::r\n{\n  mappings: [];\n}\n"),
                new Row("conn dup specification", pre
                        + "###Connection\nRelationalDatabaseConnection a::c\n{\n  store: a::db;\n  type: H2;\n  specification: LocalH2 {};\n  specification: LocalH2 {};\n  auth: DefaultH2;\n}\n")),
                0);
    }
    @Test
    void operatorPrecedenceSweep() {
        // THE 512-expression sweep (08-05 audit §1: 170/512 DIFF, invisible
        // to every gate — the corpus contains none of these shapes). Now a
        // permanent family: every 1 op 2 op 3 op 4 combination over the
        // eight operator spellings, verdict- AND byte-compared.
        String[] ops = {"+", "-", "*", "/", "<", "==", "&&", "||"};
        List<Row> rows = new ArrayList<>();
        for (String a : ops) {
            for (String b : ops) {
                for (String c : ops) {
                    String expr = "1 " + a + " 2 " + b + " 3 " + c + " 4";
                    rows.add(new Row(expr,
                            "###Pure\nfunction f::f(): Any[*]\n{\n  "
                                    + expr + ";\n}\n"));
                }
            }
        }
        runFamily("operator-precedence-sweep", rows, 0);
    }
    @Test
    void constraintClausesAndQuotedCallables() {
        // Tier-3 residue (08-05 audit R1-R4 + quoted-callable + quote-blind
        // path scanner), fixed 2026-08-13 — all oracle-verified
        String cls = "###Pure\nClass a::T { p(s: String[1]) { $s }: String[1]; }\n";
        runFamily("constraints-and-callables", List.of(
                new Row("clauses out of order", "###Pure\nClass t::C\n[\n  c1\n  (\n    ~enforcementLevel: Error\n    ~function: $this.x > 0\n  )\n]\n{ x: Integer[1]; }\n"),
                new Row("externalId after function", "###Pure\nClass t::C\n[\n  c1\n  (\n    ~function: $this.x > 0\n    ~externalId: 'ext'\n  )\n]\n{ x: Integer[1]; }\n"),
                new Row("repeated externalId", "###Pure\nClass t::C\n[\n  c1\n  (\n    ~externalId: 'a'\n    ~externalId: 'b'\n    ~function: $this.x > 0\n  )\n]\n{ x: Integer[1]; }\n"),
                new Row("empty constraints block", "###Pure\nClass t::C [ ] { x: Integer[1]; }\n"),
                new Row("full ordered clauses", "###Pure\nClass t::C\n[\n  c1\n  (\n    ~externalId: 'ext'\n    ~function: $this.x > 0\n    ~enforcementLevel: Warn\n    ~message: 'bad ' + $this.x->toString()\n  )\n]\n{ x: Integer[1]; }\n"),
                new Row("quoted callable prefix", "###Pure\nfunction f::f(x: Integer[1]): Any[*]\n{\n  'meta::pure::functions::math::abs'($x)\n}\n"),
                new Row("quoted callable arrow", "###Pure\nfunction f::f(x: Integer[1]): Any[*]\n{\n  $x->'meta::pure::functions::math::abs'()\n}\n"),
                new Row("path arg comma in string", cls + "function f::f(): Any[*]\n{\n  #/a::T/p('a,b')#\n}\n"),
                new Row("path arg slash in string", cls + "function f::f(): Any[*]\n{\n  #/a::T/p('a/b')#\n}\n")),
                0);
    }
}
