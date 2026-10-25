package com.legend.integration;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.json.Json;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.DataDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.ImportScope;
import com.legend.model.RelationalDataType;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.Protocol;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.setup.CsvSeed;
import com.legend.test.TestAssertions;
import com.legend.values.PureDateLiteral;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE STRESS CORPUS ON DEEPHAVEN, AGAINST THE ORACLE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, task 15): every
 * {@code stress::} service test over the corpus's store and its test data, planned on a Deephaven runtime, run on a
 * Deephaven server, and judged as the stress suites judge it ({@link StressSuites}: the referee's {@code EqualToJson}
 * expectation, {@link TestAssertions#equalToJson}). Lite sends a plan to Deephaven only from the browser tab (no sender
 * of Java's: docs/DEEPHAVEN_PLAN_2026_10_22.md L3s), so the run is in three steps, this tool the first and the last:
 * <ul>
 *   <li>{@code cases OUT}: {@code cases.json} -- each test, its plan on a runtime whose store is a Deephaven server's
 *       (the service's mappings, its store bound to a Deephaven connection), the plan's result type and the test's
 *       expected JSON; or why it has none (its plan refused, its data not the corpus's own) -- and {@code tables.json},
 *       the corpus's test data: each table its declared columns' Deephaven kinds and its rows' text, CSV read as the
 *       stress suites read it ({@link CsvSeed});</li>
 *   <li>the browser lane (datacube/test/deephaven/corpus.mjs) seeds a Deephaven server with the tables, sends each
 *       plan, and writes each answer's rows, or the server's refusal, one JSON line a test;</li>
 *   <li>{@code judge CASES RESULTS OUT}: each answer's rows, each value as a JDBC result's would be serialized (a
 *       timestamp the engine's nine digits, a date, a number, a boolean, a string), judged against its expectation; the
 *       verdicts' ledgers and their counts, failures by reason.</li>
 * </ul>
 * The services this does not take, by reason: a multi-execution service, one over another store or with test data of
 * its own (the server holds the corpus's one store and its one data), a test whose expectation is not JSON.
 */
final class StressOnDeephaven {

    private StressOnDeephaven() {}

    /** The corpus's store, and its test data: the tables the server holds. */
    static final String STORE = "store::DB";
    static final String DATA = "stress::TestData";

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("cases")) {
            cases(Path.of(args[1]));
        } else if (args.length == 4 && args[0].equals("judge")) {
            System.exit(judge(Path.of(args[1]), Path.of(args[2]), Path.of(args[3])));
        } else {
            throw new IllegalArgumentException("usage: cases OUT | judge CASES RESULTS OUT");
        }
    }

    // ---- CASES ------------------------------------------------------------------------------------------------------

    static void cases(Path out) throws Exception {
        Files.createDirectories(out);
        var module = Compiler.parseSources(StressCorpus.sources(List.of()));
        ModelContext ctx = Compiler.buildModel(module.model());
        List<ServiceDefinition> services = module.model().elements().stream()
                .filter(el -> el instanceof ServiceDefinition svc && svc.qualifiedName().startsWith("stress::")
                        && svc.testSuites() != null)
                .map(el -> (ServiceDefinition) el)
                .sorted(Comparator.comparing(ServiceDefinition::qualifiedName))
                .toList();
        Map<String, ModelContext> overlays = new LinkedHashMap<>();
        List<Object> cases = new ArrayList<>();
        Map<String, Integer> without = new TreeMap<>();
        int planned = 0;
        for (ServiceDefinition svc : services) {
            for (Protocol.PServiceTestSuite suite : svc.testSuites()) {
                for (Protocol.PServiceTestSuite.PSuiteTest test : suite.tests()) {
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("id", svc.qualifiedName() + " / " + suite.id() + " / " + test.id());
                    try {
                        String runtime = runtimeOf(ctx, svc, suite);
                        ModelContext on = overlays.computeIfAbsent(runtime, r -> onDeephaven(ctx, r));
                        List<String> expected = new ArrayList<>();
                        for (Protocol.PTestAssertion a : test.assertions()) {
                            if (!(a.expected() instanceof Protocol.PExternalFormatData ef)
                                    || !"application/json".equalsIgnoreCase(ef.contentType())) {
                                throw new Without("an expectation that is not JSON (" + a.id() + ")");
                            }
                            expected.add(ef.data());
                        }
                        List<ValueSpecification> statements = new ArrayList<>();
                        if (test.parameters() != null) {
                            for (Protocol.PServiceTestSuite.PSuiteParam p : test.parameters()) {
                                statements.add(new AppliedFunction("letFunction", List.of(new CString(p.name()), p.value())));
                            }
                        }
                        statements.addAll(body(svc.functionBody()));
                        com.legend.plan.QueryPlan plan;
                        try {
                            ValueSpecification resolved = Compiler.resolveQuery(statements, new ImportScope(List.of()), on);
                            plan = Compiler.queryResolved(on, resolved).plan(runtime + DEEPHAVEN);
                        } catch (RuntimeException e) {
                            throw new Without("planned: " + reason(e));
                        }
                        if (plan.shape() != com.legend.plan.ResultShape.TABULAR) {
                            throw new Without("a " + plan.shape() + " result");
                        }
                        // a tabular result's rows as the stress suites serialize them (ServiceTestRunner.serialize)
                        String format = test.serializationFormat() == null ? "DEFAULT" : test.serializationFormat();
                        if (!format.equals("PURE_TDSOBJECT") && !format.equals("RAW")) {
                            throw new Without("serialization format " + format);
                        }
                        c.put("sql", plan.sql());
                        Object type = com.legend.plan.UpstreamRelationType.of(plan.rootType());
                        c.put("type", type);
                        // the columns of an enumeration of the model, which the tab reads as their names' text
                        List<String> enumerations = new ArrayList<>();
                        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"fullPath\":\"([^\"]+)\"")
                                .matcher(Json.toCompact(type));
                        while (m.find()) {
                            if (on.findEnum(m.group(1)).isPresent() && !enumerations.contains(m.group(1))) {
                                enumerations.add(m.group(1));
                            }
                        }
                        c.put("enumerations", enumerations);
                        c.put("expected", expected);
                        planned++;
                    } catch (Without w) {
                        c.put("without", w.getMessage());
                        without.merge(bucket(w.getMessage()), 1, Integer::sum);
                    }
                    cases.add(c);
                }
            }
        }
        Files.writeString(out.resolve("cases.json"), Json.toCompact(cases), StandardCharsets.UTF_8);
        Files.writeString(out.resolve("tables.json"), Json.toCompact(tables(ctx)), StandardCharsets.UTF_8);
        System.out.printf("[deephaven] %d tests, %d planned on Deephaven; without a plan:%n", cases.size(), planned);
        without.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(e -> System.out.printf("[deephaven]   %5d  %s%n", e.getValue(), e.getKey()));
    }

    /** What a runtime's Deephaven twin is named: the runtime's, suffixed. */
    static final String DEEPHAVEN = "$deephaven";

    /** The model with `runtime`'s Deephaven twin beside it: its mappings, the corpus's store bound to a Deephaven
     *  server's connection (where the plan is sent is the sender's: no address of the plan's own). */
    private static ModelContext onDeephaven(ModelContext ctx, String runtime) {
        RuntimeDefinition r = ctx.findRuntime(runtime).orElseThrow();
        String name = runtime + DEEPHAVEN;
        ConnectionDefinition conn = new ConnectionDefinition(name + "$conn", STORE,
                ConnectionDefinition.DatabaseType.Deephaven, new ConnectionSpecification.Deephaven("localhost", 10000, true),
                new AuthenticationSpec.PreSharedKey("deephaven.psk"));
        Map<String, String> ids = new LinkedHashMap<>(r.connectionIds());
        RuntimeDefinition twin = new RuntimeDefinition(name, r.mappings(), Map.of(STORE, List.of(conn.qualifiedName())),
                List.of(), List.of(), ids);
        return ((com.legend.compiler.element.PureModelContext) ctx).withExecutionOverlay(twin, conn);
    }

    /** The service's runtime, when its test reads the corpus's store with the corpus's own data; else why not. */
    private static String runtimeOf(ModelContext ctx, ServiceDefinition svc, Protocol.PServiceTestSuite suite) {
        if (svc.runtimeRef() == null) {
            throw new Without(svc.multiExecution() != null ? "a multi-execution service" : "no runtime");
        }
        RuntimeDefinition runtime = ctx.findRuntime(svc.runtimeRef()).orElseThrow(
                () -> new Without("runtime " + svc.runtimeRef() + " not in the model"));
        if (!runtime.connectionBindings().keySet().equals(java.util.Set.of(STORE))) {
            throw new Without("a runtime over another store than " + STORE);
        }
        Protocol.PServiceTestSuite.PSuiteData data = suite.testData();
        if (data == null || data.serviceTestData() != null && !data.serviceTestData().isEmpty()) {
            throw new Without("test data of its own");
        }
        for (Protocol.PServiceTestSuite.PSuiteConnData cd : data.connectionsTestData()) {
            if (!(cd.data() instanceof Protocol.PDataReference ref) || !ref.dataElement().path().equals(DATA)) {
                throw new Without("test data of its own");
            }
        }
        return runtime.qualifiedName();
    }

    private static List<ValueSpecification> body(ValueSpecification fb) {
        return fb instanceof LambdaFunction lf && lf.parameters().isEmpty() ? lf.body() : List.of(fb);
    }

    /** The corpus's test data as the server's tables: each its declared columns' Deephaven kinds (a column the store
     *  does not declare is text) and its rows, each cell its text or null, read as the stress suites read them. */
    private static List<Object> tables(ModelContext ctx) {
        DataDefinition data = ctx.findData(DATA).orElseThrow();
        if (!(data.body().value() instanceof Protocol.PRelationalCsvData csv)) {
            throw new IllegalStateException(DATA + " is not relational CSV data");
        }
        List<Object> out = new ArrayList<>();
        for (Protocol.PRelationalCsvTable t : csv.tables()) {
            String block = t.schema() + "\n" + t.table() + "\n" + java.util.Objects.requireNonNull(t.values());
            var def = ctx.findTableDefinition(STORE, "default".equals(t.schema()) ? t.table() : t.schema() + "." + t.table());
            Map<String, String> kinds = new LinkedHashMap<>();
            def.ifPresent(d -> d.columns().forEach(col -> kinds.put(col.name(), kind(col.dataType()))));
            // its rows as the stress suites read them; a table of a header alone (a union leg with no rows) has none
            com.legend.setup.RowLoad load = null;
            for (CsvSeed.Step step : CsvSeed.steps(block, STORE, ctx, com.legend.database.Databases.dialect(
                    com.legend.database.Databases.PLATFORM))) {
                if (step instanceof CsvSeed.Step.Rows rows) {
                    load = rows.load();
                }
            }
            List<String> names = load != null ? load.columns()
                    : java.util.Arrays.stream(t.values().split("\n", 2)[0].split(",")).map(String::strip).toList();
            Map<String, Object> table = new LinkedHashMap<>();
            table.put("table", t.table());
            List<Object> columns = new ArrayList<>();
            for (String name : names) {
                columns.add(Map.of("name", name, "kind", kinds.getOrDefault(name, "String")));
            }
            // the declared columns the CSV leaves out: every one, null
            def.ifPresent(d -> d.columns().stream().filter(col -> !names.contains(col.name()))
                    .forEach(col -> columns.add(Map.of("name", col.name(), "kind", kind(col.dataType())))));
            table.put("columns", columns);
            table.put("rows", load != null ? load.rows() : List.of());
            out.add(table);
        }
        return out;
    }

    /** A declared column's Deephaven kind: the column type its values are held in (the Deephaven dialect's DDL). */
    private static String kind(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.BigInt b -> "long";
            case RelationalDataType.SmallInt s -> "int";
            case RelationalDataType.TinyInt s -> "int";
            case RelationalDataType.Integer_ i -> "int";
            case RelationalDataType.Float_ f -> "double";
            case RelationalDataType.Double_ d -> "double";
            case RelationalDataType.Real r -> "double";
            case RelationalDataType.Bit b -> "Boolean";
            case RelationalDataType.Decimal d -> "BigDecimal";
            case RelationalDataType.Numeric n -> "BigDecimal";
            case RelationalDataType.Timestamp ts -> "Instant";
            case RelationalDataType.TimestampNs ts -> "Instant";
            case RelationalDataType.Date_ d -> "LocalDate";
            case RelationalDataType.Time tm -> "LocalTime";
            case RelationalDataType.Varchar v -> "String";
            case RelationalDataType.Char_ c -> "String";
            case RelationalDataType.Binary b -> "String";
            case RelationalDataType.Varbinary v -> "String";
            case RelationalDataType.Distinct d -> "String";
            case RelationalDataType.Other o -> "String";
            case RelationalDataType.SemiStructured s -> "String";
            case RelationalDataType.Array a -> "String";
            case RelationalDataType.Object_ o -> "String";
        };
    }

    // ---- JUDGE ------------------------------------------------------------------------------------------------------

    /**
     * Each answer judged: its rows, each value as a JDBC result's is serialized ({@code ServiceTestRunner}'s cells: a
     * timestamp the engine's nine subsecond digits, a date its day), against each expectation. Writes the pass and fail
     * ledgers and prints the counts, failures by reason. Answers 0; the lane holds the counts to its floor.
     */
    static int judge(Path casesPath, Path resultsPath, Path out) throws Exception {
        Files.createDirectories(out);
        Map<String, Json.Obj> cases = new LinkedHashMap<>();
        for (Json.Node n : ((Json.Arr) Json.parse(Files.readString(casesPath))).items()) {
            Json.Obj c = (Json.Obj) n;
            cases.put(((Json.Str) c.get("id")).value(), c);
        }
        Map<String, Json.Obj> answers = new LinkedHashMap<>();
        try (BufferedReader r = Files.newBufferedReader(resultsPath)) {
            for (String line; (line = r.readLine()) != null; ) {
                if (!line.isBlank()) {
                    Json.Obj a = Json.parseObject(line);
                    answers.put(((Json.Str) a.get("id")).value(), a);
                }
            }
        }
        List<String> pass = new ArrayList<>();
        List<String> fail = new ArrayList<>();
        List<String> without = new ArrayList<>();
        Map<String, Integer> buckets = new TreeMap<>();
        for (var e : cases.entrySet()) {
            String id = e.getKey();
            Json.Obj c = e.getValue();
            if (c.getOr("without", null) instanceof Json.Str why) {
                without.add(id + " :: " + why.value());
                continue;
            }
            Json.Obj answer = answers.get(id);
            String verdict;
            if (answer == null) {
                verdict = "unanswered: the lane sent it no plan or read no rows";
            } else if (answer.getOr("error", null) instanceof Json.Str err) {
                verdict = "the server: " + err.value();
            } else {
                Object actual = rows((Json.Arr) answer.get("columns"), (int) ((Json.Num) answer.get("rowCount")).longValue());
                verdict = null;
                for (Json.Node x : ((Json.Arr) c.get("expected")).items()) {
                    String diff = TestAssertions.equalToJson(com.legend.sql.Json.parse(((Json.Str) x).value()), actual);
                    if (diff != null) {
                        verdict = "matchesOracle: " + diff;
                        break;
                    }
                }
            }
            if (verdict == null) {
                pass.add(id);
            } else {
                fail.add(id + " :: " + verdict);
                buckets.merge(bucket(verdict), 1, Integer::sum);
            }
        }
        Files.write(out.resolve("deephaven-pass.txt"), pass);
        Files.write(out.resolve("deephaven-fail.txt"), fail);
        Files.write(out.resolve("deephaven-without.txt"), without);
        System.out.printf("[deephaven] pass=%d fail=%d without a plan=%d of %d tests%n", pass.size(), fail.size(),
                without.size(), cases.size());
        buckets.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(b -> System.out.printf("[deephaven] FAIL %5d  %s%n", b.getValue(), b.getKey()));
        Files.writeString(out.resolve("deephaven-counts.json"), Json.toCompact(Map.of("pass", pass.size(),
                "fail", fail.size(), "without", without.size(), "tests", cases.size())));
        return 0;
    }

    /** An answer's rows as a tabular result serializes them: a list of rows, each a column's name to its value. */
    private static List<Object> rows(Json.Arr columns, int count) {
        List<Object> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Json.Node n : columns.items()) {
                Json.Obj col = (Json.Obj) n;
                Json.Node v = ((Json.Arr) col.get("values")).items().get(i);
                row.put(((Json.Str) col.get("name")).value(), cell(v, ((Json.Str) col.get("type")).value()));
            }
            out.add(row);
        }
        return out;
    }

    /** A value read from Deephaven (the tab's exact text for a temporal or a decimal, a bigint's digits) as a JDBC
     *  result's is serialized, by its column's Pure type. */
    static @com.legend.base.Nullable Object cell(Json.Node v, String type) {
        if (v instanceof Json.Null) {
            return null;
        }
        String text = switch (v) {
            case Json.Str s -> s.value();
            case Json.Num x -> x.isInteger() ? Long.toString(x.longValue())
                    : x.decimalValue() != null ? x.decimalValue().toPlainString()
                    : x.token() != null ? x.token() : Double.toString(x.doubleValue());
            case Json.Bool b -> String.valueOf(b.value());
            default -> v.toString();
        };
        return switch (type) {
            case "Integer" -> new BigDecimal(text).longValueExact();
            case "Float", "Number" -> Double.parseDouble(text);
            case "Decimal" -> new BigDecimal(text);
            case "Boolean" -> Boolean.parseBoolean(text);
            case "StrictDate" -> PureDateLiteral.fromLocalDate(java.time.LocalDate.parse(text.substring(0, 10))).toEngineJson();
            case "DateTime", "Date" -> PureDateLiteral.fromDatabaseTimestamp(instant(text)).toEngineJson();
            default -> text;
        };
    }

    /** A timestamp's text as the tab reads it ({@code 2024-06-03 09:30:00[.nnn]}, or ISO with a {@code Z}) as UTC. */
    private static java.time.LocalDateTime instant(String text) {
        String t = text.replace(' ', 'T');
        if (t.endsWith("Z")) {
            return java.time.LocalDateTime.ofInstant(java.time.Instant.parse(t), java.time.ZoneOffset.UTC);
        }
        return java.time.LocalDateTime.parse(t);
    }

    // ---- reasons ----------------------------------------------------------------------------------------------------

    /** A test this run does not take, and why. */
    private static final class Without extends RuntimeException {
        Without(String why) {
            super(why);
        }
    }

    private static String reason(Throwable e) {
        String m = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return m.length() > 300 ? m.substring(0, 300) : m;
    }

    /** A reason with its particulars masked: its digits, its quoted names -- so failures of one kind count together. */
    static String bucket(String reason) {
        // a row as the judge writes it ({col=value, ...}, cut short with "..."), and the values a difference names
        String r = reason.replaceAll("\\{[^}]*?(\\}|\\.\\.\\.)", "{…}")
                .replaceAll("expected [^,()]*, got [^,()]*", "expected …, got …")
                .replaceAll("'[^']*'", "'…'").replaceAll("\"[^\"]*\"", "\"…\"").replaceAll("\\d+", "N");
        return r.length() > 140 ? r.substring(0, 140) : r;
    }
}
