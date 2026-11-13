// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.legend.PlannerSurfaceTest.Kind.AUTHORED_SQL;
import static com.legend.PlannerSurfaceTest.Kind.CALLBACK;
import static com.legend.PlannerSurfaceTest.Kind.MODEL;
import static com.legend.PlannerSurfaceTest.Kind.OUTPUT;
import static com.legend.PlannerSurfaceTest.Kind.SESSION;
import static com.legend.PlannerSurfaceTest.Kind.CATALOG_ROWS;
import static com.legend.PlannerSurfaceTest.Kind.DATA_REFERENCE;
import static com.legend.PlannerSurfaceTest.Kind.HANDLE;
import static com.legend.PlannerSurfaceTest.Kind.HOST_OPERATIONS;
import static com.legend.PlannerSurfaceTest.Kind.INTERNAL;
import static com.legend.PlannerSurfaceTest.Kind.LAMBDA;
import static com.legend.PlannerSurfaceTest.Kind.MESSAGE;
import static com.legend.PlannerSurfaceTest.Kind.MODEL_TEXT;
import static com.legend.PlannerSurfaceTest.Kind.NAME;
import static com.legend.PlannerSurfaceTest.Kind.OBJECT;
import static com.legend.PlannerSurfaceTest.Kind.OPTION;
import static com.legend.PlannerSurfaceTest.Kind.PLAN_DATA;
import static com.legend.PlannerSurfaceTest.Kind.REFUSED;
import static com.legend.PlannerSurfaceTest.Kind.REQUEST;
import static com.legend.PlannerSurfaceTest.Kind.ROUTE;
import static com.legend.PlannerSurfaceTest.Kind.ROUTE_BODY;
import static com.legend.PlannerSurfaceTest.Kind.RUN_ROWS;
import static com.legend.PlannerSurfaceTest.Kind.VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE CLOSED PLANNER SURFACE (the next PR's item 1, "separation made foolproof", 2026-11-13;
 * docs/TOTAL_WALL_START_HERE_2026_11_11.md §6.1): every door a host plans through -- the boundary's
 * ({@code planner.Boundary}), the tab's exports ({@code planner.TabExports}) and the native library's
 * ({@code com.legend.nativelib}) -- is listed here with each request field's declared {@link Kind}. A door that is not
 * listed, a field a door reads that no row declares, a declared field it no longer reads, and a field that can carry
 * SQL -- the playground's {@code authored.text} alone excepted -- each fail the build, by name.
 *
 * <p>The boundary's doors are read by reflection (its public statics); each request door's fields from its source:
 * the keys its {@code only(...)} admits and every key it reads (a {@code getX("k")}, a {@code has("k")}, a reader
 * {@code f(o, "k")}), in its own body and the boundary's private helpers it calls, transitively. The exports are read
 * from their sources: each {@code @JSExport} and {@code @CEntryPoint} delegates to the one listed door its row names,
 * or is a listed native service that plans nothing. {@link #theSurfaceRefusesAPlantedDoor} and
 * {@link #theSurfaceRefusesAPlantedExport} plant a violator of each kind and see it named.
 *
 * <p>The server ({@link #ROUTES}, {@link #SERVER_FIELDS}, {@link #HTTP_ROUTES}) and the root doors
 * ({@link #ROOT_DOORS}: every public method of {@code Compiler}, {@code TypedQuery}, {@code HeldModels},
 * {@code Execution} and {@code TestExecution}, by signature, a kind per parameter) are closed the same way, and so is
 * Python ({@link #PY_KINDS}: each planner kind a listed native export's, its fields its door's parameters; every native
 * name the package spells; {@link #BRIDGE_CALLS}: the hosted DataCube's calls, a statement's passing
 * {@code across_the_bridge}).
 */
@Tag("census")
class PlannerSurfaceTest {

    /** What a request field carries -- every kind a value the planner reads, never a statement it sends as written,
     *  but {@link #AUTHORED_SQL}. */
    enum Kind {
        /** A Pure model's text (or texts), compiled by the planner. */
        MODEL_TEXT,
        /** A Pure query -- text or the protocol's lambda JSON -- compiled by the planner. */
        LAMBDA,
        /** An identifier (a database type, a runtime, a schema, a table, a column, a view): spelled by the planner as a
         *  name, quoted, never as SQL. */
        NAME,
        /** Data the plan loads, by reference: a file by the name the host keeps it under, its digest, a URL, a held
         *  value's name -- resolved and delivered by exec. */
        DATA_REFERENCE,
        /** A database's own catalog answer (columns, types), read by the dialect's catalog reader into types; an
         *  unknown type is refused or excluded by name, never spliced. */
        CATALOG_ROWS,
        /** A plan the planner wrote, handed back to it (an answer's plan half). */
        PLAN_DATA,
        /** The rows a host's database answered, decoded by the plan's columns and written by the answer's writer. */
        RUN_ROWS,
        /** A value bound or read as data: a count, a range, a knob's values, an instant. */
        VALUE,
        /** One of a closed set of words or a flag, refused by name when unknown. */
        OPTION,
        /** A held model's handle. */
        HANDLE,
        /** A host's refusal message, written into an answer. */
        MESSAGE,
        /** A JSON object whose fields are declared beside it. */
        REQUEST,
        /** A nested object of a request, its fields declared beside it. */
        OBJECT,
        /** A listed pure/v1 route's path (the server's routes: the item's next commit). */
        ROUTE,
        /** A listed pure/v1 route's body (the server's routes: the item's next commit). */
        ROUTE_BODY,
        /** A host's named operations, each closed by its {@code op} ({@code HostOperationsJson}), their fields declared
         *  in {@link #HOST_OPERATION_FIELDS}. */
        HOST_OPERATIONS,
        /** A key the door reads of the planner's own intermediate JSON, never of the request. */
        INTERNAL,
        /** A key read only to refuse it by name. */
        REFUSED,
        /** A model already parsed or compiled (a root door's: {@code ModelContext}, {@code ParsedModel}, the protocol's
         *  model). */
        MODEL,
        /** A caller's function the door calls back with a planning-side value (a held model, a parser's warnings, a
         *  test's verdicts): never a statement. */
        CALLBACK,
        /** A run's database sessions, the caller's (a run door's). */
        SESSION,
        /** Where a run's answer is written: a format and a stream, data. */
        OUTPUT,
        /** A person's own statement: Studio's SQL playground, {@code authored.text}, alone. */
        AUTHORED_SQL
    }

    /** A door's parameter: its name (the registry's: no class file keeps one), its type, its kind. */
    record Param(String name, Class<?> type, Kind kind) {
    }

    /** A door: its parameters in order, and the fields of its request -- every key it admits or reads. */
    record Door(String name, List<Param> params, Map<String, Kind> fields) {
    }

    private static Param p(String name, Kind kind) {
        return new Param(name, String.class, kind);
    }

    private static Param handle() {
        return new Param("handle", int.class, HANDLE);
    }

    private static Door door(String name, Param... params) {
        return new Door(name, List.of(params), Map.of());
    }

    private static Door door(String name, Param request, Map<String, Kind> fields) {
        return new Door(name, List.of(request), new TreeMap<>(fields));
    }

    private static Map<String, Kind> fields(Object... nameKind) {
        Map<String, Kind> out = new LinkedHashMap<>();
        for (int i = 0; i < nameKind.length; i += 2) {
            out.put((String) nameKind[i], (Kind) nameKind[i + 1]);
        }
        return out;
    }

    /** A table's catalog as a request carries it: the box's tables and the target, each its columns' catalog rows. */
    private static final Object[] CATALOG = {"columns", CATALOG_ROWS, "dataType", CATALOG_ROWS, "logicalType",
            CATALOG_ROWS, "notNull", CATALOG_ROWS, "precision", CATALOG_ROWS, "scale", CATALOG_ROWS, "name", NAME,
            "table", NAME, "schema", NAME, "convertible", OPTION};

    private static Object[] with(Object[] base, Object... more) {
        Object[] out = Arrays.copyOf(base, base.length + more.length);
        System.arraycopy(more, 0, out, base.length, more.length);
        return out;
    }

    /** THE BOUNDARY'S DOORS, each listed with its fields' kinds (2026-11-13). */
    static final Map<String, Door> BOUNDARY = listed(
            door("pureV1", p("path", ROUTE), p("rawQuery", OPTION), p("body", ROUTE_BODY)),
            door("executePlan", p("body", ROUTE_BODY), p("modelsJson", MODEL_TEXT)),
            door("executeJsonPlan", p("body", ROUTE_BODY), p("modelsJson", MODEL_TEXT)),
            door("executeJsonAnswer", p("plan", PLAN_DATA), p("rows", RUN_ROWS)),
            door("executeArrowAnswer", p("plan", PLAN_DATA), p("rows", RUN_ROWS)),
            door("refusal", p("message", MESSAGE)),
            door("planSql", p("model", MODEL_TEXT), p("query", LAMBDA), p("runtime", NAME)),
            door("plan", p("model", MODEL_TEXT), p("query", LAMBDA), p("runtime", NAME)),
            door("relationType", p("model", MODEL_TEXT), p("query", LAMBDA)),
            door("planJson", p("model", MODEL_TEXT), p("lambdaJson", LAMBDA), p("runtime", NAME)),
            door("holdModel", p("model", MODEL_TEXT)),
            door("releaseModel", handle()),
            door("planHeld", handle(), p("query", LAMBDA), p("runtime", NAME)),
            door("planJsonHeld", handle(), p("lambdaJson", LAMBDA), p("runtime", NAME)),
            door("executionPlanJsonHeld", handle(), p("lambdaJson", LAMBDA), p("runtime", NAME)),
            door("relationTypeJsonHeld", handle(), p("lambdaJson", LAMBDA)),
            door("relationTypeHeld", handle(), p("query", LAMBDA)),
            // a host's file read into its database: the file by the name the host keeps it under, its digest
            door("fileLoad", p("request", REQUEST), fields("databaseType", NAME, "file", OBJECT, "name",
                    DATA_REFERENCE, "digest", DATA_REFERENCE, "size", VALUE, "format", OPTION, "into", OBJECT,
                    "table", NAME, "model", MODEL_TEXT, "database", NAME, "schema", NAME)),
            // a remote file mounted: names and fields only, an address with no token
            door("remoteMount", p("request", REQUEST), fields("databaseType", NAME, "view", NAME, "location", OBJECT,
                    "url", DATA_REFERENCE, "bearer", DATA_REFERENCE, "shown", DATA_REFERENCE, "format", OPTION,
                    "credentials", OBJECT, "name", DATA_REFERENCE, "fields", NAME)),
            door("snapStream", p("request", REQUEST), fields("databaseType", NAME, "plane", NAME, "model", MODEL_TEXT,
                    "runtime", NAME, "lambda", LAMBDA, "schema", NAME, "table", NAME)),
            door("snapCopy", p("request", REQUEST), fields("databaseType", NAME, "model", MODEL_TEXT, "runtime", NAME,
                    "lambda", LAMBDA, "schema", NAME, "table", NAME)),
            // a table Output: the box's tables and the target as their catalogs answer them; model, runtime, source
            // and body read back from the planner's own model of them
            door("outputPlan", p("request", REQUEST), fields(with(CATALOG, "databaseType", NAME, "tables",
                    CATALOG_ROWS, "target", CATALOG_ROWS, "output", OBJECT, "mode", OPTION, "keys", NAME, "rows",
                    LAMBDA, "pkg", NAME, "connection", NAME, "model", INTERNAL, "runtime", INTERNAL, "source",
                    INTERNAL, "body", INTERNAL))),
            // THE PLAYGROUND: a person's own statement, carried as written (Authored)
            door("authored", p("request", REQUEST), fields("databaseType", NAME, "text", AUTHORED_SQL)),
            door("synthesize", p("request", REQUEST), fields("databaseType", NAME, "model", MODEL_TEXT, "database",
                    NAME, "schema", NAME, "table", NAME, "count", VALUE, "columns", OBJECT, "values", VALUE, "range",
                    VALUE, "scale", VALUE)),
            door("frameLoad", p("request", REQUEST), fields("databaseType", NAME, "held", DATA_REFERENCE, "as", NAME,
                    "live", OPTION, "schema", NAME, "relation", NAME)),
            door("databaseFromCatalog", p("catalogJson", REQUEST), fields(with(CATALOG, "databaseType", NAME, "path",
                    DATA_REFERENCE, "body", INTERNAL))),
            door("tableModel", p("tableJson", REQUEST), fields(with(CATALOG, "databaseType", NAME, "tables",
                    CATALOG_ROWS, "pkg", NAME, "connection", NAME, "body", INTERNAL))),
            door("catalogColumnsSql", p("schema", NAME), p("table", NAME)),
            door("sessionSetup", p("databaseType", NAME)),
            door("hostOperations", p("databaseType", NAME), p("operationsJson", HOST_OPERATIONS)),
            door("projectData", p("model", MODEL_TEXT), p("databaseType", NAME)),
            door("warmModel", p("model", MODEL_TEXT)),
            door("touchPrelude"),
            door("touchSystemMetamodel"),
            door("hashBootSource"),
            door("resolveBootLayer"),
            door("zoneProbe", p("utcIso", VALUE), p("zone", NAME)));

    /** THE HOST OPERATIONS' FIELDS ({@code HostOperationsJson}: every operation closed by its {@code op}). */
    static final Map<String, Kind> HOST_OPERATION_FIELDS = new TreeMap<>(fields("op", OPTION, "kind", OPTION,
            "target", OBJECT, "replace", OPTION, "from", OBJECT, "relation", OBJECT, "database", NAME, "schema", NAME,
            "name", NAME, "declared", OPTION, "names", NAME, "held", DATA_REFERENCE, "folder", DATA_REFERENCE, "url",
            DATA_REFERENCE, "lists", DATA_REFERENCE, "step", OPTION, "convert", CATALOG_ROWS, "dataType", CATALOG_ROWS,
            "logicalType", CATALOG_ROWS, "precision", CATALOG_ROWS, "scale", CATALOG_ROWS, "notNull", CATALOG_ROWS,
            // a host's file and a remote file are planned loads, a pull a planned query: each refused by name
            "file", REFUSED, "remote", REFUSED, "query", REFUSED));

    /** THE TAB'S EXPORTS: each the one boundary door it delegates to. */
    static final Map<String, String> TAB_EXPORTS = new TreeMap<>(Map.ofEntries(
            Map.entry("pureV1OrError", "pureV1"), Map.entry("executeJsonPlanOrError", "executeJsonPlan"),
            Map.entry("executeJsonAnswerOrError", "executeJsonAnswer"),
            Map.entry("executeArrowAnswerOrError", "executeArrowAnswer"), Map.entry("plan", "planSql"),
            Map.entry("planOrError", "plan"), Map.entry("relationTypeOrError", "relationType"),
            Map.entry("planJsonOrError", "planJson"), Map.entry("holdModelOrError", "holdModel"),
            Map.entry("releaseModelOrError", "releaseModel"), Map.entry("planHeldOrError", "planHeld"),
            Map.entry("planJsonHeldOrError", "planJsonHeld"),
            Map.entry("executionPlanJsonHeldOrError", "executionPlanJsonHeld"),
            Map.entry("relationTypeHeldOrError", "relationTypeHeld"), Map.entry("fileLoadOrError", "fileLoad"),
            Map.entry("remoteMountOrError", "remoteMount"), Map.entry("snapStreamOrError", "snapStream"),
            Map.entry("snapCopyOrError", "snapCopy"), Map.entry("outputPlanOrError", "outputPlan"),
            Map.entry("authoredOrError", "authored"), Map.entry("synthesizeOrError", "synthesize"),
            Map.entry("databaseFromCatalogOrError", "databaseFromCatalog"),
            Map.entry("tableModelOrError", "tableModel"), Map.entry("catalogColumnsSqlOrError", "catalogColumnsSql"),
            Map.entry("sessionSetupOrError", "sessionSetup"), Map.entry("hostOperationsOrError", "hostOperations"),
            Map.entry("projectDataOrError", "projectData"), Map.entry("warmModel", "warmModel"),
            Map.entry("warmModelOrError", "warmModel"), Map.entry("touchPrelude", "touchPrelude"),
            Map.entry("touchSystemMetamodel", "touchSystemMetamodel"), Map.entry("hashBootSource", "hashBootSource"),
            Map.entry("resolveBootLayer", "resolveBootLayer"), Map.entry("zoneProbe", "zoneProbe")));

    /** THE NATIVE LIBRARY'S EXPORTS: each the one boundary door it delegates to. */
    static final Map<String, String> NATIVE_EXPORTS = new TreeMap<>(Map.ofEntries(
            Map.entry("lite_plan_json", "planJson"), Map.entry("lite_plan_text", "plan"),
            Map.entry("lite_hold_model", "holdModel"), Map.entry("lite_release_model", "releaseModel"),
            Map.entry("lite_plan_json_held", "planJsonHeld"),
            Map.entry("lite_execution_plan_json_held", "executionPlanJsonHeld"),
            Map.entry("lite_relation_type_held", "relationTypeJsonHeld"), Map.entry("lite_plan_text_held", "planHeld"),
            Map.entry("lite_relation_type_text_held", "relationTypeHeld"), Map.entry("lite_file_load", "fileLoad"),
            Map.entry("lite_remote_mount", "remoteMount"), Map.entry("lite_snap_stream", "snapStream"),
            Map.entry("lite_snap_copy", "snapCopy"), Map.entry("lite_output_plan", "outputPlan"),
            Map.entry("lite_synthesize", "synthesize"), Map.entry("lite_frame_load", "frameLoad"),
            Map.entry("lite_database_from_catalog", "databaseFromCatalog"),
            Map.entry("lite_table_model", "tableModel"), Map.entry("lite_catalog_columns_sql", "catalogColumnsSql"),
            Map.entry("lite_session_setup", "sessionSetup"), Map.entry("lite_host_operations", "hostOperations"),
            Map.entry("lite_project_data", "projectData"), Map.entry("lite_pure_v1", "pureV1"),
            Map.entry("lite_execute_plan", "executePlan"), Map.entry("lite_execute_json_plan", "executeJsonPlan"),
            Map.entry("lite_execute_json_answer", "executeJsonAnswer"),
            Map.entry("lite_execute_arrow_answer", "executeArrowAnswer"), Map.entry("lite_warm_model", "warmModel"),
            Map.entry("lite_pure_v1_refusal", "refusal")));

    /** The native library's exports that are no planner door, each with what it is instead. */
    static final Map<String, String> NATIVE_SERVICES = new TreeMap<>(Map.of(
            "lite_site_types", "the site's types, a constant of the build (Folded's answer of them)",
            "lite_free", "the answer's memory freed",
            "lite_unfreed", "the count of answers not yet freed (a leak's witness)",
            "lite_sdlc_handle", "the model home's SDLC store, opened on a folder (its rules, sdlc-server's)",
            "lite_depot_handle", "the model home's depot, opened on a folder",
            "lite_saved_queries", "the saved queries' rules over a folder",
            "lite_saved_cubes", "the saved cubes' rules over a folder",
            "lite_warehouse_call", "the warehouse's SQL API, a database server of its own, as its binding speaks it",
            "lite_warehouse_read", "the warehouse's SQL API's read"));

    /** THE SERVER'S ROUTES ({@code PureV1Api.route}'s dispatch), each with what its body is: upstream's protocol shapes,
     *  whose fields are {@link #SERVER_FIELDS}'s. */
    static final Map<String, String> ROUTES = new TreeMap<>(Map.ofEntries(
            Map.entry("/api/pure/v1/grammar/grammarToJson/lambda", "a lambda's text"),
            Map.entry("/api/pure/v1/grammar/grammarToJson/model", "a model's text"),
            Map.entry("/api/pure/v1/grammar/jsonToGrammar/lambda", "a lambda's protocol JSON"),
            Map.entry("/api/pure/v1/grammar/jsonToGrammar/lambda/batch", "lambdas' protocol JSON"),
            Map.entry("/api/pure/v1/grammar/jsonToGrammar/model", "a model's protocol JSON"),
            Map.entry("/api/pure/v1/compilation/lambdaRelationType", "a model and a lambda"),
            Map.entry("/api/pure/v1/compilation/compile", "a model"),
            Map.entry("/api/pure/v1/compilation/lambdaReturnType", "a model and a lambda"),
            Map.entry("/api/pure/v1/execution/generatePlan", "an ExecuteInput: a function, a model, a runtime"),
            Map.entry("/api/pure/v1/execution/execute", "an ExecuteInput: a function, a model, a runtime, values"),
            Map.entry("/api/pure/v1/execution/testDataGeneration/generateSeedData", "a test-data request"),
            Map.entry("/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed",
                    "a test-data request and its row identifiers"),
            Map.entry("/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed",
                    "a test-data request"),
            Map.entry("/api/pure/v1/testData/generation/DONOTUSE_generateTestData", "a model and a query"),
            Map.entry("/api/lite/v1/compilation/compile", "a model"),
            Map.entry("/api/lite/v1/model/createClass", "a project's model and the classes to make"),
            Map.entry("/api/lite/v1/model/createAssociation", "a project's model and the associations to make"),
            Map.entry("/api/lite/v1/model/createDataSpace", "a project's model and the data space to make")));

    /** The routes' query parameters ({@code queryParam(rawQuery, ...)}). */
    static final Map<String, Kind> QUERY_PARAMETERS = new TreeMap<>(fields("renderStyle", OPTION,
            "serializationFormat", OPTION));

    /** THE SERVER'S REQUEST VOCABULARY: every key each route file reads, with its kind -- upstream's protocol shapes (a
     *  model, a lambda, a runtime, values) and lite's model changes, which write Pure model text. */
    static final Map<String, Map<String, Kind>> SERVER_FIELDS = new TreeMap<>(Map.of(
            // the pure/v1 routes; planned.* are read of the plan half a host hands back with its rows (jsonAnswer,
            // its variable `planned`): the planner's own, echoed into the answer, never sent
            "PureV1Api.java", new TreeMap<>(fields("_type", OPTION, "planned.activities", PLAN_DATA, "body", LAMBDA,
                    "planned.builder", PLAN_DATA, "code", MODEL_TEXT, "planned.columns", PLAN_DATA, "connectionValue",
                    OBJECT, "planned.execution", PLAN_DATA, "fullPath", NAME, "function", LAMBDA, "genericType", OBJECT,
                    "lambda", LAMBDA, "lowerBound", VALUE, "model", MODEL_TEXT, "multiplicity", OBJECT, "name", NAME,
                    "parameterValues", VALUE, "parameters", LAMBDA, "rawType", NAME, "renderStyle", OPTION,
                    "planned.resultColumns", PLAN_DATA, "runtime", NAME, "serializationFormat", OPTION,
                    "planned.shape", PLAN_DATA, "planned.sql", PLAN_DATA, "upperBound", VALUE, "value", VALUE)),
            "TestDataRoutes.java", new TreeMap<>(fields("_type", OPTION, "columnValuePairs", OBJECT, "function",
                    LAMBDA, "hashStrings", OPTION, "mapping", NAME, "model", MODEL_TEXT, "name", NAME,
                    "parameterValues", VALUE, "query", LAMBDA, "rowIdentifiers", OBJECT, "runtime", NAME, "schema",
                    NAME, "table", NAME, "tableRowIdentifiers", OBJECT, "value", VALUE)),
            "ModelChanges.java", new TreeMap<>(fields("_type", OPTION, "association", NAME, "associations", OBJECT,
                    "class", NAME, "classMappings", OBJECT, "classes", OBJECT, "column", NAME, "dataSpace", OBJECT,
                    "elements", MODEL_TEXT, "endColumn", VALUE, "endLine", VALUE, "genericType", OBJECT, "name", NAME,
                    "package", NAME, "path", NAME, "project", NAME, "properties", OBJECT, "property", NAME,
                    "propertyMappings", OBJECT, "pureCode", MODEL_TEXT, "relationFunction", OBJECT, "runtimeValue",
                    OBJECT, "source", MODEL_TEXT, "sourceInformation", OBJECT, "startColumn", VALUE, "startLine",
                    VALUE, "type", NAME)),
            "ModelInput.java", new TreeMap<>(fields("_type", OPTION, "code", MODEL_TEXT, "model", MODEL_TEXT)),
            // a class made from a grid: its sample rows a Data element's CSV in the model, never a statement
            "CreateClass.java", new TreeMap<>(fields("_type", OPTION, "as", NAME, "body", LAMBDA, "columns", OBJECT,
                    "connection", NAME, "connectionValue", OBJECT, "connections", OBJECT, "data", OBJECT,
                    "databaseType", NAME, "datasourceSpecification", OBJECT, "documentation", VALUE, "from", NAME,
                    "includedMappings", NAME, "lambda", LAMBDA, "mappings", NAME, "name", NAME, "package", NAME,
                    "parameters", LAMBDA, "path", NAME, "rows", VALUE, "runtime", NAME, "runtimeValue", OBJECT,
                    "samples", OBJECT, "schema", NAME, "schemas", OBJECT, "store", NAME, "storeConnections", OBJECT,
                    "table", NAME, "tables", OBJECT, "type", NAME, "value", VALUE, "where", OBJECT)),
            // an association's ends: on is a property's name of its class
            "CreateAssociation.java", new TreeMap<>(fields("_type", OPTION, "associationMappings", OBJECT, "class",
                    NAME, "ends", OBJECT, "multiplicity", OPTION, "name", NAME, "on", NAME, "package", NAME,
                    "properties", OBJECT, "unique", OPTION)),
            "CreateDataSpace.java", new TreeMap<>(fields("_type", OPTION, "classes", NAME, "description", VALUE,
                    "executionContexts", OBJECT, "name", NAME, "path", NAME, "title", VALUE))));

    /** THE HTTP SERVER'S ROUTES ({@code LegendHttpServer}): each the pure/v1 dispatcher's (the routes above), or a
     *  service that plans nothing, with what it is instead. */
    static final Map<String, String> HTTP_ROUTES = new TreeMap<>(Map.of(
            "/api/pure/v1/", "the pure/v1 dispatcher (ROUTES)",
            "/api/lite/v1/", "the pure/v1 dispatcher (ROUTES)",
            "/api/pure/v1/query", "the saved queries' and saved cubes' store: no planner",
            "/api/server/v1/currentUser", "the current user's name: no planner",
            "/engine/diagram", "a model's diagrams laid out: no planner",
            "/health", "the server's health: no planner",
            "/lsp", "the Pure language server: a model's diagnostics and completions, no plan",
            "/", "CORS's preflight, any other path 404: no planner"));

    /** The fields named for a statement that are the plan's own, read back: where, and why. */
    private static final Set<String> PLAN_STATEMENTS = Set.of("PureV1Api.java.planned.sql");

    /** The variables through which a file reads the planner's own JSON back, never a request's: their keys are read
     *  as {@code variable.key}. */
    private static final Map<String, Set<String>> INTERNAL_RECEIVERS = Map.of("PureV1Api.java", Set.of("planned"));

    /** THE ROOT DOORS (AGENTS.md: {@code Compiler}, {@code TypedQuery}, {@code HeldModels} the plan side's,
     *  {@code Execution} and {@code TestExecution} the runs'): every public method, by its signature, with its
     *  parameters' kinds. */
    static final Map<String, List<Kind>> ROOT_DOORS = new TreeMap<>(Map.ofEntries(
            // Compiler: the planner's steps A..J, each a model's text or a query in, a model or a query out
            Map.entry("Compiler#parseModel(String)", List.of(MODEL_TEXT)),
            Map.entry("Compiler#parseQuery(String)", List.of(LAMBDA)),
            Map.entry("Compiler#compileModel(String)", List.of(MODEL_TEXT)),
            Map.entry("Compiler#compileModel(Protocol.PureModelContextData)", List.of(MODEL)),
            Map.entry("Compiler#compileModel(List<Compiler.ModelSource>)", List.of(MODEL_TEXT)),
            Map.entry("Compiler#parseSources(List<Compiler.ModelSource>)", List.of(MODEL_TEXT)),
            // the parser's warnings to the caller, and the dialect a section is parsed in
            Map.entry("Compiler#parseSources(List<Compiler.ModelSource>, BiConsumer<String, String>)",
                    List.of(MODEL_TEXT, CALLBACK)),
            Map.entry("Compiler#parseSources(List<Compiler.ModelSource>, BiConsumer<String, String>, Dialect)",
                    List.of(MODEL_TEXT, CALLBACK, OPTION)),
            Map.entry("Compiler#buildModel(ParsedModel)", List.of(MODEL)),
            Map.entry("Compiler#buildModule(ParsedModel)", List.of(MODEL)),
            Map.entry("Compiler#compileAllBodies(ModelContext)", List.of(MODEL)),
            Map.entry("Compiler#compileErrors(String)", List.of(MODEL_TEXT)),
            Map.entry("Compiler#compileErrors(Protocol.PureModelContextData)", List.of(MODEL)),
            Map.entry("Compiler#query(ModelContext, String)", List.of(MODEL, LAMBDA)),
            Map.entry("Compiler#query(ModelContext, ValueSpecification)", List.of(MODEL, LAMBDA)),
            Map.entry("Compiler#queryResolved(ModelContext, ValueSpecification)", List.of(MODEL, LAMBDA)),
            Map.entry("Compiler#resolveQuery(List<ValueSpecification>, ImportScope, ModelContext)",
                    List.of(LAMBDA, NAME, MODEL)),
            Map.entry("Compiler#resolveNames(List<ValueSpecification>, ImportScope, ModelContext)",
                    List.of(LAMBDA, NAME, MODEL)),
            Map.entry("Compiler#executesOn(ModelContext, String)", List.of(MODEL, NAME)),
            Map.entry("Compiler#hasStatementEffects(ValueSpecification, ModelContext)", List.of(LAMBDA, MODEL)),
            Map.entry("Compiler#programFacts(ValueSpecification, ModelContext)", List.of(LAMBDA, MODEL)),
            Map.entry("Compiler#lowerResolved(ValueSpecification, ModelContext, String, boolean)",
                    List.of(LAMBDA, MODEL, NAME, OPTION)),
            Map.entry("Compiler#lowerResolved(ValueSpecification, ModelContext, String, boolean, String)",
                    List.of(LAMBDA, MODEL, NAME, OPTION, NAME)),
            // TypedQuery: a query typed once; its plans by a runtime's name and options
            Map.entry("TypedQuery#withBody(List<TypedSpec>)", List.of(LAMBDA)),
            Map.entry("TypedQuery#context()", List.of()),
            Map.entry("TypedQuery#specs()", List.of()),
            Map.entry("TypedQuery#resolved()", List.of()),
            Map.entry("TypedQuery#body()", List.of()),
            Map.entry("TypedQuery#resultType()", List.of()),
            Map.entry("TypedQuery#parameters()", List.of()),
            Map.entry("TypedQuery#answersADiagnostic()", List.of()),
            Map.entry("TypedQuery#expression()", List.of()),
            Map.entry("TypedQuery#target()", List.of()),
            Map.entry("TypedQuery#writes()", List.of()),
            Map.entry("TypedQuery#runtime(String)", List.of(NAME)),
            // the statement road's lowering, apart from the query door (its callers doored or the test road's: the
            // test-data routes' seed, 2026-11-13, and testplan.GeneratedTestData)
            Map.entry("TypedQuery#lower(String, boolean)", List.of(NAME, OPTION)),
            Map.entry("TypedQuery#lower(String, boolean, List<SqlExpr.PlanParam>, Set<Feature>)",
                    List.of(NAME, OPTION, PLAN_DATA, OPTION)),
            Map.entry("TypedQuery#lowerer(boolean, boolean, List<SqlExpr.PlanParam>, Set<Feature>)",
                    List.of(OPTION, OPTION, PLAN_DATA, OPTION)),
            Map.entry("TypedQuery#lowerer(boolean, boolean, List<SqlExpr.PlanParam>, Set<Feature>, ObjectSites)",
                    List.of(OPTION, OPTION, PLAN_DATA, OPTION, PLAN_DATA)),
            Map.entry("TypedQuery#plan(String)", List.of(NAME)),
            Map.entry("TypedQuery#executionPlan(String)", List.of(NAME)),
            Map.entry("TypedQuery#executionPlan(String, Set<Feature>)", List.of(NAME, OPTION)),
            Map.entry("TypedQuery#projectPlan(String)", List.of(NAME)),
            // HeldModels: a model's text held by its handle, its compiled model handed to the caller's function
            Map.entry("HeldModels#hold(String)", List.of(MODEL_TEXT)),
            Map.entry("HeldModels#release(int)", List.of(HANDLE)),
            Map.entry("HeldModels#with(int, Function<ModelContext, T>)", List.of(HANDLE, CALLBACK)),
            Map.entry("HeldModels#withText(String, Function<ModelContext, T>)", List.of(MODEL_TEXT, CALLBACK)),
            Map.entry("HeldModels#size()", List.of()),
            // Execution: the product's door, a query planned and its plan run on the caller's sessions
            Map.entry("Execution#run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish,"
                    + " Output)", List.of(LAMBDA, NAME, SESSION, SESSION, SESSION, OUTPUT)),
            Map.entry("Execution#run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish,"
                    + " Output, int)", List.of(LAMBDA, NAME, SESSION, SESSION, SESSION, OUTPUT, OPTION)),
            Map.entry("Execution#run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish,"
                    + " Set<Feature>, Output, int)", List.of(LAMBDA, NAME, SESSION, SESSION, SESSION, OPTION, OUTPUT,
                    OPTION)),
            Map.entry("Execution#run(String, String, String, SessionSource, PlanSessions.Source, Output)",
                    List.of(MODEL_TEXT, LAMBDA, NAME, SESSION, SESSION, OUTPUT)),
            Map.entry("Execution#run(String, String, String, SessionSource, PlanSessions.Source, Output, int)",
                    List.of(MODEL_TEXT, LAMBDA, NAME, SESSION, SESSION, OUTPUT, OPTION)),
            Map.entry("Execution#runOnProjectDatabases(TypedQuery, String, Output)", List.of(LAMBDA, NAME, OUTPUT)),
            // the sources a server admits: its configuration's text
            Map.entry("Execution#runOnProjectDatabases(TypedQuery, String, String, Output)",
                    List.of(LAMBDA, NAME, OPTION, OUTPUT)),
            // TestExecution: a test body planned, then run, its verdicts to the caller's listener and the referee
            Map.entry("TestExecution#planBody(TypedQuery, String, ExecuteOptions)", List.of(LAMBDA, NAME, OPTION)),
            Map.entry("TestExecution#runProgram(TypedQuery, String, SessionSource, AssertListener, SqlReplayReferee,"
                    + " ExecuteOptions)", List.of(LAMBDA, NAME, SESSION, CALLBACK, CALLBACK, OPTION)),
            Map.entry("TestExecution#runProgram(TypedQuery, String, SessionSource, AssertListener, SqlReplayReferee,"
                    + " ExecuteOptions, Output)", List.of(LAMBDA, NAME, SESSION, CALLBACK, CALLBACK, OPTION, OUTPUT)),
            Map.entry("TestExecution#runBody(TestPlan, Sessions.Session, AssertListener, SqlReplayReferee,"
                    + " ExecuteOptions, Output)", List.of(PLAN_DATA, SESSION, CALLBACK, CALLBACK, OPTION, OUTPUT)),
            Map.entry("TestExecution#runFixture(TestPlan, Sessions.Session, ExecuteOptions)",
                    List.of(PLAN_DATA, SESSION, OPTION))));

    /** The root doors' classes. */
    private static final List<Class<?>> ROOT_CLASSES = List.of(Compiler.class, TypedQuery.class, HeldModels.class,
            Execution.class, TestExecution.class);

    /** PYTHON'S PLANNER PROTOCOL ({@code _planner.KINDS}): each request kind, the native export answering it, and the
     *  request's fields in that export's argument order -- each field's kind its door's parameter's
     *  ({@link #BOUNDARY}), read through {@link #NATIVE_EXPORTS}. */
    static final Map<String, List<String>> PY_KINDS = new TreeMap<>(Map.ofEntries(
            Map.entry("hold", List.of("lite_hold_model", "model")),
            Map.entry("release", List.of("lite_release_model", "handle")),
            Map.entry("plan", List.of("lite_plan_text_held", "handle", "query", "runtime")),
            Map.entry("relationType", List.of("lite_relation_type_text_held", "handle", "query")),
            Map.entry("tableModel", List.of("lite_table_model", "table")),
            Map.entry("catalogColumnsSql", List.of("lite_catalog_columns_sql", "schema", "table")),
            Map.entry("planJson", List.of("lite_plan_json_held", "handle", "lambda", "runtime")),
            Map.entry("executionPlan", List.of("lite_execution_plan_json_held", "handle", "lambda", "runtime")),
            Map.entry("planModel", List.of("lite_plan_json", "model", "lambda", "runtime")),
            Map.entry("pureV1", List.of("lite_pure_v1", "path", "query", "body")),
            Map.entry("executePlan", List.of("lite_execute_json_plan", "body", "models")),
            Map.entry("executeAnswer", List.of("lite_execute_json_answer", "plan", "rows")),
            Map.entry("fileLoad", List.of("lite_file_load", "request")),
            Map.entry("remoteMount", List.of("lite_remote_mount", "request")),
            Map.entry("snapStream", List.of("lite_snap_stream", "request")),
            Map.entry("snapCopy", List.of("lite_snap_copy", "request")),
            Map.entry("outputPlan", List.of("lite_output_plan", "request")),
            Map.entry("synthesize", List.of("lite_synthesize", "request")),
            Map.entry("sessionSetup", List.of("lite_session_setup", "databaseType")),
            Map.entry("projectData", List.of("lite_project_data", "model", "databaseType")),
            Map.entry("hostOperations", List.of("lite_host_operations", "databaseType", "operations")),
            Map.entry("warm", List.of("lite_warm_model", "model"))));

    /** The kinds Python's planner refuses by name ({@code _planner.REFUSED}). */
    static final Set<String> PY_REFUSED = Set.of("authored");

    /** The native names Python spells by a pattern, each with what it expands to. */
    static final Map<String, Set<String>> PY_PATTERNS = Map.of("lite_{kind}_handle",
            Set.of("lite_sdlc_handle", "lite_depot_handle"));

    /** THE BRIDGE: the calls DataCube's hosted code makes of Python ({@code _headless._calls}), each with what it
     *  carries; a call that carries a statement passes it {@code across_the_bridge} -- a statement this session's
     *  planner answered, else refused by name and nothing sent. */
    static final Map<String, String> BRIDGE_CALLS = new TreeMap<>(Map.of(
            "planner", "a planner request (PY_KINDS)",
            "query", "a statement this session's planner answered (across_the_bridge)",
            "statement", "a statement this session's planner answered (across_the_bridge), its values bound",
            "fetch", "an HTTP request; a statement in its body crosses as a query does (across_the_bridge)",
            "engine", "a call of the engine's own routes, answered over a read",
            "digest", "a digest of bytes"));

    /** The bridge's calls that carry a statement: each body passes it {@code across_the_bridge}. */
    static final Set<String> BRIDGE_STATEMENTS = Set.of("query", "statement", "fetch");

    /** The playground's door and field: the one place a request carries SQL. */
    private static final String PLAYGROUND = "authored.text";

    /** A field name that names a statement: declared {@link Kind#AUTHORED_SQL} or refused, nowhere else. */
    private static final Pattern NAMES_SQL = Pattern.compile("(?i).*(sql|statement).*");

    private static Map<String, Door> listed(Door... doors) {
        Map<String, Door> out = new TreeMap<>();
        for (Door d : doors) {
            assertTrue(out.put(d.name(), d) == null, "listed twice: " + d.name());
        }
        return out;
    }

    // ---- the check

    /** Every violation of the closed surface over {@code boundary} (its doors by reflection, its request doors' fields
     *  from {@code boundarySource}), the host operations' source, the tab's adapter's source and the native library's
     *  sources -- each named. */
    static List<String> violations(Class<?> boundary, String boundarySource, String hostOperationsSource,
            String tabSource, List<String> nativeSources) {
        List<String> out = new ArrayList<>();
        // the registry's own rule: SQL at the playground alone, and a name that names a statement nowhere else
        BOUNDARY.forEach((name, d) -> {
            for (Param p : d.params()) {
                sqlRule(out, name + "(" + p.name() + ")", p.name(), p.kind());
            }
            d.fields().forEach((f, k) -> sqlRule(out, name + "." + f, f, k));
        });
        HOST_OPERATION_FIELDS.forEach((f, k) -> sqlRule(out, "a host operation's " + f, f, k));

        // the doors: the class's public statics, each listed with its parameters' types
        Map<String, Method> found = new TreeMap<>();
        for (Method m : boundary.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers())) {
                if (found.put(m.getName(), m) != null) {
                    out.add("a door with two signatures: " + m.getName());
                }
            }
        }
        for (String name : found.keySet()) {
            if (!BOUNDARY.containsKey(name)) {
                out.add("a door no row lists: " + boundary.getSimpleName() + "." + name);
            }
        }
        for (Door d : BOUNDARY.values()) {
            Method m = found.get(d.name());
            if (m == null) {
                out.add("a listed door the boundary no longer has: " + d.name());
                continue;
            }
            List<Class<?>> types = d.params().stream().<Class<?>>map(Param::type).toList();
            if (!List.of(m.getParameterTypes()).equals(types)) {
                out.add("a door's parameters differ from its row: " + d.name() + " " + List.of(m.getParameterTypes()));
            }
        }

        // each request door's fields, read from its source
        Map<String, List<String>> bodies = methods(boundarySource);
        Set<String> helpers = privateStatics(boundarySource);
        for (Door d : BOUNDARY.values()) {
            boolean request = d.params().stream().anyMatch(p -> p.kind() == REQUEST);
            if (!request || !bodies.containsKey(d.name())) {
                continue;
            }
            Set<String> read = new TreeSet<>();
            for (String body : bodies.get(d.name())) {
                read.addAll(keys(body, bodies, helpers, new java.util.HashSet<>()));
            }
            compare(out, d.name(), read, d.fields().keySet());
        }

        // the host operations' fields
        compare(out, "the host operations", keys(hostOperationsSource, Map.of(), Set.of(), new java.util.HashSet<>()),
                HOST_OPERATION_FIELDS.keySet());

        // the exports: each delegates to the one listed door its row names
        exports(out, "the tab's export", Pattern.compile("@JSExport\\s+public static \\S+ (\\w+)\\("),
                List.of(tabSource), TAB_EXPORTS, Map.of());
        exports(out, "the native export", Pattern.compile("@CEntryPoint\\(name = \"(\\w+)\"\\)"), nativeSources,
                NATIVE_EXPORTS, NATIVE_SERVICES);
        return out;
    }

    /** Every violation of the server's closed surface: its routes ({@code pureV1Source}'s dispatch), their query
     *  parameters, each route file's request vocabulary ({@code routeFiles}: a file's name to its text) and the HTTP
     *  server's routes ({@code httpSource}). */
    static List<String> serverViolations(String pureV1Source, Map<String, String> routeFiles, String httpSource) {
        List<String> out = new ArrayList<>();
        SERVER_FIELDS.forEach((file, fs) -> fs.forEach((f, k) -> sqlRule(out, file + "." + f, f, k)));
        listedAs(out, "a route", found(pureV1Source, "case \"(/api/[^\"]+)\" ->"), ROUTES.keySet());
        listedAs(out, "a query parameter", found(pureV1Source, "queryParam\\(rawQuery, \"(\\w+)\"\\)"),
                QUERY_PARAMETERS.keySet());
        listedAs(out, "a route file", routeFiles.keySet(), SERVER_FIELDS.keySet());
        routeFiles.forEach((file, text) -> {
            if (SERVER_FIELDS.containsKey(file)) {
                compare(out, file, keys(text, Map.of(), Set.of(), new java.util.HashSet<>(),
                        INTERNAL_RECEIVERS.getOrDefault(file, Set.of())), SERVER_FIELDS.get(file).keySet());
            }
        });
        listedAs(out, "an HTTP route", found(httpSource, "route\\(\"([^\"]+)\""), HTTP_ROUTES.keySet());
        return out;
    }

    /** A method's signature as the root doors' registry spells it: {@code Class#name(SimpleType, ...)}. */
    static String signature(Method m) {
        return m.getDeclaringClass().getSimpleName() + "#" + m.getName() + "(" + String.join(", ",
                Arrays.stream(m.getGenericParameterTypes()).map(t -> t.getTypeName()
                        .replaceAll("[\\w$]+\\.", "").replace('$', '.')).toList()) + ")";
    }

    /** Every violation of the root doors' surface over {@code classes}: each public method listed by its signature,
     *  its kinds one per parameter, none SQL. */
    static List<String> rootViolations(List<Class<?>> classes) {
        List<String> out = new ArrayList<>();
        Set<String> seen = new TreeSet<>();
        for (Class<?> c : classes) {
            for (Method m : c.getDeclaredMethods()) {
                if (!Modifier.isPublic(m.getModifiers()) || m.isSynthetic() || m.isBridge()) {
                    continue;
                }
                String sig = signature(m);
                seen.add(sig);
                List<Kind> kinds = ROOT_DOORS.get(sig);
                if (kinds == null) {
                    out.add("a root door no row lists: " + sig);
                } else if (kinds.size() != m.getParameterCount()) {
                    out.add("a root door's kinds are not one per parameter: " + sig);
                } else if (kinds.contains(AUTHORED_SQL)) {
                    out.add("a root door that carries SQL: " + sig);
                }
            }
        }
        for (String sig : ROOT_DOORS.keySet()) {
            if (!seen.contains(sig)) {
                out.add("a listed root door that is gone: " + sig);
            }
        }
        return out;
    }

    /** Every violation of Python's closed surface: its planner protocol ({@code plannerSource}), every native name its
     *  package spells ({@code packageSources}) and the bridge's calls ({@code headlessSource}). */
    static List<String> pythonViolations(String plannerSource, List<String> packageSources, String headlessSource) {
        List<String> out = new ArrayList<>();
        Map<String, List<String>> rows = new TreeMap<>();
        Matcher row = Pattern.compile("(?m)^    '(\\w+)': \\('(lite_\\w+)', lambda m: \\((.*)\\)\\),$")
                .matcher(plannerSource);
        while (row.find()) {
            List<String> r = new ArrayList<>(List.of(row.group(2)));
            Matcher f = Pattern.compile("m\\['(\\w+)'\\]").matcher(row.group(3));
            while (f.find()) {
                r.add(f.group(1));
            }
            rows.put(row.group(1), r);
        }
        listedAs(out, "a Python planner kind", rows.keySet(), PY_KINDS.keySet());
        rows.forEach((kind, r) -> {
            if (PY_KINDS.containsKey(kind) && !PY_KINDS.get(kind).equals(r)) {
                out.add("a Python planner kind's row differs from its listing: " + kind + " " + r);
            }
        });
        PY_KINDS.forEach((kind, r) -> {
            String door = NATIVE_EXPORTS.get(r.get(0));
            if (door == null || !BOUNDARY.containsKey(door)) {
                out.add("a Python planner kind answered by no listed door: " + kind + " (" + r.get(0) + ")");
            } else if (BOUNDARY.get(door).params().size() != r.size() - 1) {
                out.add("a Python planner kind's fields are not its door's parameters: " + kind + " -> " + door);
            } else {
                for (int i = 1; i < r.size(); i++) {
                    sqlRule(out, "python " + kind + "." + r.get(i), r.get(i),
                            BOUNDARY.get(door).params().get(i - 1).kind());
                }
            }
        });
        listedAs(out, "a Python refused kind", found(plannerSource.substring(plannerSource.indexOf("REFUSED")),
                "\\n    '(\\w+)': \""), PY_REFUSED);
        // every native name the package spells
        Set<String> names = new TreeSet<>();
        for (String text : packageSources) {
            names.addAll(found(text, "['\"](lite_[a-z_{}]+)['\"]"));
        }
        for (String name : names) {
            Set<String> expands = PY_PATTERNS.getOrDefault(name, Set.of(name));
            for (String e : expands) {
                if (!NATIVE_EXPORTS.containsKey(e) && !NATIVE_SERVICES.containsKey(e)) {
                    out.add("a native name Python spells that no row lists: " + name);
                }
            }
        }
        // the bridge
        String calls = pythonBody(headlessSource, headlessSource.indexOf("def _calls("));
        listedAs(out, "a bridge call", found(calls, "'(\\w+)': self\\._\\w+"), BRIDGE_CALLS.keySet());
        for (String call : BRIDGE_STATEMENTS) {
            int at = headlessSource.indexOf("async def _" + call + "(");
            String body = at < 0 ? "" : pythonBody(headlessSource, at);
            if (!body.contains("across_the_bridge(")) {
                out.add("a bridge call that carries a statement and passes it no across_the_bridge: " + call);
            }
        }
        return out;
    }

    /** A Python function's body: its lines until the next line indented no deeper than its {@code def}. */
    private static String pythonBody(String source, int defAt) {
        int lineStart = source.lastIndexOf('\n', defAt) + 1;
        int indent = defAt - lineStart;
        StringBuilder out = new StringBuilder();
        String[] lines = source.substring(source.indexOf('\n', defAt) + 1).split("\n", -1);
        for (String line : lines) {
            int lead = line.length() - line.stripLeading().length();
            if (!line.isBlank() && lead <= indent) {
                break;
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private static Set<String> found(String source, String regex) {
        Set<String> out = new TreeSet<>();
        Matcher m = Pattern.compile(regex).matcher(source);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    private static void sqlRule(List<String> out, String where, String name, Kind kind) {
        if (kind == AUTHORED_SQL && !where.equals(PLAYGROUND)) {
            out.add("a field that carries SQL outside the playground: " + where);
        }
        if (NAMES_SQL.matcher(name).matches() && kind != AUTHORED_SQL && kind != REFUSED
                && !(kind == PLAN_DATA && PLAN_STATEMENTS.contains(where))) {
            out.add("a field named for a statement, declared " + kind + ": " + where);
        }
    }

    private static void compare(List<String> out, String door, Set<String> read, Set<String> declared) {
        for (String k : read) {
            if (!declared.contains(k)) {
                out.add(door + " reads the field " + k + ", which no row declares"
                        + (NAMES_SQL.matcher(k).matches() ? " (a field that can carry SQL)" : ""));
            }
        }
        for (String k : declared) {
            if (!read.contains(k)) {
                out.add(door + " declares the field " + k + ", which it no longer reads");
            }
        }
    }

    /** {@code found} against {@code listed}, each {@code what} (a route, a parameter, a file) named both ways. */
    private static void listedAs(List<String> out, String what, Set<String> found, Set<String> listed) {
        for (String k : found) {
            if (!listed.contains(k)) {
                out.add(what + " no row lists: " + k + (NAMES_SQL.matcher(k).matches() ? " (named for a statement)"
                        : ""));
            }
        }
        for (String k : listed) {
            if (!found.contains(k)) {
                out.add(what + " listed and gone: " + k);
            }
        }
    }

    private static void exports(List<String> out, String what, Pattern export, List<String> sources,
            Map<String, String> listed, Map<String, String> services) {
        Set<String> seen = new TreeSet<>();
        for (String source : sources) {
            Matcher m = export.matcher(source);
            while (m.find()) {
                String name = m.group(1);
                seen.add(name);
                String body = bodyAfter(source, m.end());
                Set<String> delegates = new TreeSet<>();
                Matcher call = Pattern.compile("Boundary\\.(\\w+)\\(").matcher(body);
                while (call.find()) {
                    delegates.add(call.group(1));
                }
                if (services.containsKey(name)) {
                    if (!delegates.isEmpty()) {
                        out.add(what + " " + name + " is listed as no planner door, and reaches " + delegates);
                    }
                } else if (!listed.containsKey(name)) {
                    out.add(what + " no row lists: " + name + (delegates.isEmpty() ? "" : " (reaches " + delegates
                            + ")"));
                } else if (!delegates.equals(Set.of(listed.get(name)))) {
                    out.add(what + " " + name + " delegates to " + delegates + ", not its row's "
                            + listed.get(name));
                } else if (!BOUNDARY.containsKey(listed.get(name))) {
                    out.add(what + " " + name + " delegates to an unlisted door: " + listed.get(name));
                }
            }
        }
        for (String name : listed.keySet()) {
            if (!seen.contains(name)) {
                out.add(what + " listed and gone: " + name);
            }
        }
        for (String name : services.keySet()) {
            if (!seen.contains(name)) {
                out.add(what + " listed and gone: " + name);
            }
        }
    }

    // ---- reading a source

    private static final Pattern METHOD = Pattern.compile(
            "\\n    (public |private )?static [^=;(]*?\\b(\\w+)\\(([^)]*)\\)[^{;]*\\{");

    /** Each static method's bodies, by name. */
    private static Map<String, List<String>> methods(String source) {
        Map<String, List<String>> out = new TreeMap<>();
        Matcher m = METHOD.matcher(source);
        while (m.find()) {
            out.computeIfAbsent(m.group(2), k -> new ArrayList<>()).add(bodyAfter(source, m.end() - 1));
        }
        return out;
    }

    private static Set<String> privateStatics(String source) {
        Set<String> out = new TreeSet<>();
        Matcher m = METHOD.matcher(source);
        while (m.find()) {
            if ("private ".equals(m.group(1))) {
                out.add(m.group(2));
            }
        }
        return out;
    }

    /** The block that opens at or after {@code from}, braces matched. */
    private static String bodyAfter(String source, int from) {
        int start = source.indexOf('{', from);
        int depth = 0;
        for (int i = start; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}' && --depth == 0) {
                return source.substring(start, i + 1);
            }
        }
        throw new IllegalStateException("an unclosed block at " + start);
    }

    private static final Pattern GETTER = Pattern.compile("(\\w*)\\s*\\.(?:get\\w*|has)\\(\\s*\"([A-Za-z_]\\w*)\"");
    private static final Pattern READER = Pattern.compile("\\b[a-z]\\w*\\(\\s*\\w+\\s*,\\s*\"([A-Za-z_]\\w*)\"\\s*\\)");
    private static final Pattern ONLY = Pattern.compile("only\\(\\s*\\w+\\s*,\\s*\"[^\"]*\"((?:\\s*,\\s*\"[^\"]*\")*)\\)");
    private static final Pattern CALL = Pattern.compile("(?<![\\w.])(\\w+)\\(");

    /** The keys {@code body} admits or reads, with the private helpers it calls, transitively. */
    private static Set<String> keys(String body, Map<String, List<String>> methods, Set<String> helpers,
            Set<String> seen) {
        return keys(body, methods, helpers, seen, Set.of());
    }

    /** {@link #keys(String, Map, Set, Set)}, a key read through one of {@code internal}'s variables as
     *  {@code variable.key}. */
    private static Set<String> keys(String body, Map<String, List<String>> methods, Set<String> helpers,
            Set<String> seen, Set<String> internal) {
        Set<String> out = new TreeSet<>();
        Matcher get = GETTER.matcher(body);
        while (get.find()) {
            out.add(internal.contains(get.group(1)) ? get.group(1) + "." + get.group(2) : get.group(2));
        }
        Matcher reader = READER.matcher(body);
        while (reader.find()) {
            out.add(reader.group(1));
        }
        Matcher only = ONLY.matcher(body);
        while (only.find()) {
            Matcher key = Pattern.compile("\"([^\"]*)\"").matcher(only.group(1));
            while (key.find()) {
                out.add(key.group(1));
            }
        }
        Matcher call = CALL.matcher(body);
        while (call.find()) {
            String c = call.group(1);
            if (helpers.contains(c) && seen.add(c)) {
                for (String b : methods.get(c)) {
                    out.addAll(keys(b, methods, helpers, seen, internal));
                }
            }
        }
        return out;
    }

    private static String read(String path) {
        try {
            return Files.readString(SourceFiles.file(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> nativeSources() {
        List<String> out = new ArrayList<>();
        for (var f : SourceFiles.under("native/src/main/java/com/legend/nativelib")) {
            if (f.getFileName().toString().endsWith(".java")) {
                try {
                    out.add(Files.readString(f, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        assertTrue(out.size() >= 4, "the native library's sources read: " + out.size());
        return out;
    }

    private static final String BOUNDARY_SOURCE = "wasm/src/main/java/planner/Boundary.java";
    private static final String HOST_OPERATIONS_SOURCE = "wasm/src/main/java/planner/HostOperationsJson.java";
    private static final String TAB_SOURCE = "wasm/src/main/java/planner/TabExports.java";

    // ---- the tests

    /** The surface is closed: every door listed with its fields' kinds, no field that can carry SQL but the
     *  playground's, every export a listed door's or a listed service. */
    @Test
    void thePlannerSurfaceIsClosed() {
        assertEquals(List.of(), violations(planner.Boundary.class, read(BOUNDARY_SOURCE), read(HOST_OPERATIONS_SOURCE),
                read(TAB_SOURCE), nativeSources()), "the planner surface is not closed");
    }

    /** The check names a listed door whose request reads a statement, and a door no row lists
     *  ({@code planner.PlantedBoundary}, a test class planted for this alone). */
    @Test
    void theSurfaceRefusesAPlantedDoor() {
        List<String> found = violations(com.legend.planner.PlantedBoundary.class,
                read("core/src/test/java/com/legend/planner/PlantedBoundary.java"), read(HOST_OPERATIONS_SOURCE),
                read(TAB_SOURCE), nativeSources());
        assertTrue(found.contains("snapCopy reads the field sql, which no row declares (a field that can carry SQL)"),
                String.join("\n", found));
        assertTrue(found.contains("a door no row lists: PlantedBoundary.runSql"), String.join("\n", found));
    }

    /** The check names an export no row lists, on the tab's adapter and on the native library. */
    @Test
    void theSurfaceRefusesAPlantedExport() {
        String tab = read(TAB_SOURCE).replace("    @JSExport\n    public static String zoneProbe(", """
                    @JSExport
                    public static String runSqlOrError(String text) {
                        return Folded.of(() -> Boundary.authored(text));
                    }

                    @JSExport
                    public static String zoneProbe(""");
        List<String> natives = new ArrayList<>(nativeSources());
        natives.add("""
                @CEntryPoint(name = "lite_run_sql")
                static CCharPointer runSql(IsolateThread thread, CCharPointer sql) {
                    return answer(Folded.of(() -> Boundary.authored(text(sql))));
                }
                """);
        List<String> found = violations(planner.Boundary.class, read(BOUNDARY_SOURCE), read(HOST_OPERATIONS_SOURCE),
                tab, natives);
        assertEquals(List.of("the tab's export no row lists: runSqlOrError (reaches [authored])",
                "the native export no row lists: lite_run_sql (reaches [authored])"), found);
    }

    private static final List<String> SERVER_FILES = List.of("PureV1Api.java", "TestDataRoutes.java",
            "ModelChanges.java", "ModelInput.java", "CreateClass.java", "CreateAssociation.java",
            "CreateDataSpace.java");

    private static Map<String, String> routeFiles() {
        Map<String, String> out = new TreeMap<>();
        for (String f : SERVER_FILES) {
            out.put(f, read("core/src/main/java/com/legend/server/" + f));
        }
        return out;
    }

    private static final String HTTP_SOURCE = "core/src/main/java/com/legend/server/LegendHttpServer.java";

    /** The server's surface is closed: every route listed, every key a route file reads declared with its kind, no
     *  field that carries SQL but the plan half's own statement read back. */
    @Test
    void theServersSurfaceIsClosed() {
        Map<String, String> files = routeFiles();
        assertEquals(List.of(), serverViolations(files.get("PureV1Api.java"), files, read(HTTP_SOURCE)),
                "the server's surface is not closed");
    }

    /** The check names a route no row lists, a request field that carries SQL, and an HTTP route no row lists. */
    @Test
    void theServersSurfaceRefusesAPlantedRoute() {
        Map<String, String> files = routeFiles();
        String pureV1 = files.get("PureV1Api.java").replace(
                "            case \"/api/lite/v1/compilation/compile\" ->",
                "            case \"/api/lite/v1/runSql\" -> runSql(PureV1Api.request(body).getString(\"sql\"));\n"
                        + "            case \"/api/lite/v1/compilation/compile\" ->");
        files.put("PureV1Api.java", pureV1);
        String http = read(HTTP_SOURCE).replace("        route(\"/health\",",
                "        route(\"/api/lite/v1/sql\", exchange -> { });\n        route(\"/health\",");
        assertEquals(List.of("a route no row lists: /api/lite/v1/runSql (named for a statement)",
                "PureV1Api.java reads the field sql, which no row declares (a field that can carry SQL)",
                "an HTTP route no row lists: /api/lite/v1/sql (named for a statement)"),
                serverViolations(pureV1, files, http));
    }

    /** The root doors are closed: every public method of the five listed, by signature, its parameters' kinds. */
    @Test
    void theRootDoorsAreClosed() {
        assertEquals(List.of(), rootViolations(ROOT_CLASSES), "the root doors are not closed");
    }

    /** The check names a public method of a root door no row lists ({@code planner.PlantedRootDoor}, a test class
     *  planted for this alone). */
    @Test
    void theRootDoorsRefuseAPlantedDoor() {
        assertTrue(rootViolations(List.of(com.legend.planner.PlantedRootDoor.class))
                .contains("a root door no row lists: PlantedRootDoor#runSql(String)"));
    }

    private static List<String> pythonPackage() {
        List<String> out = new ArrayList<>();
        for (var f : SourceFiles.under("python/legend_lite")) {
            if (f.getFileName().toString().endsWith(".py")) {
                try {
                    out.add(Files.readString(f, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        assertTrue(out.size() > 10, "Python's package read: " + out.size());
        return out;
    }

    private static final String PY_PLANNER = "python/legend_lite/_planner.py";
    private static final String PY_HEADLESS = "python/legend_lite/_headless.py";

    /** Python's surface is closed: every planner kind a listed native export's, its fields its door's parameters;
     *  every native name the package spells listed; every bridge call listed, a statement's across the bridge. */
    @Test
    void pythonsSurfaceIsClosed() {
        assertEquals(List.of(), pythonViolations(read(PY_PLANNER), pythonPackage(), read(PY_HEADLESS)),
                "Python's planner surface is not closed");
    }

    /** The check names a planner kind no row lists, a native name no row lists, a bridge call no row lists, and a
     *  statement-carrying bridge call that skips the bridge's check. */
    @Test
    void pythonsSurfaceRefusesAPlantedKindAndCall() {
        String planner = read(PY_PLANNER).replace("    'warm': (", "    'runSql': ('lite_run_sql', lambda m: (m['sql'],)),\n"
                + "    'warm': (");
        String headless = read(PY_HEADLESS).replace("'engine': self._engine}", "'engine': self._engine,"
                + " 'run': self._run}").replace("statement = across_the_bridge(sql, \"the hosted DataCube's query\","
                + " self._answered)", "statement = sql");
        List<String> found = pythonViolations(planner, pythonPackage(), headless);
        assertEquals(List.of("a Python planner kind no row lists: runSql (named for a statement)",
                "a bridge call no row lists: run",
                "a bridge call that carries a statement and passes it no across_the_bridge: query"), found);
    }

    /** The registry's own rule bites: a field declared SQL away from the playground is named. */
    @Test
    void aFieldDeclaredSqlAwayFromThePlaygroundIsNamed() {
        List<String> out = new ArrayList<>();
        sqlRule(out, "snapCopy.sql", "sql", AUTHORED_SQL);
        sqlRule(out, "snapCopy.statement", "statement", NAME);
        assertEquals(List.of("a field that carries SQL outside the playground: snapCopy.sql",
                "a field named for a statement, declared NAME: snapCopy.statement"), out);
    }
}
