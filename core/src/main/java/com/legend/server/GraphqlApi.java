// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.protocol.graphql.GqlSchema;
import com.legend.protocol.PureComposer;
import com.legend.protocol.graphql.GqlToPure;
import com.legend.protocol.graphql.GqlTranslationException;
import com.legend.protocol.graphql.Introspection;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.compiler.graphql.GqlValidator;
import com.legend.compiler.graphql.GqlValidator.Limits;
import com.legend.compiler.graphql.GqlValidator.Problem;
import com.legend.compiler.graphql.GqlValidator.Validation;
import com.legend.compiler.graphql.SchemaGenerator;
import com.legend.compiler.graphql.Scope;
import com.legend.compiler.graphql.SdlPrinter;
import com.legend.json.Json;
import com.legend.model.DataSpaceDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.GqlParser;
import com.legend.protocol.spec.Gql;
import com.legend.server.PureV1Api.Answer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * legend-lite's GraphQL routes (docs/GRAPHQL_STUDY_2026_10_18.md §8.4, the first two of the four tools):
 * {@code /api/lite/v1/graphql/schema}, the SDL generated for a model's data space ({@code describe}), and
 * {@code /api/lite/v1/graphql/validate}, a document against that schema with every error, every warning and the
 * typed result shape ({@code validate}). Both take the model as {@code compile} does, a data space by name or a
 * class list to scope the schema, and {@code limits} (G17). A model the generator refuses, or a document that does
 * not parse, is answered as a model that does not compile is, 400 COMPILATION. The query and explain routes are the
 * line's PR2.
 */
public final class GraphqlApi {

    private GraphqlApi() {
    }

    public static Answer schema(String body) {
        return PureV1Api.answer(400, "COMPILATION", () -> {
            Json.Obj request = PureV1Api.request(body);
            Scoped scoped = scoped(request);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sdl", SdlPrinter.print(scoped.schema()));
            out.put("classes", scoped.scope().asList());
            return Json.toCompact(out);
        });
    }

    public static Answer validate(String body) {
        return PureV1Api.answer(400, "COMPILATION", () -> {
            Json.Obj request = PureV1Api.request(body);
            Scoped scoped = scoped(request);
            String query = request.getString("query");
            String operationName = request.getStringOr("operationName", null);
            Map<String, Object> out = new LinkedHashMap<>();
            // a document that does not parse is answered as a model that does not compile is: 400 COMPILATION
            // with the parser's message (the route's one designed sentinel is PureV1Api.answer's)
            Gql.Document doc = GqlParser.parseDocument(query, 1, 1);
            Validation v = GqlValidator.validate(doc, scoped.schema(), limits(request), operationName);
            out.put("valid", v.valid());
            if (v.shape() != null) {
                out.put("shape", v.shape());
            }
            out.put("errors", problems(v.errors()));
            out.put("warnings", problems(v.warnings()));
            return Json.toCompact(out);
        });
    }

    /**
     * {@code POST /api/lite/v1/graphql/query} (docs/GRAPHQL_STUDY_2026_10_18.md §8.1): the document run over the
     * model on its runtime, answered in the GraphQL-over-HTTP envelope. {@code model}, {@code query}; optionally
     * {@code variables}, {@code operationName}, {@code runtime} ({@code {"runtime": "demo::Runtime"}}),
     * {@code dataSpace} or {@code classes}, {@code limits}. A request the schema or the translator refuses is 400
     * with {@code errors} and no {@code data}; the document's roots run one by one, each answered under its key,
     * all or nothing: one root failing is 200 with {@code data: null} and the error at its path. Introspection
     * roots ({@code __schema}, {@code __type}) are answered from the schema. {@code extensions.pure} carries the
     * Pure each root ran, for a reader who wants the query beside its answer.
     */
    public static Answer query(String body, PureV1Api.Runner runner) {
        Json.Obj request;
        Scoped scoped;
        Gql.Document doc;
        try {
            request = PureV1Api.request(body);
            scoped = scoped(request);
            doc = GqlParser.parseDocument(request.getString("query"), 1, 1);
        } catch (RuntimeException e) {
            return envelope(400, null, List.of(problem(List.of(), String.valueOf(e.getMessage()), "REQUEST")),
                    null);
        }
        String operationName = request.getStringOr("operationName", null);
        Validation v = GqlValidator.validate(doc, scoped.schema(), limits(request), operationName);
        if (!v.valid()) {
            List<Map<String, Object>> errors = new ArrayList<>();
            for (Problem p : v.errors()) {
                errors.add(problem(pathOf(p.path()), p.message(), "VALIDATION"));
            }
            return envelope(400, null, errors, null);
        }
        GqlToPure.Translation t;
        try {
            t = GqlToPure.translate(doc, scoped.schema(), operationName,
                    GqlToPure.variables(request.getObjOr("variables", null)));
        } catch (GqlTranslationException e) {
            return envelope(400, null, List.of(problem(pathOf(e.path()), String.valueOf(e.getMessage()), e.kind().name())), null);
        }
        Map<String, Gql.Fragment> fragments = new LinkedHashMap<>();
        for (Gql.Definition d : doc.definitions()) {
            if (d instanceof Gql.Fragment f) {
                fragments.put(f.name(), f);
            }
        }
        String model = PureV1Api.modelText(request.getObj("model"));
        Map<String, Object> data = new LinkedHashMap<>();
        Map<String, Object> pure = new LinkedHashMap<>();
        for (GqlToPure.Root root : t.roots()) {
            try {
                String runtime = PureV1Api.runtimeOf(request,
                        Compiler.query(Compiler.compileModel(model), root.asLambda()).target());
                java.io.StringWriter rows = new java.io.StringWriter();
                runner.run(model, root.asLambda(), runtime, rows);
                String json = rows.toString();
                data.put(root.key(), Json.parse(json.isEmpty() ? "[]" : json));
                pure.put(root.key(), PureComposer.valueSpecification(root.expression(), PureComposer.Style.STANDARD));
            } catch (RuntimeException e) {
                // all or nothing (§8.2): no partial data
                return envelope(200, null, List.of(problem(List.of(root.key()), String.valueOf(e.getMessage()),
                        "EXECUTION")), null);
            }
        }
        for (Gql.Field f : introspectionRoots(doc, operationName)) {
            try {
                data.put(GqlToPure.key(f), Introspection.answer(f, scoped.schema(), fragments));
            } catch (GqlTranslationException e) {
                return envelope(400, null, List.of(problem(pathOf(e.path()), String.valueOf(e.getMessage()), e.kind().name())),
                        null);
            }
        }
        Map<String, Object> extensions = new LinkedHashMap<>();
        if (!pure.isEmpty()) {
            extensions.put("pure", pure);
        }
        if (!v.warnings().isEmpty()) {
            List<Map<String, Object>> warnings = new ArrayList<>();
            for (Problem p : v.warnings()) {
                warnings.add(problem(pathOf(p.path()), p.message(), "WARNING"));
            }
            extensions.put("warnings", warnings);
        }
        return envelope(200, data, List.of(), extensions.isEmpty() ? null : extensions);
    }

    /** The operation's introspection root fields, in document order. */
    private static List<Gql.Field> introspectionRoots(Gql.Document doc, @com.legend.base.Nullable String name) {
        List<Gql.Field> out = new ArrayList<>();
        for (Gql.Definition d : doc.definitions()) {
            if (d instanceof Gql.Operation op && (name == null || name.equals(op.name()))) {
                for (Gql.Selection s : op.selectionSet()) {
                    if (s instanceof Gql.Field f && Introspection.isRoot(f.name())) {
                        out.add(f);
                    }
                }
            }
        }
        return out;
    }

    private static Answer envelope(int status, @com.legend.base.Nullable Map<String, Object> data,
            List<Map<String, Object>> errors, @com.legend.base.Nullable Map<String, Object> extensions) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (!errors.isEmpty()) {
            out.put("errors", errors);
        }
        if (data != null || status == 200) {
            out.put("data", data);
        }
        if (extensions != null) {
            out.put("extensions", extensions);
        }
        return new Answer(status, Json.toCompact(out));
    }

    private static Map<String, Object> problem(List<String> path, String message, String code) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("message", message);
        if (!path.isEmpty()) {
            out.put("path", path);
        }
        Map<String, Object> ext = new LinkedHashMap<>();
        ext.put("code", code);
        out.put("extensions", ext);
        return out;
    }

    /** A dotted path ({@code Person.employees}) as the envelope's path list. */
    private static List<String> pathOf(String dotted) {
        if (dotted == null || dotted.isEmpty()) {
            return List.of();
        }
        return List.of(dotted.split("\\."));
    }

    private record Scoped(Schema schema, Scope scope) {
    }

    /** The model compiled and its schema over the data space named, the classes listed, or everything. */
    private static Scoped scoped(Json.Obj request) {
        String text = PureV1Api.modelText(request.getObj("model"));
        ModelContext ctx = Compiler.compileModel(text);
        Scope scope;
        String dataSpace = request.getStringOr("dataSpace", null);
        if (dataSpace != null) {
            scope = Scope.of(ctx, dataSpace(text, dataSpace));
        } else if (request.has("classes")) {
            scope = new Scope(request.getStringArray("classes"));
            for (String fqn : scope.classFqns()) {
                if (ctx.findClass(fqn).isEmpty()) {
                    throw new IllegalArgumentException("no class " + fqn + " in the model");
                }
            }
        } else {
            scope = Scope.all(ctx);
        }
        return new Scoped(SchemaGenerator.generate(ctx, scope), scope);
    }

    private static DataSpaceDefinition dataSpace(String modelText, String name) {
        ParsedModel parsed = Compiler.parseModel(modelText);
        Map<String, DataSpaceDefinition> byName = new LinkedHashMap<>();
        for (PackageableElement e : parsed.elements()) {
            if (e instanceof DataSpaceDefinition ds) {
                byName.put(ds.qualifiedName(), ds);
            }
        }
        DataSpaceDefinition ds = byName.get(name);
        if (ds == null) {
            throw new IllegalArgumentException("no data space " + name + " in the model");
        }
        return ds;
    }

    private static Limits limits(Json.Obj request) {
        Json.Obj l = request.getObjOr("limits", null);
        if (l == null) {
            return Limits.DEFAULT;
        }
        return new Limits(l.getIntOr("maxDepth", Limits.DEFAULT.maxDepth()),
                l.getIntOr("maxListFieldsPerLevel", Limits.DEFAULT.maxListFieldsPerLevel()));
    }

    private static List<Map<String, Object>> problems(List<Problem> problems) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Problem p : problems) {
            out.add(problem(p.path(), p.message()));
        }
        return out;
    }

    private static Map<String, Object> problem(String path, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("path", path);
        m.put("phase", "TYPE");
        m.put("message", message);
        return m;
    }
}
