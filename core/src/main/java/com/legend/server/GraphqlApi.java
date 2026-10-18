// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.GqlSchema.Schema;
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
import java.util.TreeSet;

/**
 * legend-lite's GraphQL routes (docs/GRAPHQL_STUDY_2026_10_18.md §8.4, the first two of the four tools):
 * {@code /api/lite/v1/graphql/schema}, the SDL generated for a model's data space ({@code describe}), and
 * {@code /api/lite/v1/graphql/validate}, a document against that schema with every error, every warning and the
 * typed result shape ({@code validate}). Both take the model as {@code compile} does, a data space by name or a
 * class list to scope the schema, and {@code limits} (G17). A model the generator refuses is answered as a model
 * that does not compile is, 400 COMPILATION. The query and explain routes are the line's PR2.
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
            Gql.Document doc;
            try {
                doc = GqlParser.parseDocument(query, 1, 1);
            } catch (com.legend.parser.ParseException e) {
                out.put("valid", false);
                out.put("errors", List.of(problem("", "PARSE", String.valueOf(e.getMessage()))));
                out.put("warnings", List.of());
                return Json.toCompact(out);
            }
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
            scope = new Scope(new TreeSet<>(request.getStringArray("classes")));
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
        for (PackageableElement e : parsed.elements()) {
            if (e instanceof DataSpaceDefinition ds && ds.qualifiedName().equals(name)) {
                return ds;
            }
        }
        throw new IllegalArgumentException("no data space " + name + " in the model");
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
            out.add(problem(p.path(), "TYPE", p.message()));
        }
        return out;
    }

    private static Map<String, Object> problem(String path, String phase, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("path", path);
        m.put("phase", phase);
        m.put("message", message);
        return m;
    }
}
