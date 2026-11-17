// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.server.LiteV1Api.Answer;
import com.legend.server.LiteV1Api.Models;
import com.legend.server.LiteV1Api.RefusalKind;

import java.util.Map;
import java.util.Set;

/**
 * LITE'S OWN GRAMMAR AND TYPING ROUTES (the edge program's HOST stream, 2026-11-17; the orchestrator's Q1 ruling: "a NEW
 * server file ... mounted from LiteV1Api by ONE line ... each route calls the same product Java the tab's lite exports
 * call ... not the pure/v1 router. Typed routes only, no generic dispatch"): what a query app asks a server to read and
 * type a query by -- a model's text as its protocol JSON, a lambda's text as its JSON and back, and a lambda's relation
 * type over a model -- answered from the product's own code, the calls {@code planner.Boundary}'s lite exports make
 * ({@code modelElements}, {@code parseLambda}, {@code printLambda}, {@code lambdaRelationType}; the census's A1).
 * legend-engine's {@code pure/v1} routes for the same questions are the edge's ({@code com.legend.edge.PureV1Routes}).
 *
 * <p>Each request is a closed JSON object: every key it names required, any other refused by name. Each answer is lite's
 * JSON; a refusal is {@link LiteV1Api#answer}'s, its envelope and words LiteV1Api's.
 */
public final class LiteV1Planning {
    private LiteV1Planning() {}

    /** {@code {"text", "sourceInformation"}}: a model's text as its protocol JSON ({@code PureModelContextData}). */
    public static final String MODEL_ELEMENTS = "/api/lite/v1/grammar/modelElements";
    /** {@code {"text", "sourceInformation"}}: a lambda's text as its protocol JSON. */
    public static final String PARSE_LAMBDA = "/api/lite/v1/grammar/parseLambda";
    /** {@code {"lambda", "style"}}: a lambda's protocol JSON as its Pure text, {@code {"text"}}. */
    public static final String PRINT_LAMBDA = "/api/lite/v1/grammar/printLambda";
    /** {@code {"model", "lambda"}}: a lambda's relation type over a model given as text. */
    public static final String LAMBDA_RELATION_TYPE = "/api/lite/v1/compilation/lambdaRelationType";

    /** {@code path}'s answer for {@code body}, a model given as text compiled by {@code models}. LiteV1Api routes
     *  only these paths here (its one line); any other is refused by name, never answered by another route. */
    public static Answer route(String path, String body, Models models) {
        return switch (path) {
            case MODEL_ELEMENTS -> modelElements(body);
            case PARSE_LAMBDA -> parseLambda(body);
            case PRINT_LAMBDA -> printLambda(body);
            case LAMBDA_RELATION_TYPE -> lambdaRelationType(body, models);
            default -> throw new IllegalArgumentException("not one of lite's planning routes: " + path);
        };
    }

    /** {@value #MODEL_ELEMENTS}: through the parse door, as the tab's {@code modelElements} reads it. */
    public static Answer modelElements(String body) {
        return LiteV1Api.answer(400, RefusalKind.PARSE.text(), () -> {
            Json.Obj request = closed(body, "a modelElements request", "text", "sourceInformation");
            return com.legend.door.Parse.documentJson(request.getString("text"), request.getBool("sourceInformation"));
        });
    }

    /** {@value #PARSE_LAMBDA}: text without a leading {@code |} read as a parameterless lambda, as the tab's
     *  {@code parseLambda} reads it, through the parse door. */
    public static Answer parseLambda(String body) {
        return LiteV1Api.answer(400, RefusalKind.PARSE.text(), () -> {
            Json.Obj request = closed(body, "a parseLambda request", "text", "sourceInformation");
            return com.legend.door.Parse.lambdaJson(request.getString("text"), request.getBool("sourceInformation"));
        });
    }

    /** {@value #PRINT_LAMBDA}: {@code PRETTY} or {@code STANDARD}, any other style refused by name, as the tab's
     *  {@code printLambda} prints it. */
    public static Answer printLambda(String body) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "a printLambda request", "lambda", "style");
            String style = request.getString("style");
            PureComposer.Style s = switch (style) {
                case "PRETTY" -> PureComposer.Style.PRETTY;
                case "STANDARD" -> PureComposer.Style.STANDARD;
                default -> throw new IllegalArgumentException("a lambda prints PRETTY or STANDARD, not '" + style + "'");
            };
            return Json.toCompact(Map.of("text", PureComposer.lambda(request.getObj("lambda"), s)));
        });
    }

    /** {@value #LAMBDA_RELATION_TYPE}: the lambda typed, compile-only, over the model, as the tab's
     *  {@code lambdaRelationType} types it. */
    public static Answer lambdaRelationType(String body, Models models) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "a lambdaRelationType request", "model", "lambda");
            ModelInput model = ModelInput.of(request.getObj("model"));
            com.legend.protocol.spec.LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            return model.with(models, ctx -> Json.toCompact(com.legend.plan.RelationTypeJson.of(
                    com.legend.Compiler.query(ctx, lambda).resultType())));
        });
    }

    /** {@code body} as a JSON object holding exactly {@code keys}: a key it lacks, or one it does not read, refused by
     *  name. */
    private static Json.Obj closed(String body, String what, String... keys) {
        Json.Obj request = LiteV1Api.request(body);
        Set<String> wanted = Set.of(keys);
        for (String key : request.fields().keySet()) {
            if (!wanted.contains(key)) {
                throw new IllegalArgumentException(what + " has no key " + key + " (it holds " + String.join(", ", keys) + ")");
            }
        }
        for (String key : keys) {
            if (!request.fields().containsKey(key)) {
                throw new IllegalArgumentException(what + " names no " + key + " (it holds " + String.join(", ", keys) + ")");
            }
        }
        return request;
    }
}
