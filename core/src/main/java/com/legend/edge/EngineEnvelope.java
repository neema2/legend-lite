// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;
import com.legend.server.LiteV1Api.Answer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * LEGEND-ENGINE'S ERROR ENVELOPE, the edge's own (lite's API v1 spec §3 and the audit's J18, 2026-11-17: the product
 * answers {@code {kind, message, where?, raise?}}, and the product names no {@code code: -1}, no {@code status:
 * "error"} and no {@code errorType}). These are the lines lite's {@code LiteV1Api} held until then, moved byte for byte,
 * so legend-engine's API at the edge answers exactly as it did: {@code {"code":-1, "errorType"?, "message", "status":
 * "error"}}.
 */
public final class EngineEnvelope {

    private EngineEnvelope() {
    }

    /**
     * A call's JSON, or its failure in legend-engine's error shape, with the status for each kind (measured against
     * 4.145.0, 2026-09-27):
     * <ul>
     *   <li>the text does not parse or compile, or the construct is not implemented: {@code status} with
     *       {@code errorType} (grammar and lambdaRelationType answer 400, generatePlan and execute 500);</li>
     *   <li>a malformed request, or the database refusing: 500, no {@code errorType}, the message naming the
     *       exception;</li>
     *   <li>anything else is a BUG: logged whole, answered 500 the same way -- never a dropped connection.</li>
     * </ul>
     */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call) {
        return answer(status, errorType, call, e -> String.valueOf(e.getMessage()));
    }

    /** {@link #answer(int, String, Supplier)} with a refusal of the text worded by {@code words}: upstream's sentence of
     *  a compile error's kind (D1.6, W7 of docs/records/d-plan-2026-11-16.md: {@link PureMessages#compile}). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words) {
        return answer(status, errorType, call, words, t -> t.getClass().getSimpleName());
    }

    /** {@link #answer}, an uncaught error's type named by {@code typeName}: upstream's name of it (the P2 checkpoint
     *  audit's M3/A-M4, 2026-11-16). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words, java.util.function.Function<Throwable, String> typeName) {
        return answer(status, errorType, call, words, typeName, e -> false);
    }

    /** {@link #answer}, the refusals {@code uncaught} names answered as an uncaught error instead: a kind upstream
     *  raises at run, not at compile (the P2 checkpoint audit's A-S2, 2026-11-16). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words, java.util.function.Function<Throwable, String> typeName,
            java.util.function.Predicate<Throwable> uncaught) {
        try {
            return new Answer(200, call.get());
        } catch (RuntimeException | StackOverflowError e) {
            if (refusesTheText(e) && !uncaught.test(e)) {
                return error(status, errorType, words.apply(e));
            }
            if (!(e instanceof IllegalArgumentException || e instanceof com.legend.error.DataError)) {
                e.printStackTrace();
            }
            Throwable n = e instanceof com.legend.error.DataError && e.getCause() != null ? e.getCause() : e;
            return error(500, null, typeName.apply(n) + ": " + words.apply(n));
        }
    }

    /** A host's refusal of a call it could not finish -- its database refusing the SQL, say -- as {@link #answer}
     *  answers a database's refusal: 500, no {@code errorType}. */
    public static Answer refused(String message) {
        return error(500, null, message);
    }

    /** The text does not parse or compile, or a construct is not implemented: answered with an errorType. */
    private static boolean refusesTheText(Throwable e) {
        return e instanceof com.legend.error.LegendCompileException
                || e instanceof com.legend.error.NotImplementedException
                || e instanceof com.legend.sql.DialectCapability;
    }

    /** legend-engine's error body: {@code {"code":-1, "errorType"?, "message", "status":"error"}}. */
    public static Answer error(int status, @com.legend.base.Nullable String errorType, String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", -1);
        if (errorType != null) {
            out.put("errorType", errorType);
        }
        out.put("message", message);
        out.put("status", "error");
        return new Answer(status, Json.toCompact(out));
    }
}
