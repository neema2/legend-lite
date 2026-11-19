// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lite;

import com.legend.json.Json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * LITE'S OWN WIRE, ONE SOURCE (lite's API v1 spec §2, §3; H4's P4, the user's CP-13 of 2026-11-19, and the orchestrator's
 * option (a) of that day): what every binding of lite's API answers a refusal and a request with -- its kinds
 * ({@link Kind}: the wire word, the HTTP status, a 405's {@code Allow}), its one error shape ({@link Refusal}), a closed
 * request read by name ({@link #read}), the routes lite serves ({@link #ROUTES}), and the two answers a host gives on its
 * own route ({@link #user}, {@link #notPosted}) -- in a library of {@code base} and {@code json} alone, so a host that
 * carries no compiler (the query-store page, TeaVM over the saved queries) answers in the same words as the server.
 *
 * <p>LITE'S OWN ONLY: nothing of legend-engine's (its envelope and its store words are the edge's, which may depend on
 * this, never the reverse), and no classifier of the product's exceptions ({@code server.LiteV1Api.refusal}, which reads
 * the compiler's and exec's types, builds a {@link Refusal} from them). A refusal's {@code message} is lite's words
 * today; it becomes the presenter's output when the error records land (the compatibility program's H13), its
 * {@code raise} staying the raise kind's wire name, the record's operands a sibling key then (agreed with H13,
 * 2026-11-19).
 */
public final class LiteWire {

    private LiteWire() {
    }

    /** A request body's JSON: deep enough for a model given as data or a long query's protocol tree, bounded still
     *  against a hostile body (the server's {@code LiteV1Api.REQUEST}, the same bound). */
    public static final Json.Config REQUEST = new Json.Config(4096);

    /**
     * LITE'S ERROR KINDS (lite's API v1 spec §3, the audit's J18, 2026-11-17): every refusal, on every binding, is one of
     * these, written {@code {kind, message, where?, raise?}} ({@link Refusal}). Over HTTP the status follows the kind:
     * 400 for parse, compile, unsupported, request and parameters; 405 for a method a route does not take, with its
     * {@code Allow} header; 404 for what does not exist; 422 for data; 500 for internal. There is no
     * {@code PARSER}/{@code COMPILATION}, no {@code code: -1}, no {@code status: "error"} and no {@code errorType} in the
     * product: legend-engine's envelope is the edge's ({@code com.legend.edge.EngineEnvelope}).
     */
    public enum Kind {
        /** The text does not parse. */
        PARSE(400, "parse"),
        /** The model or the query does not type or compile. */
        COMPILE(400, "compile"),
        /** A construct lite refuses by name (not yet, or by a ruling). */
        UNSUPPORTED(400, "unsupported"),
        /** A run's raise: division by zero, an index out of bounds, ... (its kind in {@link Refusal#raise}). */
        DATA(422, "data"),
        /** A run's parameter values: one missing, or one its declared type does not take (the caller's values). */
        PARAMETERS(400, "parameters"),
        /** The request itself is malformed: a key it does not read or lacks, a value of the wrong kind. */
        REQUEST(400, "request"),
        /** A known operation asked by a method it does not take (lite's operations take POST): the route exists, the
         *  method does not; answered with {@code Allow: POST} (the orchestrator's ruling of 2026-11-19, H4's routes). */
        METHOD_NOT_ALLOWED(405, "methodNotAllowed", Map.of("Allow", "POST")),
        /** A saved query or saved page the request names, or a route under {@code /api/lite/v1/}, does not exist (the
         *  user, 2026-11-17 ~23:31; spec §3; a route since the orchestrator's ruling of 2026-11-19). */
        NOT_FOUND(404, "notFound"),
        /** Anything else: a defect, said as one. */
        INTERNAL(500, "internal");

        private final int status;
        private final String wire;
        private final Map<String, String> headers;

        // each kind's word explicit (2026-11-17, notFound: lower camel, which no spelling of the constant's name gives)
        Kind(int status, String wire) {
            this(status, wire, Map.of());
        }

        Kind(int status, String wire, Map<String, String> headers) {
            this.status = status;
            this.wire = wire;
            this.headers = headers;
        }

        /** The kind as every binding writes it: {@code parse}, {@code compile}, ..., {@code notFound}. */
        public String wire() {
            return wire;
        }

        /** The HTTP status a refusal of this kind is answered with. */
        public int status() {
            return status;
        }

        /** The HTTP headers a refusal of this kind is answered with, beside its status (a 405's {@code Allow}). */
        public Map<String, String> headers() {
            return headers;
        }
    }

    /** An answer of lite's wire over HTTP: its status, its JSON and its headers (the media type JSON's). A server's own
     *  answer type is the server's ({@code server.LiteV1Api.Answer}), made from this. */
    public record Answer(int status, String json, Map<String, String> headers) {
        public Answer {
            headers = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        }
    }

    /**
     * A refusal in lite's one error shape: its {@link Kind}, lite's own words, where it happened (a position, or a site)
     * where there is one, and for a {@code data} refusal alone the raise's kind, in lower camel as every wire name is
     * ({@code divideByZero}, {@code indexOutOfBounds}, ...).
     */
    public record Refusal(Kind kind, String message, @com.legend.base.Nullable String where,
            @com.legend.base.Nullable String raise) {

        /** {@code {kind, message, where?, raise?}}, in that order. */
        public String json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("kind", kind.wire());
            out.put("message", message);
            if (where != null) {
                out.put("where", where);
            }
            if (raise != null) {
                out.put("raise", raise);
            }
            return Json.toCompact(out);
        }

        /** The refusal answered over HTTP: the kind's status and headers, its JSON. */
        public Answer answer() {
            return new Answer(kind.status(), json(), kind.headers());
        }
    }

    /** A request lite cannot read: not a JSON object, a key it does not read or one it lacks, a value of the wrong kind
     *  (kind {@code request}). */
    public static final class MalformedRequest extends IllegalArgumentException {
        public MalformedRequest(String message) {
            super(message);
        }

        public MalformedRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A route under {@code /api/lite/v1/} that lite does not serve (kind {@code notFound}; the orchestrator's ruling of
     *  2026-11-19: until then kind request, 400). */
    public static final class UnknownRoute extends IllegalArgumentException {
        public UnknownRoute(String path) {
            super(words(path));
        }

        /** The words an unknown route is refused in. */
        public static String words(String path) {
            return "no such legend-lite API: " + path;
        }
    }

    /** A refusal of {@code kind} in {@code message}'s words, answered. */
    public static Answer refuse(Kind kind, String message) {
        return new Refusal(kind, message, null, null).answer();
    }

    /** The route of lite's {@code user} operation (spec §2): a host's own. */
    public static final String USER = "/api/lite/v1/user";

    /**
     * EVERY ROUTE LITE SERVES under {@code /api/lite/v1/} by POST (spec §2): the one road's operations
     * ({@code server.LiteV1Api.operate}'s switch), {@code query/run} (a host's) and {@code user} (a host's:
     * {@link #user}) -- what tells a route that does not exist (404) from one asked by a method it does not take (405)
     * (the orchestrator's ruling of 2026-11-19). Held equal to the switch: each answered by it
     * (LiteV1PlanningTest), and the switch's cases each a row of the census's LITE_ROUTES.
     */
    public static final Set<String> ROUTES = Set.of(
            "/api/lite/v1/model/check", "/api/lite/v1/model/elements", "/api/lite/v1/query/parse",
            "/api/lite/v1/query/print", "/api/lite/v1/query/type", "/api/lite/v1/query/plan", "/api/lite/v1/query/run",
            "/api/lite/v1/model/createClass", "/api/lite/v1/model/createAssociation",
            "/api/lite/v1/model/createDataSpace", "/api/lite/v1/ts/item", "/api/lite/v1/ts/statement", USER);

    /**
     * A call of lite's routes in another method than POST -- every binding's answer (H4's P4, the user's CP-13 of
     * 2026-11-19; the orchestrator's ruling of that day): a route lite does not serve, kind {@code notFound} (the path
     * is read first); a route it serves, kind {@code methodNotAllowed}, its {@code Allow: POST}, the method asked named.
     */
    public static Answer notPosted(String method, String path) {
        if (!ROUTES.contains(path)) {
            return refuse(Kind.NOT_FOUND, UnknownRoute.words(path));
        }
        return refuse(Kind.METHOD_NOT_ALLOWED, "lite's operations take POST, not " + method + ": " + path);
    }

    /**
     * LITE'S {@code user} OPERATION (spec §2; the orchestrator's Q-b ruling of 2026-11-17): {@code {user}}, who the store
     * keeps saved queries for -- {@code who}, the host's own caller (the server's {@code SavedQueries.ANONYMOUS},
     * Python's local user, the page's configured one). POST, its request none: an empty body, or an empty object; any
     * other request refused by name, kind {@code request}; another method, kind {@code methodNotAllowed}; a path below
     * it, kind {@code notFound} -- every binding's answer, so their words cannot differ (H4's P4, 2026-11-19).
     */
    public static Answer user(String method, String path, String body, String who) {
        if (!path.equals(USER)) {
            return refuse(Kind.NOT_FOUND, UnknownRoute.words(path));
        }
        if (!"POST".equals(method)) {
            return refuse(Kind.METHOD_NOT_ALLOWED, "lite's user operation takes POST, not " + method + ": " + path);
        }
        // strip().isEmpty(), not isBlank(): text the tab writes alike (ArchitectureTest's portable-text rule)
        if (!body.strip().isEmpty()) {
            try {
                read(body, "a user request", Set.of());
            } catch (MalformedRequest refused) {
                return refuse(Kind.REQUEST, String.valueOf(refused.getMessage()));
            }
        }
        return new Answer(200, Json.toCompact(Map.of("user", who)), Map.of());
    }

    /**
     * {@code body} as lite's CLOSED request: a JSON object holding every one of {@code keys} and, where given,
     * {@code optional}'s, and no other key -- anything else refused by name ({@link MalformedRequest}).
     */
    public static Json.Obj read(String body, String what, Set<String> optional, String... keys) {
        Json.Obj request = requestObject(body, what);
        Set<String> wanted = Set.of(keys);
        String holds = keys.length == 0 && optional.isEmpty() ? "none" : String.join(", ", keys) + (optional.isEmpty()
                ? "" : "; optional " + String.join(", ", new java.util.TreeSet<>(optional)));
        for (String key : request.fields().keySet()) {
            if (!wanted.contains(key) && !optional.contains(key)) {
                throw new MalformedRequest(what + " has no key " + key + " (it holds " + holds + ")");
            }
        }
        for (String key : keys) {
            if (!request.fields().containsKey(key)) {
                throw new MalformedRequest(what + " names no " + key + " (it holds " + holds + ")");
            }
        }
        return request;
    }

    /** {@code body} as lite's request, a JSON object, else refused by name, kind {@code request}. */
    public static Json.Obj requestObject(String body, String what) {
        Json.Node n;
        try {
            n = Json.parse(body, REQUEST);
        } catch (IllegalArgumentException e) {
            throw new MalformedRequest(what + " is not a JSON object: " + e.getMessage(), e);
        }
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw new MalformedRequest(what + " is not a JSON object: the request body is not a JSON object");
    }
}
