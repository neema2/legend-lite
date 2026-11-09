package com.legend.queries;


import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * legend-engine's query store ({@code /api/pure/v1/query}, legend-engine-application-query's
 * {@code ApplicationQuery} + {@code QueryStoreManager} + the versioned Mongo DAO), in legend-lite:
 * the same endpoints, the same {@code Query} records, the same rules -- a query is created once,
 * only its owner updates, patches or deletes it, every update is a new version and the old one
 * is its history, a delete keeps the history. Read from the engine's source (4.145.0 line,
 * 2026-09-30); its Mongo cannot be run here, so these rules are ported, not measured.
 *
 * <p>ONE IMPLEMENTATION, STORAGE PER HOST (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5): these rules are the only
 * ones -- legend-lite's server, Python's native library and the page's WebAssembly module each answer the routes
 * with them -- and only where the versions are kept differs ({@link Records}): a directory, one JSON file per query
 * holding every version ({@link FileRecords}: the server's {@code --query-store DIR}, Python's {@code ll.home()/queries}),
 * or the records the page keeps in IndexedDB, handed in with each call ({@link MemoryRecords}). The rules and their
 * records reach only {@code //base} and {@code //json}, so they compile to WebAssembly. Without a store the server
 * refuses every call, as the engine does without its database ("has not been configured properly"); it never picks
 * a place of its own.
 *
 * <p>Recorded differences: a current version's {@code validUntil} is {@code null} (the engine
 * writes its far-future sentinel); {@code query/events} and {@code query/stats} are not served.
 */
public final class SavedQueries {

    /** An {@code ApplicationQueryException}: its status and the engine's {@code {"message"}} body. */
    public static final class Refusal extends RuntimeException {
        final int status;

        Refusal(String message, int status) {
            super(message);
            this.status = status;
        }
    }

    private static final Pattern VALID_ARTIFACT_ID = Pattern.compile("^[a-z][a-z0-9_]*+(-[a-z][a-z0-9_]*+)*+$");
    private static final Pattern JAVA_NAME = Pattern.compile("^[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*$");
    private static final int MAX_NUMBER_OF_QUERIES = 100;
    private static final int GET_QUERIES_LIMIT = 50;
    private static final String QUERY_PROFILE_PATH = "meta::pure::profiles::query";
    private static final String QUERY_PROFILE_TAG_DATA_SPACE = "dataSpace";

    /** The {@code Query} fields, in the engine's class order (its JSON's key order). */
    private static final List<String> FIELDS = List.of("id", "name", "description", "groupId", "artifactId",
            "versionId", "originalVersionId", "executionContext", "content", "lastUpdatedAt", "createdAt",
            "lastOpenAt", "deletedAt", "validUntil", "version", "taggedValues", "stereotypes",
            "defaultParameterValues", "owner", "gridConfig");
    /** What a search answers without (the engine's EXCLUDED_PROJECTION_FIELDS). */
    private static final List<String> NOT_IN_A_SEARCH = List.of("validUntil", "version", "content",
            "executionContext", "taggedValues", "stereotypes", "defaultParameterValues", "gridConfig");
    /** The fields a client sets; the rest are the store's (audit). */
    private static final List<String> CLIENT_FIELDS = List.of("id", "name", "description", "groupId", "artifactId",
            "versionId", "originalVersionId", "executionContext", "content", "taggedValues", "stereotypes",
            "defaultParameterValues", "gridConfig");

    /** The execution contexts served, each with its required fields and the engine's message for one missing. */
    private static final Map<String, Map<String, String>> CONTEXT_FIELDS = orderedContexts();

    private static Map<String, Map<String, String>> orderedContexts() {
        Map<String, String> explicit = new LinkedHashMap<>();
        explicit.put("mapping", "Query mapping is missing or empty");
        explicit.put("runtime", "Query runtime is missing or empty");
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        out.put("explicitExecutionContext", java.util.Collections.unmodifiableMap(explicit));
        out.put("dataSpaceExecutionContext",
                Map.of("dataSpacePath", "Query data Space execution context dataSpace path is missing or empty"));
        return java.util.Collections.unmodifiableMap(out);
    }

    /** An answer as the engine gives it: its status, and its JSON (empty for a 204). */
    public record Answer(int status, String json) {
    }

    private final Records records;
    private final LongSupplier clock;

    public SavedQueries(Records records, LongSupplier clock) {
        this.records = records;
        this.clock = clock;
    }

    // ------------------------------------------------------------------ HTTP

    /** The user a request acts as: legend-lite has no sign-in yet, so every caller is the
     *  engine's anonymous profile ({@code server/v1/currentUser} answers the same). */
    public static final String ANONYMOUS = "anonymous";

    /**
     * One {@code /api/pure/v1/query...} request: {@code rest} is the path after
     * {@code /api/pure/v1/query}. The engine's answers: 200 with JSON, 204 for a delete, and a
     * refusal as {@code {"message"}} with its status.
     */
    public static Answer answer(@com.legend.base.Nullable SavedQueries store, String method, String rest,
            @com.legend.base.Nullable String rawQuery, String body, String user) {
        if (store == null) {
            return new Answer(500, "{\"code\":-1,\"message\":\"Query store has not been configured properly"
                    + " (legend-lite: start the server with --query-store DIR)\",\"status\":\"error\"}");
        }
        try {
            String[] parts = rest.isEmpty() ? new String[0] : rest.substring(1).split("/", -1);
            String id = parts.length > 0 ? java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8) : "";
            if (parts.length > 0 && "dataCube".equals(parts[0])) {
                return new Answer(404, "{\"code\":-1,\"message\":\"no such legend-engine API in legend-lite: "
                        + "/api/pure/v1/query" + rest + "\",\"status\":\"error\"}");
            }
            return switch (method + " " + parts.length + " " + (parts.length > 1 ? parts[1] : "")) {
                case "POST 0 " -> ok(store.create(body, user));
                case "POST 1 " -> "search".equals(id) ? ok(store.search(body, user)) : notFound(rest);
                case "GET 1 " -> "batch".equals(id) ? ok(store.batch(queryParams(rawQuery, "queryIds"))) : ok(store.get(id));
                case "PUT 1 " -> ok(store.update(id, body, user));
                case "DELETE 1 " -> {
                    store.delete(id, user);
                    yield new Answer(204, "");
                }
                case "GET 2 history" -> {
                    List<String> v = queryParams(rawQuery, "version");
                    yield ok(store.history(id, v.isEmpty() ? null : Integer.valueOf(v.get(0))));
                }
                case "PUT 2 patchQuery" -> ok(store.patch(id, body, user));
                default -> notFound(rest);
            };
        } catch (Refusal r) {
            return new Answer(r.status, Json.toCompact(Map.of("message", String.valueOf(r.getMessage()))));
        } catch (java.io.UncheckedIOException | IllegalArgumentException | ClassCastException e) {
            // the store's disk failing, or a body that is not the JSON the engine takes (a bad
            // integer, a field of the wrong kind): the engine's 500, naming the exception
            return failed(e);
        }
    }

    /** The engine's 500 for an exception, naming it: {@code {"code", "message", "status"}}, in that order. */
    static Answer failed(RuntimeException e) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", -1);
        out.put("message", e.getClass().getSimpleName() + ": " + e.getMessage());
        out.put("status", "error");
        return new Answer(500, Json.toCompact(out));
    }

    private static Answer ok(String json) {
        return new Answer(200, json);
    }

    private static Answer notFound(String rest) {
        return new Answer(404, "{\"code\":-1,\"message\":\"no such legend-engine API in legend-lite: "
                + "/api/pure/v1/query" + rest + "\",\"status\":\"error\"}");
    }

    private static List<String> queryParams(@com.legend.base.Nullable String rawQuery, String name) {
        List<String> out = new ArrayList<>();
        if (rawQuery == null) {
            return out;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String k = java.net.URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            if (k.equals(name) && eq >= 0) {
                out.add(java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ the endpoints

    /** {@code POST query/search}. */
    public synchronized String search(String body, String currentUser) {
        Json.Obj spec = Json.parseObject(body.strip().isEmpty() ? "{}" : body);
        searchShape(spec);
        List<Json.Obj> matches = new ArrayList<>();
        for (Json.Obj q : latestOfAll()) {
            if (matchesSearch(q, spec, currentUser)) {
                matches.add(q);
            }
        }
        String sortBy = spec.has("sortByOption") && !(spec.get("sortByOption") instanceof Json.Null)
                ? spec.getString("sortByOption") : null;
        if (sortBy != null) {
            String field = switch (sortBy) {
                case "SORT_BY_CREATE" -> "createdAt";
                case "SORT_BY_VIEW" -> "lastOpenAt";
                case "SORT_BY_UPDATE" -> "lastUpdatedAt";
                default -> throw new IllegalArgumentException("Unknown sort-by value");
            };
            matches.sort(Comparator.comparingLong((Json.Obj q) -> longOr(q, field, Long.MIN_VALUE)).reversed());
        }
        Integer limit = spec.has("limit") && !(spec.get("limit") instanceof Json.Null) ? spec.getInt("limit") : null;
        if (limit != null && limit <= 0) {
            throw new Refusal("Limit should be greater than 0", 400);
        }
        int max = Math.min(MAX_NUMBER_OF_QUERIES, limit == null ? Integer.MAX_VALUE : limit);
        List<Json.Obj> limited = new ArrayList<>(matches.subList(0, Math.min(max, matches.size())));
        // the engine's last step: the current user's queries first, otherwise in order (a stable sort)
        limited.sort(Comparator.comparingInt((Json.Obj q) -> currentUser.equals(stringOr(q, "owner")) ? 0 : 1));
        List<Object> out = new ArrayList<>();
        for (Json.Obj q : limited) {
            out.add(view(q, NOT_IN_A_SEARCH));
        }
        return Json.toCompact(out);
    }

    /** {@code GET query/batch?queryIds=...}. */
    public synchronized String batch(List<String> queryIds) {
        if (queryIds.size() > GET_QUERIES_LIMIT) {
            throw new Refusal("Can't fetch more than " + GET_QUERIES_LIMIT + " queries", 400);
        }
        List<Object> out = new ArrayList<>();
        TreeSet<String> notFound = new TreeSet<>();
        for (String id : new java.util.LinkedHashSet<>(queryIds)) {
            Json.Obj q = latest(id);
            if (q == null) {
                notFound.add(id);
            } else {
                out.add(view(q, List.of()));
            }
        }
        if (!notFound.isEmpty()) {
            throw new Refusal("Can't find queries for the following ID(s):\n" + String.join("\n", notFound), 500);
        }
        return Json.toCompact(out);
    }

    /** {@code GET query/{id}}: the query, its {@code lastOpenAt} now (no new version). */
    public synchronized String get(String id) {
        List<Json.Obj> versions = versions(id);
        Json.Obj q = current(versions);
        if (q == null) {
            throw new Refusal("Can't find query with ID '" + id + "'", 404);
        }
        Json.Obj opened = with(q, "lastOpenAt", Json.num(clock.getAsLong()));
        versions.set(versions.indexOf(q), opened);
        write(id, versions);
        return Json.toCompact(view(opened, List.of()));
    }

    /** {@code GET query/{id}/history[?version=n]}: its earlier (and deleted) versions, or one version. */
    public synchronized String history(String id, @com.legend.base.Nullable Integer version) {
        List<Json.Obj> versions = versions(id);
        if (version != null) {
            for (Json.Obj v : versions) {
                if (v.getInt("version") == version) {
                    return Json.toCompact(List.of(view(v, List.of())));
                }
            }
            if (versions.isEmpty()) {
                throw new Refusal("Can't find query with ID '" + id + "'", 404);
            }
            throw new Refusal("Can't find version '" + version + "' for query with ID '" + id + "'", 404);
        }
        List<Object> out = new ArrayList<>();
        for (Json.Obj v : versions) {
            if (!(v.get("validUntil") instanceof Json.Null)) {
                out.add(view(v, List.of()));
            }
        }
        if (out.isEmpty() && current(versions) == null) {
            throw new Refusal("Can't find query with ID '" + id + "'", 404);
        }
        return Json.toCompact(out);
    }

    /** {@code POST query}: a new query, owned by the caller, version 1. */
    public synchronized String create(String body, String currentUser) {
        Json.Obj query = Json.parseObject(body);
        queryShape(query);
        validate(query);
        String id = query.getString("id");
        List<Json.Obj> versions = versions(id);
        if (current(versions) != null) {
            throw new Refusal("Query with ID '" + id + "' already existed", 400);
        }
        long now = clock.getAsLong();
        Map<String, Json.Node> fields = clientFields(query);
        fields.put("createdAt", Json.num(now));
        fields.put("lastUpdatedAt", Json.num(now));
        fields.put("lastOpenAt", Json.num(now));
        fields.put("version", Json.num(1));
        fields.put("owner", Json.str(currentUser));
        Json.Obj created = ordered(fields);
        versions.add(created);
        write(id, versions);
        return Json.toCompact(view(created, List.of()));
    }

    /** {@code PUT query/{id}}: the owner's new version; the old one becomes history. */
    public synchronized String update(String id, String body, String currentUser) {
        Json.Obj query = Json.parseObject(body);
        queryShape(query);
        validate(query);
        if (!id.equals(query.getString("id"))) {
            throw new Refusal("Updating query ID is not supported", 400);
        }
        return Json.toCompact(view(newVersion(id, clientFields(query), currentUser), List.of()));
    }

    /** {@code PUT query/{id}/patchQuery}: the fields the body sets over the current version, as a new version. */
    public synchronized String patch(String id, String body, String currentUser) {
        Json.Obj patch = Json.parseObject(body);
        queryShape(patch);
        get(id); // the engine patches what getQuery answers, which marks it opened
        Json.Obj current = java.util.Objects.requireNonNull(current(versions(id)), id);
        Map<String, Json.Node> fields = clientFields(current);
        for (String f : CLIENT_FIELDS) {
            if (patch.has(f) && !(patch.get(f) instanceof Json.Null)) {
                fields.put(f, patch.get(f));
            }
        }
        return Json.toCompact(view(newVersion(id, fields, currentUser), List.of()));
    }

    /** {@code DELETE query/{id}}: the owner's; the query leaves search and get, its versions stay history. */
    public synchronized void delete(String id, String currentUser) {
        List<Json.Obj> versions = versions(id);
        Json.Obj q = current(versions);
        if (q == null) {
            throw new Refusal("Can't find query with ID '" + id + "'", 404);
        }
        if (!(q.get("owner") instanceof Json.Null) && !currentUser.equals(q.getString("owner"))) {
            throw new Refusal("Only owner can delete the query", 403);
        }
        long now = clock.getAsLong();
        Json.Obj deleted = with(with(q, "deletedAt", Json.num(now)), "validUntil", Json.num(now));
        versions.set(versions.indexOf(q), deleted);
        write(id, versions);
    }

    // ------------------------------------------------------------------ rules

    private Json.Obj newVersion(String id, Map<String, Json.Node> fields, String currentUser) {
        List<Json.Obj> versions = versions(id);
        Json.Obj prior = current(versions);
        if (prior == null) {
            throw new Refusal("Can't find query with ID '" + id + "'", 404);
        }
        if (!(prior.get("owner") instanceof Json.Null) && !currentUser.equals(prior.getString("owner"))) {
            throw new Refusal("Only owner can update the query", 403);
        }
        long now = clock.getAsLong();
        versions.set(versions.indexOf(prior), with(prior, "validUntil", Json.num(now)));
        fields.put("createdAt", prior.get("createdAt"));
        fields.put("lastUpdatedAt", Json.num(now));
        fields.put("lastOpenAt", Json.num(now));
        fields.put("version", Json.num(prior.getInt("version") + 1L));
        fields.put("owner", prior.get("owner") instanceof Json.Null ? Json.str(currentUser) : prior.get("owner"));
        Json.Obj next = ordered(fields);
        versions.add(next);
        write(id, versions);
        return next;
    }

    /** The engine's {@code validateQuery}. */
    private static void validate(Json.Obj q) {
        nonEmpty(q, "id", "Query ID is missing or empty");
        nonEmpty(q, "name", "Query name is missing or empty");
        nonEmpty(q, "groupId", "Query project group ID is missing or empty");
        nonEmpty(q, "artifactId", "Query project artifact ID is missing or empty");
        nonEmpty(q, "versionId", "Query project version is missing or empty");
        Json.Node ctx = q.has("executionContext") ? q.get("executionContext") : Json.nil();
        if (ctx instanceof Json.Obj c) {
            String ctxType = String.valueOf(stringOr(c, "_type"));
            Map<String, String> required = CONTEXT_FIELDS.get(ctxType);
            if (required == null) {
                throw new Refusal("Query execution context of _type '" + ctxType
                        + "' is not served by legend-lite (" + String.join(", ", CONTEXT_FIELDS.keySet()) + ")", 400);
            }
            required.forEach((field, message) -> nonEmpty(c, field, message));
        }
        nonEmpty(q, "content", "Query content is missing or empty");
        if (!JAVA_NAME.matcher(q.getString("groupId")).matches()) {
            throw new Refusal("Query project group ID is invalid", 400);
        }
        if (!VALID_ARTIFACT_ID.matcher(q.getString("artifactId")).matches()) {
            throw new Refusal("Query project artifact ID is invalid", 400);
        }
    }

    private static void nonEmpty(Json.Obj o, String field, String message) {
        String v = stringOr(o, field);
        if (v == null || v.isEmpty()) {
            throw new Refusal(message, 400);
        }
    }

    private static boolean matchesSearch(Json.Obj q, Json.Obj spec, String currentUser) {
        if (spec.has("searchTermSpecification") && spec.get("searchTermSpecification") instanceof Json.Obj term) {
            String searchTerm = stringOr(term, "searchTerm");
            if (searchTerm == null) {
                throw new Refusal("Query search spec expecting a search term", 500);
            }
            boolean includeOwner = term.getBoolOr("includeOwner", false);
            String owner = stringOr(q, "owner");
            if (term.getBoolOr("exactMatchName", false)) {
                if (!(searchTerm.equals(stringOr(q, "name")) || (includeOwner && searchTerm.equals(owner)))) {
                    return false;
                }
            } else {
                String lower = searchTerm.toLowerCase(java.util.Locale.ROOT);
                boolean hit = searchTerm.equals(stringOr(q, "id"))
                        || containsIgnoreCase(stringOr(q, "name"), lower)
                        || (includeOwner && containsIgnoreCase(owner, lower));
                if (!hit) {
                    return false;
                }
            }
        }
        if (spec.getBoolOr("showCurrentUserQueriesOnly", false)) {
            String owner = stringOr(q, "owner");
            if (owner != null && !owner.equals(currentUser)) {
                return false;
            }
        }
        if (spec.has("projectCoordinates") && spec.get("projectCoordinates") instanceof Json.Arr coords
                && !coords.items().isEmpty()) {
            boolean any = false;
            for (Json.Node n : coords.items()) {
                Json.Obj c = (Json.Obj) n;
                boolean same = c.getString("groupId").equals(stringOr(q, "groupId"))
                        && c.getString("artifactId").equals(stringOr(q, "artifactId"))
                        && (stringOr(c, "version") == null || c.getString("version").equals(stringOr(q, "versionId")));
                any |= same;
            }
            if (!any) {
                return false;
            }
        }
        if (spec.has("taggedValues") && spec.get("taggedValues") instanceof Json.Arr wanted && !wanted.items().isEmpty()) {
            boolean all = spec.getBoolOr("combineTaggedValuesCondition", false);
            boolean result = all;
            List<String> dataSpaces = new ArrayList<>();
            for (Json.Node n : wanted.items()) {
                Json.Obj tv = (Json.Obj) n;
                Json.Obj tag = tv.getObj("tag");
                String value = taggedValueText(tv);
                boolean has = hasTaggedValue(q, tag.getString("profile"), tag.getString("value"), value);
                result = all ? result && has : result || has;
                if (QUERY_PROFILE_PATH.equals(tag.getString("profile")) && QUERY_PROFILE_TAG_DATA_SPACE.equals(tag.getString("value"))) {
                    dataSpaces.add(value);
                }
            }
            if (!dataSpaces.isEmpty() && q.get("executionContext") instanceof Json.Obj ctx
                    && "dataSpaceExecutionContext".equals(stringOr(ctx, "_type"))
                    && dataSpaces.contains(stringOr(ctx, "dataSpacePath"))) {
                result = true;
            }
            if (!result) {
                return false;
            }
        }
        if (spec.has("stereotypes") && spec.get("stereotypes") instanceof Json.Arr wanted && !wanted.items().isEmpty()) {
            boolean any = false;
            for (Json.Node n : wanted.items()) {
                Json.Obj s = (Json.Obj) n;
                any |= hasStereotype(q, s.getString("profile"), s.getString("value"));
            }
            return any;
        }
        return true;
    }

    /** A tagged value's text: the wire writes it as a string (the engine's CString serializer). */
    private static @com.legend.base.Nullable String taggedValueText(Json.Obj tv) {
        Json.Node v = tv.get("value");
        if (v instanceof Json.Str s) {
            return s.value();
        }
        if (v instanceof Json.Obj o) {
            return stringOr(o, "value");
        }
        return null;
    }

    private static boolean hasTaggedValue(Json.Obj q, String profile, String tag, @com.legend.base.Nullable String value) {
        if (!(q.has("taggedValues") && q.get("taggedValues") instanceof Json.Arr tvs)) {
            return false;
        }
        for (Json.Node n : tvs.items()) {
            Json.Obj tv = (Json.Obj) n;
            Json.Obj t = tv.getObj("tag");
            if (profile.equals(stringOr(t, "profile")) && tag.equals(stringOr(t, "value"))
                    && java.util.Objects.equals(value, taggedValueText(tv))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasStereotype(Json.Obj q, String profile, String value) {
        if (!(q.has("stereotypes") && q.get("stereotypes") instanceof Json.Arr ss)) {
            return false;
        }
        for (Json.Node n : ss.items()) {
            Json.Obj s = (Json.Obj) n;
            if (profile.equals(stringOr(s, "profile")) && value.equals(stringOr(s, "value"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsIgnoreCase(@com.legend.base.Nullable String text, String lowerNeedle) {
        return text != null && text.toLowerCase(java.util.Locale.ROOT).contains(lowerNeedle);
    }

    // ------------------------------------------------------------------ shapes
    //
    // A body's fields of the kinds the engine's classes take them as; another kind is the 500 for a body the engine
    // cannot read. Checked before the rules read the body, not left to a failed cast: the page's WebAssembly has no
    // ClassCastException (a failed cast there stops its module), so only a check answers the same on every host. A
    // query's tagged values and stereotypes are checked as it is written, so the searches that read them later can.

    private static final Predicate<Json.Node> TEXT = n -> n instanceof Json.Str;
    private static final Predicate<Json.Node> FLAG = n -> n instanceof Json.Bool;
    private static final Predicate<Json.Node> NUMBER = n -> n instanceof Json.Num;
    private static final Predicate<Json.Node> OBJECT = n -> n instanceof Json.Obj;

    /** The engine's {@code QuerySearchSpecification}, as far as the rules read it. */
    private static void searchShape(Json.Obj spec) {
        String where = "QuerySearchSpecification";
        if (field(spec, "searchTermSpecification", OBJECT, "an object", false, where) instanceof Json.Obj term) {
            field(term, "searchTerm", TEXT, "a string", false, "QuerySearchTermSpecification");
            field(term, "exactMatchName", FLAG, "a boolean", false, "QuerySearchTermSpecification");
            field(term, "includeOwner", FLAG, "a boolean", false, "QuerySearchTermSpecification");
        }
        field(spec, "showCurrentUserQueriesOnly", FLAG, "a boolean", false, where);
        field(spec, "combineTaggedValuesCondition", FLAG, "a boolean", false, where);
        field(spec, "sortByOption", TEXT, "a string", false, where);
        field(spec, "limit", NUMBER, "a number", false, where);
        for (Json.Obj c : objects(spec, "projectCoordinates", where, "QueryProjectCoordinates")) {
            field(c, "groupId", TEXT, "a string", true, "QueryProjectCoordinates");
            field(c, "artifactId", TEXT, "a string", true, "QueryProjectCoordinates");
            field(c, "version", TEXT, "a string", false, "QueryProjectCoordinates");
        }
        tagsShape(spec, where);
    }

    /** The engine's {@code Query}, as far as the rules read it: its tagged values and stereotypes. */
    private static void queryShape(Json.Obj q) {
        tagsShape(q, "Query");
    }

    private static void tagsShape(Json.Obj o, String where) {
        for (Json.Obj tv : objects(o, "taggedValues", where, "TaggedValue")) {
            if (field(tv, "tag", OBJECT, "an object", true, "TaggedValue") instanceof Json.Obj tag) {
                field(tag, "profile", TEXT, "a string", true, "TagPtr");
                field(tag, "value", TEXT, "a string", true, "TagPtr");
            }
        }
        for (Json.Obj s : objects(o, "stereotypes", where, "StereotypePtr")) {
            field(s, "profile", TEXT, "a string", true, "StereotypePtr");
            field(s, "value", TEXT, "a string", true, "StereotypePtr");
        }
    }

    /** {@code o}'s {@code field}, of its kind; null when it is absent or null and need not be there. */
    private static Json.@com.legend.base.Nullable Node field(Json.Obj o, String field, Predicate<Json.Node> kind,
            String kindName, boolean required, String where) {
        Json.Node n = o.has(field) ? o.get(field) : Json.nil();
        if (n instanceof Json.Null) {
            if (required) {
                throw new IllegalArgumentException(where + "." + field + " is missing");
            }
            return null;
        }
        if (!kind.test(n)) {
            throw new IllegalArgumentException(where + "." + field + " is not " + kindName);
        }
        return n;
    }

    /** {@code o}'s list {@code field}, each item an object ({@code item}); empty when it is absent or null. */
    private static List<Json.Obj> objects(Json.Obj o, String field, String where, String item) {
        Json.Node n = o.has(field) ? o.get(field) : Json.nil();
        if (n instanceof Json.Null) {
            return List.of();
        }
        if (!(n instanceof Json.Arr a)) {
            throw new IllegalArgumentException(where + "." + field + " is not a list");
        }
        List<Json.Obj> out = new ArrayList<>();
        for (Json.Node i : a.items()) {
            if (!(i instanceof Json.Obj each)) {
                throw new IllegalArgumentException(where + "." + field + " holds something not " + item);
            }
            out.add(each);
        }
        return out;
    }

    // ------------------------------------------------------------------ records

    /** A query as the API writes it: every field in the engine's order, null when absent. */
    private static Map<String, Object> view(Json.Obj q, List<String> blank) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String f : FIELDS) {
            out.put(f, blank.contains(f) || !q.has(f) ? Json.nil() : q.get(f));
        }
        return out;
    }

    private static Map<String, Json.Node> clientFields(Json.Obj q) {
        Map<String, Json.Node> out = new LinkedHashMap<>();
        for (String f : CLIENT_FIELDS) {
            out.put(f, q.has(f) ? q.get(f) : Json.nil());
        }
        return out;
    }

    private static Json.Obj ordered(Map<String, Json.Node> fields) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
        for (String f : FIELDS) {
            out.put(f, fields.getOrDefault(f, Json.nil()));
        }
        return new Json.Obj(out);
    }

    private static Json.Obj with(Json.Obj o, String field, Json.Node value) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(o.fields());
        out.put(field, value);
        return new Json.Obj(out);
    }

    private static @com.legend.base.Nullable String stringOr(Json.Obj o, String field) {
        return o.has(field) && o.get(field) instanceof Json.Str s ? s.value() : null;
    }

    private static long longOr(Json.Obj o, String field, long def) {
        return o.has(field) && o.get(field) instanceof Json.Num n ? n.longValue() : def;
    }

    /** The current version: the one with no {@code validUntil}. */
    private static @com.legend.base.Nullable Json.Obj current(List<Json.Obj> versions) {
        for (Json.Obj v : versions) {
            if (v.get("validUntil") instanceof Json.Null) {
                return v;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ storage (the host's: Records)

    private @com.legend.base.Nullable Json.Obj latest(String id) {
        return current(versions(id));
    }

    private List<Json.Obj> latestOfAll() {
        List<Json.Obj> out = new ArrayList<>();
        // by id (the records' order), then by creation: the engine's natural order is insertion order
        for (List<Json.Obj> versions : records.all().values()) {
            Json.Obj q = current(versions);
            if (q != null) {
                out.add(q);
            }
        }
        out.sort(Comparator.comparingLong((Json.Obj q) -> longOr(q, "createdAt", 0)));
        return out;
    }

    private List<Json.Obj> versions(String id) {
        return new ArrayList<>(records.versions(id));
    }

    private void write(String id, List<Json.Obj> versions) {
        records.write(id, versions);
    }
}
