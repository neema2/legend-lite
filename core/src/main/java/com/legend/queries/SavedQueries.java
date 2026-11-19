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
        private final @com.legend.base.Nullable Kind kind;
        private final List<String> operands;

        /** A refusal's kind, where the store names one (D1.8, W8 of docs/records/d-plan-2026-11-16.md, 2026-11-16):
         *  its message is lite's sentence; legend-engine's is the compatibility edge's ({@code edge.PureMessages.store}),
         *  for the routes mounted at legend-engine's path. */
        public enum Kind {
            /** No saved query has the ID: the ID. */
            QUERY_NOT_FOUND("no saved query has the ID '%1$s'"),
            /** Some of several IDs name no saved query: those IDs, one per line. */
            QUERIES_NOT_FOUND("no saved query has the ID(s):\n%1$s"),
            /** A saved query has no such version: the version, the ID. */
            VERSION_NOT_FOUND("the saved query '%2$s' has no version '%1$s'"),
            /** A path the store does not serve: the path under the store, its mount's (B7, the orchestrator's ruling (3)
             *  of 2026-11-17: lite's words for lite's mount; legend-engine's path and words the edge's, StoreWords). */
            NO_SUCH_ROUTE("no such legend-lite API: /api/lite/v1/queries%1$s"),
            // J9 of the edge audit (2026-11-17): every refusal a kind with its facts, lite's sentence here,
            // legend-engine's the edge's (StoreWords); a page's refusals are a query's
            /** A required field missing or empty: its name in lite's words, its field (the edge's key). */
            FIELD_MISSING("the %1$s is missing or empty"),
            /** An execution context of a _type the store does not serve: the _type, the served ones. */
            UNSERVED_CONTEXT("the execution context of _type '%1$s' is not served (%2$s)"),
            /** The project group ID is not a Java name. */
            INVALID_GROUP_ID("the project group ID is invalid"),
            /** The project artifact ID is not an artifact ID. */
            INVALID_ARTIFACT_ID("the project artifact ID is invalid"),
            /** The ID is taken: the ID. */
            ID_TAKEN("the ID '%1$s' is already taken"),
            /** An update would change the ID. */
            ID_CHANGED("the ID cannot be changed"),
            /** Only the owner may do it: the act (update, delete). */
            NOT_OWNER("only the owner can %1$s it"),
            /** A search's limit of 0 or less. */
            LIMIT_NOT_POSITIVE("the limit must be greater than 0"),
            /** More IDs asked at once than the store serves: the most it serves. */
            TOO_MANY("at most %1$s can be fetched at once"),
            /** A search specification with no search term. */
            NO_SEARCH_TERM("the search specification has no search term"),
            /** A body's field the store cannot read -- missing, or not of its kind (PR #118 fix round, 2026-11-18: lite's
             *  words name the request's field, never legend-engine's class): legend-engine's class, the field, the
             *  problem in legend-engine's words, the field's path in the request, the problem in lite's words. */
            FIELD_UNREAD("the request's %4$s %5$s"),
            /** A request the store cannot read otherwise -- its body not JSON of the shape the store takes, a parameter or
             *  an option no value of it names -- raised where the request is read (PR #118 fix round, 2026-11-18: a
             *  malformed request, lite's {@code request}; any other failure stays {@link #MALFORMED}): the exception's
             *  class and message legend-engine names, lite's words of the problem. */
            UNREADABLE("the request could not be read: %3$s"),
            /** The store is not configured: which store (query, page). */
            STORE_NOT_CONFIGURED("the %1$s store is not configured: start the server with --query-store DIR"),
            /** A request the store could not read, or its disk failing: the exception's class, its message. */
            MALFORMED("the request could not be served: %2$s");

            private final String sentence;

            Kind(String sentence) {
                this.sentence = sentence;
            }

            String sentence(List<String> operands) {
                return String.format(java.util.Locale.ROOT, sentence, operands.toArray());
            }
        }

        Refusal(String message, int status) {
            super(message);
            this.status = status;
            this.kind = null;
            this.operands = List.of();
        }

        Refusal(Kind kind, List<String> operands, int status) {
            super(kind.sentence(operands));
            this.status = status;
            this.kind = kind;
            this.operands = List.copyOf(operands);
        }

        public @com.legend.base.Nullable Kind kind() {
            return kind;
        }

        public List<String> operands() {
            return operands;
        }
    }

    /** A refusal in the product's words: its own message. */
    public static final java.util.function.Function<Refusal, String> LITES_WORDS = r -> String.valueOf(r.getMessage());

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

    /** The execution contexts served, each with its required fields and the field's name in lite's words. */
    private static final Map<String, Map<String, String>> CONTEXT_FIELDS = orderedContexts();

    private static Map<String, Map<String, String>> orderedContexts() {
        Map<String, String> explicit = new LinkedHashMap<>();
        explicit.put("mapping", "execution context's mapping");
        explicit.put("runtime", "execution context's runtime");
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        out.put("explicitExecutionContext", java.util.Collections.unmodifiableMap(explicit));
        out.put("dataSpaceExecutionContext",
                Map.of("dataSpacePath", "execution context's data space path"));
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
        return answer(store, method, rest, rawQuery, body, user, LITES_WORDS);
    }

    /** {@link #answer(SavedQueries, String, String, String, String, String)}, a refusal worded by {@code words} (the
     *  edge's route at legend-engine's path: legend-engine's words; D1.8). */
    public static Answer answer(@com.legend.base.Nullable SavedQueries store, String method, String rest,
            @com.legend.base.Nullable String rawQuery, String body, String user,
            java.util.function.Function<Refusal, String> words) {
        if (store == null) {
            return refused(new Refusal(Refusal.Kind.STORE_NOT_CONFIGURED, List.of("query"), 500), words);
        }
        return served(() -> {
            String[] parts = rest.isEmpty() ? new String[0] : rest.substring(1).split("/", -1);
            String id = parts.length > 0 ? pathSegment(parts[0]) : "";
            if (parts.length > 0 && "dataCube".equals(parts[0])) {
                return notFound(rest, words);
            }
            return switch (method + " " + parts.length + " " + (parts.length > 1 ? parts[1] : "")) {
                case "POST 0 " -> ok(store.create(body, user));
                case "POST 1 " -> "search".equals(id) ? ok(store.search(body, user)) : notFound(rest, words);
                case "GET 1 " -> "batch".equals(id) ? ok(store.batch(queryParams(rawQuery, "queryIds"))) : ok(store.get(id));
                case "PUT 1 " -> ok(store.update(id, body, user));
                case "DELETE 1 " -> {
                    store.delete(id, user);
                    yield new Answer(204, "");
                }
                case "GET 2 history" -> {
                    List<String> v = queryParams(rawQuery, "version");
                    yield ok(store.history(id, v.isEmpty() ? null : version(v.get(0))));
                }
                case "PUT 2 patchQuery" -> ok(store.patch(id, body, user));
                default -> notFound(rest, words);
            };
        }, words);
    }

    /** A route served, its refusal or failure answered in {@code words} -- the one catch both stores share (J9 of the
     *  edge audit, 2026-11-17): a {@link Refusal} is its kind's answer -- a request the store cannot read is one,
     *  raised where it is read ({@link Refusal.Kind#UNREADABLE}, {@link Refusal.Kind#FIELD_UNREAD}; lite's route answers
     *  it as a malformed request: PR #118 fix round, 2026-11-18); any other failure, the store's disk failing among
     *  them, is {@link Refusal.Kind#MALFORMED}: each a 500 naming the exception, as legend-engine answers both. */
    static Answer served(java.util.function.Supplier<Answer> route, java.util.function.Function<Refusal, String> words) {
        try {
            return route.get();
        } catch (Refusal r) {
            return refused(r, words);
        } catch (java.io.UncheckedIOException | IllegalArgumentException | ClassCastException e) {
            return failed(e, words);
        }
    }

    /** A body's field the store cannot read ({@link Refusal.Kind#FIELD_UNREAD}): legend-engine's class {@code where},
     *  its {@code field} and {@code problem}; lite's words name the field by its {@code path} in the request and say
     *  the problem as {@code liteProblem}. */
    static Refusal unread(String where, String field, String problem, String path, String liteProblem) {
        return new Refusal(Refusal.Kind.FIELD_UNREAD, List.of(where, field, problem, path, liteProblem), 500);
    }

    /** {@link #unread(String, String, String, String, String)} where lite says the problem in the same words. */
    static Refusal unread(String where, String field, String problem, String path) {
        return unread(where, field, problem, path, problem);
    }

    /** A request the store cannot read ({@link Refusal.Kind#UNREADABLE}), raised where it is read: the class and
     *  message legend-engine names ({@code engineClass}, {@code engineMessage}), lite's words of it. */
    static Refusal unreadable(String engineClass, String engineMessage, String liteWords) {
        return new Refusal(Refusal.Kind.UNREADABLE, List.of(engineClass, engineMessage, liteWords), 500);
    }

    /** The request's {@code body} as the JSON object the store reads, or refused where it is not one (a malformed
     *  request, {@link Refusal.Kind#UNREADABLE}): read where the request is read, so a failure anywhere else -- the
     *  store reading its own files -- stays the store's fault. */
    static Json.Obj requestBody(String body) {
        Json.Node parsed;
        try {
            parsed = Json.parse(body);
        } catch (IllegalArgumentException e) {
            throw unreadable(e.getClass().getSimpleName(), String.valueOf(e.getMessage()),
                    "its body is not JSON: " + e.getMessage());
        }
        if (!(parsed instanceof Json.Obj o)) {
            // the words Json.parseObject refused it in until then, so the edge's answer is unchanged byte for byte
            throw unreadable("IllegalArgumentException", "Expected top-level JSON object, got "
                    + parsed.getClass().getSimpleName(), "its body is not a JSON object");
        }
        return o;
    }

    /** A path segment of the request, percent-decoded, or refused where it is not percent-encoded (a malformed request,
     *  {@link Refusal.Kind#UNREADABLE}: PH-1 of the PR #118 fix batch's audit, 2026-11-18 -- the decoder's exception
     *  was the store's failure, 500 {@code internal} on lite's route, on every host). legend-engine's words are the
     *  decoder's exception, as {@link Refusal.Kind#MALFORMED} wrote them until then, so the edge's answer is unchanged. */
    static String pathSegment(String segment) {
        return decoded(segment, "its path segment '" + segment + "'");
    }

    /** {@code text} percent-decoded, or refused as {@code what} not percent-encoded ({@link Refusal.Kind#UNREADABLE}). */
    private static String decoded(String text, String what) {
        try {
            return java.net.URLDecoder.decode(text, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw unreadable(e.getClass().getSimpleName(), String.valueOf(e.getMessage()),
                    what + " is not percent-encoded");
        }
    }

    /** The {@code version} parameter as a number, or refused where it is none ({@link Refusal.Kind#UNREADABLE}). */
    private static int version(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw unreadable(e.getClass().getSimpleName(), String.valueOf(e.getMessage()),
                    "its version '" + text + "' is not a number");
        }
    }

    /** A refusal answered: its status, and {@code {"kind", "message"}} -- its kind as data (lower case; {@code refused}
     *  where it names none) and its words its mount's ({@code words}: lite's on lite's route; the edge's mount at
     *  legend-engine's path rewrites the shape too, edge.StoreWords). J9 of the edge audit, 2026-11-17: the engine's
     *  envelope ({@code code}, {@code status}) is the edge's alone. */
    static Answer refused(Refusal r, java.util.function.Function<Refusal, String> words) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("kind", r.kind() == null ? "refused" : r.kind().name().toLowerCase(java.util.Locale.ROOT));
        out.put("message", words.apply(r));
        return new Answer(r.status, Json.toCompact(out));
    }

    /** A 500 for an exception: {@link Refusal.Kind#MALFORMED}, naming it. */
    static Answer failed(RuntimeException e, java.util.function.Function<Refusal, String> words) {
        return refused(new Refusal(Refusal.Kind.MALFORMED, List.of(e.getClass().getSimpleName(),
                String.valueOf(e.getMessage())), 500), words);
    }

    private static Answer ok(String json) {
        return new Answer(200, json);
    }

    /** A path the store does not serve ({@code rest}, under the store), worded by its mount's {@code words}. */
    static Answer notFound(String rest, java.util.function.Function<Refusal, String> words) {
        return refused(new Refusal(Refusal.Kind.NO_SUCH_ROUTE, List.of(rest), 404), words);
    }

    private static List<String> queryParams(@com.legend.base.Nullable String rawQuery, String name) {
        List<String> out = new ArrayList<>();
        if (rawQuery == null) {
            return out;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String what = "its parameter '" + pair + "'";
            String k = decoded(eq < 0 ? pair : pair.substring(0, eq), what);
            if (k.equals(name) && eq >= 0) {
                out.add(decoded(pair.substring(eq + 1), what));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ the endpoints

    /** {@code POST query/search}. */
    public synchronized String search(String body, String currentUser) {
        Json.Obj spec = requestBody(body.strip().isEmpty() ? "{}" : body);
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
                default -> throw unreadable("IllegalArgumentException", "Unknown sort-by value",
                        "its sort option '" + sortBy + "' is none of SORT_BY_CREATE, SORT_BY_VIEW and SORT_BY_UPDATE");
            };
            matches.sort(Comparator.comparingLong((Json.Obj q) -> longOr(q, field, Long.MIN_VALUE)).reversed());
        }
        Integer limit = spec.has("limit") && !(spec.get("limit") instanceof Json.Null) ? spec.getInt("limit") : null;
        if (limit != null && limit <= 0) {
            throw new Refusal(Refusal.Kind.LIMIT_NOT_POSITIVE, List.of(), 400);
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
            throw new Refusal(Refusal.Kind.TOO_MANY, List.of(Integer.toString(GET_QUERIES_LIMIT)), 400);
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
            throw new Refusal(Refusal.Kind.QUERIES_NOT_FOUND, List.of(String.join("\n", notFound)), 500);
        }
        return Json.toCompact(out);
    }

    /** {@code GET query/{id}}: the query, its {@code lastOpenAt} now (no new version). */
    public synchronized String get(String id) {
        List<Json.Obj> versions = versions(id);
        Json.Obj q = current(versions);
        if (q == null) {
            throw new Refusal(Refusal.Kind.QUERY_NOT_FOUND, List.of(id), 404);
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
                throw new Refusal(Refusal.Kind.QUERY_NOT_FOUND, List.of(id), 404);
            }
            throw new Refusal(Refusal.Kind.VERSION_NOT_FOUND, List.of(String.valueOf(version), id), 404);
        }
        List<Object> out = new ArrayList<>();
        for (Json.Obj v : versions) {
            if (!(v.get("validUntil") instanceof Json.Null)) {
                out.add(view(v, List.of()));
            }
        }
        if (out.isEmpty() && current(versions) == null) {
            throw new Refusal(Refusal.Kind.QUERY_NOT_FOUND, List.of(id), 404);
        }
        return Json.toCompact(out);
    }

    /** {@code POST query}: a new query, owned by the caller, version 1. */
    public synchronized String create(String body, String currentUser) {
        Json.Obj query = requestBody(body);
        queryShape(query);
        validate(query);
        String id = query.getString("id");
        List<Json.Obj> versions = versions(id);
        if (current(versions) != null) {
            throw new Refusal(Refusal.Kind.ID_TAKEN, List.of(id), 400);
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
        Json.Obj query = requestBody(body);
        queryShape(query);
        validate(query);
        if (!id.equals(query.getString("id"))) {
            throw new Refusal(Refusal.Kind.ID_CHANGED, List.of(), 400);
        }
        return Json.toCompact(view(newVersion(id, clientFields(query), currentUser), List.of()));
    }

    /** {@code PUT query/{id}/patchQuery}: the fields the body sets over the current version, as a new version. */
    public synchronized String patch(String id, String body, String currentUser) {
        Json.Obj patch = requestBody(body);
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
            throw new Refusal(Refusal.Kind.QUERY_NOT_FOUND, List.of(id), 404);
        }
        if (!(q.get("owner") instanceof Json.Null) && !currentUser.equals(q.getString("owner"))) {
            throw new Refusal(Refusal.Kind.NOT_OWNER, List.of("delete"), 403);
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
            throw new Refusal(Refusal.Kind.QUERY_NOT_FOUND, List.of(id), 404);
        }
        if (!(prior.get("owner") instanceof Json.Null) && !currentUser.equals(prior.getString("owner"))) {
            throw new Refusal(Refusal.Kind.NOT_OWNER, List.of("update"), 403);
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
        nonEmpty(q, "id", "ID");
        nonEmpty(q, "name", "name");
        nonEmpty(q, "groupId", "project group ID");
        nonEmpty(q, "artifactId", "project artifact ID");
        nonEmpty(q, "versionId", "project version");
        Json.Node ctx = q.has("executionContext") ? q.get("executionContext") : Json.nil();
        if (ctx instanceof Json.Obj c) {
            String ctxType = String.valueOf(stringOr(c, "_type"));
            Map<String, String> required = CONTEXT_FIELDS.get(ctxType);
            if (required == null) {
                throw new Refusal(Refusal.Kind.UNSERVED_CONTEXT, List.of(ctxType,
                        String.join(", ", CONTEXT_FIELDS.keySet())), 400);
            }
            required.forEach((field, message) -> nonEmpty(c, field, message));
        }
        nonEmpty(q, "content", "content");
        if (!JAVA_NAME.matcher(q.getString("groupId")).matches()) {
            throw new Refusal(Refusal.Kind.INVALID_GROUP_ID, List.of(), 400);
        }
        if (!VALID_ARTIFACT_ID.matcher(q.getString("artifactId")).matches()) {
            throw new Refusal(Refusal.Kind.INVALID_ARTIFACT_ID, List.of(), 400);
        }
    }

    /** {@code field} of {@code o} refused where it is missing or empty: {@link Refusal.Kind#FIELD_MISSING}, its name
     *  in lite's words ({@code name}) and the field (the edge's key to legend-engine's sentence). */
    private static void nonEmpty(Json.Obj o, String field, String name) {
        String v = stringOr(o, field);
        if (v == null || v.isEmpty()) {
            throw new Refusal(Refusal.Kind.FIELD_MISSING, List.of(name, field), 400);
        }
    }

    private static boolean matchesSearch(Json.Obj q, Json.Obj spec, String currentUser) {
        if (spec.has("searchTermSpecification") && spec.get("searchTermSpecification") instanceof Json.Obj term) {
            String searchTerm = stringOr(term, "searchTerm");
            if (searchTerm == null) {
                throw new Refusal(Refusal.Kind.NO_SEARCH_TERM, List.of(), 500);
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

    /** A search term's fields' place in the request, lite's words of it. */
    private static final String TERM = "searchTermSpecification.";

    /** The engine's {@code QuerySearchSpecification}, as far as the rules read it. */
    private static void searchShape(Json.Obj spec) {
        String where = "QuerySearchSpecification";
        if (field(spec, "searchTermSpecification", OBJECT, "an object", false, where, "") instanceof Json.Obj term) {
            field(term, "searchTerm", TEXT, "a string", false, "QuerySearchTermSpecification", TERM);
            field(term, "exactMatchName", FLAG, "a boolean", false, "QuerySearchTermSpecification", TERM);
            field(term, "includeOwner", FLAG, "a boolean", false, "QuerySearchTermSpecification", TERM);
        }
        field(spec, "showCurrentUserQueriesOnly", FLAG, "a boolean", false, where, "");
        field(spec, "combineTaggedValuesCondition", FLAG, "a boolean", false, where, "");
        field(spec, "sortByOption", TEXT, "a string", false, where, "");
        field(spec, "limit", NUMBER, "a number", false, where, "");
        for (Json.Obj c : objects(spec, "projectCoordinates", where, "QueryProjectCoordinates")) {
            field(c, "groupId", TEXT, "a string", true, "QueryProjectCoordinates", "projectCoordinates[].");
            field(c, "artifactId", TEXT, "a string", true, "QueryProjectCoordinates", "projectCoordinates[].");
            field(c, "version", TEXT, "a string", false, "QueryProjectCoordinates", "projectCoordinates[].");
        }
        tagsShape(spec, where);
    }

    /** The engine's {@code Query}, as far as the rules read it: its tagged values and stereotypes. */
    private static void queryShape(Json.Obj q) {
        tagsShape(q, "Query");
    }

    private static void tagsShape(Json.Obj o, String where) {
        for (Json.Obj tv : objects(o, "taggedValues", where, "TaggedValue")) {
            if (field(tv, "tag", OBJECT, "an object", true, "TaggedValue", "taggedValues[].") instanceof Json.Obj tag) {
                field(tag, "profile", TEXT, "a string", true, "TagPtr", "taggedValues[].tag.");
                field(tag, "value", TEXT, "a string", true, "TagPtr", "taggedValues[].tag.");
            }
        }
        for (Json.Obj s : objects(o, "stereotypes", where, "StereotypePtr")) {
            field(s, "profile", TEXT, "a string", true, "StereotypePtr", "stereotypes[].");
            field(s, "value", TEXT, "a string", true, "StereotypePtr", "stereotypes[].");
        }
    }

    /** {@code o}'s {@code field}, of its kind; null when it is absent or null and need not be there. {@code in}: the
     *  field's place in the request ("" at its top, {@code searchTermSpecification.} inside it), lite's words of it. */
    private static Json.@com.legend.base.Nullable Node field(Json.Obj o, String field, Predicate<Json.Node> kind,
            String kindName, boolean required, String where, String in) {
        Json.Node n = o.has(field) ? o.get(field) : Json.nil();
        if (n instanceof Json.Null) {
            if (required) {
                throw unread(where, field, "is missing", in + field);
            }
            return null;
        }
        if (!kind.test(n)) {
            throw unread(where, field, "is not " + kindName, in + field);
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
            throw unread(where, field, "is not a list", field);
        }
        List<Json.Obj> out = new ArrayList<>();
        for (Json.Node i : a.items()) {
            if (!(i instanceof Json.Obj each)) {
                throw unread(where, field, "holds something not " + item, field, "holds something that is not an object");
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
