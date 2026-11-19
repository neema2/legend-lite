package com.legend.queries;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * DataCube's saved pages (legend-engine's {@code /api/pure/v1/query/dataCube}: {@code ApplicationQuery} and
 * {@code DataCubeQueryStoreManager}, read at 4.145.0; docs/DATACUBE_SAVE_SHARE_2026_09_28.md step 0), in legend-lite:
 * the same record and the same rules, ported from DataCube's own (datacube/src/cube-store.ts's RuleStore, which these
 * replace: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6) --
 * <ul>
 *   <li>the client supplies the id; creating an id that exists is refused;</li>
 *   <li>the owner is forced to the current user; only the owner updates or deletes;</li>
 *   <li>reading a page stamps {@code lastOpenAt};</li>
 *   <li>a search answers light records (no content): the current user's first, then the chosen sort (newest first),
 *   then the limit; a term matches the id, the name or the owner.</li>
 * </ul>
 *
 * <p>ONE IMPLEMENTATION, STORAGE PER HOST, as {@link SavedQueries}: legend-lite's server, Python's native library and
 * the page's WebAssembly module answer with these rules; only where the pages are kept differs ({@link Records}): a
 * directory, one {@code <id>.json} each ({@link FileCubeRecords}: Python's {@code ll.home()/pages}, the server's
 * {@code --query-store DIR/dataCube}), or the page's IndexedDB rows handed in ({@link MemoryCubeRecords}). A page is
 * kept as a ROW: its record with its content as the protocol's exact JSON text, so a calculated column's {@code 12.30}
 * or an integer past 2^53 comes back as written; the wire carries the content as JSON. The user is the host's to say
 * (the page's configured name, Python's "this user", the server's anonymous caller): there is no sign-in yet (G10).
 */
public final class SavedCubes {

    /** Upstream's cap on one search's answer. */
    public static final int MAX_SEARCH = 100;

    /**
     * A page's id, as a page makes one ({@code crypto.randomUUID}): letters, digits, "-" and "_" -- refused otherwise on
     * every host, since one host keeps a page as a file of that name (Python's, the server's), and a name must never
     * reach outside its directory (no separator) nor be a device Windows keeps in every folder (CON, NUL, COM1, ...).
     */
    private static final java.util.regex.Pattern ID = java.util.regex.Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,127}");
    private static final java.util.regex.Pattern DEVICE =
            java.util.regex.Pattern.compile("(?i)(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])");

    /** Where a host keeps its pages' rows (each its record, its {@code content} JSON text), by id. */
    public interface Records {
        /** Every row, by its id, in the ids' order. */
        Map<String, Json.Obj> all();

        @com.legend.base.Nullable Json.Obj find(String id);

        void put(Json.Obj row);

        void remove(String id);
    }

    private final Records records;
    private final LongSupplier clock;

    public SavedCubes(Records records, LongSupplier clock) {
        this.records = records;
        this.clock = clock;
    }

    /**
     * One {@code /api/pure/v1/query/dataCube...} request ({@code rest}: the path after it; {@code rawQuery}: its query
     * string, or null), as {@code user}: 200 with JSON, 204 for a delete, and a refusal as {@code {"message"}} with its
     * status, as {@link SavedQueries}' are. Not served yet, and answered as paths it has not: upstream's
     * {@code dataCube/batch}, {@code dataCube/events} and {@code dataCube/stats} (the ones that take a query string).
     */
    public static SavedQueries.Answer answer(@com.legend.base.Nullable SavedCubes store, String method, String rest,
            @com.legend.base.Nullable String rawQuery, String body, String user) {
        return answer(store, method, rest, rawQuery, body, user, SavedQueries.LITES_WORDS);
    }

    /** {@link #answer(SavedCubes, String, String, String, String, String)}, a refusal worded by {@code words} (D1.8). */
    public static SavedQueries.Answer answer(@com.legend.base.Nullable SavedCubes store, String method, String rest,
            @com.legend.base.Nullable String rawQuery, String body, String user,
            java.util.function.Function<SavedQueries.Refusal, String> words) {
        if (store == null) {
            return SavedQueries.refused(new SavedQueries.Refusal(SavedQueries.Refusal.Kind.STORE_NOT_CONFIGURED,
                    java.util.List.of("page"), 500), words);
        }
        return SavedQueries.served(() -> {
            String[] parts = rest.isEmpty() ? new String[0] : rest.substring(1).split("/", -1);
            String id = parts.length > 0 ? SavedQueries.pathSegment(parts[0]) : "";
            // a page named in the path (get, update, delete): an id a page makes, on every host
            if (parts.length == 1 && !"POST".equals(method)) {
                pageId(id);
            }
            return switch (method + " " + parts.length) {
                case "POST 0" -> ok(store.create(object(body), user));
                case "POST 1" -> "search".equals(id) ? ok(store.search(object(body), user)) : notFound(rest, words);
                case "GET 1" -> ok(store.get(id));
                case "PUT 1" -> ok(store.update(id, object(body), user));
                case "DELETE 1" -> {
                    store.delete(id, user);
                    yield new SavedQueries.Answer(204, "");
                }
                default -> notFound(rest, words);
            };
        }, words);
    }

    private static SavedQueries.Answer ok(Object json) {
        return new SavedQueries.Answer(200, Json.toCompact(json));
    }

    /** A path the page store does not serve, under the query store's {@code /dataCube} ({@link SavedQueries#notFound}). */
    private static SavedQueries.Answer notFound(String rest,
            java.util.function.Function<SavedQueries.Refusal, String> words) {
        return SavedQueries.notFound("/dataCube" + rest, words);
    }

    private static Json.Obj object(String body) {
        return SavedQueries.requestBody(body.strip().isEmpty() ? "{}" : body);
    }

    // ------------------------------------------------------------------ the rules

    /** {@code POST dataCube/search}. */
    public synchronized List<Object> search(Json.Obj spec, String user) {
        searchShape(spec);
        List<Json.Obj> found = new ArrayList<>(records.all().values());
        if (spec.has("searchTermSpecification") && spec.get("searchTermSpecification") instanceof Json.Obj term) {
            String t = text(term, "searchTerm");
            boolean exact = bool(term, "exactMatchName");
            boolean includeOwner = bool(term, "includeOwner");
            String lower = t.toLowerCase(java.util.Locale.ROOT);
            found.removeIf(q -> !(exact
                    ? t.equals(text(q, "name")) || (includeOwner && q.has("owner") && t.equals(text(q, "owner")))
                    : t.equals(text(q, "id")) || text(q, "name").toLowerCase(java.util.Locale.ROOT).contains(lower)
                            || text(q, "owner").toLowerCase(java.util.Locale.ROOT).contains(lower)));
        }
        if (bool(spec, "showCurrentUserQueriesOnly")) {
            found.removeIf(q -> !user.equals(text(q, "owner")));
        }
        String field = switch (text(spec, "sortByOption")) {
            case "SORT_BY_CREATE" -> "createdAt";
            case "SORT_BY_UPDATE" -> "lastUpdatedAt";
            case "SORT_BY_VIEW" -> "lastOpenAt";
            default -> null;
        };
        Comparator<Json.Obj> order = Comparator.comparingInt(q -> user.equals(text(q, "owner")) ? 0 : 1);
        if (field != null) {
            order = order.thenComparing(Comparator.comparingLong((Json.Obj q) -> time(q, field)).reversed());
        }
        found.sort(order);
        // clamped by its value as a double: a limit past a long's range (1e30) is the cap, never a wrapped count
        int limit = spec.has("limit") && spec.get("limit") instanceof Json.Num n
                ? (int) Math.max(0, Math.min(MAX_SEARCH, Math.floor(n.doubleValue()))) : MAX_SEARCH;
        List<Object> out = new ArrayList<>();
        for (Json.Obj q : found.subList(0, Math.min(limit, found.size()))) {
            LinkedHashMap<String, Json.Node> light = new LinkedHashMap<>(q.fields());
            light.remove("content");
            out.add(new Json.Obj(light));
        }
        return out;
    }

    /** {@code GET dataCube/{id}}: the page, its {@code lastOpenAt} stamped. */
    public synchronized Json.Obj get(String id) {
        Json.Obj found = records.find(id);
        if (found == null) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.QUERY_NOT_FOUND, java.util.List.of(id), 404);
        }
        Json.Obj opened = with(found, Map.of("lastOpenAt", Json.num(clock.getAsLong())));
        // read for the wire first: a content that cannot be read is the 500 with nothing written
        Json.Obj answer = wire(opened);
        records.put(opened);
        return answer;
    }

    /** {@code POST dataCube}: made with the client's id, its owner this user. */
    public synchronized Json.Obj create(Json.Obj query, String user) {
        String id = validate(query);
        if (records.find(id) != null) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.ID_TAKEN, java.util.List.of(id), 400);
        }
        Json.Node now = Json.num(clock.getAsLong());
        Map<String, Json.Node> stamps = new LinkedHashMap<>();
        stamps.put("owner", Json.str(user));
        stamps.put("createdAt", now);
        stamps.put("lastUpdatedAt", now);
        stamps.put("lastOpenAt", now);
        Json.Obj created = with(row(query), stamps);
        records.put(created);
        return wire(created);
    }

    /** {@code PUT dataCube/{id}}: only the owner's; its id never changed, its creation kept. */
    public synchronized Json.Obj update(String id, Json.Obj query, String user) {
        if (!id.equals(validate(query))) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.ID_CHANGED, java.util.List.of(), 400);
        }
        Json.Obj current = records.find(id);
        if (current == null) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.QUERY_NOT_FOUND, java.util.List.of(id), 404);
        }
        if (current.has("owner") && !user.equals(text(current, "owner"))) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.NOT_OWNER,
                    java.util.List.of("update", text(current, "owner"), user), 403);
        }
        Json.Node now = Json.num(clock.getAsLong());
        Map<String, Json.Node> stamps = new LinkedHashMap<>();
        stamps.put("owner", Json.str(user));
        if (current.has("createdAt")) {
            stamps.put("createdAt", current.get("createdAt"));
        }
        stamps.put("lastUpdatedAt", now);
        stamps.put("lastOpenAt", now);
        Json.Obj updated = with(row(query), stamps);
        records.put(updated);
        return wire(updated);
    }

    /** {@code DELETE dataCube/{id}}: only the owner's. */
    public synchronized void delete(String id, String user) {
        Json.Obj current = records.find(id);
        if (current == null) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.QUERY_NOT_FOUND, java.util.List.of(id), 404);
        }
        if (current.has("owner") && !user.equals(text(current, "owner"))) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.NOT_OWNER,
                    java.util.List.of("delete", text(current, "owner"), user), 403);
        }
        records.remove(id);
    }

    /** The page's id, once its record is one the rules take (the engine's {@code validateQuery}, as far as a page's). */
    private static String validate(Json.Obj q) {
        if (!(q.has("id") && q.get("id") instanceof Json.Str id && !id.value().isEmpty())) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.FIELD_MISSING, java.util.List.of("ID", "id"), 400);
        }
        if (!(q.has("name") && q.get("name") instanceof Json.Str n && !n.value().isEmpty())) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.FIELD_MISSING, java.util.List.of("name", "name"), 400);
        }
        if (!q.has("content") || q.get("content") instanceof Json.Null) {
            throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.FIELD_MISSING, java.util.List.of("content", "pageContent"), 400);
        }
        pageId(id.value());
        // the fields the rules read later, of their kinds (a check, not a cast: see searchShape)
        for (String f : List.of("description", "owner")) {
            if (q.has(f) && !(q.get(f) instanceof Json.Str || q.get(f) instanceof Json.Null)) {
                throw SavedQueries.unread("DataCubeQuery", f, "is not a string", f);
            }
        }
        return id.value();
    }

    /** Whether {@code id} is one a page makes ({@link #ID}). */
    static boolean isPageId(String id) {
        return ID.matcher(id).matches() && !DEVICE.matcher(id).matches();
    }

    /** Refused unless {@code id} is one a page makes ({@link #ID}). */
    static void pageId(String id) {
        if (!isPageId(id)) {
            throw new SavedQueries.Refusal("a saved page's id is letters, digits, \"-\" and \"_\" (as a page makes one),"
                    + " not '" + id + "'", 400);
        }
    }

    /**
     * The search specification's fields, of the kinds the engine's classes take them as; another kind is the 500 for a
     * body it cannot read. Checked before the rules read the body, not left to a failed cast: in the page's WebAssembly
     * a failed cast stops the module, so only a check answers the same on every host.
     */
    private static void searchShape(Json.Obj spec) {
        if (spec.has("searchTermSpecification") && !(spec.get("searchTermSpecification") instanceof Json.Null)) {
            if (!(spec.get("searchTermSpecification") instanceof Json.Obj term)) {
                throw SavedQueries.unread("QuerySearchSpecification", "searchTermSpecification", "is not an object",
                        "searchTermSpecification");
            }
            if (!(term.has("searchTerm") && term.get("searchTerm") instanceof Json.Str)) {
                throw new SavedQueries.Refusal(SavedQueries.Refusal.Kind.NO_SEARCH_TERM, java.util.List.of(), 500);
            }
            kind(term, "exactMatchName", "QuerySearchTermSpecification", true, "searchTermSpecification.");
            kind(term, "includeOwner", "QuerySearchTermSpecification", true, "searchTermSpecification.");
        }
        kind(spec, "showCurrentUserQueriesOnly", "QuerySearchSpecification", true, "");
        kind(spec, "sortByOption", "QuerySearchSpecification", false, "");
        if (spec.has("limit") && !(spec.get("limit") instanceof Json.Num || spec.get("limit") instanceof Json.Null)) {
            throw SavedQueries.unread("QuerySearchSpecification", "limit", "is not a number", "limit");
        }
    }

    /** {@code o}'s {@code field}, when it has one: a boolean ({@code flag}) or a string; {@code in}, its place in the
     *  request. */
    private static void kind(Json.Obj o, String field, String where, boolean flag, String in) {
        if (!o.has(field) || o.get(field) instanceof Json.Null) {
            return;
        }
        Json.Node n = o.get(field);
        if (flag ? !(n instanceof Json.Bool) : !(n instanceof Json.Str)) {
            throw SavedQueries.unread(where, field, "is not a " + (flag ? "boolean" : "string"), in + field);
        }
    }

    /**
     * Whether {@code row} is the page {@code id} as the rules keep it: its id that one, its content's text a string.
     * The records a host keeps are checked with it, never cast: one that is not a page (a file written by something
     * else, cut short; a row of another shape) is LEFT OUT on every host -- not listed, not found, so a page saved under
     * its id replaces it -- and every other page still opens.
     */
    static boolean isRow(String id, Json.Obj row) {
        return row.has("id") && row.get("id") instanceof Json.Str s && s.value().equals(id)
                && row.has("content") && row.get("content") instanceof Json.Str;
    }

    /**
     * A record from the wire as the row kept: upstream's {@code DataCubeQuery} fields only, in the record's order (the
     * stamps the rules set after), its content the exact JSON text. Nothing else a body carries is kept: a page keeps
     * only what each host keeps exactly (the page's rows are structured clones, which keep a string exactly, a number
     * past 2^53 not).
     */
    private static Json.Obj row(Json.Obj query) {
        LinkedHashMap<String, Json.Node> kept = new LinkedHashMap<>();
        for (Map.Entry<String, Json.Node> e : query.fields().entrySet()) {
            if (FIELDS.contains(e.getKey())) {
                kept.put(e.getKey(), e.getValue());
            }
        }
        return with(new Json.Obj(kept), Map.of("content", Json.str(Json.toCompact(query.get("content")))));
    }

    /** Upstream's {@code DataCubeQuery}: the fields a page's record has. */
    private static final List<String> FIELDS = List.of("id", "name", "description", "content", "owner", "createdAt",
            "lastUpdatedAt", "lastOpenAt");

    /** A row as the wire carries it: its content JSON again, its numbers as written. */
    private static Json.Obj wire(Json.Obj row) {
        return with(row, Map.of("content", Json.parse(text(row, "content"))));
    }

    /** {@code o} with {@code fields} set: one it has keeps its place, a new one goes last. */
    private static Json.Obj with(Json.Obj o, Map<String, Json.Node> fields) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(o.fields());
        out.putAll(fields);
        return new Json.Obj(out);
    }

    private static String text(Json.Obj o, String field) {
        return o.has(field) && o.get(field) instanceof Json.Str s ? s.value() : "";
    }

    private static boolean bool(Json.Obj o, String field) {
        return o.has(field) && o.get(field) instanceof Json.Bool b && b.value();
    }

    private static long time(Json.Obj o, String field) {
        return o.has(field) && o.get(field) instanceof Json.Num n ? n.longValue() : 0;
    }
}
