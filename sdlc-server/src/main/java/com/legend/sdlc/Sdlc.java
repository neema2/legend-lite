package com.legend.sdlc;

import com.legend.base.Nullable;
import com.legend.depot.ArtifactSource;
import com.legend.depot.Depot;
import com.legend.depot.Resolver;
import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * THE SDLC's RULES, written once (design S21): upstream legend-sdlc's REST API and lite's text routes
 * (S15) as one {@link #handle} over a {@link Storage}. The server wraps it in HTTP over a git backend;
 * the page runs it compiled to WebAssembly over memory it persists in IndexedDB; one conformance suite
 * (sdlc-client/test/conformance.ts) holds both. The rules, statuses and messages are legend-sdlc's
 * GitLab backend's (studio/docs/SDLC_CONTRACT_SLICE1.md); departures are lite's, listed in
 * sdlc-client/README.md and marked DEPARTURE here.
 *
 * What it stores is text (design S5): one {@code .pure} file per element at {@code <package path>/<Name>.pure},
 * {@code project.json} beside them, imports refused at save as upstream SDLC refuses them (S20, v0).
 * Entities are derived on read by the {@link Grammar}, never stored. Revisions are git commits.
 *
 * TeaVM-safe by rule: no files, sockets, threads, reflection or {@code java.security}; JSON through
 * {@code //json}. One caller at a time: whoever hosts it serializes calls.
 */
public final class Sdlc {
    /** An answer: its status and its JSON body ({@code null} for 204). */
    public record Response(int status, @Nullable String body) {}

    /** The compiler, as the rules use it. */
    public interface Grammar {
        /** Pure text to its protocol JSON ({@code {"_type":"data","elements":[...]}}), or a refusal (a RuntimeException). */
        String modelJson(String text);

        /** A whole model's compile errors, every one, as legend-lite's {@code compilation/compile} finds them: none when it compiles. */
        List<String> compile(String model);
    }

    /** The project structure every project here declares (upstream's latest; the layout itself is lite's, S5). */
    private static final int STRUCTURE_VERSION = 13;
    private static final String LINE = "master";

    private static final Pattern ENTITY_PATH = Pattern.compile("^(?!meta::)[A-Za-z0-9_]+(::[A-Za-z0-9_]+)*::[A-Za-z0-9_$]+$");
    // upstream's rule (GitLabWorkspaceApi.java:1140-1180); and, lite's, no `.lock` (git refuses such ref names) or
    // `.tmp` suffix (a storage's staging name) -- a workspace id becomes a ref name and a file name
    private static final Pattern WORKSPACE_ID = Pattern.compile("^(?!.*\\.(lock|tmp)$)([A-Za-z0-9_]([A-Za-z0-9_-]|\\.(?!\\.))*[A-Za-z0-9_]|[A-Za-z0-9_])$");
    /** A commit id as git writes it: what a literal revision in a URL or a body must be before it is looked up. */
    private static final Pattern COMMIT_ID = Pattern.compile("^[0-9a-f]{40}$");
    // ProjectStructure.java:93, and SourceVersion.isName (dotted Java identifiers, keywords excluded)
    private static final Pattern ARTIFACT_ID = Pattern.compile("^[a-z][a-z\\d_]*(-[a-z][a-z\\d_]*)*$");
    private static final Pattern JAVA_IDENTIFIER = Pattern.compile("^[A-Za-z_$][A-Za-z\\d_$]*$");
    private static final Set<String> JAVA_KEYWORDS = Set.of("abstract", "assert", "boolean", "break", "byte", "case",
            "catch", "char", "class", "const", "continue", "default", "do", "double", "else", "enum", "extends", "final",
            "finally", "float", "for", "goto", "if", "implements", "import", "instanceof", "int", "interface", "long",
            "native", "new", "package", "private", "protected", "public", "return", "short", "static", "strictfp",
            "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void", "volatile",
            "while", "true", "false", "null", "_");

    private final Storage storage;
    private final Git git;
    private final String userId;
    private final String userName;
    private final Grammar grammar;
    private final LongSupplier clock;
    /** Entities by the blob they were read from: a blob is immutable, so is its entity. */
    private final Map<String, Json.Obj> derived = new HashMap<>();

    public Sdlc(Storage storage, String userId, String userName, Grammar grammar, LongSupplier clock) {
        this.storage = storage;
        this.git = new Git(storage);
        this.userId = userId;
        this.userName = userName;
        this.grammar = grammar;
        this.clock = clock;
    }

    /** A refusal: its status, legend-sdlc's words, and (a JSON error) details. */
    private static final class Refusal extends RuntimeException {
        final int status;
        final @Nullable String details;

        Refusal(String message, int status) {
            this(message, status, null);
        }

        Refusal(String message, int status, @Nullable String details) {
            super(message);
            this.status = status;
            this.details = details;
        }
    }

    /** Upstream's 501 for a capability this backend does not have ({@code UnsupportedCapabilityExceptionMapper}). */
    private static final class Unsupported extends RuntimeException {
        final String capability;

        Unsupported(String capability) {
            super(capability);
            this.capability = capability;
        }
    }

    /** What 501s name as the backend. */
    private String backendType = "page";

    public Sdlc backendType(String type) {
        this.backendType = type;
        return this;
    }

    // =====================================================================================================
    // the one entry point
    // =====================================================================================================

    /** One request: {@code target} is the path and query under the API root, e.g. {@code /projects/x?limit=1}. */
    public Response handle(String method, String target, @Nullable String body) {
        try {
            int q = target.indexOf('?');
            String path = q < 0 ? target : target.substring(0, q);
            Map<String, List<String>> query = q < 0 ? Map.of() : query(target.substring(q + 1));
            List<String> parts = new ArrayList<>();
            for (String s : path.split("/")) if (!s.isEmpty()) parts.add(decode(s, false));
            return route(method, parts, query, body);
        } catch (Unsupported e) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("capability", e.capability);
            out.put("backendType", backendType);
            out.put("message", "The backend \"" + backendType + "\" does not support " + e.capability);
            return new Response(501, Json.toCompact(out));
        } catch (Refusal e) {
            return error(e.status, String.valueOf(e.getMessage()), e.details);
        } catch (RuntimeException e) {
            return error(500, e.getMessage() == null ? e.getClass().getName() : e.getMessage(), null);
        }
    }

    // =====================================================================================================
    // routes
    // =====================================================================================================

    private Response route(String method, List<String> parts, Map<String, List<String>> q, @Nullable String body) {
        String a = parts.isEmpty() ? "" : parts.get(0);
        String b = parts.size() > 1 ? parts.get(1) : "";
        if (parts.size() == 1 && a.equals("currentUser") && method.equals("GET")) {
            return ok(map("userId", userId, "name", userName));
        }
        if (a.equals("auth") && parts.size() == 2 && method.equals("GET")) {
            if (b.equals("authorized")) return ok(Boolean.TRUE);
            if (b.equals("termsOfServiceAcceptance")) return ok(List.of());
        }
        if (a.equals("server") && b.equals("features") && parts.size() == 2 && method.equals("GET")) {
            return ok(map("canCreateProject", Boolean.TRUE, "canCreateVersion", Boolean.FALSE));
        }
        if (a.equals("configuration") && b.equals("latestProjectStructureVersion") && parts.size() == 2 && method.equals("GET")) {
            return ok(map("version", STRUCTURE_VERSION, "extensionVersion", Json.nil()));
        }
        if (!a.equals("projects")) throw new Refusal("HTTP 404 Not Found", 404);

        if (parts.size() == 1) {
            if (method.equals("POST")) return ok(createProject(parse(body)));
            if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
            return ok(listProjects(q));
        }

        String p = b;
        // every segment that becomes a storage key is checked first: a decoded segment may hold anything
        if (!isProjectId(p)) throw new Refusal("Invalid project id: \"" + p + "\"", 400);
        List<String> rest = parts.subList(2, parts.size());
        if (rest.isEmpty()) {
            if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
            return ok(projectView(project(p)));
        }
        String c = rest.get(0);
        if (c.equals("reviews")) return reviews(method, p, rest.subList(1, rest.size()), q, body);
        if (c.equals("versions")) return versions(method, p, rest.subList(1, rest.size()), q, body);
        if (c.equals("patches")) throw new Unsupported("PATCHES");
        if (c.equals("conflictResolution") && rest.size() == 1 && method.equals("GET")) {
            project(p);
            return ok(List.of());
        }
        if (c.equals("workspaces")) {
            if (rest.size() == 1) {
                if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
                project(p);
                // the page has one user: `owned` changes nothing
                List<Object> out = new ArrayList<>();
                for (String w : workspaceIds(p)) out.add(workspaceView(p, w));
                return ok(out);
            }
            String w = rest.get(1);
            if (!WORKSPACE_ID.matcher(w).matches()) throw invalidWorkspace(w);
            List<String> sub = rest.subList(2, rest.size());
            if (sub.isEmpty()) {
                switch (method) {
                    case "GET" -> {
                        workspaceHead(p, w, wsOf(p, w));
                        return ok(workspaceView(p, w));
                    }
                    case "POST" -> {
                        return ok(createWorkspace(p, w));
                    }
                    case "DELETE" -> {
                        // upstream: deleting what is not there succeeds (contract quirk 3); its conflict resolution goes too
                        storage.delete(workspaceRef(p, w));
                        storage.delete(conflictRef(p, w));
                        return new Response(204, null);
                    }
                    default -> throw new Refusal("HTTP 405 Method Not Allowed", 405);
                }
            }
            if (sub.size() == 1 && method.equals("GET") && sub.get(0).equals("outdated")) {
                String head = workspaceHead(p, w, wsOf(p, w));
                String line = lineHead(p);
                return ok(!head.equals(line) && !isAncestor(line, head));
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("update")) {
                return ok(updateWorkspace(p, w));
            }
            if (sub.get(0).equals("conflictResolution")) return conflictResolution(method, p, w, sub.subList(1, sub.size()), q, body);
            if (sub.size() == 1 && method.equals("GET") && sub.get(0).equals("inConflictResolutionMode")) {
                workspaceHead(p, w, wsOf(p, w));
                return ok(storage.get(conflictRef(p, w)) != null);
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("pureChanges")) {
                return pureChanges(p, w, parse(body));
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("entityChanges")) {
                // DEPARTURE (S20): a JSON save is printed to text first (S5), and the model printer is not built yet
                workspaceHead(p, w, wsOf(p, w));
                throw new Unsupported("ENTITY_CHANGES");
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("configuration")) {
                return updateConfiguration(p, w, parse(body));
            }
            return reads(p, w, sub, method, q);
        }
        return reads(p, null, rest, method, q);
    }

    /** The read routes under a project or a workspace: configuration, revisions, entities, text. */
    private Response reads(String p, @Nullable String w, List<String> sub, String method, Map<String, List<String>> q) {
        if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
        String revision = "HEAD";
        boolean named = false;
        List<String> at = sub;
        if (!at.isEmpty() && at.get(0).equals("revisions")) {
            if (at.size() == 1) return ok(revisionList(p, w, q));
            revision = at.get(1);
            named = true;
            at = at.subList(2, at.size());
            if (at.isEmpty()) return ok(revisionView(resolve(p, w, revision)));
        }
        String id = resolve(p, w, revision);
        String desc = (named ? "revision " + revision + " of " : "") + (w == null ? "project " + p : wsOf(p, w));
        return readsAt(id, desc, at, q);
    }

    /** What a commit holds -- its configuration, entities, text -- for the routes under a revision or a version. */
    private Response readsAt(String id, String desc, List<String> at, Map<String, List<String>> q) {
        Map<String, String> files = entityFiles(git.readTree(git.readCommit(id).tree()));
        String what = at.isEmpty() ? "" : at.get(0);
        if (what.equals("configuration") && at.size() == 1) return ok(configView(config(id)));
        if (what.equals("entities") && at.size() == 1) {
            return ok(filtered(entities(files, first(q, "excludeInvalid", "false").equals("true")), q));
        }
        if (what.equals("entities") && at.size() == 2) {
            String path = at.get(1);
            String blob = files.get(path);
            if (blob == null) throw new Refusal("Unknown entity " + path + " for " + desc, 404);
            return ok(entityOf(path, blob));
        }
        if (what.equals("pure") && at.size() == 1) {
            List<Object> out = new ArrayList<>();
            for (Map.Entry<String, String> f : files.entrySet()) out.add(map("path", f.getKey(), "pureCode", git.readBlob(f.getValue())));
            return ok(out);
        }
        if (what.equals("pure") && at.size() == 2) {
            String path = at.get(1);
            String blob = files.get(path);
            if (blob == null) throw new Refusal("Unknown entity " + path + " for " + desc, 404);
            return ok(map("path", path, "pureCode", git.readBlob(blob)));
        }
        throw new Refusal("HTTP 404 Not Found", 404);
    }

    // =====================================================================================================
    // projects, workspaces, revisions
    // =====================================================================================================

    private static String wsOf(String p, String w) {
        return "user workspace " + w + " of project " + p;
    }

    private static String wsIn(String p, String w) {
        return "user workspace " + w + " in project " + p;
    }

    // THE KEYS. Every key built from outside data (a URL, a body, a project.json) is built here, and each builder
    // refuses an id that is not one -- so no path, separator or `..` can reach a Storage, whatever route it came by.

    private static String checked(String p) {
        if (!isProjectId(p)) throw new Refusal("Invalid project id: \"" + p + "\"", 400);
        return p;
    }

    private String lineRef(String p) {
        return "ref/" + checked(p) + "/" + LINE;
    }

    private String projectKey(String p) {
        return "project/" + checked(p);
    }

    private String workspaceRef(String p, String w) {
        if (!WORKSPACE_ID.matcher(w).matches()) throw invalidWorkspace(w);
        return "ref/" + checked(p) + "/workspace/" + userId + "/" + w;
    }

    /** A workspace's conflict resolution (upstream's CONFLICT_RESOLUTION access type): its own head while it lasts. */
    private String conflictRef(String p, String w) {
        if (!WORKSPACE_ID.matcher(w).matches()) throw invalidWorkspace(w);
        return "ref/" + checked(p) + "/conflictResolution/" + userId + "/" + w;
    }

    private Json.Obj project(String p) {
        String record = storage.get(projectKey(p));
        if (record == null) throw new Refusal("Unknown project: " + p, 404);
        return Json.parseObject(record);
    }

    private String lineHead(String p) {
        project(p);
        String head = storage.get(lineRef(p));
        if (head == null) throw new IllegalStateException("the SDLC lost the line of project " + p);
        return head;
    }

    /** A workspace's head, or the 404 in the phrasing the caller's context uses ({@code desc}). */
    private String workspaceHead(String p, String w, String desc) {
        project(p);
        String head = storage.get(workspaceRef(p, w));
        if (head == null) throw new Refusal("Unknown: " + desc, 404);
        return head;
    }

    private List<String> workspaceIds(String p) {
        String prefix = "ref/" + checked(p) + "/workspace/" + userId + "/";
        List<String> out = new ArrayList<>();
        for (String k : storage.keys(prefix)) out.add(k.substring(prefix.length()));
        return out;
    }

    /**
     * DEPARTURE (lite): the one of {@code ids} equal to {@code id} ignoring case, or null. Ids differing only in
     * case are one id here (upstream's GitLab branches are not), since a repository on macOS or Windows keeps
     * them as one file: two of them would silently share one ref.
     */
    private static @Nullable String sameIgnoringCase(List<String> ids, String id) {
        for (String i : ids) if (i.equalsIgnoreCase(id)) return i;
        return null;
    }

    private boolean isAncestor(String ancestor, String of) {
        return git.reaches(of, ancestor);
    }

    /** The first commit of a history (upstream: a project line's BASE is its oldest commit). */
    private String root(String head) {
        List<String> all = git.history(head);
        for (String id : all) if (git.readCommit(id).parents().isEmpty()) return id;
        throw new IllegalStateException("history of " + head + " has no first commit");
    }

    /** Where a workspace was made from: the newest commit both it and the project line reach (git's merge base). */
    private String mergeBase(String line, String workspace) {
        Set<String> onLine = new HashSet<>(git.history(line));
        for (String at : git.history(workspace)) if (onLine.contains(at)) return at;
        throw new IllegalStateException("workspace " + workspace + " shares no history with its project line");
    }

    /**
     * A revision named in a URL: an alias (case-insensitive: BASE; HEAD, CURRENT, LATEST) or an id on the
     * scope's history (contract §4). DEPARTURE: entity and text reads check a literal id too (upstream
     * reads any commit of the repository through any workspace URL, quirk 6).
     */
    private String resolve(String p, @Nullable String w, String r) {
        String head = w == null ? lineHead(p) : workspaceHead(p, w, wsIn(p, w));
        String alias = r.toLowerCase(Locale.ROOT);
        if (alias.equals("base")) return w == null ? root(head) : mergeBase(lineHead(p), head);
        if (alias.equals("head") || alias.equals("current") || alias.equals("latest")) return head;
        String desc = w == null ? "project " + p : wsIn(p, w);
        if (!COMMIT_ID.matcher(r).matches() || !git.isCommit(r) || !isAncestor(r, head)) {
            throw new Refusal("Revision " + r + " is unknown for " + desc, 404);
        }
        return r;
    }

    private Map<String, Object> revisionView(String id) {
        Git.Commit c = git.readCommit(id);
        return map("id", id,
                "authorName", c.author(),
                "authoredTimestamp", Instant.ofEpochSecond(c.authorSeconds()).toString(),
                "committerName", c.committer(),
                "committedTimestamp", Instant.ofEpochSecond(c.committerSeconds()).toString(),
                "message", c.message());
    }

    private Map<String, Object> projectView(Json.Obj p) {
        return map("projectId", p.getString("projectId"), "name", p.getString("name"),
                "description", p.getString("description"), "tags", p.getStringArray("tags"), "webUrl", Json.nil());
    }

    private Map<String, Object> workspaceView(String p, String w) {
        return map("projectId", p, "userId", userId, "workspaceId", w);
    }

    /** A revision's {@code project.json}. */
    private Json.Obj config(String commit) {
        String blob = git.readTree(git.readCommit(commit).tree()).get("project.json");
        if (blob == null) throw new IllegalStateException("revision " + commit + " has no project.json");
        return Json.parseObject(git.readBlob(blob));
    }

    /** {@code SimpleProjectConfiguration} as served: creator order, nulls written (contract §7). */
    private static Map<String, Object> configView(Json.Obj c) {
        Json.Obj version = c.getObj("projectStructureVersion");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("projectId", c.getString("projectId"));
        out.put("projectType", c.getStringOr("projectType", null));
        out.put("projectStructureVersion", map("version", version.getInt("version"),
                "extensionVersion", orNull(version, "extensionVersion")));
        out.put("platformConfigurations", c.getOr("platformConfigurations", null));
        out.put("groupId", c.getString("groupId"));
        out.put("artifactId", c.getString("artifactId"));
        out.put("projectDependencies", c.getOr("projectDependencies", Json.arr()));
        out.put("metamodelDependencies", c.getOr("metamodelDependencies", Json.arr()));
        out.put("artifactGenerations", c.getOr("artifactGenerations", Json.arr()));
        out.put("runDependencyTests", c.getOr("runDependencyTests", null));
        out.put("produceShadedServiceJar", c.getOr("produceShadedServiceJar", null));
        return out;
    }

    private List<Object> listProjects(Map<String, List<String>> q) {
        String limitText = firstOrNull(q, "limit");
        Integer limit = null;
        if (limitText != null) {
            try {
                limit = Integer.parseInt(limitText);
            } catch (NumberFormatException e) {
                throw new Refusal("HTTP 404 Not Found", 404); // JAX-RS: an unconvertible query parameter
            }
            if (limit < 0) throw new Refusal("Invalid limit: " + limit, 400);
        }
        List<Object> out = new ArrayList<>();
        if (limit != null && limit == 0) return out;
        String search = firstOrNull(q, "search");
        List<String> tags = q.getOrDefault("tag", List.of());
        List<String> excluded = q.getOrDefault("excludeTag", List.of());
        for (String key : storage.keys("project/")) {
            Json.Obj p = Json.parseObject(String.valueOf(storage.get(key)));
            List<String> own = p.getStringArray("tags");
            boolean found = search == null
                    || p.getString("name").toLowerCase(Locale.ROOT).contains(search.toLowerCase(Locale.ROOT))
                    || p.getString("description").toLowerCase(Locale.ROOT).contains(search.toLowerCase(Locale.ROOT));
            if (!found) continue;
            if (!tags.isEmpty() && tags.stream().noneMatch(own::contains)) continue;
            if (excluded.stream().anyMatch(own::contains)) continue;
            out.add(projectView(p));
            if (limit != null && out.size() == limit) break;
        }
        return out;
    }

    private Map<String, Object> createProject(Json.@Nullable Node body) {
        if (!(body instanceof Json.Obj c)) throw new Refusal("Input required to create project", 400);
        String name = string(c, "name");
        if (name == null || name.isEmpty()) throw new Refusal("name may not be null or empty", 400);
        String description = string(c, "description");
        if (description == null) throw new Refusal("description may not be null", 400);
        String groupId = string(c, "groupId");
        if (groupId == null || groupId.isEmpty() || !isJavaName(groupId)) {
            throw new Refusal("Invalid groupId: " + groupId, 400);
        }
        String artifactId = string(c, "artifactId");
        if (artifactId == null || !ARTIFACT_ID.matcher(artifactId).matches()) {
            throw new Refusal("Invalid artifactId: " + artifactId + ". ArtifactId must follow pattern that starts with a lowercase letter and can include lowercase letters, digits, underscores, and hyphens between segments.", 400);
        }
        String typeText = string(c, "type");
        String type = typeText == null ? "MANAGED" : typeText.toUpperCase(Locale.ROOT);
        if (!type.equals("MANAGED") && !type.equals("EMBEDDED")) throw new Refusal("Invalid type: " + type, 400);
        // DEPARTURE (lite): a project is named by its coordinates, `groupId:artifactId` -- the id upstream's
        // own project.json uses for a dependency -- not a GitLab number; one project per coordinates.
        String projectId = groupId + ":" + artifactId;
        String same = sameIgnoringCase(storage.keys("project/"), "project/" + projectId);
        if (same != null) {
            throw new Refusal("Failed to create project: " + name + ": a project with coordinates " + same.substring("project/".length()) + " already exists", 409);
        }
        List<String> tags = new ArrayList<>();
        Json.Arr tagArr = c.getArrOr("tags", null);
        if (tagArr != null) for (Json.Node t : tagArr.items()) if (t instanceof Json.Str s) tags.add(s.value());

        Map<String, Object> config = new TreeMap<>(); // project.json: upstream writes it with its keys sorted
        config.put("artifactGenerations", List.of());
        config.put("artifactId", artifactId);
        config.put("groupId", groupId);
        config.put("metamodelDependencies", List.of());
        config.put("projectDependencies", List.of());
        config.put("projectId", projectId);
        config.put("projectStructureVersion", map("version", STRUCTURE_VERSION));
        config.put("projectType", type);
        Map<String, String> files = new TreeMap<>();
        files.put("project.json", git.writeBlob(Json.toPretty(config)));
        String first = commit(List.of(), files, "Build project structure");

        Map<String, Object> record = map("projectId", projectId, "name", name, "description", description, "tags", tags);
        storage.put(projectKey(projectId), Json.toCompact(record));
        storage.put(lineRef(projectId), first);
        return projectView(Json.parseObject(Json.toCompact(record)));
    }

    /** upstream's refusal of a workspace id (GitLabWorkspaceApi.java:1140-1180), now asked on every route. */
    private static Refusal invalidWorkspace(String w) {
        return new Refusal("Invalid workspace id: \"" + w + "\". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain \"..\" and may not start or end with '.' or '-'.", 400);
    }

    /** A project id here is {@code groupId:artifactId}, by the rules a project is created with. */
    static boolean isProjectId(String p) {
        int colon = p.indexOf(':');
        return colon > 0 && isJavaName(p.substring(0, colon)) && ARTIFACT_ID.matcher(p.substring(colon + 1)).matches();
    }

    private Map<String, Object> createWorkspace(String p, String w) {
        if (!WORKSPACE_ID.matcher(w).matches()) throw invalidWorkspace(w);
        String line = lineHead(p);
        String same = sameIgnoringCase(workspaceIds(p), w);
        if (same != null && !same.equals(w)) {
            throw new Refusal("Error creating " + wsOf(p, w) + ": workspace " + same + " already exists, and ids differing only in case are one", 409);
        }
        String existing = storage.get(workspaceRef(p, w));
        // upstream: already there AT the line's head is a no-op; elsewhere, GitLab's "Branch already exists" as a 500
        if (existing != null && !existing.equals(line)) {
            throw new Refusal("Error creating " + wsOf(p, w) + ": Branch already exists", 500);
        }
        if (existing == null) storage.put(workspaceRef(p, w), line);
        return workspaceView(p, w);
    }

    /** A commit of {@code files} (all of the tree: path → blob) on {@code parents}; its id. */
    private String commit(List<String> parents, Map<String, String> files, String message) {
        long now = clock.getAsLong() / 1000;
        return git.writeCommit(new Git.Commit(git.writeTree(files), parents, userId, now, userId, now, message));
    }

    // =====================================================================================================
    // revision lists (contract slice 2 §3)
    // =====================================================================================================

    private List<Object> revisionList(String p, @Nullable String w, Map<String, List<String>> q) {
        String head = w == null ? lineHead(p) : workspaceHead(p, w, wsIn(p, w));
        Integer limit = intParam(q, "limit");
        if (limit != null && limit < 0) throw new Refusal("Invalid limit: " + limit, 400);
        Instant since = instantParam(q, "since");
        Instant until = instantParam(q, "until");
        List<Object> out = new ArrayList<>();
        if (limit != null && limit == 0) return out;
        for (String id : git.history(head)) {
            Instant at = Instant.ofEpochSecond(git.readCommit(id).committerSeconds());
            if (since != null && at.isBefore(since)) continue;
            if (until != null && at.isAfter(until)) continue;
            out.add(revisionView(id));
            if (limit != null && out.size() == limit) break;
        }
        return out;
    }

    // =====================================================================================================
    // reviews (contract slice 2 §1): a workspace's way onto the project line
    // =====================================================================================================

    private static final Pattern REVIEW_ID = Pattern.compile("^\\d+$");

    private String reviewKey(String p, String id) {
        if (!REVIEW_ID.matcher(id).matches()) throw new Refusal("Invalid id: " + id, 400);
        return "review/" + checked(p) + "/" + id;
    }

    private Response reviews(String method, String p, List<String> sub, Map<String, List<String>> q, @Nullable String body) {
        project(p);
        if (sub.isEmpty()) {
            if (method.equals("GET")) return ok(listReviews(p, q));
            if (method.equals("POST")) return ok(reviewView(p, createReview(p, parse(body))));
            throw new Refusal("HTTP 405 Method Not Allowed", 405);
        }
        String id = sub.get(0);
        if (!REVIEW_ID.matcher(id).matches()) throw new Refusal("Invalid id: " + id, 400);
        Map<String, Object> review = review(p, id);
        if (sub.size() == 1) {
            if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
            return ok(reviewView(p, review));
        }
        String action = sub.get(1);
        if (sub.size() == 2 && method.equals("GET") && action.equals("approval")) {
            return ok(map("approvedBy", listOf(review.get("approvedBy"))));
        }
        if (sub.size() != 2 || !method.equals("POST")) throw new Refusal("HTTP 404 Not Found", 404);
        String now = Instant.ofEpochMilli(clock.getAsLong()).toString();
        switch (action) {
            case "close", "reject" -> {
                requireState(review, "OPEN");
                review.put("state", "CLOSED");
                review.put("closedAt", now);
                review.put("commits", commitsOf(p, review));
            }
            case "reopen" -> {
                requireState(review, "CLOSED");
                review.put("state", "OPEN");
                review.put("closedAt", Json.nil());
            }
            case "approve" -> {
                List<Object> approvers = new ArrayList<>(listOf(review.get("approvedBy")));
                Map<String, Object> me = map("name", userName, "userId", userId);
                if (approvers.stream().noneMatch(a -> userId.equals(field(a, "userId")))) approvers.add(me);
                review.put("approvedBy", approvers);
            }
            case "revokeApproval" -> {
                List<Object> approvers = new ArrayList<>(listOf(review.get("approvedBy")));
                approvers.removeIf(a -> userId.equals(field(a, "userId")));
                review.put("approvedBy", approvers);
            }
            case "commit" -> commitReview(p, id, review, parse(body), now);
            default -> throw new Refusal("HTTP 404 Not Found", 404);
        }
        review.put("lastUpdatedAt", now);
        storage.put(reviewKey(p, id), Json.toCompact(review));
        return ok(reviewView(p, review));
    }

    private Map<String, Object> review(String p, String id) {
        String record = storage.get(reviewKey(p, id));
        if (record == null) throw new Refusal("Unknown review in project " + p + ": " + id, 404);
        return new LinkedHashMap<>(Json.parseObject(record).fields());
    }

    private static void requireState(Map<String, Object> review, String expected) {
        String actual = String.valueOf(field(review, "state"));
        if (!actual.equals(expected)) {
            throw new Refusal("Review is not " + expected.toLowerCase(Locale.ROOT) + " (state: " + actual.toLowerCase(Locale.ROOT) + ")", 409);
        }
    }

    /** The {@code Review} as served, in the interface's order (contract slice 2 §0.5). */
    private static Map<String, Object> reviewView(String p, Map<String, Object> r) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", r.get("id"));
        out.put("projectId", p);
        for (String f : List.of("workspaceId", "workspaceType", "title", "description", "createdAt", "lastUpdatedAt",
                "closedAt", "committedAt", "state", "author", "commitRevisionId")) {
            Object v = r.get(f);
            out.put(f, v == null ? Json.nil() : v);
        }
        out.put("webURL", Json.nil());
        out.put("labels", r.containsKey("labels") ? r.get("labels") : List.of());
        return out;
    }

    private Map<String, Object> createReview(String p, Json.@Nullable Node body) {
        if (!(body instanceof Json.Obj c)) throw new Refusal("Input required to create review", 400);
        String w = string(c, "workspaceId");
        // DEPARTURE: upstream answers a missing workspaceId with a 500 NPE (slice 2 quirk 3)
        if (w == null) throw new Refusal("id may not be null", 400);
        if (!WORKSPACE_ID.matcher(w).matches()) throw invalidWorkspace(w);
        String type = string(c, "workspaceType");
        if (type != null && !type.equalsIgnoreCase("USER")) throw new Refusal("Unknown: group workspace " + w + " of project " + p, 404);
        String title = string(c, "title");
        if (title == null) throw new Refusal("title may not be null", 400);
        String description = string(c, "description");
        if (description == null) throw new Refusal("description may not be null", 400);
        workspaceHead(p, w, wsOf(p, w));
        // DEPARTURE: upstream lets GitLab refuse a second open review for a workspace, answered as a 500 (quirk 2)
        for (Map<String, Object> other : allReviews(p)) {
            if ("OPEN".equals(field(other, "state")) && w.equals(field(other, "workspaceId"))) {
                throw new Refusal("Error submitting changes from " + wsOf(p, w) + " for review: an open review already exists for it: "
                        + field(other, "id"), 409);
            }
        }
        String seqKey = "seq/" + checked(p) + "/review";
        String last = storage.get(seqKey);
        String id = String.valueOf(last == null ? 1 : Long.parseLong(last.trim()) + 1);
        storage.put(seqKey, id);
        String now = Instant.ofEpochMilli(clock.getAsLong()).toString();
        List<Object> labels = new ArrayList<>();
        Json.Arr given = c.getArrOr("labels", null);
        if (given != null) labels.addAll(given.items());
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("workspaceId", w);
        r.put("workspaceType", "USER");
        r.put("title", title);
        r.put("description", description);
        r.put("createdAt", now);
        r.put("lastUpdatedAt", now);
        r.put("closedAt", Json.nil());
        r.put("committedAt", Json.nil());
        r.put("state", "OPEN");
        r.put("author", map("name", userName, "userId", userId));
        r.put("commitRevisionId", Json.nil());
        r.put("labels", labels);
        r.put("approvedBy", List.of());
        storage.put(reviewKey(p, id), Json.toCompact(r));
        return r;
    }

    private List<Map<String, Object>> allReviews(String p) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String key : storage.keys("review/" + checked(p) + "/")) {
            out.add(new LinkedHashMap<>(Json.parseObject(String.valueOf(storage.get(key))).fields()));
        }
        // newest first, as GitLab lists merge requests
        out.sort((a, b) -> Long.compare(Long.parseLong(String.valueOf(field(b, "id"))), Long.parseLong(String.valueOf(field(a, "id")))));
        return out;
    }

    /** The commits a review brings: those its workspace has that the project line had not (git's diff range). */
    private List<String> commitsOf(String p, Map<String, Object> review) {
        if ("OPEN".equals(field(review, "state"))) {
            String head = storage.get(workspaceRef(p, String.valueOf(field(review, "workspaceId"))));
            if (head != null) {
                Set<String> base = new HashSet<>(git.history(mergeBase(lineHead(p), head)));
                List<String> out = new ArrayList<>();
                for (String id : git.history(head)) if (!base.contains(id)) out.add(id);
                return out;
            }
        }
        List<String> out = new ArrayList<>();
        for (Object o : listOf(review.get("commits"))) out.add(String.valueOf(o instanceof Json.Str s ? s.value() : o));
        return out;
    }

    private List<Object> listReviews(String p, Map<String, List<String>> q) {
        String stateText = firstOrNull(q, "state");
        String state = stateText == null || stateText.isEmpty() ? null : stateText.toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
        if (state != null && !List.of("OPEN", "COMMITTED", "CLOSED", "UNKNOWN").contains(state)) throw new Refusal("HTTP 400 Bad Request", 400);
        Set<String> revisionIds = new HashSet<>(q.getOrDefault("revisionIds", List.of()));
        String workspaceRegex = firstOrNull(q, "workspaceIdRegex");
        Pattern wsPattern = workspaceRegex == null ? null : regex(workspaceRegex);
        Set<String> types = new HashSet<>();
        for (String t : q.getOrDefault("workspaceTypes", List.of())) types.add(t.toUpperCase(Locale.ROOT));
        Instant since = instantParam(q, "since");
        Instant until = instantParam(q, "until");
        Integer limit = intParam(q, "limit");
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> r : allReviews(p)) {
            String own = String.valueOf(field(r, "state"));
            if (state != null && !state.equals("UNKNOWN") && !state.equals(own)) continue;
            if (!revisionIds.isEmpty() && commitsOf(p, r).stream().noneMatch(revisionIds::contains)) continue;
            if ((since != null || until != null) && !inTime(r, state, since, until)) continue;
            if (wsPattern != null && !wsPattern.matcher(String.valueOf(field(r, "workspaceId"))).find()) continue;
            if (!types.isEmpty() && !types.contains(String.valueOf(field(r, "workspaceType")))) continue;
            out.add(reviewView(p, r));
            // DEPARTURE: the limit counts what passed every filter (upstream applies it before the workspace filter, quirk 1)
            if (limit != null && limit > 0 && out.size() == limit) break;
        }
        return out;
    }

    /** Contract slice 2 §1.1 step 6: which timestamps a state's time window looks at. */
    private static boolean inTime(Map<String, Object> r, @Nullable String state, @Nullable Instant since, @Nullable Instant until) {
        java.util.function.Predicate<String> within = f -> {
            Object v = field(r, f);
            if (!(v instanceof String s)) return false;
            Instant at = Instant.parse(s);
            return (since == null || !at.isBefore(since)) && (until == null || !at.isAfter(until));
        };
        if ("OPEN".equals(state)) return within.test("createdAt") || within.test("lastUpdatedAt");
        if ("CLOSED".equals(state)) return within.test("closedAt") || within.test("lastUpdatedAt");
        if ("COMMITTED".equals(state)) return within.test("committedAt") || within.test("lastUpdatedAt");
        if (within.test("lastUpdatedAt")) return true;
        return switch (String.valueOf(field(r, "state"))) {
            case "COMMITTED" -> within.test("committedAt");
            case "CLOSED" -> within.test("closedAt");
            default -> within.test("createdAt");
        };
    }

    /**
     * Commits a review: the workspace merged onto the project line as one merge commit with the given
     * message (GitLab's default merge method), the workspace then deleted (upstream removes the source
     * branch; Studio relies on it). Refused, in upstream's words, for a review that is not open, for
     * dependencies that are not released versions, and -- lite's guard, design S7 -- for a merge that
     * conflicts or does not compile, so the project line always compiles.
     */
    private void commitReview(String p, String id, Map<String, Object> review, Json.@Nullable Node body, String now) {
        if (!(body instanceof Json.Obj cmd)) throw new Refusal("Input required to commit review", 400);
        String message = string(cmd, "message");
        if (message == null) throw new Refusal("message may not be null", 400);
        requireState(review, "OPEN");
        String w = String.valueOf(field(review, "workspaceId"));
        String head = storage.get(workspaceRef(p, w));
        if (head == null) {
            throw new Refusal("Review " + id + " in project " + p + " is not in a committable state: its workspace no longer exists", 409);
        }
        String line = lineHead(p);
        String base = mergeBase(line, head);
        Map<String, String> merged = merge(git.readTree(git.readCommit(base).tree()), git.readTree(git.readCommit(line).tree()),
                git.readTree(git.readCommit(head).tree()), id, p);
        // what lands is the MERGED tree: its project.json (the line's dependencies may have moved since the workspace was
        // made) is what the dependency rule and the gate read
        String mergedConfig = merged.get("project.json");
        if (mergedConfig == null) throw new IllegalStateException("review " + id + " would land a tree without project.json");
        Json.Obj config = Json.parseObject(git.readBlob(mergedConfig));
        List<String> improper = new ArrayList<>();
        for (Json.Node d : dependencies(config)) {
            Json.Obj dep = (Json.Obj) d;
            String pid = dep.getStringOr("projectId", null);
            String vid = dep.getStringOr("versionId", null);
            if (pid == null || pid.isBlank() || vid == null || !VERSION.matcher(vid).matches()) {
                improper.add("<SimpleProjectDependency " + pid + ":" + vid + ">");
            }
        }
        if (!improper.isEmpty()) throw new Refusal("Cannot create a review with the following dependencies: " + String.join(", ", improper), 409);
        List<String> errors = grammar.compile(model(merged, config));
        if (!errors.isEmpty()) {
            throw new Refusal("Review " + id + " in project " + p + " is not in a committable state: the project would not compile: "
                    + String.join("; ", errors), 409);
        }
        List<String> commits = commitsOf(p, review);
        String mergeCommit = commit(List.of(line, head), merged, message);
        review.put("state", "COMMITTED");
        review.put("committedAt", now);
        review.put("lastUpdatedAt", now);
        review.put("commitRevisionId", mergeCommit);
        review.put("commits", commits);
        // in an order a crash between any two steps can be recovered from: the line moves, the review says so, and
        // only then does the workspace go (a leftover workspace can be deleted; a lost review cannot be rebuilt)
        storage.put(lineRef(p), mergeCommit);
        storage.put(reviewKey(p, id), Json.toCompact(review));
        storage.delete(workspaceRef(p, w));
    }

    /** A review's merge: the merged tree, or upstream's refusal naming the conflicting files. */
    private static Map<String, String> merge(Map<String, String> base, Map<String, String> ours, Map<String, String> theirs, String id, String p) {
        Merged m = merge3(base, ours, theirs);
        if (!m.conflicts().isEmpty()) {
            throw new Refusal("Could not commit review " + id + " in project " + p + " because of a conflict: "
                    + "the project line changed the same files since the workspace was made: " + String.join(", ", m.conflicts()), 409);
        }
        return m.tree();
    }

    /** A three-way merge: the merged tree (path → blob), and the paths changed on both sides, differently. */
    private record Merged(Map<String, String> tree, List<String> conflicts) {}

    /** Three-way, file by file: a file changed on one side only takes that side; changed on both, differently, is a conflict. */
    private static Merged merge3(Map<String, String> base, Map<String, String> ours, Map<String, String> theirs) {
        Set<String> paths = new java.util.TreeSet<>(base.keySet());
        paths.addAll(ours.keySet());
        paths.addAll(theirs.keySet());
        Map<String, String> out = new TreeMap<>();
        List<String> conflicts = new ArrayList<>();
        for (String path : paths) {
            String b = base.get(path);
            String o = ours.get(path);
            String t = theirs.get(path);
            String result;
            if (java.util.Objects.equals(o, t)) result = o;
            else if (java.util.Objects.equals(o, b)) result = t;
            else if (java.util.Objects.equals(t, b)) result = o;
            else {
                conflicts.add(path);
                continue;
            }
            if (result != null) out.put(path, result);
        }
        return new Merged(out, conflicts);
    }

    /**
     * Upstream's workspace update ({@code POST …/workspaces/{w}/update}, WorkspaceApi.updateWorkspace): the workspace
     * rebased onto the project line's head -- each of its commits replayed there, file by file, with its author and
     * message -- reported as {@code {status, workspaceMergeBaseRevisionId, workspaceRevisionId}}: NO_OP when the
     * workspace already has the line's head, UPDATED when it was rebased, CONFLICT when the line and the workspace both
     * changed a file, differently: then, as upstream (GitLabWorkspaceApi.createConflictResolution), a conflict-resolution
     * workspace is made -- the line's head with the workspace's own changes over it, the workspace's text winning -- and
     * the workspace itself is left as it was until the resolution is accepted or discarded. lite's report also names the
     * conflicting files ({@code conflicts}).
     */
    private Map<String, Object> updateWorkspace(String p, String w) {
        String head = workspaceHead(p, w, wsOf(p, w));
        String line = lineHead(p);
        if (head.equals(line) || isAncestor(line, head)) return updateReport("NO_OP", line, head, List.of());
        String base = mergeBase(line, head);
        Map<String, String> baseTree = git.readTree(git.readCommit(base).tree());
        Map<String, String> headTree = git.readTree(git.readCommit(head).tree());
        Merged merged = merge3(baseTree, git.readTree(git.readCommit(line).tree()), headTree);
        if (!merged.conflicts().isEmpty()) {
            Map<String, String> tree = git.readTree(git.readCommit(line).tree());
            Set<String> paths = new java.util.TreeSet<>(baseTree.keySet());
            paths.addAll(headTree.keySet());
            for (String path : paths) {
                String b = baseTree.get(path);
                String o = headTree.get(path);
                if (java.util.Objects.equals(b, o)) continue;
                if (o == null) tree.remove(path);
                else tree.put(path, o);
            }
            String resolution = commit(List.of(line), tree, "Conflict resolution of " + wsOf(p, w));
            storage.put(conflictRef(p, w), resolution);
            return updateReport("CONFLICT", line, resolution, merged.conflicts());
        }
        // the workspace's own commits, oldest first (its history is a line from base: each save is one commit on the last)
        List<String> own = new ArrayList<>();
        for (String at = head; !at.equals(base); ) {
            own.add(0, at);
            Git.Commit c = git.readCommit(at);
            if (c.parents().size() != 1) throw new IllegalStateException("workspace commit " + at + " has " + c.parents().size() + " parents");
            at = c.parents().get(0);
        }
        String at = line;
        Map<String, String> tree = git.readTree(git.readCommit(line).tree());
        long now = clock.getAsLong() / 1000;
        for (String id : own) {
            Git.Commit c = git.readCommit(id);
            Map<String, String> before = git.readTree(git.readCommit(c.parents().get(0)).tree());
            Map<String, String> after = git.readTree(c.tree());
            Set<String> changed = new java.util.TreeSet<>(before.keySet());
            changed.addAll(after.keySet());
            for (String path : changed) {
                String b = before.get(path);
                String a = after.get(path);
                if (java.util.Objects.equals(a, b)) continue;
                if (a == null) tree.remove(path);
                else tree.put(path, a);
            }
            at = git.writeCommit(new Git.Commit(git.writeTree(tree), List.of(at), c.author(), c.authorSeconds(), userId, now, c.message()));
        }
        if (!tree.equals(merged.tree())) throw new IllegalStateException("replaying workspace " + w + " of project " + p + " did not give its merge");
        storage.put(workspaceRef(p, w), at);
        return updateReport("UPDATED", line, at, List.of());
    }

    /**
     * Upstream's conflict resolution of a user workspace ({@code …/workspaces/{w}/conflictResolution}, made by a CONFLICT
     * update): read it (GET, and its {@code pure}, {@code entities}, {@code configuration}); whether the line has moved
     * on since ({@code outdated}); discard it (DELETE: the workspace stays as it was); discard the workspace's changes
     * ({@code discardChanges}: the workspace becomes the line's head); or accept it ({@code accept}: the resolution, with
     * the changes given, becomes the workspace). DEPARTURE (lite): accept takes text changes ({@code changes}, as
     * {@code pureChanges} does); upstream's {@code entityChanges} are refused until the model printer exists (S20), as
     * on the workspace's own route.
     */
    private Response conflictResolution(String method, String p, String w, List<String> sub, Map<String, List<String>> q, @Nullable String body) {
        workspaceHead(p, w, wsOf(p, w));
        String desc = "conflict resolution of " + wsOf(p, w);
        String head = storage.get(conflictRef(p, w));
        if (head == null) throw new Refusal("Unknown: " + desc, 404);
        String what = sub.isEmpty() ? "" : sub.get(0);
        if (sub.isEmpty() && method.equals("GET")) return ok(workspaceView(p, w));
        if (sub.isEmpty() && method.equals("DELETE")) {
            storage.delete(conflictRef(p, w));
            return new Response(204, null);
        }
        if (what.equals("outdated") && sub.size() == 1 && method.equals("GET")) {
            String line = lineHead(p);
            return ok(!head.equals(line) && !isAncestor(line, head));
        }
        if (what.equals("discardChanges") && sub.size() == 1 && method.equals("POST")) {
            storage.put(workspaceRef(p, w), lineHead(p));
            storage.delete(conflictRef(p, w));
            return new Response(204, null);
        }
        if (what.equals("accept") && sub.size() == 1 && method.equals("POST")) {
            if (!(parse(body) instanceof Json.Obj cmd)) throw new Refusal("Input required to accept conflict resolution", 400);
            if (cmd.getOr("entityChanges", null) instanceof Json.Arr ec && !ec.items().isEmpty()) throw new Unsupported("ENTITY_CHANGES");
            String message = messageOf(cmd);
            List<PureOp> ops = pureOps(cmd);
            String next = ops.isEmpty() ? null : applyPure(head, ops, message);
            storage.put(workspaceRef(p, w), next == null ? head : next);
            storage.delete(conflictRef(p, w));
            return new Response(204, null);
        }
        if (method.equals("GET") && (what.equals("pure") || what.equals("entities") || what.equals("configuration"))) return readsAt(head, desc, sub, q);
        throw new Refusal("HTTP 404 Not Found", 404);
    }

    private static Map<String, Object> updateReport(String status, String mergeBase, String revision, List<String> conflicts) {
        Map<String, Object> out = map("status", status, "workspaceMergeBaseRevisionId", mergeBase, "workspaceRevisionId", revision);
        if (!conflicts.isEmpty()) out.put("conflicts", conflicts);
        return out;
    }

    // =====================================================================================================
    // versions (contract slice 2 §2): a version is a tag on the project line
    // =====================================================================================================

    private static final Pattern VERSION = Pattern.compile("^(0|[1-9]\\d{0,9})\\.(0|[1-9]\\d{0,9})\\.(0|[1-9]\\d{0,9})$");

    private Response versions(String method, String p, List<String> sub, Map<String, List<String>> q, @Nullable String body) {
        project(p);
        if (sub.isEmpty()) {
            if (method.equals("GET")) {
                List<Object> out = new ArrayList<>();
                for (int[] v : versionsOf(p, q)) out.add(versionView(p, v));
                return ok(out);
            }
            if (method.equals("POST")) return ok(createVersion(p, parse(body)));
            throw new Refusal("HTTP 405 Method Not Allowed", 405);
        }
        if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
        if (sub.get(0).equals("latest") && sub.size() == 1) {
            List<int[]> all = versionsOf(p, q);
            return all.isEmpty() ? new Response(204, null) : ok(versionView(p, all.get(0)));
        }
        String text = sub.get(0);
        if (!VERSION.matcher(text).matches()) throw new Refusal("Invalid version string: \"" + text + "\"", 400);
        String commit = storage.get(versionRef(p, text));
        if (commit == null) throw new Refusal("Version " + text + " is unknown for project " + p, 404);
        if (sub.size() == 1) return ok(versionView(p, parseVersion(text)));
        // DEPARTURE: upstream writes "version <v>project <p>" in these messages (slice 2 quirk 14)
        return readsAt(commit, "version " + text + " of project " + p, sub.subList(1, sub.size()), q);
    }

    private String versionRef(String p, String v) {
        if (!VERSION.matcher(v).matches()) throw new Refusal("Invalid version string: \"" + v + "\"", 400);
        return "ref/" + checked(p) + "/version/" + v;
    }

    private String noteKey(String p, String v) {
        if (!VERSION.matcher(v).matches()) throw new Refusal("Invalid version string: \"" + v + "\"", 400);
        return "note/" + checked(p) + "/" + v;
    }

    /** The commit a version names, or null when the project or version is unknown -- or not an id at all (a dependency's). */
    private @Nullable String versionCommit(String p, String v) {
        return isProjectId(p) && VERSION.matcher(v).matches() ? storage.get(versionRef(p, v)) : null;
    }

    private static int[] parseVersion(String v) {
        String[] parts = v.split("\\.");
        return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2])};
    }

    private static String versionText(int[] v) {
        return v[0] + "." + v[1] + "." + v[2];
    }

    /** A project's versions, newest first, filtered component by component as upstream does (slice 2 §2.4). */
    private List<int[]> versionsOf(String p, Map<String, List<String>> q) {
        String prefix = "ref/" + checked(p) + "/version/";
        List<int[]> out = new ArrayList<>();
        String[][] bounds = {{"major", "minMajor", "maxMajor"}, {"minor", "minMinor", "maxMinor"}, {"patch", "minPatch", "maxPatch"}};
        for (String key : storage.keys(prefix)) {
            String text = key.substring(prefix.length());
            if (!VERSION.matcher(text).matches()) continue;
            int[] v = parseVersion(text);
            boolean keep = true;
            for (int i = 0; i < 3; i++) {
                Integer exact = intParam(q, bounds[i][0]);
                Integer min = intParam(q, bounds[i][1]);
                Integer max = intParam(q, bounds[i][2]);
                if (exact != null) keep &= v[i] == exact;
                else keep &= (min == null || v[i] >= min) && (max == null || v[i] <= max);
            }
            if (keep) out.add(v);
        }
        out.sort((a, b) -> a[0] != b[0] ? Integer.compare(b[0], a[0]) : a[1] != b[1] ? Integer.compare(b[1], a[1]) : Integer.compare(b[2], a[2]));
        return out;
    }

    private Map<String, Object> versionView(String p, int[] v) {
        String text = versionText(v);
        String notes = storage.get(noteKey(p, text));
        return map("id", map("majorVersion", v[0], "minorVersion", v[1], "patchVersion", v[2]),
                "projectId", p, "revisionId", String.valueOf(storage.get(versionRef(p, text))),
                "notes", notes == null ? Json.nil() : notes);
    }

    /**
     * Cuts a version (slice 2 §2.3): the next number after the latest, on the project line's head (or the
     * revision given, which must be on the line), then -- lite's gate, design S7 -- only if that revision
     * compiles with its dependencies.
     */
    private Map<String, Object> createVersion(String p, Json.@Nullable Node body) {
        if (!(body instanceof Json.Obj cmd)) throw new Refusal("Input required to create version", 400);
        String line = lineHead(p);
        if ("EMBEDDED".equals(config(line).getStringOr("projectType", null))) {
            throw new Refusal("Creating a version of a project of type EMBEDDED is not allowed", 409);
        }
        String type = string(cmd, "versionType");
        // DEPARTURE: upstream answers a missing versionType with a 500 NPE (slice 2 quirk 12)
        if (type == null) throw new Refusal("type may not be null", 400);
        type = type.toUpperCase(Locale.ROOT);
        if (!List.of("MAJOR", "MINOR", "PATCH").contains(type)) throw new Refusal("Unable to process JSON", 400, "versionType must be one of MAJOR, MINOR, PATCH");
        List<int[]> all = versionsOf(p, Map.of());
        int[] latest = all.isEmpty() ? new int[] {0, 0, 0} : all.get(0);
        int[] next = switch (type) {
            case "MAJOR" -> new int[] {latest[0] + 1, 0, 0};
            case "MINOR" -> new int[] {latest[0], latest[1] + 1, 0};
            default -> new int[] {latest[0], latest[1], latest[2] + 1};
        };
        String v = versionText(next);
        String revision = string(cmd, "revisionId");
        String commit = revision == null ? line : revision;
        if (revision != null && (!COMMIT_ID.matcher(revision).matches() || !git.isCommit(revision) || !isAncestor(revision, line))) {
            throw new Refusal("Revision " + revision + " is unknown in project " + p, 400);
        }
        Json.Obj config = config(commit);
        List<String> errors = grammar.compile(model(git.readTree(git.readCommit(commit).tree()), config));
        if (!errors.isEmpty()) {
            throw new Refusal("Version " + v + " of project " + p + " does not compile: " + String.join("; ", errors), 409);
        }
        storage.put(versionRef(p, v), commit);
        String notes = string(cmd, "notes");
        if (notes != null) storage.put(noteKey(p, v), notes);
        return versionView(p, next);
    }

    // =====================================================================================================
    // the project's configuration, and the model a gate compiles
    // =====================================================================================================

    private static List<Json.Node> dependencies(Json.Obj config) {
        Json.Arr deps = config.getArrOr("projectDependencies", null);
        return deps == null ? List.of() : deps.items();
    }

    /**
     * {@code POST …/workspaces/{w}/configuration} ({@code UpdateProjectConfigurationCommand}): the project
     * dependencies added and removed, as one revision of {@code project.json}. Only the dependencies are
     * served here (Studio's project configuration editor); the other fields are kept.
     */
    private Response updateConfiguration(String p, String w, Json.@Nullable Node body) {
        if (!(body instanceof Json.Obj cmd)) throw new Refusal("Input required to update project configuration", 400);
        String message = string(cmd, "message");
        if (message == null) throw new Refusal("message may not be null", 400);
        String head = workspaceHead(p, w, wsIn(p, w));
        Json.Obj config = config(head);
        List<Json.Node> deps = new ArrayList<>(dependencies(config));
        Json.Arr remove = cmd.getArrOr("projectDependenciesToRemove", null);
        if (remove != null) for (Json.Node r : remove.items()) {
            String pid = ((Json.Obj) r).getStringOr("projectId", null);
            deps.removeIf(d -> java.util.Objects.equals(((Json.Obj) d).getStringOr("projectId", null), pid));
        }
        Json.Arr add = cmd.getArrOr("projectDependenciesToAdd", null);
        if (add != null) for (Json.Node a : add.items()) {
            Json.Obj dep = (Json.Obj) a;
            String pid = dep.getStringOr("projectId", null);
            String vid = dep.getStringOr("versionId", null);
            if (pid == null || vid == null) throw new Refusal("Invalid project dependency: " + Json.toCompact(dep), 400);
            deps.removeIf(d -> pid.equals(((Json.Obj) d).getStringOr("projectId", null)));
            Map<String, Object> clean = new TreeMap<>();
            clean.put("projectId", pid);
            clean.put("versionId", vid);
            deps.add(Json.of(clean));
        }
        deps.sort((x, y) -> String.valueOf(((Json.Obj) x).getStringOr("projectId", "")).compareTo(String.valueOf(((Json.Obj) y).getStringOr("projectId", ""))));
        Map<String, Object> next = new TreeMap<>(config.fields());
        next.put("projectDependencies", deps);
        Map<String, String> tree = new TreeMap<>(git.readTree(git.readCommit(head).tree()));
        tree.put("project.json", git.writeBlob(Json.toPretty(next)));
        String commit = commit(List.of(head), tree, message);
        storage.put(workspaceRef(p, w), commit);
        return ok(revisionView(commit));
    }

    /**
     * The model a gate compiles: the dependencies' files (their released versions, transitively,
     * nearest wins as Depot resolves them), then the tree's own, each file in its own Pure section.
     */
    private String model(Map<String, String> tree, Json.Obj config) {
        StringBuilder text = new StringBuilder();
        for (String commit : dependencyCommits(config)) appendFiles(text, git.readTree(git.readCommit(commit).tree()));
        appendFiles(text, tree);
        return text.toString();
    }

    private void appendFiles(StringBuilder text, Map<String, String> tree) {
        for (Map.Entry<String, String> f : tree.entrySet()) {
            if (!f.getKey().endsWith(".pure")) continue;
            text.append("###Pure\n").append(git.readBlob(f.getValue())).append('\n');
        }
    }

    /**
     * The commits of a configuration's dependencies, transitively, as Depot resolves them (its
     * {@link Resolver}: nearest wins), so a gate compiles against exactly the closure Depot serves.
     */
    private List<String> dependencyCommits(Json.Obj config) {
        List<String> commits = new ArrayList<>();
        for (ArtifactSource.Dependency d : Resolver.closure(declaredOf(config), this::declared)) {
            String commit = versionCommit(d.key(), d.versionId());
            if (commit == null) {
                throw new Refusal("Unknown dependency: version " + d.versionId() + " of project " + d.key(), 409);
            }
            commits.add(commit);
        }
        return commits;
    }

    private @Nullable List<ArtifactSource.Dependency> declared(ArtifactSource.Dependency d) {
        String commit = versionCommit(d.key(), d.versionId());
        return commit == null ? null : declaredOf(config(commit));
    }

    /** {@code project.json}'s dependencies as Depot reads them ({@code projectId} is {@code group:artifact}). */
    private static List<ArtifactSource.Dependency> declaredOf(Json.Obj config) {
        List<ArtifactSource.Dependency> out = new ArrayList<>();
        for (Json.Node n : dependencies(config)) {
            Json.Obj dep = (Json.Obj) n;
            String pid = String.valueOf(dep.getStringOr("projectId", ""));
            int colon = pid.indexOf(':');
            List<String> exclusions = new ArrayList<>();
            Json.Arr ex = dep.getArrOr("exclusions", null);
            if (ex != null) for (Json.Node e : ex.items()) exclusions.add(String.valueOf(((Json.Obj) e).getStringOr("projectId", "")));
            out.add(new ArtifactSource.Dependency(colon < 0 ? pid : pid.substring(0, colon), colon < 0 ? "" : pid.substring(colon + 1),
                    String.valueOf(dep.getStringOr("versionId", "")), exclusions));
        }
        return out;
    }

    /**
     * SDLC-lite's versions as Depot's {@link ArtifactSource} (design S22's first source): a project's
     * releases are its version tags, its snapshot its project line, each read from its commit.
     */
    public ArtifactSource artifacts() {
        return new ArtifactSource() {
            @Override
            public List<Project> projects() {
                List<Project> out = new ArrayList<>();
                for (String key : storage.keys("project/")) {
                    String id = key.substring(8);
                    int colon = id.indexOf(':');
                    out.add(new Project(id.substring(0, colon), id.substring(colon + 1), id));
                }
                return out;
            }

            @Override
            public List<String> versions(String groupId, String artifactId) {
                List<String> out = new ArrayList<>();
                if (!isProjectId(groupId + ":" + artifactId)) return out;
                for (int[] v : versionsOf(groupId + ":" + artifactId, Map.of())) out.add(versionText(v));
                return out;
            }

            @Override
            public boolean hasSnapshot(String groupId, String artifactId) {
                return isProjectId(groupId + ":" + artifactId) && storage.get(lineRef(groupId + ":" + artifactId)) != null;
            }

            @Override
            public @Nullable Release release(String groupId, String artifactId, String versionId) {
                String p = groupId + ":" + artifactId;
                if (!isProjectId(p)) return null;
                String commit = versionId.equals(Depot.SNAPSHOT) ? storage.get(lineRef(p)) : versionCommit(p, versionId);
                if (commit == null) return null;
                Map<String, String> files = entityFiles(git.readTree(git.readCommit(commit).tree()));
                List<Object> texts = new ArrayList<>();
                for (Map.Entry<String, String> f : files.entrySet()) texts.add(map("path", f.getKey(), "pureCode", git.readBlob(f.getValue())));
                return new Release(declaredOf(config(commit)), Json.toCompact(entities(files, false)), Json.toCompact(texts));
            }
        };
    }

    // =====================================================================================================
    // text and entities
    // =====================================================================================================

    /** A tree's element files, keyed by entity path (`a/b/C.pure` → `a::b::C`), in path order. */
    private static Map<String, String> entityFiles(Map<String, String> tree) {
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, String> f : tree.entrySet()) {
            if (f.getKey().endsWith(".pure")) {
                out.put(f.getKey().substring(0, f.getKey().length() - 5).replace("/", "::"), f.getValue());
            }
        }
        return out;
    }

    /** Where an element's file sits in a revision's tree: {@code <package path>/<Name>.pure} (design S5). */
    private static String filePathOf(String entityPath) {
        return entityPath.replace("::", "/") + ".pure";
    }

    /** What reading a file's text gave: the entity, or the refusals. */
    private record Read(Json.@Nullable Obj entity, List<String> errors) {}

    /**
     * A file's text read by the grammar: exactly one element (and at most one section index, with no
     * imports), at {@code path}, of a type an SDLC stores. legend-sdlc's own rules for a {@code .pure}
     * file (PureEntitySerializer.java:165-260), then lite's: the element must be the one the file is named for.
     */
    private Read read(String path, String text) {
        List<Json.Node> elements;
        try {
            elements = Json.parseObject(grammar.modelJson(text)).getArr("elements").items();
        } catch (RuntimeException e) {
            return new Read(null, List.of(e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
        }
        List<Json.Obj> sections = new ArrayList<>();
        List<Json.Obj> others = new ArrayList<>();
        for (Json.Node n : elements) {
            Json.Obj e = (Json.Obj) n;
            if ("sectionIndex".equals(e.getStringOr("_type", null))) sections.add(e);
            else others.add(e);
        }
        if (sections.size() > 1) return new Read(null, List.of("Expected at most one SectionIndex, found " + sections.size()));
        if (others.isEmpty()) return new Read(null, List.of("No element found"));
        if (others.size() > 1) return new Read(null, List.of("Expected one element, found " + others.size()));
        for (Json.Obj s : sections) {
            Json.Arr secs = s.getArrOr("sections", null);
            if (secs == null) continue;
            for (Json.Node sec : secs.items()) {
                Json.Arr imports = ((Json.Obj) sec).getArrOr("imports", null);
                if (imports != null && !imports.items().isEmpty()) {
                    return new Read(null, List.of("Imports in Pure files are not currently supported"));
                }
            }
        }
        Json.Obj element = others.get(0);
        String found = element.getStringOr("package", null) + "::" + element.getStringOr("name", null);
        List<String> errors = new ArrayList<>();
        if (!found.equals(path)) errors.add("Mismatch between entity path (\"" + path + "\") and the element's path (\"" + found + "\")");
        String type = String.valueOf(element.getStringOr("_type", null));
        String classifierPath = Classifiers.of(type);
        if (classifierPath == null) errors.add("Unsupported element type: " + type);
        if (!errors.isEmpty()) return new Read(null, errors);
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("path", Json.str(path));
        fields.put("classifierPath", Json.str(String.valueOf(classifierPath)));
        fields.put("content", element);
        return new Read(new Json.Obj(fields), errors);
    }

    private Json.Obj entityOf(String path, String blob) {
        Json.Obj cached = derived.get(blob);
        if (cached != null) return cached;
        Read r = read(path, git.readBlob(blob));
        if (r.entity() == null) {
            throw new Refusal("Error deserializing entity \"" + path + "\" from file \"" + filePathOf(path) + "\": "
                    + String.join("; ", r.errors()), 500);
        }
        derived.put(blob, r.entity());
        return r.entity();
    }

    /** Every entity of a revision, by path (DEPARTURE: a stable order; upstream's is hash order, quirk 14). */
    private List<Json.Obj> entities(Map<String, String> files, boolean excludeInvalid) {
        List<Json.Obj> out = new ArrayList<>();
        for (Map.Entry<String, String> f : files.entrySet()) {
            try {
                out.add(entityOf(f.getKey(), f.getValue()));
            } catch (Refusal e) {
                if (!excludeInvalid) throw e;
            }
        }
        return out;
    }

    // ---- entity filters (contract §5, EntityAccessResource.java:40-246) ----

    private static Pattern regex(String source) {
        try {
            return Pattern.compile(source, Pattern.CASE_INSENSITIVE);
        } catch (RuntimeException e) {
            return Pattern.compile(Pattern.quote(source), Pattern.CASE_INSENSITIVE);
        }
    }

    private static List<Json.Obj> filtered(List<Json.Obj> entities, Map<String, List<String>> q) {
        List<String> classifiers = q.getOrDefault("classifierPath", List.of());
        List<String> packages = q.getOrDefault("package", List.of());
        boolean subPackages = !first(q, "includeSubPackages", "true").equals("false");
        String name = firstOrNull(q, "name");
        Pattern nameRegex = name == null ? null : regex(name);
        Set<String> stereotypes = new HashSet<>(q.getOrDefault("stereotype", List.of()));
        Map<String, List<Pattern>> tagged = new LinkedHashMap<>();
        for (String tv : q.getOrDefault("taggedValue", List.of())) {
            int slash = tv.indexOf('/');
            String tag = (slash < 0 ? tv : tv.substring(0, slash)).trim();
            String rx = slash < 0 ? "" : tv.substring(slash + 1).trim();
            tagged.computeIfAbsent(tag, k -> new ArrayList<>()).add(regex(rx));
        }
        List<Json.Obj> out = new ArrayList<>();
        for (Json.Obj e : entities) {
            String path = e.getString("path");
            int cut = path.lastIndexOf("::");
            String pkg = path.substring(0, cut);
            String simple = path.substring(cut + 2);
            if (!packages.isEmpty() && packages.stream().noneMatch(p -> pkg.equals(p) || (subPackages && pkg.startsWith(p + "::")))) continue;
            if (nameRegex != null && !nameRegex.matcher(simple).find()) continue;
            if (!classifiers.isEmpty() && !classifiers.contains(e.getString("classifierPath"))) continue;
            Json.Obj content = e.getObj("content");
            if (!stereotypes.isEmpty()) {
                boolean hit = false;
                Json.Arr own = content.getArrOr("stereotypes", null);
                if (own != null) for (Json.Node s : own.items()) {
                    Json.Obj so = (Json.Obj) s;
                    if (stereotypes.contains(so.getStringOr("profile", null) + "." + so.getStringOr("value", null))) hit = true;
                }
                if (!hit) continue;
            }
            if (!tagged.isEmpty()) {
                boolean hit = false;
                Json.Arr own = content.getArrOr("taggedValues", null);
                if (own != null) for (Json.Node t : own.items()) {
                    Json.Obj to = (Json.Obj) t;
                    Json.Obj tagObj = to.getObjOr("tag", null);
                    String tag = tagObj == null ? "" : tagObj.getStringOr("profile", null) + "." + tagObj.getStringOr("value", null);
                    String value = to.getStringOr("value", null);
                    if (value == null) continue;
                    for (Pattern rx : tagged.getOrDefault(tag, List.of())) if (rx.matcher(value).find()) hit = true;
                }
                if (!hit) continue;
            }
            out.add(e);
        }
        return out;
    }

    // ---- saving text ----

    private static String shown(Json.@Nullable Node n) {
        if (n == null || n instanceof Json.Null) return "null";
        if (n instanceof Json.Str s) return s.value();
        return Json.toCompact(n);
    }

    /**
     * {@code POST …/pureChanges} (S15): legend-sdlc's {@code entityChanges} rules (contract §6) over text.
     * Every change is checked -- shape, then its text read by the grammar -- and all errors answered as one
     * 400 in upstream's layout; none is a 204; then the lock; then the operations, in upstream's words.
     * DEPARTURES: the stale-revision 409 comes before the operations (upstream checks the operations
     * against the stale state first, so a stale save may answer 500, quirk 8); two changes to one path are
     * refused (quirk 10).
     */
    private Response pureChanges(String p, String w, Json.@Nullable Node body) {
        if (!(body instanceof Json.Obj cmd)) throw new Refusal("Input required to perform entity changes", 400);
        // upstream's order of refusals: the changes' shape, the message, then each change
        if (!(cmd.getOr("changes", Json.arr()) instanceof Json.Arr)) throw new Refusal("Unable to process JSON", 400, "changes must be an array");
        String message = messageOf(cmd);
        List<PureOp> parsed = pureOps(cmd);
        if (parsed.isEmpty()) return new Response(204, null);

        String head = workspaceHead(p, w, wsIn(p, w));
        Json.Node revisionId = cmd.getOr("revisionId", null);
        if (revisionId != null && !shown(revisionId).equals(head)) {
            String r = shown(revisionId);
            throw new Refusal("Expected revision " + r + " of " + wsOf(p, w) + " to be at revision " + r
                    + "; instead it was at revision " + head, 409);
        }
        String next = applyPure(head, parsed, message);
        if (next == null) return new Response(204, null);
        storage.put(workspaceRef(p, w), next);
        return ok(revisionView(next));
    }

    /** One text change of a pure-changes command, read and checked. */
    private record PureOp(String type, String path, @Nullable String text) {}

    private static String messageOf(Json.Obj cmd) {
        if (!(cmd.getOr("message", null) instanceof Json.Str messageNode)) throw new Refusal("message may not be null", 400);
        return messageNode.value();
    }

    /** A command's text changes, each read for its one element; every problem refused at once, in upstream's layout. */
    private List<PureOp> pureOps(Json.Obj cmd) {
        Json.Node changesNode = cmd.getOr("changes", Json.arr());
        if (!(changesNode instanceof Json.Arr changes)) throw new Refusal("Unable to process JSON", 400, "changes must be an array");
        List<String> problems = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        List<PureOp> parsed = new ArrayList<>();
        int i = 0;
        for (Json.Node raw : changes.items()) {
            i++;
            List<String> errors = new ArrayList<>();
            String header;
            if (!(raw instanceof Json.Obj change)) {
                header = "(null)";
                errors.add("Invalid entity change: " + shown(raw));
            } else {
                Json.Node type = change.getOr("type", null);
                Json.Node path = change.getOr("path", null);
                Json.Node code = change.getOr("pureCode", null);
                header = "(<PureChange type=" + shown(type) + " path=" + shown(path) + ">)";
                String t = type instanceof Json.Str s ? s.value() : null;
                if (type == null) errors.add("Missing entity change type");
                else if (!"CREATE".equals(t) && !"MODIFY".equals(t) && !"DELETE".equals(t)) errors.add("Invalid entity change type: " + shown(type));
                if (path == null) errors.add("Missing entity path");
                else if (!(path instanceof Json.Str ps) || !ENTITY_PATH.matcher(ps.value()).matches()) errors.add("Invalid entity path: " + shown(path));
                else if (!seen.add(ps.value())) errors.add("Duplicate entity path: " + ps.value());
                if ("DELETE".equals(t) && code != null) errors.add("Unexpected Pure code");
                if (("CREATE".equals(t) || "MODIFY".equals(t)) && !(code instanceof Json.Str)) errors.add("Missing Pure code");
                String text = code instanceof Json.Str cs ? cs.value() : null;
                if (errors.isEmpty() && text != null && !"DELETE".equals(t)) errors.addAll(read(shown(path), text).errors());
                if (errors.isEmpty()) parsed.add(new PureOp(String.valueOf(t), shown(path), text));
            }
            if (!errors.isEmpty()) {
                StringBuilder sb = new StringBuilder("\tEntity change #").append(i).append(' ').append(header).append(':');
                for (String e : errors) sb.append("\n\t\t").append(e);
                problems.add(sb.toString());
            }
        }
        if (!problems.isEmpty()) throw new Refusal("There are entity change errors:\n" + String.join("\n", problems), 400);
        return parsed;
    }

    /** The changes as one commit on {@code head}, with upstream's operation rules; null when they change nothing. */
    private @Nullable String applyPure(String head, List<PureOp> parsed, String message) {
        Map<String, String> tree = new TreeMap<>(git.readTree(git.readCommit(head).tree()));
        boolean changed = false;
        for (PureOp change : parsed) {
            String operation = "<PureChange type=" + change.type() + " path=" + change.path() + ">";
            String file = filePathOf(change.path());
            boolean exists = tree.containsKey(file);
            if (change.type().equals("CREATE") && exists) {
                throw new Refusal("Unable to handle operation " + operation + ": entity \"" + change.path() + "\" already exists", 500);
            }
            if (!change.type().equals("CREATE") && !exists) {
                throw new Refusal("Unable to handle operation " + operation + ": could not find entity \"" + change.path() + "\"", 500);
            }
            if (change.type().equals("DELETE")) {
                tree.remove(file);
                changed = true;
                continue;
            }
            String blob = git.writeBlob(String.valueOf(change.text()));
            if (blob.equals(tree.get(file))) continue; // the same text: a no-op, as upstream's same bytes
            tree.put(file, blob);
            changed = true;
        }
        return changed ? commit(List.of(head), tree, message) : null;
    }

    // =====================================================================================================
    // plumbing
    // =====================================================================================================

    private static boolean isJavaName(String s) {
        for (String part : s.split("\\.", -1)) {
            if (!JAVA_IDENTIFIER.matcher(part).matches() || JAVA_KEYWORDS.contains(part)) return false;
        }
        return true;
    }

    private static @Nullable String string(Json.Obj o, String field) {
        return o.getOr(field, null) instanceof Json.Str s ? s.value() : null;
    }

    private static Json.@Nullable Node parse(@Nullable String body) {
        if (body == null || body.isEmpty()) return null;
        try {
            return Json.parse(body);
        } catch (RuntimeException e) {
            throw new Refusal("Unable to process JSON", 400, e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }

    private static Response ok(Object body) {
        return new Response(200, Json.toCompact(body));
    }

    /** An {@code ExtendedErrorMessage}: code, message, [details], timestamp; nulls omitted. */
    private Response error(int status, String message, @Nullable String details) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", status);
        out.put("message", message);
        if (details != null) out.put("details", details);
        out.put("timestamp", Instant.ofEpochMilli(clock.getAsLong()).toString());
        return new Response(status, Json.toCompact(out));
    }

    /** A map in the order given: key, value, key, value… */
    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) out.put((String) kv[i], kv[i + 1]);
        return out;
    }

    /** An integer query parameter; one that is not an integer is JAX-RS's 404 (contract §0.2). */
    private static @Nullable Integer intParam(Map<String, List<String>> q, String key) {
        String text = firstOrNull(q, key);
        if (text == null) return null;
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new Refusal("HTTP 404 Not Found", 404);
        }
    }

    /** An instant query parameter (ISO-8601, `Z` or an offset); one that does not parse is upstream's 500 (slice 2 §0.1). */
    private static @Nullable Instant instantParam(Map<String, List<String>> q, String key) {
        String text = firstOrNull(q, key);
        if (text == null || text.isEmpty()) return null;
        try {
            return java.time.OffsetDateTime.parse(text).toInstant();
        } catch (RuntimeException e) {
            throw new Refusal("Could not convert \"" + text + "\": Could not parse \"" + text + "\"", 500);
        }
    }

    /** A record's field: its JSON node's value for a string, or the node. */
    private static @Nullable Object field(Object record, String key) {
        Object v = record instanceof Map<?, ?> m ? m.get(key) : record instanceof Json.Obj o ? o.fields().get(key) : null;
        if (v instanceof Json.Str s) return s.value();
        if (v instanceof Json.Null) return null;
        return v;
    }

    /** A record's list field, whether written as JSON or as Java. */
    private static List<?> listOf(@Nullable Object v) {
        if (v instanceof Json.Arr a) return a.items();
        if (v instanceof List<?> l) return l;
        return List.of();
    }

    private static @Nullable String firstOrNull(Map<String, List<String>> q, String key) {
        List<String> v = q.get(key);
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    /** A field's value, a JSON null when absent or null (configuration fields upstream writes as null). */
    private static Json.Node orNull(Json.Obj o, String field) {
        Json.Node n = o.getOr(field, null);
        return n == null ? Json.nil() : n;
    }

    private static String first(Map<String, List<String>> q, String key, String otherwise) {
        List<String> v = q.get(key);
        return v == null || v.isEmpty() ? otherwise : v.get(0);
    }

    private static Map<String, List<String>> query(String text) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (String pair : text.split("&")) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            String k = decode(eq < 0 ? pair : pair.substring(0, eq), true);
            String v = eq < 0 ? "" : decode(pair.substring(eq + 1), true);
            out.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
        }
        return out;
    }

    /** Percent-decoding as UTF-8 ({@code java.net.URLDecoder} is not in the WebAssembly class library). */
    private static String decode(String s, boolean plusIsSpace) {
        if (s.indexOf('%') < 0 && !(plusIsSpace && s.indexOf('+') >= 0)) return s;
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        int i = 0;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (ch == '%' && i + 2 < s.length()) {
                String hex = s.substring(i + 1, i + 3);
                if (!hex.matches("[0-9A-Fa-f]{2}")) throw new Refusal("HTTP 400 Bad Request", 400);
                bytes.write(Integer.parseInt(hex, 16));
                i += 3;
            } else if (ch == '+' && plusIsSpace) {
                bytes.write(' ');
                i++;
            } else {
                int cp = s.codePointAt(i);
                byte[] b = new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8);
                bytes.write(b, 0, b.length);
                i += Character.charCount(cp);
            }
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }
}
