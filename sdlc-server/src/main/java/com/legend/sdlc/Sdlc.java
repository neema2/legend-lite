package com.legend.sdlc;

import com.legend.base.Nullable;
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
 * (project-store/test/conformance.ts) holds both. The rules, statuses and messages are legend-sdlc's
 * GitLab backend's (studio/docs/SDLC_CONTRACT_SLICE1.md); departures are lite's, listed in
 * project-store/README.md and marked DEPARTURE here.
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

    /** Pure text to its protocol JSON ({@code {"_type":"data","elements":[...]}}), or a refusal (a RuntimeException). */
    public interface Grammar {
        String modelJson(String text);
    }

    /** The project structure every project here declares (upstream's latest; the layout itself is lite's, S5). */
    private static final int STRUCTURE_VERSION = 13;
    private static final String LINE = "master";

    private static final Pattern ENTITY_PATH = Pattern.compile("^(?!meta::)[A-Za-z0-9_]+(::[A-Za-z0-9_]+)*::[A-Za-z0-9_$]+$");
    private static final Pattern WORKSPACE_ID = Pattern.compile("^([A-Za-z0-9_]([A-Za-z0-9_-]|\\.(?!\\.))*[A-Za-z0-9_]|[A-Za-z0-9_])$");
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
        List<String> rest = parts.subList(2, parts.size());
        if (rest.isEmpty()) {
            if (!method.equals("GET")) throw new Refusal("HTTP 405 Method Not Allowed", 405);
            return ok(projectView(project(p)));
        }
        String c = rest.get(0);
        if (c.equals("reviews")) throw new Unsupported("REVIEWS");
        if (c.equals("versions")) throw new Unsupported("VERSIONS");
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
                        // upstream: deleting what is not there succeeds (contract quirk 3)
                        storage.delete(workspaceRef(p, w));
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
            if (sub.size() == 1 && method.equals("GET") && sub.get(0).equals("inConflictResolutionMode")) {
                workspaceHead(p, w, wsOf(p, w));
                return ok(Boolean.FALSE);
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("pureChanges")) {
                return pureChanges(p, w, parse(body));
            }
            if (sub.size() == 1 && method.equals("POST") && sub.get(0).equals("entityChanges")) {
                // DEPARTURE (S20): a JSON save is printed to text first (S5), and the model printer is not built yet
                workspaceHead(p, w, wsOf(p, w));
                throw new Unsupported("ENTITY_CHANGES");
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
            if (at.size() == 1) throw new Refusal("HTTP 404 Not Found", 404); // revision history: not in this slice
            revision = at.get(1);
            named = true;
            at = at.subList(2, at.size());
            if (at.isEmpty()) return ok(revisionView(resolve(p, w, revision)));
        }
        String id = resolve(p, w, revision);
        String desc = (named ? "revision " + revision + " of " : "") + (w == null ? "project " + p : wsOf(p, w));
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

    private String lineRef(String p) {
        return "ref/" + p + "/" + LINE;
    }

    private String workspaceRef(String p, String w) {
        return "ref/" + p + "/workspace/" + userId + "/" + w;
    }

    private Json.Obj project(String p) {
        String record = storage.get("project/" + p);
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
        String prefix = "ref/" + p + "/workspace/" + userId + "/";
        List<String> out = new ArrayList<>();
        for (String k : storage.keys(prefix)) out.add(k.substring(prefix.length()));
        return out;
    }

    private boolean isAncestor(String ancestor, String of) {
        for (String at = of; at != null; at = git.readCommit(at).parent()) {
            if (at.equals(ancestor)) return true;
        }
        return false;
    }

    /** The first commit of a history (upstream: a project line's BASE is its oldest commit). */
    private String root(String head) {
        String at = head;
        for (String parent = git.readCommit(at).parent(); parent != null; parent = git.readCommit(at).parent()) at = parent;
        return at;
    }

    /** Where a workspace was made from: the newest of its commits the project line also has (git's merge base). */
    private String mergeBase(String line, String workspace) {
        Set<String> onLine = new HashSet<>();
        for (String at = line; at != null; at = git.readCommit(at).parent()) onLine.add(at);
        for (String at = workspace; at != null; at = git.readCommit(at).parent()) {
            if (onLine.contains(at)) return at;
        }
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
        if (!git.isCommit(r) || !isAncestor(r, head)) throw new Refusal("Revision " + r + " is unknown for " + desc, 404);
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
        if (storage.get("project/" + projectId) != null) {
            throw new Refusal("Failed to create project: " + name + ": a project with coordinates " + projectId + " already exists", 409);
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
        String first = commit(null, files, "Build project structure");

        Map<String, Object> record = map("projectId", projectId, "name", name, "description", description, "tags", tags);
        storage.put("project/" + projectId, Json.toCompact(record));
        storage.put(lineRef(projectId), first);
        return projectView(Json.parseObject(Json.toCompact(record)));
    }

    private Map<String, Object> createWorkspace(String p, String w) {
        if (!WORKSPACE_ID.matcher(w).matches()) {
            throw new Refusal("Invalid workspace id: \"" + w + "\". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain \"..\" and may not start or end with '.' or '-'.", 400);
        }
        String line = lineHead(p);
        String existing = storage.get(workspaceRef(p, w));
        // upstream: already there AT the line's head is a no-op; elsewhere, GitLab's "Branch already exists" as a 500
        if (existing != null && !existing.equals(line)) {
            throw new Refusal("Error creating " + wsOf(p, w) + ": Branch already exists", 500);
        }
        if (existing == null) storage.put(workspaceRef(p, w), line);
        return workspaceView(p, w);
    }

    /** A commit of {@code files} (all of the tree: path → blob) on {@code parent}; its id. */
    private String commit(@Nullable String parent, Map<String, String> files, String message) {
        long now = clock.getAsLong() / 1000;
        return git.writeCommit(new Git.Commit(git.writeTree(files), parent, userId, now, userId, now, message));
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
        Json.Node changesNode = cmd.getOr("changes", Json.arr());
        if (!(changesNode instanceof Json.Arr changes)) throw new Refusal("Unable to process JSON", 400, "changes must be an array");
        if (!(cmd.getOr("message", null) instanceof Json.Str messageNode)) throw new Refusal("message may not be null", 400);
        String message = messageNode.value();

        List<String> problems = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        record Change(String type, String path, @Nullable String text) {}
        List<Change> parsed = new ArrayList<>();
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
                if (errors.isEmpty()) parsed.add(new Change(String.valueOf(t), shown(path), text));
            }
            if (!errors.isEmpty()) {
                StringBuilder sb = new StringBuilder("\tEntity change #").append(i).append(' ').append(header).append(':');
                for (String e : errors) sb.append("\n\t\t").append(e);
                problems.add(sb.toString());
            }
        }
        if (!problems.isEmpty()) throw new Refusal("There are entity change errors:\n" + String.join("\n", problems), 400);
        if (parsed.isEmpty()) return new Response(204, null);

        String head = workspaceHead(p, w, wsIn(p, w));
        Json.Node revisionId = cmd.getOr("revisionId", null);
        if (revisionId != null && !shown(revisionId).equals(head)) {
            String r = shown(revisionId);
            throw new Refusal("Expected revision " + r + " of " + wsOf(p, w) + " to be at revision " + r
                    + "; instead it was at revision " + head, 409);
        }
        Map<String, String> tree = new TreeMap<>(git.readTree(git.readCommit(head).tree()));
        boolean changed = false;
        for (Change change : parsed) {
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
        if (!changed) return new Response(204, null);
        String next = commit(head, tree, message);
        storage.put(workspaceRef(p, w), next);
        return ok(revisionView(next));
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
                bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
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
