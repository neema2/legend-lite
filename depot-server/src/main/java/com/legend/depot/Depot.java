package com.legend.depot;

import com.legend.base.Nullable;
import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * DEPOT-LITE's RULES, written once (design S22): upstream legend-depot's read API -- the routes Studio,
 * Query and DataCube call (studio/docs/DEPOT_CONTRACT.md) -- as one {@link #handle} over an
 * {@link ArtifactSource}. It runs in the model home's server and, compiled to WebAssembly, in the page.
 * Departures (design S8, §4) are marked DEPARTURE: a missing version is 404 (upstream 500), aliases
 * {@code latest}/{@code head} in any case, versions listed in version order; plus lite's text route
 * {@code POST /projects/dependencies/pure} (dependencies' files, for the in-tab compile).
 *
 * TeaVM-safe by rule, as sdlc-server's rules. One caller at a time.
 */
public final class Depot {
    public record Response(int status, @Nullable String body) {}

    public static final String SNAPSHOT = "master-SNAPSHOT";

    private final ArtifactSource source;
    private final LongSupplier clock;

    public Depot(ArtifactSource source, LongSupplier clock) {
        this.source = source;
        this.clock = clock;
    }

    private static final class Refusal extends RuntimeException {
        final int status;

        Refusal(String message, int status) {
            super(message);
            this.status = status;
        }
    }

    /** An {@code Optional.empty()} answered by upstream: 404 with no body. */
    private static final class Empty extends RuntimeException {}

    /** One request: {@code target} is the path and query under the API root, e.g. {@code /projects/g/a/versions}. */
    public Response handle(String method, String target, @Nullable String body) {
        try {
            int q = target.indexOf('?');
            String path = q < 0 ? target : target.substring(0, q);
            Map<String, List<String>> query = q < 0 ? Map.of() : query(target.substring(q + 1));
            List<String> parts = new ArrayList<>();
            for (String s : path.split("/")) if (!s.isEmpty()) parts.add(decode(s, false));
            return route(method, parts, query, body);
        } catch (Empty e) {
            return new Response(404, null);
        } catch (Refusal e) {
            return error(e.status, String.valueOf(e.getMessage()));
        } catch (RuntimeException e) {
            return error(500, e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }

    private Response route(String method, List<String> p, Map<String, List<String>> q, @Nullable String body) {
        int n = p.size();
        String a = n > 0 ? p.get(0) : "";
        if (a.equals("project-configurations") && method.equals("GET")) {
            if (n == 1) {
                List<Object> out = new ArrayList<>();
                for (ArtifactSource.Project project : source.projects()) out.add(projectData(project));
                return ok(out);
            }
            if (n == 3) {
                for (ArtifactSource.Project project : source.projects()) {
                    if (project.groupId().equals(p.get(1)) && project.artifactId().equals(p.get(2))) return ok(projectData(project));
                }
                throw new Empty();
            }
        }
        if (a.equals("versions") && n == 4 && method.equals("GET")) {
            String v = alias(p.get(1), p.get(2), p.get(3));
            ArtifactSource.Release r = v == null ? null : source.release(p.get(1), p.get(2), v);
            if (r == null || v == null) throw new Empty();
            return ok(versionDto(p.get(1), p.get(2), v, r));
        }
        if (a.equals("projects") && n == 2 && method.equals("POST") && p.get(1).equals("dependenciesFromArtifactDependencies")) {
            return ok(dependenciesOf(parseDependencies(body), bool(q, "transitive"), bool(q, "includeOrigin"), false));
        }
        if (a.equals("projects") && n == 3 && method.equals("POST") && p.get(1).equals("dependencies") && p.get(2).equals("pure")) {
            return ok(dependenciesOf(parseDependencies(body), !"false".equals(first(q, "transitive")), true, true));
        }
        if (a.equals("projects") && n >= 4 && p.get(3).equals("versions") && method.equals("GET")) {
            String g = p.get(1);
            String art = p.get(2);
            if (n == 4) {
                // DEPARTURE: in version order (upstream answers Mongo's)
                List<String> versions = new ArrayList<>(source.versions(g, art));
                versions.sort(Resolver::compare);
                if (bool(q, "snapshots") && source.hasSnapshot(g, art)) versions.add(SNAPSHOT);
                return ok(versions);
            }
            String asked = p.get(4);
            String v = resolve(g, art, asked);
            ArtifactSource.Release r = release(g, art, v, asked);
            if (n == 5) return new Response(200, r.entitiesJson());
            String what = p.get(5);
            if (what.equals("pure") && n == 6) return new Response(200, r.filesJson());
            if (what.equals("entities") && n == 7) {
                for (Json.Node e : Json.parse(r.entitiesJson()) instanceof Json.Arr arr ? arr.items() : List.<Json.Node>of()) {
                    if (p.get(6).equals(((Json.Obj) e).getStringOr("path", null))) return ok(e);
                }
                throw new Empty();
            }
            if (what.equals("dependencies") && n == 6) {
                boolean transitive = bool(q, "transitive");
                List<ArtifactSource.Dependency> deps = transitive
                        ? Resolver.closure(r.dependencies(), this::declared)
                        : r.dependencies();
                List<Object> out = new ArrayList<>();
                for (ArtifactSource.Dependency d : deps) out.add(versionEntities(d.groupId(), d.artifactId(), resolve(d.groupId(), d.artifactId(), d.versionId()), false));
                if (bool(q, "includeOrigin")) out.add(versionEntities(g, art, v, false));
                return ok(out);
            }
        }
        throw new Refusal("HTTP 404 Not Found", 404);
    }

    // ---- versions and aliases ----

    /** A version as asked: {@code latest} → the highest release, {@code head} → the snapshot (DEPARTURE: any case); null when none. */
    private @Nullable String alias(String g, String a, String v) {
        String lower = v.toLowerCase(Locale.ROOT);
        if (lower.equals("latest")) {
            List<String> versions = new ArrayList<>(source.versions(g, a));
            versions.sort(Resolver::compare);
            return versions.isEmpty() ? null : versions.get(versions.size() - 1);
        }
        if (lower.equals("head")) return source.hasSnapshot(g, a) ? SNAPSHOT : null;
        return v;
    }

    private String resolve(String g, String a, String v) {
        String resolved = alias(g, a, v);
        if (resolved == null) throw new Refusal("project version not found for " + g + "-" + a + "-" + v, 404);
        return resolved;
    }

    private ArtifactSource.Release release(String g, String a, String v, String asked) {
        ArtifactSource.Release r = source.release(g, a, v);
        // DEPARTURE (design S8): 404, where upstream answers this with a 500
        if (r == null) throw new Refusal("project version not found for " + g + "-" + a + "-" + asked, 404);
        return r;
    }

    private @Nullable List<ArtifactSource.Dependency> declared(ArtifactSource.Dependency d) {
        String v = alias(d.groupId(), d.artifactId(), d.versionId());
        ArtifactSource.Release r = v == null ? null : source.release(d.groupId(), d.artifactId(), v);
        return r == null ? null : r.dependencies();
    }

    // ---- dependencies (contract §1.2) ----

    private List<ArtifactSource.Dependency> parseDependencies(@Nullable String body) {
        if (body == null || body.isEmpty()) throw new Refusal("Unable to process JSON", 400);
        Json.Node parsed;
        try {
            parsed = Json.parse(body);
        } catch (RuntimeException e) {
            throw new Refusal("Unable to process JSON", 400);
        }
        if (!(parsed instanceof Json.Arr arr)) throw new Refusal("Unable to process JSON", 400);
        List<ArtifactSource.Dependency> out = new ArrayList<>();
        for (Json.Node n : arr.items()) {
            Json.Obj o = (Json.Obj) n;
            // `versionId` (Studio's) wins over `version` (the creator's)
            String v = o.getStringOr("versionId", o.getStringOr("version", null));
            if (v == null) throw new Refusal("cannot find project version, versionId cannot be null", 400);
            List<String> exclusions = new ArrayList<>();
            Json.Arr ex = o.getArrOr("exclusions", null);
            if (ex != null) for (Json.Node e : ex.items()) {
                Json.Obj eo = (Json.Obj) e;
                exclusions.add(eo.getStringOr("groupId", "") + ":" + eo.getStringOr("artifactId", ""));
            }
            out.add(new ArtifactSource.Dependency(o.getString("groupId"), o.getString("artifactId"), v, exclusions));
        }
        return out;
    }

    /**
     * Upstream's {@code dependenciesFromArtifactDependencies}: the nearest-wins closure of the requested
     * versions, the roots included; without {@code transitive}, only the roots' direct dependencies; with
     * {@code includeOrigin}, the roots too. {@code files}: lite's text form, each version's files.
     */
    private List<Object> dependenciesOf(List<ArtifactSource.Dependency> asked, boolean transitive, boolean includeOrigin, boolean files) {
        List<ArtifactSource.Dependency> roots = new ArrayList<>();
        for (ArtifactSource.Dependency d : asked) {
            String v = resolve(d.groupId(), d.artifactId(), d.versionId());
            release(d.groupId(), d.artifactId(), v, d.versionId());
            roots.add(new ArtifactSource.Dependency(d.groupId(), d.artifactId(), v, d.exclusions()));
        }
        List<ArtifactSource.Dependency> members = new ArrayList<>(Resolver.closure(roots, this::declared));
        if (!transitive) {
            Set<String> direct = new HashSet<>();
            for (ArtifactSource.Dependency r : roots) {
                List<ArtifactSource.Dependency> declared = declared(r);
                if (declared != null) for (ArtifactSource.Dependency d : declared) direct.add(d.key() + ":" + d.versionId());
            }
            members.removeIf(m -> !direct.contains(m.key() + ":" + m.versionId()));
        }
        if (includeOrigin) {
            for (ArtifactSource.Dependency r : roots) {
                if (members.stream().noneMatch(m -> m.key().equals(r.key()) && m.versionId().equals(r.versionId()))) members.add(r);
            }
        }
        List<Object> out = new ArrayList<>();
        for (ArtifactSource.Dependency m : members) out.add(versionEntities(m.groupId(), m.artifactId(), m.versionId(), files));
        return out;
    }

    /** {@code ProjectVersionEntities} (contract §1.1), or lite's text twin when {@code files}. */
    private Map<String, Object> versionEntities(String g, String a, String v, boolean files) {
        ArtifactSource.Release r = release(g, a, v, v);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("groupId", g);
        out.put("artifactId", a);
        out.put("versionId", v);
        if (files) {
            out.put("files", Json.parse(r.filesJson()));
        } else {
            out.put("versionedEntity", Boolean.FALSE);
            out.put("entities", Json.parse(r.entitiesJson()));
        }
        return out;
    }

    // ---- shapes (contract §3, §4) ----

    private Map<String, Object> projectData(ArtifactSource.Project p) {
        List<String> versions = new ArrayList<>(source.versions(p.groupId(), p.artifactId()));
        versions.sort(Resolver::compare);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("groupId", p.groupId());
        out.put("artifactId", p.artifactId());
        out.put("defaultBranch", null);
        out.put("projectId", p.projectId());
        out.put("latestVersion", versions.isEmpty() ? null : versions.get(versions.size() - 1));
        return out;
    }

    private static Map<String, Object> versionDto(String g, String a, String v, ArtifactSource.Release r) {
        List<Object> deps = new ArrayList<>();
        for (ArtifactSource.Dependency d : r.dependencies()) {
            Map<String, Object> dep = new LinkedHashMap<>();
            dep.put("groupId", d.groupId());
            dep.put("artifactId", d.artifactId());
            dep.put("versionId", d.versionId());
            deps.add(dep);
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("dependencies", deps);
        data.put("properties", List.of());
        data.put("manifestProperties", null);
        data.put("deprecated", Boolean.FALSE);
        data.put("excluded", Boolean.FALSE);
        data.put("exclusionReason", null);
        data.put("excludedDependencies", Map.of());
        data.put("dependencyExclusions", Map.of());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("groupId", g);
        out.put("artifactId", a);
        out.put("versionId", v);
        out.put("versionData", data);
        return out;
    }

    // ---- plumbing ----

    private static Response ok(Object body) {
        return new Response(200, Json.toCompact(body));
    }

    /** Depot's error body: code, message, and the timestamp as Jackson writes an {@code Instant} without JavaTimeModule. */
    private Response error(int status, String message) {
        long ms = clock.getAsLong();
        Map<String, Object> ts = new LinkedHashMap<>();
        ts.put("epochSecond", ms / 1000);
        ts.put("nano", (ms % 1000) * 1_000_000);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", status);
        out.put("message", message);
        out.put("timestamp", ts);
        return new Response(status, Json.toCompact(out));
    }

    private static boolean bool(Map<String, List<String>> q, String key) {
        return "true".equals(first(q, key));
    }

    private static @Nullable String first(Map<String, List<String>> q, String key) {
        List<String> v = q.get(key);
        return v == null || v.isEmpty() ? null : v.get(0);
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
