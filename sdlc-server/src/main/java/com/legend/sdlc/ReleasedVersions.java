package com.legend.sdlc;

import com.legend.base.Nullable;
import com.legend.depot.ArtifactSource;
import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * THE VERSIONS A SITE RELEASES (docs/ONE_CONNECTION_DESK_2026_10_28.md D5): a read-only layer of the model home, each
 * version loaded from the site's files at every open -- the page's from its configuration's releases folder, a server's
 * from the same folder -- and never written to a storage, so a site's update is never shadowed by an old copy. Depot
 * reads it beside the SDLC's own versions ({@link #over}), a released version first; the SDLC refuses a person's version
 * at a released version's coordinates ({@link Sdlc#releasedAt}), so the two never answer for the same version.
 *
 * <p>A version's files are kept as the site gives them -- a file may hold many elements, as a site's do -- and its
 * entities read from them by the model home's grammar at lite's dialect (D1: a file holding lite's own connection words
 * is an entity like any other) when first asked for, then kept: a page reading a version's files alone (its model's
 * text, {@code modelText}) never pays for them (measured 2026-10-28 in the page's WebAssembly: the trader screens'
 * 2.3 MB read in 2.7 s, every open, before this). A version's hash is its files' (each path and text, in order): what a
 * workspace started from it records, so a version changed under the same number is said by name.
 */
public final class ReleasedVersions implements ArtifactSource {
    /** A file of a version: its path in the release and its Pure text. */
    public record File(String path, String pureCode) {}

    private record Version(Release release, String hash) {}

    /** A version's entities, read from its files when first asked for and kept. */
    private static final class Entities implements Supplier<String> {
        private final Sdlc.Grammar grammar;
        private final String version;
        private final List<File> files;
        /** kept once read: two readers at once read it twice at worst (no lock: the page's WebAssembly has none), and
         *  keep equal text */
        private volatile @Nullable String read;

        Entities(Sdlc.Grammar grammar, String version, List<File> files) {
            this.grammar = grammar;
            this.version = version;
            this.files = files;
        }

        @Override
        public String get() {
            String kept = read;
            if (kept == null) {
                kept = read();
                read = kept;
            }
            return kept;
        }

        /** Each file's elements as entities, in order: refused, by name, for a file the grammar cannot read or an
         *  element no SDLC stores. */
        private String read() {
            List<Object> entities = new ArrayList<>();
            for (File f : files) {
                List<Json.Node> elements;
                try {
                    elements = Json.parseObject(grammar.modelJson(f.pureCode())).getArr("elements").items();
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(version + ": " + f.path() + " is not read: "
                            + (e.getMessage() == null ? e.getClass().getName() : e.getMessage()), e);
                }
                for (Json.Node n : elements) {
                    Json.Obj element = (Json.Obj) n;
                    String type = String.valueOf(element.getStringOr("_type", null));
                    if (type.equals("sectionIndex")) continue;
                    String classifierPath = Classifiers.of(type);
                    if (classifierPath == null) {
                        throw new IllegalArgumentException(version + ": " + f.path() + " holds an element of type " + type
                                + ", which no SDLC stores");
                    }
                    LinkedHashMap<String, Json.Node> entity = new LinkedHashMap<>();
                    entity.put("path", Json.str(element.getStringOr("package", null) + "::" + element.getStringOr("name", null)));
                    entity.put("classifierPath", Json.str(classifierPath));
                    entity.put("content", element);
                    entities.add(new Json.Obj(entity));
                }
            }
            return Json.toCompact(entities);
        }
    }

    private final Sdlc.Grammar grammar;
    /** Each version, by {@code group:artifact:version}, in the order released. */
    private final Map<String, Version> versions = new LinkedHashMap<>();

    public ReleasedVersions(Sdlc.Grammar grammar) {
        this.grammar = grammar;
    }

    private static String key(String groupId, String artifactId, String versionId) {
        return groupId + ":" + artifactId + ":" + versionId;
    }

    /**
     * Version {@code versionId} of {@code groupId:artifactId} released with {@code files}, its entities read from them
     * when first asked for (refused then, by name, for a file the grammar cannot read or an element no SDLC stores);
     * released again with other files, the latest is kept (the site was updated: what it serves now is the version).
     */
    public void add(String groupId, String artifactId, String versionId, List<File> files) {
        List<Object> texts = new ArrayList<>();
        StringBuilder all = new StringBuilder();
        for (File f : files) {
            LinkedHashMap<String, Object> text = new LinkedHashMap<>();
            text.put("path", f.path());
            text.put("pureCode", f.pureCode());
            texts.add(text);
            all.append(f.path()).append('\0').append(f.pureCode()).append('\0');
        }
        String hash = Sha1.hex(Sha1.digest(all.toString().getBytes(StandardCharsets.UTF_8)));
        Entities entities = new Entities(grammar, "version " + versionId + " of " + groupId + ":" + artifactId, List.copyOf(files));
        versions.put(key(groupId, artifactId, versionId), new Version(new Release(List.of(), entities, Json.toCompact(texts)), hash));
    }

    /** Whether {@code group:artifact:version} is a version released here. */
    public boolean holds(String gav) {
        return versions.containsKey(gav);
    }

    /** A released version's hash (its files'), or null when none is released at those coordinates. */
    public @Nullable String hash(String groupId, String artifactId, String versionId) {
        Version v = versions.get(key(groupId, artifactId, versionId));
        return v == null ? null : v.hash();
    }

    @Override
    public List<Project> projects() {
        Set<String> seen = new LinkedHashSet<>();
        List<Project> out = new ArrayList<>();
        for (String gav : versions.keySet()) {
            String ga = gav.substring(0, gav.lastIndexOf(':'));
            if (!seen.add(ga)) continue;
            int colon = ga.indexOf(':');
            out.add(new Project(ga.substring(0, colon), ga.substring(colon + 1), ga));
        }
        return out;
    }

    @Override
    public List<String> versions(String groupId, String artifactId) {
        String prefix = groupId + ":" + artifactId + ":";
        List<String> out = new ArrayList<>();
        for (String gav : versions.keySet()) if (gav.startsWith(prefix) && gav.indexOf(':', prefix.length()) < 0) out.add(gav.substring(prefix.length()));
        return out;
    }

    @Override
    public boolean hasSnapshot(String groupId, String artifactId) {
        // a released version is a version, never a line's head
        return false;
    }

    @Override
    public @Nullable Release release(String groupId, String artifactId, String versionId) {
        Version v = versions.get(key(groupId, artifactId, versionId));
        return v == null ? null : v.release();
    }

    /**
     * The model home's versions: these released ones first, then {@code own} (the SDLC's: a person's projects and the
     * versions they made), each project once. A version is answered by the layer that holds it -- never both: the SDLC
     * refuses a person's version at a released version's coordinates.
     */
    public ArtifactSource over(ArtifactSource own) {
        ReleasedVersions released = this;
        return new ArtifactSource() {
            @Override
            public List<Project> projects() {
                List<Project> out = new ArrayList<>(released.projects());
                Set<String> seen = new LinkedHashSet<>();
                for (Project p : out) seen.add(p.groupId() + ":" + p.artifactId());
                for (Project p : own.projects()) if (seen.add(p.groupId() + ":" + p.artifactId())) out.add(p);
                return out;
            }

            @Override
            public List<String> versions(String groupId, String artifactId) {
                List<String> out = new ArrayList<>(released.versions(groupId, artifactId));
                for (String v : own.versions(groupId, artifactId)) if (!out.contains(v)) out.add(v);
                return out;
            }

            @Override
            public boolean hasSnapshot(String groupId, String artifactId) {
                return own.hasSnapshot(groupId, artifactId);
            }

            @Override
            public @Nullable Release release(String groupId, String artifactId, String versionId) {
                Release r = released.release(groupId, artifactId, versionId);
                return r != null ? r : own.release(groupId, artifactId, versionId);
            }
        };
    }
}
