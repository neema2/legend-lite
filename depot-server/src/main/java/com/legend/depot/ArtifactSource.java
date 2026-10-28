package com.legend.depot;

import com.legend.base.Nullable;

import java.util.List;
import java.util.function.Supplier;

/**
 * Where Depot-lite reads published versions from (design S22) -- upstream Depot's artifact repository,
 * as an interface. The first source is SDLC-lite's version tags (sdlc-server's {@code TagSource}); a
 * Maven repository (GitLab's package registry, Artifactory, a directory) is another. Depot never sees
 * where the versions come from, so it can run as its own service.
 */
public interface ArtifactSource {
    /** A project the source knows. */
    record Project(String groupId, String artifactId, String projectId) {}

    /** A dependency as declared: a project's coordinates, a version, and what to exclude below it ({@code g:a}). */
    record Dependency(String groupId, String artifactId, String versionId, List<String> exclusions) {
        public String key() {
            return groupId + ":" + artifactId;
        }
    }

    /**
     * A published version: what it declares it depends on, its entities (upstream's {@code Entity[]} JSON)
     * and its files (lite's {@code [{path, pureCode}]} JSON). Its entities are made when first asked for: a site's
     * released version reads them from its files then (docs/ONE_CONNECTION_DESK_2026_10_28.md D5), which a reader of
     * its files alone never pays (measured 2026-10-28: the trader screens' 2.3 MB, 2.7 s of a page's open).
     */
    record Release(List<Dependency> dependencies, Supplier<String> entities, String filesJson) {
        /** Its entities, made now if they were not yet; a version whose files cannot be read is refused here. */
        public String entitiesJson() {
            return entities.get();
        }
    }

    List<Project> projects();

    /** A project's released versions, any order; empty for a project the source does not know. */
    List<String> versions(String groupId, String artifactId);

    /** Whether the project's line has a snapshot ({@code master-SNAPSHOT}, upstream's {@code head}). */
    boolean hasSnapshot(String groupId, String artifactId);

    /** A version ({@code x.y.z} or {@code master-SNAPSHOT}), or null when there is none. */
    @Nullable Release release(String groupId, String artifactId, String versionId);
}
