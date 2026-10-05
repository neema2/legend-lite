package com.legend.depot;

import com.legend.base.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * One dependency closure, the way upstream Depot computes it with Aether's nearest-wins
 * (studio/docs/DEPOT_CONTRACT.md §1.2 step 3): the requested roots sorted by group then artifact as the
 * first level, each node's declared dependencies sorted the same way below it; a project chosen once --
 * the occurrence nearest the roots wins, between siblings the higher version, otherwise the first in
 * depth-first order (which, level by level, is breadth-first order); a loser's dependencies are never
 * visited; exclusions remove a project (and so its dependencies) from below the node that declares them.
 * Upstream's resolution and SDLC-lite's gates both use this, so a version compiles against exactly the
 * closure Depot serves.
 */
public final class Resolver {
    private Resolver() {}

    /** What a version declares, or null when the version does not exist. */
    public interface Declared extends Function<ArtifactSource.Dependency, @Nullable List<ArtifactSource.Dependency>> {}

    private record Node(ArtifactSource.Dependency dependency, Set<String> excluded, int parent) {}

    /** The chosen versions, roots first, then level by level. Unknown versions are kept, as leaves (upstream's Aether). */
    public static List<ArtifactSource.Dependency> closure(List<ArtifactSource.Dependency> roots, Declared declared) {
        Comparator<ArtifactSource.Dependency> byCoordinates = Comparator.comparing(ArtifactSource.Dependency::groupId)
                .thenComparing(ArtifactSource.Dependency::artifactId);
        List<ArtifactSource.Dependency> level0 = new ArrayList<>(roots);
        level0.sort(byCoordinates);
        Map<String, ArtifactSource.Dependency> chosen = new LinkedHashMap<>();
        List<Node> level = new ArrayList<>();
        for (ArtifactSource.Dependency d : level0) level.add(new Node(d, new HashSet<>(d.exclusions()), -1));
        while (!level.isEmpty()) {
            // at one depth: the first occurrence of each project, except that siblings keep the higher version
            Map<String, Node> winners = new LinkedHashMap<>();
            for (Node n : level) {
                String key = n.dependency().key();
                if (chosen.containsKey(key)) continue;
                Node seen = winners.get(key);
                if (seen == null) winners.put(key, n);
                else if (seen.parent() == n.parent() && compare(n.dependency().versionId(), seen.dependency().versionId()) > 0) winners.put(key, n);
            }
            List<Node> next = new ArrayList<>();
            int index = 0;
            for (Node n : winners.values()) {
                chosen.put(n.dependency().key(), n.dependency());
                List<ArtifactSource.Dependency> children = declared.apply(n.dependency());
                if (children != null) {
                    List<ArtifactSource.Dependency> sorted = new ArrayList<>(children);
                    sorted.sort(byCoordinates);
                    for (ArtifactSource.Dependency c : sorted) {
                        if (n.excluded().contains(c.key())) continue;
                        Set<String> excluded = new HashSet<>(n.excluded());
                        excluded.addAll(c.exclusions());
                        next.add(new Node(c, excluded, index));
                    }
                }
                index++;
            }
            level = next;
        }
        return new ArrayList<>(chosen.values());
    }

    /** Maven's order for plain {@code x.y.z} versions (numerically, part by part); anything else after, by text. */
    public static int compare(String a, String b) {
        String[] x = a.split("\\.");
        String[] y = b.split("\\.");
        for (int i = 0; i < Math.max(x.length, y.length); i++) {
            String p = i < x.length ? x[i] : "0";
            String q = i < y.length ? y[i] : "0";
            boolean pn = p.matches("\\d+");
            boolean qn = q.matches("\\d+");
            int c = pn && qn ? Long.compare(Long.parseLong(p), Long.parseLong(q)) : pn ? -1 : qn ? 1 : p.compareTo(q);
            if (c != 0) return c;
        }
        return 0;
    }
}
