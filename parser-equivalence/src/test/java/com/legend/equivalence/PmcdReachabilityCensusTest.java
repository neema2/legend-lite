package com.legend.equivalence;

import com.legend.testing.Repo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** PROBE (PMCD reachability): the DEFINITIVE in-scope roster — a static
 *  walk of the protocol type graph from PureModelContextData (fields +
 *  generics + Jackson subtype expansions). A protocol tag is in
 *  text-parity scope iff its class is reachable; reachable ∧ uncovered
 *  (per protocol-roster.txt) = the true fixture worklist, package
 *  heuristics retired. Diagnostic. */
class PmcdReachabilityCensusTest {

    @Test
    void reachability() throws Exception {
        // ---- tag -> class (and class -> subtypes) from every jar ----
        Map<String, String> tagToClass = new TreeMap<>();
        Map<String, Set<String>> parentToChildren = new HashMap<>();
        for (java.nio.file.Path jarPath : com.legend.testing.Repo.listed("legend.engine.jars")) {
            String entry = jarPath.toString();
            if (!entry.endsWith(".jar") || !entry.contains("legend-engine")) {
                continue;
            }
            try (JarFile jar = new JarFile(entry)) {
                Enumeration<JarEntry> es = jar.entries();
                while (es.hasMoreElements()) {
                    String name = es.nextElement().getName();
                    if (!name.endsWith(".class") || !name.startsWith(
                            "org/finos/legend/engine/protocol/")) {
                        continue;
                    }
                    String cls = name.substring(0, name.length() - 6)
                            .replace('/', '.');
                    try {
                        Class<?> c = Class.forName(cls, false,
                                getClass().getClassLoader());
                        JsonSubTypes st = c.getAnnotation(JsonSubTypes.class);
                        if (st != null) {
                            for (JsonSubTypes.Type t : st.value()) {
                                tagToClass.putIfAbsent(t.name(),
                                        t.value().getName());
                                parentToChildren.computeIfAbsent(
                                        c.getName(), k -> new HashSet<>())
                                        .add(t.value().getName());
                            }
                        }
                    } catch (Throwable ignored) {
                        // unloadable — skip
                    }
                }
            } catch (Throwable ignored) {
                // unreadable jar — skip
            }
        }
        org.finos.legend.engine.protocol.pure.v1.extension
                .PureProtocolExtensionLoader.extensions().forEach(ext ->
                ext.getExtraProtocolSubTypeInfoCollectors().forEach(c ->
                        c.value().forEach(info -> info.getSubTypes().forEach(
                                p -> {
                                    tagToClass.putIfAbsent(p.getTwo(),
                                            p.getOne().getName());
                                    parentToChildren.computeIfAbsent(
                                            info.getSuperType().getName(),
                                            k -> new HashSet<>())
                                            .add(p.getOne().getName());
                                }))));

        // ---- BFS from the PMCD root over fields + subtype edges ----
        Set<String> reachable = new HashSet<>();
        Deque<Class<?>> queue = new ArrayDeque<>();
        queue.add(Class.forName("org.finos.legend.engine.protocol.pure"
                + ".v1.model.context.PureModelContextData"));
        while (!queue.isEmpty()) {
            Class<?> c = queue.poll();
            if (c == null || c.isPrimitive()
                    || c.getName().startsWith("java.")
                    || c.getName().startsWith("com.fasterxml.")
                    || !reachable.add(c.getName())) {
                continue;
            }
            // subtype expansion (Jackson polymorphism)
            for (String child : parentToChildren.getOrDefault(c.getName(),
                    Set.of())) {
                try {
                    queue.add(Class.forName(child, false,
                            getClass().getClassLoader()));
                } catch (Throwable ignored) {
                    // skip
                }
            }
            // superclass chain (fields + its subtype registrations)
            if (c.getSuperclass() != null) {
                queue.add(c.getSuperclass());
            }
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                collectTypes(f.getGenericType(), queue);
            }
        }

        // ---- verdicts over the uncovered set ----
        // SELF-SUFFICIENT (user ruling 2026-08-21 — no skipping class
        // on a gate roster): the roster this reads is materialized HERE
        // when absent (DEEP_AUDIT §11c: this was a raw
        // NoSuchFileException ERROR dependent on class run order).
        Path roster = Repo.out("protocol-roster.txt");
        if (!Files.exists(roster)) {
            ProtocolRosterCensusTest.materializeRoster();
        }
        List<String> lines = Files.readAllLines(roster);
        Map<String, List<String>> inScope = new TreeMap<>();
        int outOfScope = 0;
        int inScopeCount = 0;
        for (String line : lines) {
            String[] parts = line.split("\t");
            if (parts.length < 3 || !"UNCOVERED".equals(parts[2])) {
                continue;
            }
            if (reachable.contains(parts[1])) {
                inScopeCount++;
                String pkg = parts[1].substring(0,
                        parts[1].lastIndexOf('.'))
                        .replace("org.finos.legend.engine.protocol.", "");
                inScope.computeIfAbsent(pkg, k -> new java.util.ArrayList<>())
                        .add(parts[0]);
            } else {
                outOfScope++;
            }
        }
        System.out.println("@@ reachable protocol classes: "
                + reachable.size());
        System.out.println("@@ uncovered & IN-SCOPE (fixture worklist): "
                + inScopeCount + "; uncovered & UNREACHABLE (proven out): "
                + outOfScope);
        inScope.forEach((pkg, tags) -> System.out.println("@@ IN [" + pkg
                + "] " + String.join(", ", tags)));
    }

    private static void collectTypes(Type t, Deque<Class<?>> queue) {
        if (t instanceof Class<?> c) {
            if (c.isArray()) {
                collectTypes(c.getComponentType(), queue);
            } else {
                queue.add(c);
            }
        } else if (t instanceof ParameterizedType p) {
            collectTypes(p.getRawType(), queue);
            for (Type a : p.getActualTypeArguments()) {
                collectTypes(a, queue);
            }
        }
    }
}
