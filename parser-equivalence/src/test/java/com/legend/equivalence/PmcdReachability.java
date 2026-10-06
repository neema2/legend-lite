package com.legend.equivalence;

import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** THE PROTOCOL'S REACHABLE CLASSES, an upstream record: a static walk of legend-engine's protocol type graph from
 *  PureModelContextData, over fields, generics and Jackson's subtype registrations (the annotations, and the
 *  protocol extensions' collectors). A protocol tag is in text-parity scope iff its class is reachable. It reads
 *  the engine's jars alone, so only an upgrade moves it: //parser-equivalence:pmcd_reachability writes it, committed
 *  as parser-equivalence/pmcd-reachable.tsv. {@link PmcdWorklist} joins it with our roster, on request. */
public final class PmcdReachability {

    private PmcdReachability() {}

    /** {@code args[0]}: the record's path (the action's {@code {OUT}}). */
    public static void main(String[] args) throws Exception {
        // ---- class -> subtypes, from every legend-engine jar on the class path ----
        Map<String, Set<String>> parentToChildren = new HashMap<>();
        int unloadable = 0;
        int unreadable = 0;
        for (Path jarPath : com.legend.testing.ProgramPaths.listed("legend.engine.jars")) {
            String entry = jarPath.toString();
            if (!entry.endsWith(".jar") || !entry.contains("legend-engine")) {
                continue;
            }
            try (JarFile jar = new JarFile(entry)) {
                Enumeration<JarEntry> es = jar.entries();
                while (es.hasMoreElements()) {
                    String name = es.nextElement().getName();
                    if (!name.endsWith(".class") || !name.startsWith("org/finos/legend/engine/protocol/")) {
                        continue;
                    }
                    String cls = name.substring(0, name.length() - 6).replace('/', '.');
                    try {
                        Class<?> c = Class.forName(cls, false, PmcdReachability.class.getClassLoader());
                        JsonSubTypes st = c.getAnnotation(JsonSubTypes.class);
                        if (st != null) {
                            for (JsonSubTypes.Type t : st.value()) {
                                parentToChildren.computeIfAbsent(c.getName(), k -> new HashSet<>())
                                        .add(t.value().getName());
                            }
                        }
                    } catch (Throwable e) {
                        unloadable++;
                    }
                }
            } catch (Throwable e) {
                unreadable++;
            }
        }
        org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtensionLoader.extensions().forEach(ext ->
                ext.getExtraProtocolSubTypeInfoCollectors().forEach(c ->
                        c.value().forEach(info -> info.getSubTypes().forEach(p ->
                                parentToChildren.computeIfAbsent(info.getSuperType().getName(), k -> new HashSet<>())
                                        .add(p.getOne().getName())))));

        // ---- BFS from the PMCD root over fields + subtype edges ----
        Set<String> reachable = new HashSet<>();
        Deque<Class<?>> queue = new ArrayDeque<>();
        queue.add(Class.forName("org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData"));
        while (!queue.isEmpty()) {
            Class<?> c = queue.poll();
            if (c == null || c.isPrimitive()
                    || c.getName().startsWith("java.")
                    || c.getName().startsWith("com.fasterxml.")
                    || !reachable.add(c.getName())) {
                continue;
            }
            // subtype expansion (Jackson polymorphism)
            for (String child : parentToChildren.getOrDefault(c.getName(), Set.of())) {
                try {
                    queue.add(Class.forName(child, false, PmcdReachability.class.getClassLoader()));
                } catch (Throwable e) {
                    unloadable++;
                }
            }
            // superclass chain (fields + its subtype registrations)
            if (c.getSuperclass() != null) {
                queue.add(c.getSuperclass());
            }
            for (Field f : c.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers())) {
                    collectTypes(f.getGenericType(), queue);
                }
            }
        }

        // the classes sorted, one per line, LF on every platform; what the walk skipped is said, not silent
        StringBuilder out = new StringBuilder("# reachable from PureModelContextData over legend-engine's protocol jars: "
                + reachable.size() + " classes; " + unloadable + " unloadable classes and " + unreadable
                + " unreadable jars skipped\n");
        for (String c : new TreeSet<>(reachable)) {
            out.append(c).append('\n');
        }
        Files.writeString(Path.of(args[0]), out.toString(), StandardCharsets.UTF_8);
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
