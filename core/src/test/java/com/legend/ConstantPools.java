// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * A CLASS FILE'S CONSTANT POOL, READ AS TEXT (the GUARD stream, 2026-11-17; the edge audit's C.3 and C.6). A dependency
 * graph (ArchUnit) sees field accesses, calls and references; it does not see what a class holds only as constants:
 * a pattern switch's labels ({@code SwitchBootstraps.enumSwitch} takes them as strings), a class literal handed to
 * reflection or {@code EnumSet.allOf}, a type named only in a descriptor, a string concatenated by
 * {@code invokedynamic}. Every one of those leaves its text in the constant pool: this reads each class's pool as the
 * set of its UTF-8 entries (names, descriptors and string constants alike), so a guard can ask "which classes name X".
 * A reader of the JVM class-file format (JVMS 4.4), JDK-only, no bytecode library.
 */
final class ConstantPools {

    private ConstantPools() {
    }

    /** A class's pool as text: every UTF-8 entry (names, descriptors and string constants alike), and the string
     *  constants alone (the UTF-8 entries a {@code CONSTANT_String} names: a literal, a folded or borrowed constant, a
     *  switch label, a concatenation's recipe). */
    record Pool(Set<String> utf8, Set<String> strings) {
    }

    /** The UTF-8 entries of the class file {@code bytes}' constant pool, in pool order. */
    static Set<String> utf8(byte[] bytes) {
        return pool(bytes).utf8();
    }

    /** The class file {@code bytes}' pool, read as text ({@link Pool}). */
    static Pool pool(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != 0xCAFEBABE) {
                throw new IllegalArgumentException("not a class file");
            }
            in.readUnsignedShort();
            in.readUnsignedShort();
            int count = in.readUnsignedShort();
            String[] texts = new String[count];
            List<Integer> stringRefs = new ArrayList<>();
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                switch (tag) {
                    case 1 -> texts[i] = in.readUTF();
                    case 8 -> stringRefs.add(in.readUnsignedShort());
                    case 7, 16, 19, 20 -> in.skipNBytes(2);
                    case 15 -> in.skipNBytes(3);
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> in.skipNBytes(4);
                    case 5, 6 -> {
                        in.skipNBytes(8);
                        i++;
                    }
                    default -> throw new IllegalArgumentException("constant pool tag " + tag + " at " + i);
                }
            }
            Set<String> utf8 = new LinkedHashSet<>();
            for (String t : texts) {
                if (t != null) {
                    utf8.add(t);
                }
            }
            Set<String> strings = new LinkedHashSet<>();
            for (int ref : stringRefs) {
                strings.add(texts[ref]);
            }
            return new Pool(utf8, strings);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Every class of {@code jars} (its binary name, {@code a.b.C$D}) with its constant pool's UTF-8 entries. */
    static Map<String, Set<String>> ofJars(List<Path> jars) {
        Map<String, Set<String>> out = new TreeMap<>();
        poolsOfJars(jars).forEach((name, pool) -> out.put(name, pool.utf8()));
        return out;
    }

    /** Every class of {@code jars} (its binary name) with its pool ({@link Pool}). */
    static Map<String, Pool> poolsOfJars(List<Path> jars) {
        Map<String, Pool> out = new TreeMap<>();
        for (Path jar : jars) {
            try (JarFile f = new JarFile(jar.toFile())) {
                Enumeration<JarEntry> entries = f.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String n = e.getName();
                    // a multi-release jar's versioned class (META-INF/versions/N/a/b/C.class) is the same class, read
                    // too: its pool joins the base one's (the review of 08adb5c923, 2026-11-17)
                    if (n.matches("META-INF/versions/\\d+/.+")) {
                        n = n.replaceFirst("META-INF/versions/\\d+/", "");
                    }
                    if (!n.endsWith(".class") || n.endsWith("module-info.class") || n.startsWith("META-INF/")) {
                        continue;
                    }
                    try (var in = f.getInputStream(e)) {
                        Pool read = pool(in.readAllBytes());
                        out.merge(n.substring(0, n.length() - ".class".length()).replace('/', '.'), read, (a, b) -> {
                            Set<String> u = new LinkedHashSet<>(a.utf8());
                            u.addAll(b.utf8());
                            Set<String> st = new LinkedHashSet<>(a.strings());
                            st.addAll(b.strings());
                            return new Pool(u, st);
                        });
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    /** Every resource of {@code jars} -- each entry that is no class file, directory or manifest (a .pure prelude, a
     *  .tsv table, a service file) -- by "jar name!/path", with its text read as UTF-8. */
    static Map<String, String> resourcesOfJars(List<Path> jars) {
        Map<String, String> out = new TreeMap<>();
        for (Path jar : jars) {
            try (JarFile f = new JarFile(jar.toFile())) {
                Enumeration<JarEntry> entries = f.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String n = e.getName();
                    if (e.isDirectory() || n.endsWith(".class") || n.equals("META-INF/MANIFEST.MF")) {
                        continue;
                    }
                    try (var in = f.getInputStream(e)) {
                        out.put(jar.getFileName() + "!/" + n, new String(in.readAllBytes(),
                                java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    /** Whether {@code word} stands in {@code text} as a whole word: no letter, digit or underscore either side (a pool
     *  entry holds a token inside a longer string, a descriptor or a concatenation's recipe). */
    static boolean hasWord(String text, String word) {
        for (int i = text.indexOf(word); i >= 0; i = text.indexOf(word, i + 1)) {
            int end = i + word.length();
            if ((i == 0 || !wordChar(text.charAt(i - 1))) && (end == text.length() || !wordChar(text.charAt(end)))) {
                return true;
            }
        }
        return false;
    }

    private static boolean wordChar(char c) {
        return c == '_' || (c < 128 && Character.isLetterOrDigit(c));
    }

    /** The jars a test's system property lists (as core/BUILD.bazel's {@code rlocationpaths} spell them). */
    static List<Path> jars(String... properties) {
        List<Path> out = new ArrayList<>();
        for (String p : properties) {
            for (Path jar : com.legend.testing.Runfile.listed(p)) {
                out.add(jar);
            }
        }
        return out;
    }

    /** The class file of {@code c}, from its class loader. */
    static byte[] bytesOf(Class<?> c) {
        String resource = c.getName().replace('.', '/') + ".class";
        try (var in = c.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("no class file for " + c.getName());
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
