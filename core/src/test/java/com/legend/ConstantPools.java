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
    record Pool(Set<String> utf8, Set<String> strings, Set<String> indyTypes, Map<String, Integer> indySites) {
        Pool(Set<String> utf8, Set<String> strings) {
            this(utf8, strings, Set.of(), Map.of());
        }

        Pool(Set<String> utf8, Set<String> strings, Set<String> indyTypes) {
            this(utf8, strings, indyTypes, Map.of());
        }
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
            int[] natDescriptor = new int[count];
            List<Integer> indyNats = new ArrayList<>();
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                switch (tag) {
                    case 1 -> texts[i] = in.readUTF();
                    case 8 -> stringRefs.add(in.readUnsignedShort());
                    case 7, 16, 19, 20 -> in.skipNBytes(2);
                    case 15 -> in.skipNBytes(3);
                    case 12 -> {
                        in.readUnsignedShort();
                        natDescriptor[i] = in.readUnsignedShort();
                    }
                    case 18 -> {
                        in.readUnsignedShort();
                        indyNats.add(in.readUnsignedShort());
                    }
                    case 3, 4, 9, 10, 11, 17 -> in.skipNBytes(4);
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
            // each invokedynamic's call-site type's return: for a lambda or a method reference (LambdaMetafactory),
            // the functional interface it implements; for a concatenation, String; for a pattern switch, int
            // and their number per type: one pool entry per call site (each lambda its own bootstrap, so its own
            // entry; javac shares an entry only between identical method references) -- a second lambda of a pinned
            // type is a second site (the review of C.12, 2026-11-17)
            Set<String> indyTypes = new LinkedHashSet<>();
            Map<String, Integer> indySites = new TreeMap<>();
            for (int nat : indyNats) {
                String descriptor = texts[natDescriptor[nat]];
                String ret = descriptor.substring(descriptor.indexOf(')') + 1);
                if (ret.startsWith("L") && ret.endsWith(";")) {
                    String type = ret.substring(1, ret.length() - 1).replace('/', '.');
                    indyTypes.add(type);
                    indySites.merge(type, 1, Integer::sum);
                }
            }
            return new Pool(utf8, strings, indyTypes, indySites);
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
                            Set<String> it = new LinkedHashSet<>(a.indyTypes());
                            it.addAll(b.indyTypes());
                            // a versioned class is the same class: its sites are the larger of the two, not their sum
                            Map<String, Integer> sites = new TreeMap<>(a.indySites());
                            b.indySites().forEach((t, k) -> sites.merge(t, k, Math::max));
                            return new Pool(u, st, it, sites);
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
                    String key = jar.getFileName() + "!/" + n;
                    // refused by name, never read as junk (the review of 139f03aba2, 2026-11-17): an archive inside the
                    // jar, a resource that is no UTF-8 text, a key two jars share
                    if (n.matches(".*\\.(jar|zip|war|gz|tgz|xz)$")) {
                        throw new IllegalStateException(key + ": a nested archive this scan cannot read");
                    }
                    String text;
                    try (var in = f.getInputStream(e)) {
                        text = java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                                .decode(java.nio.ByteBuffer.wrap(in.readAllBytes())).toString();
                    } catch (java.nio.charset.CharacterCodingException bad) {
                        throw new IllegalStateException(key + ": a resource that is no UTF-8 text, which this scan cannot"
                                + " read", bad);
                    }
                    if (out.put(key, text) != null) {
                        throw new IllegalStateException(key + ": two jars of one name hold this resource");
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
        // one jar named by two properties is read once (a jar the edge's list and the hosts' list both carry)
        java.util.Set<Path> out = new LinkedHashSet<>();
        for (String p : properties) {
            for (Path jar : com.legend.testing.Runfile.listed(p)) {
                out.add(jar.toAbsolutePath().normalize());
            }
        }
        return new ArrayList<>(out);
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
