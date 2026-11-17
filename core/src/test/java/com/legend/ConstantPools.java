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

    /** The UTF-8 entries of the class file {@code bytes}' constant pool, in pool order. */
    static Set<String> utf8(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != 0xCAFEBABE) {
                throw new IllegalArgumentException("not a class file");
            }
            in.readUnsignedShort();
            in.readUnsignedShort();
            int count = in.readUnsignedShort();
            Set<String> out = new LinkedHashSet<>();
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                switch (tag) {
                    case 1 -> out.add(in.readUTF());
                    case 7, 8, 16, 19, 20 -> in.skipNBytes(2);
                    case 15 -> in.skipNBytes(3);
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> in.skipNBytes(4);
                    case 5, 6 -> {
                        in.skipNBytes(8);
                        i++;
                    }
                    default -> throw new IllegalArgumentException("constant pool tag " + tag + " at " + i);
                }
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Every class of {@code jars} (its binary name, {@code a.b.C$D}) with its constant pool's UTF-8 entries. */
    static Map<String, Set<String>> ofJars(List<Path> jars) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (Path jar : jars) {
            try (JarFile f = new JarFile(jar.toFile())) {
                Enumeration<JarEntry> entries = f.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String n = e.getName();
                    if (!n.endsWith(".class") || n.equals("module-info.class") || n.startsWith("META-INF/")) {
                        continue;
                    }
                    try (var in = f.getInputStream(e)) {
                        out.put(n.substring(0, n.length() - ".class".length()).replace('/', '.'), utf8(in.readAllBytes()));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
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
