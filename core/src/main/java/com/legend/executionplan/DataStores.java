// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * WHERE A RUN'S DATA REFERENCES RESOLVE (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.7, 2026-11-09): a directory per
 * namespace, configured by whoever runs -- data, never a function (the resolver function the planner called until
 * then, {@code PlanningOptions.resources}, is deleted). Planning reads a referenced file through it once, for its
 * digest and size (knowledge); the run reads it again and delivers only bytes whose digest is the plan's. It is not
 * part of any plan: a plan names a reference, and each runner its own stores.
 *
 * @param roots each namespace's directory
 */
public record DataStores(Map<DataRef.Namespace, Path> roots) {

    public DataStores {
        EnumMap<DataRef.Namespace, Path> copy = new EnumMap<>(DataRef.Namespace.class);
        roots.forEach((namespace, root) -> copy.put(Objects.requireNonNull(namespace, "namespace"),
                Objects.requireNonNull(root, "root").toAbsolutePath().normalize()));
        roots = Collections.unmodifiableMap(copy);
    }

    /** No store: a run whose plan names no data, or refuses each reference by name. */
    public static DataStores none() {
        return new DataStores(Map.of());
    }

    /** The test resources under {@code root}. */
    public static DataStores resources(Path root) {
        return new DataStores(Map.of(DataRef.Namespace.RESOURCE, root));
    }

    /**
     * The bytes {@code name} names in {@code namespace}: refused by name where these stores have no store for the
     * namespace ({@link IllegalStateException}) or no file of that name ({@link java.io.FileNotFoundException}, in
     * an {@link UncheckedIOException}).
     */
    public byte[] read(DataRef.Namespace namespace, String name) {
        Path root = roots.get(namespace);
        if (root == null) {
            throw new IllegalStateException("data '" + name + "' in namespace " + namespace + ": this run was given"
                    + " no store for it");
        }
        Path file = root.resolve(DataRef.requireName(name)).normalize();
        if (!file.startsWith(root) || !Files.isRegularFile(file)) {
            throw new UncheckedIOException(new java.io.FileNotFoundException("data '" + name + "' in namespace "
                    + namespace + ": no file of that name in its store"));
        }
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The SHA-256 of {@code bytes}, lowercase hex: a reference's digest. */
    public static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("the JDK has no SHA-256", e);
        }
    }
}
