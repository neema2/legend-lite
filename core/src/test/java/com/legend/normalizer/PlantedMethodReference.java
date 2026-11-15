// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.model.PackageableElement;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * A class PLANTED in the normalizer's package that names the model index's writer by reference
 * ({@code ModelBuilder::add}) and never calls it, for one test alone (the next PR's item 1, "separation made
 * foolproof", 2026-11-13: the method rules read calls, not references): {@code ArchitectureTest
 * .theMethodRulesBiteAReference} shows {@code normalizerNeverWritesIntoTheModelIndex}'s rule fails on it. A test class:
 * the rule's real check reads the product's classes, never this one.
 */
public final class PlantedMethodReference {

    private PlantedMethodReference() {
    }

    public static BiConsumer<ModelBuilder, List<PackageableElement>> writing() {
        return ModelBuilder::add;
    }
}
