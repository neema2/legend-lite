// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * Classes PLANTED in exec's package to make an id (drawn, built, parsed, named), for one test alone: {@code ArchitectureTest
 * .theNoValueRuleBitesAPlantedRandomUuid} shows {@code ArchitectureTest.execMakesNoValueOfItsOwn} fails on it. A test
 * class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedRandomUuid {

    private PlantedRandomUuid() {
    }

    /** An id exec would have made of its own: the defect the rule refuses. */
    public static java.util.UUID minted() {
        return java.util.UUID.randomUUID();
    }

    /** An id built from two longs (the re-audit's note 6, 2026-11-12). */
    public static final class Built {
        private Built() {
        }

        public static java.util.UUID built(long hi, long lo) {
            return new java.util.UUID(hi, lo);
        }
    }

    /** An id parsed from text. */
    public static final class Parsed {
        private Parsed() {
        }

        public static java.util.UUID parsed(String text) {
            return java.util.UUID.fromString(text);
        }
    }

    /** An id named from bytes. */
    public static final class Named {
        private Named() {
        }

        public static java.util.UUID named(byte[] name) {
            return java.util.UUID.nameUUIDFromBytes(name);
        }
    }
}
