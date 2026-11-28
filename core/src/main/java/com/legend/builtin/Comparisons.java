// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.model.FunctionId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SPIKE (slice 7, twins): THE COMPARISON FAMILIES. A comparison is one of ten families (an equality, a membership, a
 * position, a de-duplication, four orderings), and each family holds more than one function: lite's own overloads,
 * and beside each the door's function that compares by three number types and a date's written precision
 * ({@code ...AsWritten}); the equality also has legend-engine's, whose empty side is answered by where the call runs.
 *
 * <p>A pass that reads a comparison's STRUCTURE (a join's condition, a rewrite to EXISTS, a membership's list) asks a
 * family here and so treats every function of it alike. A pass that relies on lite's own meaning of the values (a
 * refusal only lite makes, lite's plan of two whole sides) names lite's functions ({@code Pure.AT_...}) and so leaves a
 * door's function alone. The lowering's rule is per function.
 */
public final class Comparisons {

    private Comparisons() {
    }

    public static final List<FunctionId> EQUAL = FunctionId.all(Pure.AT_BOOLEAN_EQUAL,
            Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN, Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN_EMPTY_BY_PLACE);
    public static final List<FunctionId> EQ = FunctionId.all(Pure.AT_BOOLEAN_EQ, Pure.AT_BOOLEAN_EQ_AS_WRITTEN);
    public static final List<FunctionId> LESS_THAN =
            FunctionId.all(Pure.AT_BOOLEAN_LESS_THAN, Pure.AT_BOOLEAN_LESS_THAN_AS_WRITTEN);
    public static final List<FunctionId> LESS_THAN_EQUAL =
            FunctionId.all(Pure.AT_BOOLEAN_LESS_THAN_EQUAL, Pure.AT_BOOLEAN_LESS_THAN_EQUAL_AS_WRITTEN);
    public static final List<FunctionId> GREATER_THAN =
            FunctionId.all(Pure.AT_BOOLEAN_GREATER_THAN, Pure.AT_BOOLEAN_GREATER_THAN_AS_WRITTEN);
    public static final List<FunctionId> GREATER_THAN_EQUAL =
            FunctionId.all(Pure.AT_BOOLEAN_GREATER_THAN_EQUAL, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL_AS_WRITTEN);
    public static final List<FunctionId> CONTAINS =
            FunctionId.all(Pure.AT_COLLECTION_CONTAINS, Pure.AT_COLLECTION_CONTAINS_AS_WRITTEN);
    public static final List<FunctionId> IN = FunctionId.all(Pure.AT_COLLECTION_IN, Pure.AT_COLLECTION_IN_AS_WRITTEN);
    public static final List<FunctionId> INDEX_OF =
            FunctionId.all(Pure.AT_COLLECTION_INDEX_OF, Pure.AT_COLLECTION_INDEX_OF_AS_WRITTEN);
    public static final List<FunctionId> REMOVE_DUPLICATES =
            FunctionId.all(Pure.AT_COLLECTION_REMOVE_DUPLICATES, Pure.AT_COLLECTION_REMOVE_DUPLICATES_AS_WRITTEN);

    /** A door's function -> lite's overload it stands beside. */
    private static final Map<FunctionId, FunctionId> LITES;
    /** Lite's overload -> the function beside it that compares by three number types and a written precision. */
    private static final Map<FunctionId, FunctionId> AS_WRITTEN;
    /** A door's function's qualified name -> lite's. */
    private static final Map<String, String> LITES_NAME;

    static {
        Map<FunctionId, FunctionId> lites = new LinkedHashMap<>();
        Map<FunctionId, FunctionId> written = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        List<List<FunctionId>> own = List.of(Pure.AT_BOOLEAN_EQUAL, Pure.AT_BOOLEAN_EQ, Pure.AT_BOOLEAN_LESS_THAN,
                Pure.AT_BOOLEAN_LESS_THAN_EQUAL, Pure.AT_BOOLEAN_GREATER_THAN, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL,
                Pure.AT_COLLECTION_CONTAINS, Pure.AT_COLLECTION_IN, Pure.AT_COLLECTION_INDEX_OF,
                Pure.AT_COLLECTION_REMOVE_DUPLICATES);
        List<List<FunctionId>> doors = List.of(Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN, Pure.AT_BOOLEAN_EQ_AS_WRITTEN,
                Pure.AT_BOOLEAN_LESS_THAN_AS_WRITTEN, Pure.AT_BOOLEAN_LESS_THAN_EQUAL_AS_WRITTEN,
                Pure.AT_BOOLEAN_GREATER_THAN_AS_WRITTEN, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL_AS_WRITTEN,
                Pure.AT_COLLECTION_CONTAINS_AS_WRITTEN, Pure.AT_COLLECTION_IN_AS_WRITTEN,
                Pure.AT_COLLECTION_INDEX_OF_AS_WRITTEN, Pure.AT_COLLECTION_REMOVE_DUPLICATES_AS_WRITTEN);
        List<String> ownNames = List.of("meta::pure::functions::boolean::equal", "meta::pure::functions::boolean::eq",
                "meta::pure::functions::boolean::lessThan", "meta::pure::functions::boolean::lessThanEqual",
                "meta::pure::functions::boolean::greaterThan", "meta::pure::functions::boolean::greaterThanEqual",
                "meta::pure::functions::collection::contains", "meta::pure::functions::collection::in",
                "meta::pure::functions::collection::indexOf", "meta::pure::functions::collection::removeDuplicates");
        for (int f = 0; f < own.size(); f++) {
            List<FunctionId> o = own.get(f);
            List<FunctionId> d = doors.get(f);
            if (o.size() != d.size()) {
                throw new IllegalStateException("a comparison family and its doors' functions differ in size: "
                        + ownNames.get(f));
            }
            for (int i = 0; i < o.size(); i++) {
                lites.put(d.get(i), o.get(i));
                written.put(o.get(i), d.get(i));
            }
            String name = ownNames.get(f);
            names.put(Pure.Lite.PKG + name.substring(name.lastIndexOf(':') + 1) + "AsWritten", name);
        }
        lites.put(Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN_EMPTY_BY_PLACE.get(0), Pure.AT_BOOLEAN_EQUAL.get(0));
        names.put(Pure.Lite.PKG + "equalAsWrittenEmptyByPlace", "meta::pure::functions::boolean::equal");
        LITES = java.util.Collections.unmodifiableMap(lites);
        AS_WRITTEN = java.util.Collections.unmodifiableMap(written);
        LITES_NAME = java.util.Collections.unmodifiableMap(names);
    }

    /** Whether {@code id} is one of lite's own overloads of the ten families: a comparison a door reads its own way. */
    public static boolean readByADoor(FunctionId id) {
        return AS_WRITTEN.containsKey(id);
    }

    /** Whether {@code id} is a door's function (either door's). */
    public static boolean aDoors(FunctionId id) {
        return LITES.containsKey(id);
    }

    /** Whether {@code id} is legend-engine's equality: its empty side is answered by where the call runs. */
    public static boolean emptySideByPlace(FunctionId id) {
        return Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN_EMPTY_BY_PLACE.contains(id);
    }

    /** Lite's overload {@code id} stands beside; {@code id} itself where it is not a door's function. */
    public static FunctionId lites(FunctionId id) {
        FunctionId own = LITES.get(id);
        return own == null ? id : own;
    }

    /** The function beside lite's overload {@code lites} that compares by three number types and a written
     *  precision. */
    public static FunctionId asWritten(FunctionId lites) {
        FunctionId door = AS_WRITTEN.get(lites);
        if (door == null) {
            throw new IllegalStateException("not a comparison a door reads its own way: " + lites);
        }
        return door;
    }

    /** As {@link #asWritten}; for the equality, legend-engine's (the empty side by where the call runs). */
    public static FunctionId asWrittenEmptySideByPlace(FunctionId lites) {
        return Pure.AT_BOOLEAN_EQUAL.contains(lites) ? Pure.AT_BOOLEAN_EQUAL_AS_WRITTEN_EMPTY_BY_PLACE.get(0)
                : asWritten(lites);
    }

    /** The qualified name a pass that recognises a comparison by its name compares against: lite's, for a door's
     *  function; {@code qualifiedName} itself otherwise. */
    public static String name(String qualifiedName) {
        String own = LITES_NAME.get(qualifiedName);
        return own == null ? qualifiedName : own;
    }
}
