// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

/**
 * A GraphQL document the translator cannot spell as Pure (docs/GRAPHQL_STUDY_2026_10_18.md §6): INVALID when the
 * document is wrong against the schema in a way the validator leaves to the translator (a clause with no key, a
 * variable the shape depends on and has no value), UNSUPPORTED when a construct of the line is not built yet (the
 * message names the landing that builds it). The island seam and the endpoint turn each kind into the error shape
 * its reader expects.
 */
public final class GqlTranslationException extends RuntimeException {

    public enum Kind { INVALID, UNSUPPORTED }

    private final Kind kind;
    private final String path;

    public GqlTranslationException(Kind kind, String path, String message) {
        super(message);
        this.kind = kind;
        this.path = path;
    }

    public Kind kind() {
        return kind;
    }

    /** The response path of the selection the problem is at ({@code Person.employees}), empty at the root. */
    public String path() {
        return path;
    }

    public static GqlTranslationException invalid(String path, String message) {
        return new GqlTranslationException(Kind.INVALID, path, message);
    }

    public static GqlTranslationException unsupported(String path, String message) {
        return new GqlTranslationException(Kind.UNSUPPORTED, path, message);
    }
}
