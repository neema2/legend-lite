package com.legend.parser;

import com.legend.lexer.TokenType;

/**
 * The kinds of top-level packageable elements the shallow scanner
 * recognises. There is exactly one constant per top-level keyword that
 * {@link ElementParser} dispatches on.
 *
 * <p>Used by {@link ModelIndexer} to label each range it records, and by
 * {@link ModelOrchestrator} to dispatch single-element deep parsing back
 * into the appropriate {@link ElementParser} entry point.
 */
public enum ElementKind {
    CLASS,
    ASSOCIATION,
    ENUM,
    PROFILE,
    FUNCTION,
    SERVICE,
    RUNTIME,
    CONNECTION,
    DATABASE,
    MAPPING;

    /**
     * Map a leading {@link TokenType} to its element kind, or {@code null}
     * if the token does not begin a top-level element.
     *
     * <p>{@code NATIVE} is mapped to {@link #CLASS} because a {@code native}
     * declaration is just a flavour of class; the indexer treats the
     * {@code native} keyword as a one-token prefix on the element.
     *
     * <p>{@code SINGLE_CONNECTION_RUNTIME} is mapped to {@link #RUNTIME}
     * because it is the same top-level shape ({@code Runtime name { ... }})
     * with an additional inline connection.
     */
    public static ElementKind fromHeaderToken(TokenType t) {
        return switch (t) {
            case CLASS, NATIVE -> CLASS;
            case ASSOCIATION -> ASSOCIATION;
            case ENUM -> ENUM;
            case PROFILE -> PROFILE;
            case FUNCTION -> FUNCTION;
            case SERVICE -> SERVICE;
            case RUNTIME, SINGLE_CONNECTION_RUNTIME -> RUNTIME;
            case RELATIONAL_DATABASE_CONNECTION -> CONNECTION;
            case DATABASE -> DATABASE;
            case MAPPING -> MAPPING;
            default -> null;
        };
    }
}
