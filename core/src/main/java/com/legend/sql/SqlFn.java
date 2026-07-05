package com.legend.sql;

/**
 * THE semantic-function vocabulary of the SQL IR — the {@code CoreFn} of the
 * back half. Every {@link SqlExpr.Call} names one of these; every dialect
 * renders ALL of them (an exhaustive switch, enforced by javac — adding a
 * semantic function without a spelling in every dialect is a compile error,
 * not a runtime surprise). Lowering emits MEANING; dialects own spelling.
 */
public enum SqlFn {
    // Boolean composition
    AND, OR, NOT,
    // Comparison
    EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
    // Arithmetic (DIVIDE forces float; MOD is always-positive; REM keeps sign)
    PLUS, MINUS, TIMES, DIVIDE, MOD, REM, NEGATE, ABS,
    // Null / membership
    IS_NULL, IS_NOT_NULL, IN, COALESCE,
    // Strings
    CONCAT, LENGTH, UPPER, LOWER, GREATEST, LEAST,
    // Lists — SEMANTIC operations (encodings are the dialect's: DuckDB uses
    // list lambdas; Postgres would use unnest subqueries; SQLite json_each).
    // LIST_EXISTS/LIST_FOR_ALL carry (collection, predicate-lambda) and
    // include Pure's empty-collection semantics (exists([])=false,
    // forAll([])=true) — the dialect's expansion must honor them.
    LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_CONTAINS, LIST_GET,
    LIST_EXISTS, LIST_FOR_ALL,
    // UNNEST: explode a collection into rows; PLACEMENT (select-list vs
    // LATERAL FROM) is dialect assembly.
    UNNEST,
    // Variant navigation: logical JSON access; text-extraction idioms are rendering
    VARIANT_ELEMENTS, VARIANT_GET
}
