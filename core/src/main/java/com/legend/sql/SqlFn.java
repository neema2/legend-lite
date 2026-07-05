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
    // Lists
    LIST_FILTER, LIST_TRANSFORM, LIST_REDUCE, LIST_CONCAT, LIST_CONTAINS,
    LIST_BOOL_OR, LIST_BOOL_AND, LIST_EXTRACT, WRAP_LIST, UNNEST,
    // Variant navigation: -> (JSON access) and ->> (text extraction, pre-CAST)
    VARIANT_ELEMENTS, VARIANT_GET, VARIANT_GET_TEXT
}
