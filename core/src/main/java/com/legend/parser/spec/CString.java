package com.legend.parser.spec;

import java.util.Objects;

/**
 * String literal. Example Pure source: {@code 'hello world'}.
 *
 * <p>The {@code value} is the unquoted, unescaped source text &mdash; the
 * surrounding single quotes are stripped and backslash escapes
 * ({@code \\}, {@code \'}, {@code \n}, {@code \t}, {@code \r}) are
 * resolved by the parser so consumers see the logical string content.
 */
public record CString(String value) implements ValueSpecification {
    public CString {
        Objects.requireNonNull(value, "value");
    }
}
