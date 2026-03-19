package com.gs.legend.ast;

/** Boolean literal. Example: {@code true} or {@code false} */
public record CBoolean(boolean value) implements ValueSpecification {
}
