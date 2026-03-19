package com.gs.legend.ast;

/** Float literal. Example: {@code 3.14} */
public record CFloat(double value) implements ValueSpecification {
}
