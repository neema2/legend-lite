package com.gs.legend.ast;

/** String literal. Example: {@code 'hello world'} */
public record CString(String value) implements ValueSpecification {
}
