package com.gs.legend.ast;

/** StrictTime literal (time only, no date). Example: {@code %10:30:00} */
public record CStrictTime(String value) implements ValueSpecification {
}
