package com.gs.legend.ast;

import java.math.BigDecimal;

/** Decimal literal. Example: {@code 3.14159265358979323846} */
public record CDecimal(BigDecimal value) implements ValueSpecification {
}
