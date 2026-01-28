package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Array of column specifications: ~[col1, col2, col3:lambda]
 */
public record ColumnSpecArray(List<PureExpression> specs) implements PureExpression {
}
