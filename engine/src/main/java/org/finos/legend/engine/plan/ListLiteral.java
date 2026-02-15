// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a list/array literal in SQL.
 * Maps Pure ArrayLiteral to DuckDB's native list type: [elem1, elem2, ...]
 *
 * Example:
 * Pure: [1, 2, 3]
 * SQL: [1, 2, 3]
 */
public record ListLiteral(List<Expression> elements) implements Expression {

    public ListLiteral {
        Objects.requireNonNull(elements, "Elements cannot be null");
    }

    /**
     * Factory method for creating a list literal.
     */
    public static ListLiteral of(List<Expression> elements) {
        return new ListLiteral(elements);
    }

    /**
     * Factory method for creating a list literal from varargs.
     */
    public static ListLiteral of(Expression... elements) {
        return new ListLiteral(List.of(elements));
    }

    /**
     * Returns true if this list is empty.
     */
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    /**
     * Returns the number of elements in this list.
     */
    public int size() {
        return elements.size();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public PureType type() {
        return PureType.LIST;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0)
                sb.append(", ");
            sb.append(elements.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
