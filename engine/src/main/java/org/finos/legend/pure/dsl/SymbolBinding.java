// Copyright 2026 Goldman Sachs
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

package org.finos.legend.pure.dsl;

import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.plan.Expression;

/**
 * Represents a variable binding in the symbol table.
 * 
 * Used for:
 * - ROW: Lambda row parameters (e.g., $t in filter(t | $t.col > 5))
 * - RELATION: Let-bound relations (e.g., let tds = #TDS...#; $tds->filter(...))
 * - SCALAR: Let-bound scalar values (e.g., let x = 5; $x + 1)
 */
public record SymbolBinding(String name, BindingKind kind, Object value,
        java.util.Map<String, org.finos.legend.engine.plan.GenericType> columnTypes) {

    public enum BindingKind {
        /** Row variable - value is the table alias (String) */
        ROW,
        /** Relation variable - value is RelationNode to inline */
        RELATION,
        /** Scalar variable - value is Expression */
        SCALAR
    }

    /**
     * Creates a ROW binding for lambda row parameters.
     * 
     * @param name       Variable name (e.g., "t")
     * @param tableAlias The SQL table alias (e.g., "t0")
     */
    public static SymbolBinding row(String name, String tableAlias) {
        return new SymbolBinding(name, BindingKind.ROW, tableAlias, java.util.Map.of());
    }

    /**
     * Creates a ROW binding with source relation column types.
     * Used for join/asOfJoin lambdas where each parameter carries its source schema.
     * Follows legend-engine's pattern of typing lambda parameters with RelationType.
     *
     * @param name        Variable name (e.g., "l" or "r")
     * @param tableAlias  The SQL table alias (e.g., "left_src")
     * @param columnTypes Column name → GenericType from the source relation
     */
    public static SymbolBinding row(String name, String tableAlias,
            java.util.Map<String, org.finos.legend.engine.plan.GenericType> columnTypes) {
        return new SymbolBinding(name, BindingKind.ROW, tableAlias,
                columnTypes != null ? columnTypes : java.util.Map.of());
    }

    /**
     * Creates a RELATION binding for let-bound relations.
     * 
     * @param name     Variable name (e.g., "tds")
     * @param relation The compiled RelationNode to inline
     */
    public static SymbolBinding relation(String name, RelationNode relation) {
        return new SymbolBinding(name, BindingKind.RELATION, relation, java.util.Map.of());
    }

    /**
     * Creates a SCALAR binding for let-bound scalars.
     * 
     * @param name       Variable name (e.g., "x")
     * @param expression The scalar expression value
     */
    public static SymbolBinding scalar(String name, Expression expression) {
        return new SymbolBinding(name, BindingKind.SCALAR, expression, java.util.Map.of());
    }

    /**
     * Gets the table alias for ROW bindings.
     * 
     * @throws IllegalStateException if not a ROW binding
     */
    public String tableAlias() {
        if (kind != BindingKind.ROW) {
            throw new IllegalStateException("tableAlias() called on " + kind + " binding");
        }
        return (String) value;
    }

    /**
     * Gets the relation node for RELATION bindings.
     * 
     * @throws IllegalStateException if not a RELATION binding
     */
    public RelationNode relationNode() {
        if (kind != BindingKind.RELATION) {
            throw new IllegalStateException("relationNode() called on " + kind + " binding");
        }
        return (RelationNode) value;
    }

    /**
     * Gets the expression for SCALAR bindings.
     * 
     * @throws IllegalStateException if not a SCALAR binding
     */
    public Expression scalarExpression() {
        if (kind != BindingKind.SCALAR) {
            throw new IllegalStateException("scalarExpression() called on " + kind + " binding");
        }
        return (Expression) value;
    }
}
