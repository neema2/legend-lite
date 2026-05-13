package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * Sealed expression tree for relational contexts: join conditions, filter
 * conditions, view column expressions, and mapping property RHS expressions.
 *
 * <p>This is a <strong>different sub-grammar</strong> from Pure value
 * expressions (which need {@code SpecParser} in Phase C and stay as raw text
 * until then &mdash; decision D-1). Relational expressions are small and
 * structurally local; they are parsed eagerly in Phase B.
 *
 * <p>Grammar mapped to Pure's {@code dbOperation} rule hierarchy:
 * <pre>
 *   dbOperation        &rarr; dbJoinOperation | dbBooleanOperation
 *   dbBooleanOperation &rarr; dbAtomicOperation (("and"|"or") dbOperation)?
 *   dbAtomicOperation  &rarr; group | functionCall | columnRef | joinNavigation
 *                          | targetColumn | constant
 *                          (comparison | isNull | isNotNull)?
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.RelationalOperation} for
 * the Database-context variants. Mapping-context variants
 * ({@code ScopeBlock}, {@code EnumTransform}, {@code BindingTransform}) will
 * be added to {@code permits} in sub-slice B.4b when the corresponding
 * mapping property parsing lands.
 */
public sealed interface RelationalOperation
        permits RelationalOperation.ColumnRef,
                RelationalOperation.TargetColumnRef,
                RelationalOperation.Literal,
                RelationalOperation.FunctionCall,
                RelationalOperation.Comparison,
                RelationalOperation.BooleanOp,
                RelationalOperation.IsNull,
                RelationalOperation.IsNotNull,
                RelationalOperation.Group,
                RelationalOperation.ArrayLiteral,
                RelationalOperation.JoinNavigation {

    /**
     * A column reference.
     *
     * <p>Two shapes are valid, both fully-qualified:
     * <ul>
     *   <li><strong>Qualified:</strong> {@code TABLE.COLUMN} or
     *       {@code [DB] TABLE.COLUMN}.</li>
     *   <li><strong>Three-part:</strong> {@code SCHEMA.TABLE.COLUMN} &mdash;
     *       the parser folds the schema into {@code table} as
     *       {@code "SCHEMA.TABLE"}.</li>
     * </ul>
     *
     * <p><strong>No implicit-table form.</strong> Bare column identifiers
     * (e.g. {@code IS_ACTIVE} alone) are <em>not</em> represented in the AST.
     * In Database-context expressions (Filter / Join / MultiGrainFilter / view
     * filter), engine rejects bare identifiers at parse time with
     * {@code "Missing table or alias for column 'X'"}; core/ matches that
     * behavior. In Mapping-context expressions (class mapping property RHS),
     * engine resolves bare identifiers at parse time using the enclosing class
     * mapping's main table; B.4b will do the same.
     *
     * <p>Result: every {@code ColumnRef} in the parser output has a
     * non-{@code null} {@code table}.
     *
     * <p><strong>Parse-time database contract.</strong>
     * {@code databaseName} is non-null <em>iff</em> the database is
     * unambiguous from parse-time information &mdash; specifically:
     * <ul>
     *   <li>the user wrote an explicit {@code [DB]} qualifier, OR</li>
     *   <li>this is a mapping-context bare identifier and resolves
     *       unambiguously to the enclosing class mapping's main
     *       table (which carries its own database).</li>
     * </ul>
     * In every other case (notably a qualified {@code T.COL} without
     * an explicit {@code [DB]} in either Database or Mapping context),
     * {@code databaseName} is {@code null} at parse time. The reason
     * is that {@code T} may live in the enclosing scope's database or
     * in any of its includes; the database is resolved by Phase D
     * using the enclosing element's scope. This matches FINOS engine,
     * which fills in {@code TableAlias.database} during binding, not
     * parsing.
     *
     * @param databaseName the database fully resolved at parse time, or
     *                     {@code null} when resolution requires
     *                     scope-walk through includes (Phase D)
     * @param table        the table name (or folded {@code "SCHEMA.TABLE"});
     *                     never {@code null}
     * @param column       the column name; never {@code null}
     */
    record ColumnRef(String databaseName, String table, String column) implements RelationalOperation {
        public ColumnRef {
            Objects.requireNonNull(table, "Table cannot be null");
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /** Self-join target column: {@code &lcub;target&rcub;.COLUMN}. */
    record TargetColumnRef(String column) implements RelationalOperation {
        public TargetColumnRef {
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /**
     * A literal value: string, integer (stored as {@code Long}), or floating
     * point (stored as {@code Double}). The {@code value} field carries the
     * boxed Java representation so a single record covers all three.
     */
    record Literal(Object value) implements RelationalOperation {
        public Literal {
            Objects.requireNonNull(value, "Literal value cannot be null");
        }
        public static Literal string(String s) { return new Literal(s); }
        public static Literal integer(long n)  { return new Literal(n); }
        public static Literal decimal(double d) { return new Literal(d); }
    }

    /** Function call: {@code name(arg, arg, ...)}. */
    record FunctionCall(String name, List<RelationalOperation> args) implements RelationalOperation {
        public FunctionCall {
            Objects.requireNonNull(name, "Function name cannot be null");
            args = args != null ? List.copyOf(args) : List.of();
        }
    }

    /**
     * Binary comparison: {@code left op right} where {@code op} is a
     * canonical {@link ComparisonOp}. Source-form synonyms ({@code =} vs
     * {@code ==}, {@code !=} vs {@code <>}) are collapsed at parse time so
     * downstream consumers see a closed enum.
     */
    record Comparison(RelationalOperation left, ComparisonOp op, RelationalOperation right) implements RelationalOperation {
        public Comparison {
            Objects.requireNonNull(left, "Left cannot be null");
            Objects.requireNonNull(op, "Op cannot be null");
            Objects.requireNonNull(right, "Right cannot be null");
        }
        public static Comparison eq(RelationalOperation l, RelationalOperation r) {
            return new Comparison(l, ComparisonOp.EQ, r);
        }
    }

    /**
     * Boolean combination: {@code left op right} where {@code op} is one
     * of {@link LogicalOp#AND} or {@link LogicalOp#OR}.
     */
    record BooleanOp(RelationalOperation left, LogicalOp op, RelationalOperation right) implements RelationalOperation {
        public BooleanOp {
            Objects.requireNonNull(left, "Left cannot be null");
            Objects.requireNonNull(op, "Op cannot be null");
            Objects.requireNonNull(right, "Right cannot be null");
        }
    }

    /** {@code expr is null}. */
    record IsNull(RelationalOperation operand) implements RelationalOperation {
        public IsNull {
            Objects.requireNonNull(operand, "Operand cannot be null");
        }
    }

    /** {@code expr is not null}. */
    record IsNotNull(RelationalOperation operand) implements RelationalOperation {
        public IsNotNull {
            Objects.requireNonNull(operand, "Operand cannot be null");
        }
    }

    /** Parenthesised grouping: {@code ( expr )}. */
    record Group(RelationalOperation inner) implements RelationalOperation {
        public Group {
            Objects.requireNonNull(inner, "Inner cannot be null");
        }
    }

    /** Array literal: {@code [e1, e2, ...]} as it appears inside function args. */
    record ArrayLiteral(List<RelationalOperation> elements) implements RelationalOperation {
        public ArrayLiteral {
            elements = elements != null ? List.copyOf(elements) : List.of();
        }
    }

    /**
     * A join navigation chain: {@code [DB]@J1 > (LEFT) @J2 ... | terminal}.
     *
     * @param databaseName the explicit {@code [DB]} qualifier on the outermost
     *                     hop, or {@code null} if none was written
     * @param chain        the chain of hops in source order; never empty
     * @param terminal     optional terminal expression after the {@code |}
     *                     marker; {@code null} when omitted
     */
    record JoinNavigation(
            String databaseName,
            List<JoinChainElement> chain,
            RelationalOperation terminal) implements RelationalOperation {
        public JoinNavigation {
            Objects.requireNonNull(chain, "Chain cannot be null");
            if (chain.isEmpty()) throw new IllegalArgumentException("Join chain cannot be empty");
            chain = List.copyOf(chain);
        }
    }
}
