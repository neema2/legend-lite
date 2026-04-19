package com.gs.legend.compiler;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

/**
 * Complete type signature of a Pure expression: type + multiplicity.
 * Every expression produces exactly one of these.
 *
 * <p>
 * In Pure, {@code Integer[1]} is a type ({@code Integer}) plus a multiplicity ({@code [1]}).
 * This record bundles them so they cannot be set independently — they always travel together.
 *
 * <p>
 * Examples:
 * <pre>
 * ExpressionType(INTEGER, ONE)                       — 3 + 4
 * ExpressionType(STRING, MANY)                       — ['a', 'b', 'c']
 * ExpressionType(ClassType("Person"), MANY)          — Person.all()
 * ExpressionType(Relation(schema), MANY)             — Person.all()->project(~name)
 * ExpressionType(INTEGER, ZERO_ONE)                  — [1,2,3]->head()
 * </pre>
 *
 * @param type         The Pure type (Integer, Relation, ClassType, etc.)
 * @param multiplicity The cardinality ([1], [*], [0..1], [1..*])
 */
public record ExpressionType(Type type, Multiplicity multiplicity) {

    public ExpressionType {
        java.util.Objects.requireNonNull(type, "ExpressionType.type must not be null");
        java.util.Objects.requireNonNull(multiplicity, "ExpressionType.multiplicity must not be null");
    }

    // ===== Convenience factories =====

    /** Scalar single value: type[1]. */
    public static ExpressionType one(Type type) {
        return new ExpressionType(type, Multiplicity.ONE);
    }

    /** Optional single value: type[0..1]. */
    public static ExpressionType zeroOrOne(Type type) {
        return new ExpressionType(type, Multiplicity.ZERO_ONE);
    }

    /** Collection: type[*]. */
    public static ExpressionType many(Type type) {
        return new ExpressionType(type, Multiplicity.MANY);
    }

    /** One or more: type[1..*]. */
    public static ExpressionType oneMany(Type type) {
        return new ExpressionType(type, Multiplicity.ONE_MANY);
    }

    // ===== Derived checks =====

    /** True if this is a relational (tabular) expression. */
    public boolean isRelation() {
        return type instanceof Type.Relation;
    }

    /** True if this is a scalar expression (not relational). */
    public boolean isScalar() {
        return !(type instanceof Type.Relation);
    }

    /** True if multiplicity allows multiple values. */
    public boolean isMany() {
        return multiplicity.isMany();
    }

    /** True if this is a singular optional value. */
    public boolean isOptional() {
        return multiplicity.equals(Multiplicity.ZERO_ONE);
    }

    /** Extract the Type.Schema if this is a Relation, otherwise null. */
    public com.gs.legend.model.m3.Type.Schema schema() {
        return type instanceof Type.Relation r ? r.schema() : null;
    }

    @Override
    public String toString() {
        return type.typeName() + multiplicity;
    }
}
