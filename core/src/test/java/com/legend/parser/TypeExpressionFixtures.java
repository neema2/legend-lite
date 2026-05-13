package com.legend.parser;

import com.legend.parser.TypeExpression;

import com.legend.parser.Multiplicity;

import com.legend.parser.TypeExpression.Column;
import com.legend.parser.TypeExpression.Generic;
import com.legend.parser.TypeExpression.NameRef;
import com.legend.parser.TypeExpression.Op;
import com.legend.parser.TypeExpression.RelationType;
import com.legend.parser.TypeExpression.SchemaAlgebra;
import com.legend.parser.TypeExpression.TypedParameter;

import java.util.List;

/**
 * Test-only fixtures for hand-building {@link TypeExpression} ASTs in
 * assertions.
 *
 * <p>These shorthands keep structural pins readable without polluting
 * the production AST classes with single-letter factories. Tests
 * {@code import static} from this class.
 *
 * <p>The naming is deliberately terse because tests use these heavily;
 * the only readers are people writing assertions against parser output:
 * <ul>
 *   <li>{@link #nr(String)} &mdash; {@link NameRef}</li>
 *   <li>{@link #tg(String, TypeExpression...)} &mdash; {@link Generic}</li>
 *   <li>{@link #tp(TypeExpression, Multiplicity)} &mdash; {@link TypedParameter}</li>
 *   <li>{@link #col(String, TypeExpression, Multiplicity)} &mdash; {@link Column}</li>
 *   <li>{@link #sa(TypeExpression, Op, TypeExpression)} &mdash; {@link SchemaAlgebra}</li>
 *   <li>{@link #rel(Column...)} &mdash; {@link RelationType}</li>
 * </ul>
 */
public final class TypeExpressionFixtures {

    private TypeExpressionFixtures() {}

    /** Bare named type reference, e.g. {@code nr("Integer")} or
     *  {@code nr("meta::pure::metamodel::type::String")}. */
    public static NameRef nr(String name) {
        return new NameRef(name);
    }

    /** Overload accepting a {@link com.legend.parser.element.ClassDefinition}
     *  directly &mdash; convenient for assertions that reference the
     *  {@code Pure} stdlib catalog, e.g. {@code nr(Pure.INTEGER)}. */
    public static NameRef nr(com.legend.parser.element.ClassDefinition cls) {
        return new NameRef(cls.qualifiedName());
    }

    /** Generic type application, e.g. {@code tg("List", nr("Person"))}. */
    public static Generic tg(String head, TypeExpression... args) {
        return new Generic(head, List.of(args));
    }

    /** Overload accepting a {@link com.legend.parser.element.ClassDefinition}
     *  for the head, e.g. {@code tg(Pure.RELATION, nr("T"))}. */
    public static Generic tg(com.legend.parser.element.ClassDefinition head, TypeExpression... args) {
        return new Generic(head.qualifiedName(), List.of(args));
    }

    /** A {@code Type[mult]} parameter shape used by {@link TypeExpression.FunctionType}. */
    public static TypedParameter tp(TypeExpression type, Multiplicity multiplicity) {
        return new TypedParameter(type, multiplicity);
    }

    /** A column declaration inside a {@link RelationType}, e.g.
     *  {@code col("age", nr("Integer"), Multiplicity.exactly(1))}. */
    public static Column col(String name, TypeExpression type, Multiplicity multiplicity) {
        return new Column(name, type, multiplicity);
    }

    /** Schema-algebra binary node, e.g. {@code sa(nr("T"), Op.UNION, nr("R"))}
     *  for {@code T+R}. */
    public static SchemaAlgebra sa(TypeExpression left, Op op, TypeExpression right) {
        return new SchemaAlgebra(left, op, right);
    }

    /** Relation literal type, e.g. {@code rel(col("age", nr("Integer"), one))}
     *  for the parser shape of {@code (age:Integer)}. */
    public static RelationType rel(Column... columns) {
        return new RelationType(List.of(columns));
    }
}
