package com.gs.legend.ast;

/**
 * Root of all Pure expression AST nodes.
 *
 * <p>
 * This sealed interface mirrors the legend-engine protocol
 * {@code org.finos.legend.engine.protocol.pure.m3.valuespecification.ValueSpecification}
 * hierarchy, using Java records for immutability and pattern matching.
 *
 * <p>
 * The parser produces ONLY these types — no semantic interpretation,
 * no type checking, no SQL generation. All function calls are generic
 * {@link AppliedFunction} nodes; the compiler is responsible for interpreting
 * them.
 *
 * <h3>Type hierarchy</h3>
 * 
 * <pre>
 * ValueSpecification
 * ├── AppliedFunction        — function application: filter(), project(), etc.
 * ├── AppliedProperty        — property access: $x.name
 * ├── PackageableElementPtr  — element reference: Person, store::MyDb
 * ├── TypeAnnotation          — type annotation: @Integer, @Relation<(...)>, @?
 * ├── ClassInstance           — DSL extension: ColSpec, ColSpecArray, GraphFetchTree
 * ├── Collection             — list: [a, b, c]
 * ├── Variable               — variable reference: $x
 * ├── LambdaFunction         — lambda: {x | $x.name}
 * ├── CInteger, CFloat, CDecimal, CString, CBoolean
 * ├── CDateTime, CStrictDate, CStrictTime, CLatestDate
 * ├── CByteArray
 * ├── EnumValue              — enum: JoinKind.INNER
 * └── UnitInstance            — unit: 5 kilogram
 * </pre>
 */
public sealed interface ValueSpecification permits
        AppliedFunction,
        AppliedProperty,
        PackageableElementPtr,
        TypeAnnotation,
        ClassInstance,
        PureCollection,
        Variable,
        LambdaFunction,
        CInteger,
        CFloat,
        CDecimal,
        CString,
        CBoolean,
        CDateTime,
        CStrictDate,
        CStrictTime,
        CLatestDate,
        CByteArray,
        EnumValue,
        UnitInstance {
}
