package com.legend.parser.spec;

/**
 * Root of all Pure expression AST nodes &mdash; the output of
 * {@link com.legend.parser.SpecParser}.
 *
 * <p>Sealed hierarchy mirroring the engine protocol
 * {@code org.finos.legend.engine.protocol.pure.m3.valuespecification.ValueSpecification},
 * implemented as Java records for immutability and pattern matching. Record
 * names and field names match the engine's verbatim (see
 * {@code core/README.md} &sect; "Element / Spec symmetry"); this maximises
 * test-corpus portability and lets downstream layers swap between core's
 * standalone parser output and the engine's protocol shapes by mechanical
 * renaming.
 *
 * <h2>Scope &mdash; C.1 (leaf expressions)</h2>
 * The {@code permits} clause currently lists only the variants emitted by
 * {@link com.legend.parser.SpecParser} as of Phase C.1: literals,
 * {@link Variable}, and {@link PureCollection}. Subsequent phases extend
 * the clause:
 * <ul>
 *   <li><strong>C.2</strong> &mdash; {@code AppliedProperty}, {@code AppliedFunction}</li>
 *   <li><strong>C.3</strong> &mdash; operator-desugared {@code AppliedFunction},
 *       {@code NewInstance}</li>
 *   <li><strong>C.4</strong> &mdash; {@code LambdaFunction}, code-block
 *       statement carrier (TBD)</li>
 *   <li><strong>C.5</strong> &mdash; milestoning carriers, {@code TypeAnnotation},
 *       {@code PackageableElementPtr}, {@code EnumValue}, {@code CDecimal},
 *       {@code CByteArray}, {@code UnitInstance}, {@code ColumnInstance}</li>
 * </ul>
 *
 * <p>Adding a variant in a later phase is an additive edit to this
 * {@code permits} clause plus a new record file in this package; existing
 * pattern matches surface the addition as an unhandled-case compiler
 * warning (or error in switch expressions), which is the desired behaviour.
 *
 * <h2>Pure data contract</h2>
 * The parser produces ONLY these types. No semantic interpretation, no
 * type checking, no name resolution. Function calls are generic
 * {@code AppliedFunction} nodes regardless of native vs user-defined
 * status &mdash; the compiler interprets them. Variables carry their
 * source-level name (without the {@code $} prefix); typing arrives later
 * if/when introduced via lambda parameter annotations.
 */
public sealed interface ValueSpecification permits
        AppliedFunction,
        AppliedProperty,
        CBoolean,
        CDate,
        CDecimal,
        CFloat,
        CInteger,
        CLatestDate,
        CString,
        CTime,
        ColumnInstance,
        EnumValue,
        LambdaFunction,
        NewInstance,
        NewInstanceCast,
        PackageableElementPtr,
        PureCollection,
        TypeAnnotation,
        Variable {
}
