/**
 * Pure value-specification AST &mdash; the output shape of
 * {@link com.legend.parser.SpecParser}.
 *
 * <p>All variants are {@code record}s implementing the sealed
 * {@link com.legend.parser.spec.ValueSpecification} root. The hierarchy
 * is closed at compile time: adding a variant requires editing the
 * {@code permits} clause of {@link com.legend.parser.spec.ValueSpecification},
 * which surfaces all consumer switches as exhaustiveness violations
 * &mdash; the desired refactor signal.
 *
 * <p>Naming follows engine's protocol records verbatim ({@code CInteger},
 * {@code LambdaFunction}, {@code AppliedFunction}, ...) so test corpora
 * port between core and engine with mechanical renames only. See
 * {@code core/README.md} &sect; "Element / Spec symmetry" for the
 * rationale.
 *
 * <p><strong>Phase status.</strong> As of C.1 this package contains
 * literal variants, {@link com.legend.parser.spec.Variable}, and
 * {@link com.legend.parser.spec.PureCollection}. C.2 adds property
 * paths and function applications; C.3 adds operators and new-instance
 * expressions; C.4 adds lambdas, {@code let}, and code blocks; C.5
 * closes out milestoning carriers and the remaining engine variants.
 */
package com.legend.parser.spec;
