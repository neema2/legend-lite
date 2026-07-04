package com.legend.compiler.element;

/**
 * Sealed parent of the two <em>nominal</em> element kinds &mdash;
 * {@link TypedClass} and {@link TypedEnum} &mdash; i.e. the user-declared types
 * that a {@link com.legend.compiler.element.type.Type.ClassType} /
 * {@link com.legend.compiler.element.type.Type.EnumType} reference resolves to
 * by FQN.
 *
 * <p>Class and enum are kept as distinct records (the distinction is
 * irreducible: consumers branch on it for exhaustiveness, navigation, and
 * storage mapping &mdash; see this package's {@code package-info}). This common
 * parent exists so that:
 *
 * <ul>
 *   <li><b>shared handling is unified</b> &mdash; code that treats "any nominal
 *       type" identically (lookup, hashing, caching, the kind manifest) can
 *       accept a {@code TypedNominal} rather than {@code TypedElement};</li>
 *   <li><b>a {@code switch} over the two is exhaustiveness-checked by javac</b>
 *       &mdash; adding a third nominal kind forces every such switch to handle
 *       it, with no stringly-typed {@code kind} flag and no {@code default}
 *       fallback (AGENTS.md invariant 4).</li>
 * </ul>
 */
public sealed interface TypedNominal extends TypedElement
        permits TypedClass, TypedEnum {
}
