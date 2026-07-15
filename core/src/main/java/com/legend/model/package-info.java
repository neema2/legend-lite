/**
 * Parsed declarations &mdash; output of step B (model parse).
 *
 * <p>One record per top-level packageable element kind, all implementing
 * the sealed {@link com.legend.model.PackageableElement} root.
 * Class names match engine's {@code com.gs.legend.model.def} verbatim
 * (e.g. {@code ClassDefinition}, {@code MappingDefinition},
 * {@code AssociationDefinition}, ...) so parity tests can compare
 * record-by-record.
 *
 * <p>These records are <strong>untyped</strong>: they carry the structure
 * the parser saw, with simple-name type references (resolved later by
 * {@code NameResolver}) and no semantic types attached.
 *
 * <h2>Status (Phase B.1)</h2>
 * Only {@link com.legend.model.ClassDefinition} ships so far.
 * Sub-slices B.2-B.4 add the remaining element kinds.
 */
package com.legend.model;
