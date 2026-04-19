package com.gs.legend.parser;

import com.gs.legend.model.def.ImportScope;
import com.gs.legend.model.def.PackageableElement;

import java.util.List;

/**
 * Result of parsing a Pure model source — the list of element definitions plus
 * the {@link ImportScope} collected from {@code import} statements.
 *
 * <p>Returned by {@link PureModelParser#parseDefinition()}. Lives in
 * {@code com.gs.legend.parser} so downstream layers (model, cache, compiler) don't
 * have to depend on any specific parser implementation package.
 */
public record ParseResult(List<PackageableElement> definitions, ImportScope imports) {}
