package com.gs.legend.parser;

import com.gs.legend.compiler.BuiltinRegistry;
import com.gs.legend.compiler.PType;
import com.gs.legend.model.def.FunctionDefinition;
import com.gs.legend.model.def.ImportScope;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.model.m3.Multiplicity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests for {@link NameResolver}'s structural walkers. Each test targets a specific path
 * where simple names could leak past resolution to downstream FQN-only consumers like
 * {@code ModelContext.findType}.
 */
class NameResolverTest {

    /**
     * Regression test for the {@code PType.RelationTypeVar} column-walking bug.
     *
     * <p>Inline relation types in user function signatures (e.g.,
     * {@code Relation<(name:String[1], age:Integer[1])>}) parse into
     * {@code PType.Parameterized("Relation", [PType.RelationTypeVar(columns=[Column("age",
     * PType.Concrete("Integer"), ONE), ...])])}. Before the audit fix, {@code resolvePType}
     * treated {@code RelationTypeVar} as a leaf and did not walk its columns — simple type
     * names inside leaked past NameResolver and caused {@code Unknown type: 'Integer'}
     * downstream.
     */
    @Test
    void resolvesConcreteNamesInsideRelationTypeVarColumns() {
        // Build: function test::foo(r: Relation<(age:Integer[1])>[1]):Integer[1]
        PType relationColType = new PType.Concrete("Integer");
        PType.RelationTypeVar rtv = new PType.RelationTypeVar(List.of(
                new PType.RelationTypeVar.Column("age", relationColType, Multiplicity.ONE)));
        PType parsedType = new PType.Parameterized("Relation", List.of(rtv));

        var param = new FunctionDefinition.ParameterDefinition(
                "r", "Relation", 1, 1, null, parsedType);
        var funcDef = new FunctionDefinition(
                "test::foo",
                List.of(param),
                "Integer",   // returnType — simple name, will also get FQN'd
                1, 1,
                "$r->size()"); // body (any non-blank string; NameResolver doesn't parse it)

        var imports = new ImportScope();
        for (String fqn : BuiltinRegistry.BUILTIN_IMPORTS) {
            imports.addImport(fqn);
        }
        Set<String> knownFqns = Set.copyOf(BuiltinRegistry.BUILTIN_IMPORTS);

        List<PackageableElement> resolved = NameResolver.resolveDefinitions(
                List.of(funcDef), imports, knownFqns);

        var resolvedFunc = (FunctionDefinition) resolved.get(0);
        var resolvedParam = resolvedFunc.parameters().get(0);
        var resolvedParameterized = assertInstanceOf(PType.Parameterized.class, resolvedParam.parsedType());
        var resolvedRtv = assertInstanceOf(PType.RelationTypeVar.class, resolvedParameterized.typeArgs().get(0));
        var resolvedCol = resolvedRtv.columns().get(0);
        var resolvedColType = assertInstanceOf(PType.Concrete.class, resolvedCol.type());

        assertEquals(
                "meta::pure::metamodel::type::Integer",
                resolvedColType.name(),
                "RelationTypeVar column type should be resolved to FQN (was simple 'Integer' before audit fix)");
    }
}
