// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;
import com.legend.model.FunctionId;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.testing.Engine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every path of the table builder, over hand-written registrations. */
class ImplementationTableTest {

    private static final String UPPER_ID = "meta::pure::functions::string::toUpper_String_1__String_1_";
    private static final String LOWER_ID = "meta::pure::functions::string::toLower_String_1__String_1_";

    private static NativeFunctionDefinition catalog(String id) {
        return Objects.requireNonNull(Pure.nativeFunctionById(id), id);
    }

    private static List<Function> declare(String source) {
        List<Function> out = new ArrayList<>();
        for (PackageableElement e : Engine.model(source).elements()) {
            if (e instanceof Function f) {
                out.add(f);
            }
        }
        return out;
    }

    /** No registrations: bodied declarations default to Body, natives to Unimplemented. */
    private static Registrations none(List<NativeFunctionDefinition> catalog) {
        return new Registrations(catalog, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Set.of(), Map.of());
    }

    @Test
    void defaultsAreBodyForABodyAndUnimplementedForANative() {
        List<Function> ds = new ArrayList<>(List.of(catalog(UPPER_ID)));
        ds.addAll(declare("function my::pkg::twice(x:Integer[1]):Integer[1] { $x }"));
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(ds), none(List.of(catalog(UPPER_ID))));
        assertEquals(2, t.rows().size());
        assertInstanceOf(Implementation.Unimplemented.class, t.of(new FunctionId(UPPER_ID)));
        assertInstanceOf(Implementation.Body.class, t.of(new FunctionId("my::pkg::twice_Integer_1__Integer_1_")));
        assertEquals(List.of(), t.dangling());
        assertEquals(List.of(), t.conflicts());
    }

    @Test
    void aLoweringKeyIsMatchedToItsCatalogDefinitionWhole() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper),
                Map.of(Implementation.Position.SCALAR, Set.of(com.legend.model.FunctionId.of(upper))),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        Implementation row = t.of(new FunctionId(UPPER_ID));
        assertInstanceOf(Implementation.Intrinsic.class, row);
        assertEquals(Set.of(Implementation.Position.SCALAR), ((Implementation.Intrinsic) row).positions());
    }

    @Test
    void aKeyNoDeclarationHasIsDangling() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper),
                Map.of(Implementation.Position.SCALAR, Set.of(new com.legend.model.FunctionId("no::such::function_String_1__String_1_"))),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        assertEquals(1, t.dangling().size());
        assertTrue(t.dangling().get(0).contains("no::such::function"));
    }

    @Test
    void aFamilyMemberAndAFormAreReadFromTheirDefinitionsAndFqns() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        NativeFunctionDefinition lower = catalog(LOWER_ID);
        Registrations r = new Registrations(List.of(upper, lower), Map.of(), Map.of(),
                Map.of(NativeFn.Verdict.class, List.of(upper)),
                Map.of(CoreFn.FILTER, Set.of(lower.qualifiedName())),
                Map.of(), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper, lower)), r);
        Implementation u = t.of(new FunctionId(UPPER_ID));
        assertInstanceOf(Implementation.Intrinsic.class, u);
        assertEquals(Set.of(NativeFn.Verdict.class), ((Implementation.Intrinsic) u).families());
        Implementation l = t.of(new FunctionId(LOWER_ID));
        assertInstanceOf(Implementation.Form.class, l);
        assertEquals(CoreFn.FILTER, ((Implementation.Form) l).form());
        assertEquals(List.of(), t.conflicts());
    }

    @Test
    void aFormThatAlsoHasARuleKeepsItAsItsDelegate() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper),
                Map.of(Implementation.Position.SCALAR, Set.of(com.legend.model.FunctionId.of(upper))), Map.of(), Map.of(),
                Map.of(CoreFn.MAP, Set.of(upper.qualifiedName())), Map.of(), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        Implementation.Form f = (Implementation.Form) t.of(new FunctionId(UPPER_ID));
        assertEquals(Set.of(Implementation.Position.SCALAR), f.alsoLowered());
        assertEquals(List.of(), t.conflicts());
    }

    @Test
    void aRefusalBesideAnImplementationIsAConflict() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper),
                Map.of(Implementation.Position.SCALAR, Set.of(com.legend.model.FunctionId.of(upper))), Map.of(), Map.of(),
                Map.of(), Map.of(upper.qualifiedName(), "an effect"), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        assertEquals(1, t.conflicts().size());
        assertInstanceOf(Implementation.Refused.class, t.of(new FunctionId(UPPER_ID)));
    }

    @Test
    void twoFormsOnOneDeclarationIsAConflict() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper), Map.of(), Map.of(), Map.of(),
                Map.of(CoreFn.MAP, Set.of(upper.qualifiedName()), CoreFn.FILTER, Set.of(upper.qualifiedName())),
                Map.of(), Map.of(), Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        assertEquals(1, t.conflicts().size());
    }

    @Test
    void refusalsCarryTheirKindAndWalledMembersAreReportedApart() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        List<Function> ds = new ArrayList<>(List.of(upper));
        ds.addAll(declare("function my::pkg::printer(x:Integer[1]):Integer[1] { $x }"));
        Registrations r = new Registrations(List.of(upper), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(upper.qualifiedName(), "an effect"),
                Map.of("my::pkg::printer", new WalledBodies.Wall(WalledBodies.Kind.ENGINE_MACHINERY, "the printer"),
                        "my::pkg::Cls$prop$derived", new WalledBodies.Wall(WalledBodies.Kind.CANNOT_IMPLEMENT, "x")),
                Set.of(), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(ds), r);
        Implementation.Refused walledNative = (Implementation.Refused) t.of(new FunctionId(UPPER_ID));
        assertEquals(Implementation.Reason.CANNOT_IMPLEMENT, walledNative.reason());
        Implementation.Refused body = (Implementation.Refused) t.of(new FunctionId("my::pkg::printer_Integer_1__Integer_1_"));
        assertEquals(Implementation.Reason.ENGINE_MACHINERY, body.reason());
        assertEquals(List.of("my::pkg::Cls$prop$derived"), t.memberWalls());
        assertEquals(List.of(), t.dangling());
    }

    @Test
    void aSubsumedFqnNoDeclarationHasIsDangling() {
        NativeFunctionDefinition upper = catalog(UPPER_ID);
        Registrations r = new Registrations(List.of(upper), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Set.of("no::such::program"), Map.of());
        ImplementationTable t = ImplementationTable.build(DeclarationTable.of(List.of(upper)), r);
        assertEquals(List.of("subsumed no::such::program"), t.dangling());
    }
}
