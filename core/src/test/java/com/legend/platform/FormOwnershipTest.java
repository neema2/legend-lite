// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.Pure;
import com.legend.model.NativeFunctionDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The owner map is the WHOLE of qualified form dispatch (audit burn 2026-09-25:
 *  the name-tail fallback and the three alias arms are deleted). */
class FormOwnershipTest {

    /** Every catalog native spelled like a form's parse name is owned by a
     *  form — else a qualified call to it would fall to the generic path where
     *  its bare spelling would have taken the form. */
    @Test
    void everyCatalogNativeSpelledLikeAFormIsOwned() {
        Map<String, CoreFn> forms = CoreFn.parseNames();
        List<String> unowned = new ArrayList<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            String fqn = n.qualifiedName();
            String simple = fqn.substring(fqn.lastIndexOf("::") + 2);
            CoreFn form = forms.get(simple);
            if (form == null || Pure.INTERNAL_DESUGAR.contains(simple)) {
                continue;
            }
            if (CoreFn.of(fqn).isEmpty()) {
                unowned.add(fqn + " (spelled like " + form + ")");
            }
        }
        assertEquals(List.of(), unowned, "catalog natives a form's bare spelling would take, unowned by it");
    }

    /** The three former alias arms are ordinary ownership. */
    @Test
    void theFormerAliasesAreOwned() {
        assertEquals(CoreFn.EXTEND, CoreFn.of("meta::pure::tds::extend").orElseThrow());
        assertEquals(CoreFn.DISTINCT, CoreFn.of("meta::pure::tds::distinct").orElseThrow());
        assertEquals(CoreFn.EVAL, CoreFn.of("meta::pure::functions::relation::eval").orElseThrow());
        assertEquals(CoreFn.VALIDATE, CoreFn.of("meta::relational::validation::validate").orElseThrow());
    }
}
