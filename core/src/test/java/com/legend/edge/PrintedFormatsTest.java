// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * THE READER OF A LAST-STEP FORMAT (the edition program's step 8; the user's ruling of 2026-11-28, D7): a template is
 * read as legend-pure reads one, only to know which argument stands under which directive; a computed Float under
 * {@code %s} or {@code %r} is the one argument the door prints. Run with a database:
 * {@code UpstreamPrintsTest.aLastStepsFormatHasItsComputedFloatsPrintedInJava} and the recorded cases
 * {@code float.format.*} of {@code EdgeCasesGoldenTest}.
 */
class PrintedFormatsTest {

    private static TypedSpec last(String query) {
        List<TypedSpec> body = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                com.legend.testbackend.StorelessRuntime.with("###Pure\nClass t::Marker {}\n",
                        com.legend.database.Databases.PLATFORM)), query).body();
        return body.get(body.size() - 1);
    }

    private static List<Character> letters(String template) {
        List<PrintedFormats.Slot> slots = PrintedFormats.slots(template);
        return slots == null ? null : slots.stream().map(PrintedFormats.Slot::letter).toList();
    }

    @Test
    void aTemplatesDirectivesAreReadAsLegendPureReadsThem() {
        assertEquals(List.of('s', 'r', 'd', 'f'), letters("a %s b %r c %d d %f"));
        // a doubled percent takes no argument; a padded Integer is d, a Float with a precision F (it rounds)
        assertEquals(List.of('s'), letters("100%% of %s"));
        assertEquals(List.of('d', 'F'), letters("%05d %.2f"));
        // a date's pattern is skipped to its closing brace, a brace inside quoted text is the pattern's own
        assertEquals(List.of('t', 's'), letters("%t{yyyy-MM-dd} %s"));
        assertEquals(List.of('t', 's'), letters("%t{yyyy\"}%s\"} %s"));
        assertEquals(List.of('t'), letters("%t"));
        // the place of each directive's letter
        assertEquals(List.of(new PrintedFormats.Slot(1, 's'), new PrintedFormats.Slot(6, 'F')),
                PrintedFormats.slots("%s %.3f"));
        // not a template legend-pure reads to its end: left to the run, which refuses it
        assertNull(letters("%q"));
        assertNull(letters("50%"));
        assertNull(letters("%0x"));
        assertNull(letters("%.2"));
        assertNull(letters("%t{yyyy"));
    }

    @Test
    void aCallIsReadWhereItsTemplateIsALiteralAndItsArgumentsAreOneValueEach() {
        PrintedFormats.Read read = PrintedFormats.of(last("|format('%s is %r of %d (%s)',"
                + " [(1.0 / 3.0), (1.5e-8 * 1.0), 3, 'x'])"));
        assertNotNull(read);
        // the computed Floats under %s and %r: never the Integer, the text, a Float literal or a Float under %f
        assertEquals(List.of(0, 1), read.floats(List.of()));
        assertEquals(List.of(), PrintedFormats.of(last("|format('%s %f %.2f', [1.5, (1.0 / 3.0), (1.0 / 3.0)])"))
                .floats(List.of()));
        // a literal through its let is a literal
        List<TypedSpec> body = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                com.legend.testbackend.StorelessRuntime.with("###Pure\nClass t::Marker {}\n",
                        com.legend.database.Databases.PLATFORM)),
                "{|let x = 1.5; let y = $x * 2.0; format('%s %s', [$x, $y]);}").body();
        assertEquals(List.of(1), PrintedFormats.of(body.get(2)).floats(body.subList(0, 2)));
        // not read: fewer or more arguments than directives, a list under one directive, a template that is no
        // literal, an unknown directive, and a call that is no format
        assertNull(PrintedFormats.of(last("|format('%s %s', [(1.0 / 3.0)])")));
        assertNull(PrintedFormats.of(last("|format('%s', [(1.0 / 3.0), (2.0 / 3.0)])")));
        assertNull(PrintedFormats.of(last("{|let t = '%s'; format($t, [(1.0 / 3.0)]);}")));
        assertNull(PrintedFormats.of(last("|(1.0 / 3.0)->toString()")));
    }

    @Test
    void aTextIsPutInItsArgumentsPlaceUnderS() {
        PrintedFormats.Read read = PrintedFormats.of(last("|format('%s is %r of %d', [(1.0 / 3.0), (1.5e-8 * 1.0), 3])"));
        TypedSpec with = read.with(new java.util.TreeMap<>(Map.of(1, "0.000000015")));
        PrintedFormats.Read again = PrintedFormats.of(with);
        assertNotNull(again);
        // %r of a text would quote it: the directive is %s now, the others as written
        assertEquals("%s is %s of %d", again.template().value());
        assertEquals("0.000000015", ((TypedCString) again.args().get(1)).value());
        assertEquals(List.of(0), again.floats(List.of()));
    }
}
