package com.legend.protocol;

import com.legend.protocol.Protocol.PClass;
import com.legend.protocol.Protocol.PProperty;
import com.legend.protocol.Protocol.PSection;
import com.legend.protocol.Protocol.PSectionIndex;
import com.legend.protocol.Protocol.PureModelContextData;
import com.legend.protocol.SourceInfo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Structural facts of {@link ProtocolEmitter} that need no oracle: an absent
 * upper bound is omitted, strings escape the way Jackson escapes. The
 * byte-identity pins that lived here (17 hand-copied JSON strings, captured
 * 2026-08-04 from engine 4.133.0 and re-derived by nothing) are the seeds of
 * the LIVE differential now — {@code ProtocolSeedParityTest} in
 * parser-equivalence compares the same sources against the oracle on every
 * run (upstream boundary batch 6, 2026-09-11); core holds no wire golden.
 */
class ProtocolEmitterTest {

    /** {@code NON_NULL}: a {@code [1..*]} upper bound is null upstream and vanishes from the wire. */
    @Test
    void nullUpperBoundIsOmittedNotEmittedAsNull() {
        PClass c = new PClass("m", "C", List.of(), List.of(),
                List.of(new PProperty("xs",
                        new com.legend.protocol.TypeExpression.NameRef("String", new SourceInfo("", 1, 1, 1, 1)),
                        new com.legend.protocol.Multiplicity.Concrete(1, null),
                        List.of(), List.of(),
                        new SourceInfo("", 1, 1, 1, 1), null)),
                List.of(), List.of(), List.of(), List.of(), false, new SourceInfo("", 1, 1, 1, 1));
        String json = ProtocolEmitter.emit(new PureModelContextData(List.of(c)));

        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"multiplicity\":{\"lowerBound\":1}"),
                "an absent upper bound must be omitted entirely, not rendered as null: " + json);
        org.junit.jupiter.api.Assertions.assertFalse(json.contains("upperBound"), json);
    }

    /** Strings are escaped the way Jackson escapes them, or the bytes diverge on any quoted name. */
    @Test
    void stringEscapingMatchesJackson() {
        PClass c = new PClass("m", "A\"B\\C\nD", List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of(), List.of(), false, new SourceInfo("", 1, 1, 1, 1));
        assertEquals("\"A\\\"B\\\\C\\nD\"",
                ProtocolEmitter.emit(new PureModelContextData(List.of(c)))
                        .replaceAll(".*\"name\":(\".*?[^\\\\]\"),\"original.*", "$1"));
    }
}
