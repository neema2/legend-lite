// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.model.ModelFromProtocol;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ParseException;
import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A text as the compiler's model (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 8's last step): the one parser reads the
 * text into its records ({@link PmcdParser}: the engine's section walk and import scoping, at every dialect), and the
 * compiler's one door ({@link ModelFromProtocol}) makes them its model -- the same door a model given as JSON goes
 * through. The parser builds records and nothing else; a text meets the compiler's model here, so neither package
 * depends on the other (ArchitectureTest, invariant 7c).
 */
public final class ModelFromText {

    private ModelFromText() {
    }

    /** THE parse entry -- ONE way in: the caller NAMES the level (provenance routing); there is no default and no
     *  named-entry synonym. {@link Dialect#LEGEND_LITE} is the product surface, {@link Dialect#LEGEND_ENGINE} the
     *  drop-in/parity surface, {@link Dialect#LEGEND_PLATFORM} the bootstrap/m2 superset (naming it in production
     *  trips the guardrail; the platform's own declarations are its consumers). */
    public static ParsedModel parse(String source, Dialect dialect) {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        PmcdParser.Parsed parsed = PmcdParser.parse(Objects.requireNonNull(source, "source"),
                Objects.requireNonNull(dialect, "dialect"));
        List<ParsedModel.UnclaimedSection> unclaimed = new ArrayList<>();
        for (PmcdParser.UnclaimedSection u : parsed.unclaimed()) {
            unclaimed.add(new ParsedModel.UnclaimedSection(u.name(), u.startOffset(), u.endOffset()));
        }
        try {
            return ModelFromProtocol.of(parsed.model(), source, unclaimed);
        } catch (ModelFromProtocol.Unrepresentable u) {
            throw parseError(u);
        }
    }

    /** One element the parser read from a text ({@code ElementParser.parseSingle}: the IDE's slice of one), as the
     *  compiler's model. */
    public static PackageableElement element(Protocol.Element parsed) {
        // the final audit of 9af16fbe58's J5 (2026-11-14): this entry asked nothing
        com.legend.base.RunScope.planning("read a parsed element into the model (phase B)");
        try {
            return ModelFromProtocol.element(Objects.requireNonNull(parsed, "parsed"));
        } catch (ModelFromProtocol.Unrepresentable u) {
            throw parseError(u);
        }
    }

    /** A shape the compiler's model cannot represent is the text's error, at the element (every element parsed from
     *  a text has its span: one without is the parser's bug, never placed at 1:1). */
    private static ParseException parseError(ModelFromProtocol.Unrepresentable u) {
        ParsedModel.Position at = u.at();
        if (at == null) {
            throw new IllegalStateException(u.path() + " was parsed without a span: " + u.reason(), u);
        }
        return new ParseException(u.reason(), at.line(), at.column());
    }
}
