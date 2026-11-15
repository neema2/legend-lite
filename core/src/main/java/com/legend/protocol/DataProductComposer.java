// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;

/**
 * {@code ###DataProduct}'s DataProduct printed as lite's grammar reads it ({@code DataProductSectionGrammar}, register
 * S408) -- over the record, {@link Protocol.PDataProduct}: the forms legend-studio shows written, kept exactly, and
 * lite's own for a model group and an access point with no target. The engine's default group (its access points
 * written outside a group) prints as they were written.
 */
final class DataProductComposer {

    private DataProductComposer() {
    }

    private static final String DEFAULT_GROUP = "default";
    private static final String DEFAULT_GROUP_DESCRIPTION = "The default access group";

    static String dataProduct(Protocol.PDataProduct p, PureComposer.Style style) {
        StringBuilder b = new StringBuilder("DataProduct ")
                .append(DomainComposer.annotations(p.stereotypes(), p.taggedValues()))
                .append(Composing.elementPath(p.pkg(), p.name())).append("\n{\n");
        if (p.title() != null) {
            b.append(TAB).append("title: ").append(quoted(p.title())).append('\n');
        }
        if (p.description() != null) {
            b.append(TAB).append("description: ").append(quoted(p.description())).append('\n');
        }
        b.append(TAB).append("accessPoints: [\n");
        List<Protocol.PAccessPointGroup> groups = p.accessPointGroups();
        boolean loose = groups.size() == 1 && groups.get(0).mapping() == null
                && groups.get(0).id().equals(DEFAULT_GROUP)
                && DEFAULT_GROUP_DESCRIPTION.equals(groups.get(0).description())
                && groups.get(0).stereotypes().isEmpty();
        List<String> items = new ArrayList<>();
        if (loose) {
            for (Protocol.PLakehouseAccessPoint ap : groups.get(0).accessPoints()) {
                items.add(TAB + TAB + accessPoint(ap, style));
            }
        } else {
            for (Protocol.PAccessPointGroup g : groups) {
                items.add(group(g, style));
            }
        }
        b.append(String.join(",\n", items)).append('\n');
        b.append(TAB).append("]\n");
        return b.append('}').toString();
    }

    private static String group(Protocol.PAccessPointGroup g, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(TAB + TAB).append(g.id()).append(' ');
        b.append(DomainComposer.annotations(g.stereotypes(), List.of()));
        if (g.description() != null) {
            b.append('(').append(quoted(g.description())).append(") ");
        }
        if (g.mapping() != null) {
            b.append("Model { mapping: ").append(g.mapping()).append("; } ");
        }
        b.append("[\n");
        List<String> points = new ArrayList<>();
        for (Protocol.PLakehouseAccessPoint ap : g.accessPoints()) {
            points.add(TAB + TAB + TAB + accessPoint(ap, style));
        }
        b.append(String.join(",\n", points)).append('\n');
        return b.append(TAB).append(TAB).append(']').toString();
    }

    private static String accessPoint(Protocol.PLakehouseAccessPoint ap, PureComposer.Style style) {
        if (ap.reproducible()) {
            throw Composing.refused("the access point " + ap.id() + " is reproducible: lite knows no written form for it");
        }
        StringBuilder b = new StringBuilder(ap.id());
        if (ap.description() != null) {
            b.append(" (").append(quoted(ap.description())).append(')');
        }
        b.append(": LH(");
        if (ap.targetEnvironment() != null) {
            b.append(ap.targetEnvironment()).append(", ");
        }
        b.append(PureComposer.lambda(ap.func(), style)).append(')');
        if (ap.classification() != null) {
            b.append(ap.classification());
        }
        return b.toString();
    }

    private static String quoted(String s) {
        return Composing.convertString(s, true);
    }
}
