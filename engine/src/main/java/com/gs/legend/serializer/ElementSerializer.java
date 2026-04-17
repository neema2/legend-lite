package com.gs.legend.serializer;

import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.model.def.ProfileDefinition;
import com.gs.legend.util.Json;

/**
 * Serializes {@link PackageableElement}s to the Phase B v1 JSON schema.
 *
 * <p>Every element serializes to a self-contained JSON document keyed by
 * {@code schemaVersion}, {@code kind}, and {@code fqn} at the top level —
 * see {@code docs/BAZEL_IMPLEMENTATION_PLAN.md §3.2} for the schema.
 *
 * <p>Output is deterministic and pretty-printed so fixture diffs are reviewable
 * and golden byte-equality tests are meaningful.
 *
 * <p><b>Incremental rollout:</b> this class is being built up one element kind
 * at a time. Unsupported kinds throw {@link UnsupportedOperationException};
 * {@link #supports(PackageableElement)} tells callers what's available.
 */
public final class ElementSerializer {

    /** Schema version emitted by this serializer. Bumped on incompatible changes. */
    public static final int SCHEMA_VERSION = 1;

    private ElementSerializer() { }

    /** Returns true if the serializer has an implementation for this element kind. */
    public static boolean supports(PackageableElement element) {
        return element instanceof EnumDefinition || element instanceof ProfileDefinition;
    }

    /** Serializes an element to its JSON text. */
    public static String serialize(PackageableElement element) {
        Json.Writer w = Json.writer();
        w.beginObject();
        w.field("schemaVersion", SCHEMA_VERSION);
        writeKindAndPayload(w, element);
        w.endObject();
        return w.toString();
    }

    private static void writeKindAndPayload(Json.Writer w, PackageableElement element) {
        if (element instanceof EnumDefinition e) {
            w.field("kind", "enum");
            w.field("fqn", e.qualifiedName());
            w.fieldStringArray("values", e.values());
        } else if (element instanceof ProfileDefinition p) {
            w.field("kind", "profile");
            w.field("fqn", p.qualifiedName());
            w.fieldStringArray("stereotypes", p.stereotypes());
            w.fieldStringArray("tags", p.tags());
        } else {
            throw new UnsupportedOperationException(
                    "ElementSerializer does not yet support " + element.getClass().getSimpleName()
                            + " — see docs/BAZEL_IMPLEMENTATION_PLAN.md §3.3 for the rollout plan");
        }
    }

    /** Canonical filename for an element on disk: {@code pkg__sub__Name.json}. */
    public static String filenameFor(String fqn) {
        return fqn.replace("::", "__") + ".json";
    }
}
