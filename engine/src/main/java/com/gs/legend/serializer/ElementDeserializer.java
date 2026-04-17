package com.gs.legend.serializer;

import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.model.def.ProfileDefinition;

/**
 * Deserializes element JSON (Phase B v1 schema) back into the record form
 * produced by the parser.
 *
 * <p>Strict on {@code schemaVersion} — unknown versions throw immediately so
 * we never silently misinterpret a future schema.
 *
 * <p>Strict on {@code kind} — unknown or missing kind throws. Incremental
 * rollout: kinds not yet implemented here throw
 * {@link UnsupportedOperationException} with a pointer to the plan doc.
 */
public final class ElementDeserializer {

    private ElementDeserializer() { }

    public static PackageableElement deserialize(String json) {
        Json.Obj root = Json.parseObject(json);
        int version = root.getInt("schemaVersion");
        if (version != ElementSerializer.SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported element schema version: " + version
                            + " (this deserializer understands v" + ElementSerializer.SCHEMA_VERSION + ")");
        }
        String kind = root.getString("kind");
        return switch (kind) {
            case "enum" -> deserializeEnum(root);
            case "profile" -> deserializeProfile(root);
            default -> throw new UnsupportedOperationException(
                    "ElementDeserializer does not yet support kind=\"" + kind + "\""
                            + " — see docs/BAZEL_IMPLEMENTATION_PLAN.md §3.3 for the rollout plan");
        };
    }

    private static EnumDefinition deserializeEnum(Json.Obj root) {
        String fqn = root.getString("fqn");
        var values = root.getStringArray("values");
        return new EnumDefinition(fqn, values);
    }

    private static ProfileDefinition deserializeProfile(Json.Obj root) {
        String fqn = root.getString("fqn");
        var stereotypes = root.getStringArray("stereotypes");
        var tags = root.getStringArray("tags");
        return new ProfileDefinition(fqn, stereotypes, tags);
    }
}
