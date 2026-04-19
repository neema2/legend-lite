package com.gs.legend.compiler;

import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.def.ProfileDefinition;
import com.gs.legend.model.def.StereotypeApplication;
import com.gs.legend.model.m3.Property;
import com.gs.legend.model.m3.PureClass;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage B of phase 2.5e: the {@code meta::pure::profiles::equality} profile with stereotype
 * {@code Key} is shipped as a platform builtin so legend-pure's equality stereotypes on
 * {@code Pair<U,V>} and {@code List<T>} properties (landed in commit 2c) canonicalize cleanly
 * without requiring user imports.
 */
class EqualityProfileTest {

    @Test
    void platformProfileIsRegisteredInBuiltinRegistry() {
        ProfileDefinition prof = BuiltinRegistry.findPlatformProfile(
                "meta::pure::profiles::equality").orElseThrow();
        assertEquals("meta::pure::profiles::equality", prof.qualifiedName());
        assertTrue(prof.stereotypes().contains("Key"),
                "equality profile should declare the Key stereotype, got: " + prof.stereotypes());
    }

    @Test
    void equalityProfileIsSeededIntoFreshBuilder() {
        // The profile must be registered in every new PureModelBuilder instance
        // alongside platform enums, so downstream lookups by FQN or simple name succeed.
        PureModelBuilder builder = new PureModelBuilder();
        ProfileDefinition seeded = builder.getProfile("meta::pure::profiles::equality");
        assertNotNull(seeded,
                "equality profile should be registered in a fresh PureModelBuilder");
        assertTrue(seeded.stereotypes().contains("Key"));
    }

    @Test
    void stereotypeApplicationCanonicalizesToEqualityProfileFqn() {
        // End-to-end: a user-authored `<<equality.Key>>` on a property must resolve through
        // the import scope to the full FQN by the time it's stored on Property.
        String source = """
                Class model::HasKey
                {
                    <<equality.Key>> id: String[1];
                    name: String[1];
                }
                """;
        PureModelBuilder builder = new PureModelBuilder().addSource(source);
        PureClass hasKey = builder.findClass("model::HasKey").orElseThrow();

        Property idProp = hasKey.findLocalProperty("id").orElseThrow();
        StereotypeApplication app = idProp.stereotypes().stream()
                .filter(s -> "Key".equals(s.stereotypeName()))
                .findFirst().orElseThrow(() -> new AssertionError(
                        "Expected Key stereotype on 'id', got: " + idProp.stereotypes()));

        assertEquals("meta::pure::profiles::equality", app.profileName(),
                "Stereotype profile should canonicalize to FQN via BUILTIN_IMPORTS");
    }

    @Test
    void hasStereotypeHelperReflectsPropagatedStereotype() {
        // Property.hasStereotype() is the ergonomic check downstream code will use
        // (e.g. when computing structural-equality keys from Pair / List).
        String source = """
                Class model::HasKey
                {
                    <<equality.Key>> id: String[1];
                    name: String[1];
                }
                """;
        PureClass hasKey = new PureModelBuilder().addSource(source)
                .findClass("model::HasKey").orElseThrow();

        Property id = hasKey.findLocalProperty("id").orElseThrow();
        Property name = hasKey.findLocalProperty("name").orElseThrow();

        assertTrue(id.hasStereotype("meta::pure::profiles::equality", "Key"));
        assertFalse(name.hasStereotype("meta::pure::profiles::equality", "Key"));
    }
}
