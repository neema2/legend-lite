package com.gs.legend.serializer;

import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.model.def.ProfileDefinition;
import com.gs.legend.model.PureModelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase B round-trip and golden-fixture coverage.
 *
 * <p>Each element kind gets:
 * <ul>
 *   <li><b>Round-trip:</b> serialize → deserialize → equals on the record</li>
 *   <li><b>Golden fixture:</b> serialize the element parsed from the smoke corpus
 *     and compare byte-for-byte to a hand-written fixture file. This locks the
 *     schema — any drift in field order, indentation, or whitespace trips the test.</li>
 * </ul>
 *
 * <p>Rollout is incremental (see {@code docs/BAZEL_IMPLEMENTATION_PLAN.md §3.3}).
 * Right now only {@code enum} and {@code profile} kinds are covered. New element
 * kinds add matching test cases here alongside the fixture file.
 */
class ElementSerializerTest {

    // ==================== Enum ====================

    @Test
    @DisplayName("Enum round-trips via serialize → deserialize")
    void enumRoundTrips() {
        var original = EnumDefinition.of("refdata::Rating", "AAA", "AA", "A", "BBB", "BB", "B");

        String json = ElementSerializer.serialize(original);
        PackageableElement restored = ElementDeserializer.deserialize(json);

        assertEquals(original, restored, "Enum must round-trip to an equal record");
    }

    @Test
    @DisplayName("Enum serialize matches hand-written golden fixture byte-for-byte")
    void enumMatchesGoldenFixture() {
        var rating = extractEnum("refdata::Rating");
        String actual = ElementSerializer.serialize(rating);
        String expected = loadResource("bazel_smoke/refdata/elements/refdata__Rating.json");
        assertEquals(expected.trim(), actual.trim(),
                "Serialized enum must match the golden fixture — schema drift detected");
    }

    @Test
    @DisplayName("Enum serialization is idempotent (second pass matches first pass)")
    void enumSerializationIsIdempotent() {
        var original = EnumDefinition.of("refdata::Rating", "AAA", "AA", "A");
        String pass1 = ElementSerializer.serialize(original);
        PackageableElement restored = ElementDeserializer.deserialize(pass1);
        String pass2 = ElementSerializer.serialize(restored);
        assertEquals(pass1, pass2, "Serialization must be idempotent across round-trips");
    }

    // ==================== Profile ====================

    @Test
    @DisplayName("Profile round-trips via serialize → deserialize")
    void profileRoundTrips() {
        var original = new ProfileDefinition(
                "refdata::RefDataProfile",
                List.of("rootEntity", "referenceData"),
                List.of("description", "domain"));

        String json = ElementSerializer.serialize(original);
        PackageableElement restored = ElementDeserializer.deserialize(json);

        assertEquals(original, restored, "Profile must round-trip to an equal record");
    }

    @Test
    @DisplayName("Profile serialize matches hand-written golden fixture byte-for-byte")
    void profileMatchesGoldenFixture() {
        var profile = extractProfile("refdata::RefDataProfile");
        String actual = ElementSerializer.serialize(profile);
        String expected = loadResource("bazel_smoke/refdata/elements/refdata__RefDataProfile.json");
        assertEquals(expected.trim(), actual.trim(),
                "Serialized profile must match the golden fixture — schema drift detected");
    }

    @Test
    @DisplayName("Profile with empty stereotypes/tags round-trips")
    void profileWithEmptyListsRoundTrips() {
        var original = new ProfileDefinition("pkg::Empty", List.of(), List.of());
        String json = ElementSerializer.serialize(original);
        var restored = (ProfileDefinition) ElementDeserializer.deserialize(json);
        assertEquals(List.of(), restored.stereotypes());
        assertEquals(List.of(), restored.tags());
    }

    // ==================== Schema guard rails ====================

    @Test
    @DisplayName("Unknown schemaVersion fails fast")
    void unknownSchemaVersionThrows() {
        String bogus = """
                {
                  "schemaVersion": 999,
                  "kind": "enum",
                  "fqn": "pkg::X",
                  "values": ["A"]
                }""";
        var ex = assertThrows(IllegalArgumentException.class,
                () -> ElementDeserializer.deserialize(bogus));
        assertTrue(ex.getMessage().contains("schema version"),
                "Error message must mention schema version; got: " + ex.getMessage());
    }

    @Test
    @DisplayName("Unknown element kind fails with a plan-doc pointer")
    void unknownKindThrows() {
        String bogus = """
                {
                  "schemaVersion": 1,
                  "kind": "nebula",
                  "fqn": "pkg::X"
                }""";
        var ex = assertThrows(UnsupportedOperationException.class,
                () -> ElementDeserializer.deserialize(bogus));
        assertTrue(ex.getMessage().contains("nebula"),
                "Error should name the unknown kind; got: " + ex.getMessage());
    }

    @Test
    @DisplayName("Filename helper turns FQN into double-underscore form")
    void filenameForUsesDoubleUnderscores() {
        assertEquals("refdata__Rating.json", ElementSerializer.filenameFor("refdata::Rating"));
        assertEquals("a__b__c__Foo.json", ElementSerializer.filenameFor("a::b::c::Foo"));
    }

    // ==================== Test helpers ====================

    /**
     * Parses the smoke corpus and returns the requested enum. Ensures golden fixtures
     * track the actual parser output rather than what we <i>think</i> the parser produces.
     */
    private static EnumDefinition extractEnum(String fqn) {
        PureModelBuilder builder = loadSmokeRefdata();
        return builder.findEnum(fqn)
                .orElseThrow(() -> new AssertionError("Enum not found in smoke corpus: " + fqn));
    }

    private static ProfileDefinition extractProfile(String fqn) {
        PureModelBuilder builder = loadSmokeRefdata();
        ProfileDefinition profile = builder.getProfile(fqn);
        if (profile == null) throw new AssertionError("Profile not found in smoke corpus: " + fqn);
        return profile;
    }

    private static PureModelBuilder loadSmokeRefdata() {
        return new PureModelBuilder().addSource(loadResource("bazel_smoke/refdata/model.pure"));
    }

    private static String loadResource(String path) {
        try (InputStream is = ElementSerializerTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Resource not on classpath: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load resource " + path, e);
        }
    }
}
