package com.legend.sdlc;

import com.legend.depot.ArtifactSource;
import com.legend.depot.Depot;
import com.legend.json.Json;
import com.legend.sdlc.grammar.CoreGrammar;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The versions a site releases (docs/ONE_CONNECTION_DESK_2026_10_28.md D5): read from its files, entities at lite's
 * dialect -- a file holding a Deephaven connection an entity like any other -- answered by Depot before the SDLC's own,
 * and never a person's version at their coordinates.
 */
class ReleasedVersionsTest {
    /** A site's file of several elements, one of lite's own words among them (D1). */
    private static final String MODEL = """
            Class demo::Trade { qty: Integer[1]; }
            ###Relational
            Database demo::DB ( Table TRADE (QTY INTEGER) )
            ###Connection
            RelationalDatabaseConnection demo::OnDeephaven
            {
              store: demo::DB;
              type: Deephaven;
              specification: Deephaven { host: 'localhost'; port: 10000; useTls: true; };
              auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
            }
            """;

    private static ReleasedVersions released() {
        ReleasedVersions r = new ReleasedVersions(new CoreGrammar());
        r.add("demo", "desk", "1.0.0", List.of(new ReleasedVersions.File("desk/model.pure", MODEL)));
        return r;
    }

    @Test
    void aVersionsEntitiesAreReadFromItsFilesAtLitesDialect() {
        ArtifactSource.Release v = released().release("demo", "desk", "1.0.0");
        List<Json.Node> entities = ((Json.Arr) Json.parse(v.entitiesJson())).items();
        assertEquals(List.of("demo::Trade", "demo::DB", "demo::OnDeephaven"),
                entities.stream().map((e) -> ((Json.Obj) e).getString("path")).toList());
        Json.Obj connection = ((Json.Obj) entities.get(2)).getObj("content").getObj("connectionValue");
        assertEquals("deephaven", connection.getObj("datasourceSpecification").getString("_type"));
        assertEquals("preSharedKey", connection.getObj("authenticationStrategy").getString("_type"));
        // its files as the site gives them
        Json.Obj file = (Json.Obj) ((Json.Arr) Json.parse(v.filesJson())).items().get(0);
        assertEquals("desk/model.pure", file.getString("path"));
        assertEquals(MODEL, file.getString("pureCode"));
    }

    @Test
    void aVersionsHashIsItsFiles() {
        ReleasedVersions r = released();
        String before = r.hash("demo", "desk", "1.0.0");
        r.add("demo", "desk", "1.0.0", List.of(new ReleasedVersions.File("desk/model.pure", MODEL + "\n")));
        assertNotEquals(before, r.hash("demo", "desk", "1.0.0"));
        assertNull(r.hash("demo", "desk", "2.0.0"));
    }

    @Test
    void aFileItCannotReadIsRefusedByName() {
        ReleasedVersions r = new ReleasedVersions(new CoreGrammar());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                r.add("demo", "bad", "1.0.0", List.of(new ReleasedVersions.File("bad.pure", "Class demo::X { ]"))));
        assertTrue(e.getMessage().contains("bad.pure is not read"), e.getMessage());
    }

    @Test
    void depotAnswersAReleasedVersionBesideThePersonsOwn_andThePersonsNeverTakesItsCoordinates() {
        ReleasedVersions r = released();
        Sdlc sdlc = new Sdlc(new MemoryStorage(), "me", "Me", new CoreGrammar(), () -> 0L);
        sdlc.releasedAt(r::holds);
        Sdlc.Response made = sdlc.handle("POST", "/projects",
                "{\"name\":\"mine\",\"description\":\"\",\"groupId\":\"demo\",\"artifactId\":\"mine\"}");
        assertEquals(200, made.status(), made.body());
        Depot depot = new Depot(r.over(sdlc.artifacts()), () -> 0L);
        Depot.Response projects = depot.handle("GET", "/project-configurations", null);
        assertTrue(projects.body().contains("\"artifactId\":\"desk\"") && projects.body().contains("\"artifactId\":\"mine\""), projects.body());
        Depot.Response versions = depot.handle("GET", "/projects/demo/desk/versions", null);
        assertEquals(200, versions.status(), versions.body());
        assertTrue(versions.body().contains("1.0.0"), versions.body());

        // the person's copy of the desk at the coordinates the site releases: refused, by name
        Sdlc.Response copy = sdlc.handle("POST", "/projects",
                "{\"name\":\"desk\",\"description\":\"\",\"groupId\":\"demo\",\"artifactId\":\"desk\"}");
        assertEquals(200, copy.status(), copy.body());
        Sdlc.Response major = sdlc.handle("POST", "/projects/demo:desk/versions", "{\"versionType\":\"MAJOR\"}");
        assertEquals(409, major.status(), major.body());
        assertTrue(major.body().contains("is a version this model home's site releases"), major.body());
        Sdlc.Response patch = sdlc.handle("POST", "/projects/demo:desk/versions", "{\"versionType\":\"PATCH\"}");
        assertEquals(200, patch.status(), patch.body());
    }
}
