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
    void aVersionsFilesAreReadWithoutItsEntities_aFileItCannotReadRefusedByNameWhenTheyAreAskedFor() {
        ReleasedVersions r = new ReleasedVersions(new CoreGrammar());
        r.add("demo", "bad", "1.0.0", List.of(new ReleasedVersions.File("bad.pure", "Class demo::X { ]")));
        ArtifactSource.Release v = r.release("demo", "bad", "1.0.0");
        // its files and its hash, as the site gives them: a page reading its model's text never parses it
        assertEquals("bad.pure", ((Json.Obj) ((Json.Arr) Json.parse(v.filesJson())).items().get(0)).getString("path"));
        assertTrue(r.hash("demo", "bad", "1.0.0") != null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, v::entitiesJson);
        assertTrue(e.getMessage().contains("version 1.0.0 of demo:bad: bad.pure is not read"), e.getMessage());
        // Depot answers the entities with the refusal, by name
        Depot.Response entities = new Depot(r, () -> 0L).handle("GET", "/projects/demo/bad/versions/1.0.0", null);
        assertEquals(500, entities.status(), entities.body());
        assertTrue(entities.body().contains("bad.pure is not read"), entities.body());
    }

    @Test
    void aWorkspaceStartsFromAReleasedVersion_eachElementAFileOfItsOwn_itsHashRecorded() {
        ReleasedVersions r = released();
        Sdlc sdlc = new Sdlc(new MemoryStorage(), "me", "Me", new CoreGrammar(), () -> 0L);
        sdlc.released(r);
        // the person's project at the release's coordinates made with it: none was there
        Sdlc.Response made = sdlc.handle("POST", "/projects/demo:desk/workspaces/elsewhere", "{\"releasedVersion\":\"1.0.0\"}");
        assertEquals(200, made.status(), made.body());
        Json.Obj from = Json.parseObject(made.body()).getObj("releasedFrom");
        assertEquals("1.0.0", from.getString("versionId"));
        assertEquals(r.hash("demo", "desk", "1.0.0"), from.getString("hash"));
        assertEquals(false, from.getBool("changed"));
        // each element its own file: its section's header, then its text from the release's file, byte for byte
        Sdlc.Response pure = sdlc.handle("GET", "/projects/demo:desk/workspaces/elsewhere/pure", null);
        assertEquals(200, pure.status(), pure.body());
        java.util.Map<String, String> texts = new java.util.TreeMap<>();
        for (Json.Node n : ((Json.Arr) Json.parse(pure.body())).items()) texts.put(((Json.Obj) n).getString("path"), ((Json.Obj) n).getString("pureCode"));
        assertEquals(java.util.Set.of("demo::Trade", "demo::DB", "demo::OnDeephaven"), texts.keySet());
        assertEquals("###Pure\nClass demo::Trade { qty: Integer[1]; }\n", texts.get("demo::Trade"));
        assertEquals("###Relational\nDatabase demo::DB ( Table TRADE (QTY INTEGER) )\n", texts.get("demo::DB"));
        assertTrue(texts.get("demo::OnDeephaven").startsWith("###Connection\nRelationalDatabaseConnection demo::OnDeephaven\n"), texts.get("demo::OnDeephaven"));
        assertTrue(texts.get("demo::OnDeephaven").endsWith("keyVaultReference: 'deephaven.psk'; };\n}\n"), texts.get("demo::OnDeephaven"));
        // its entities read from those files, as any workspace's
        Sdlc.Response entities = sdlc.handle("GET", "/projects/demo:desk/workspaces/elsewhere/entities", null);
        assertEquals(200, entities.status(), entities.body());
        assertEquals(3, ((Json.Arr) Json.parse(entities.body())).items().size());

        // started again: refused, by name; a version the site does not release: refused, by name
        Sdlc.Response again = sdlc.handle("POST", "/projects/demo:desk/workspaces/elsewhere", "{\"releasedVersion\":\"1.0.0\"}");
        assertEquals(409, again.status(), again.body());
        Sdlc.Response none = sdlc.handle("POST", "/projects/demo:desk/workspaces/other", "{\"releasedVersion\":\"9.9.9\"}");
        assertEquals(404, none.status(), none.body());
        assertTrue(none.body().contains("is not a version this model home's site releases"), none.body());

        // the site changes the version under the same number: the workspace says so
        r.add("demo", "desk", "1.0.0", List.of(new ReleasedVersions.File("desk/model.pure", MODEL + "\n")));
        Sdlc.Response view = sdlc.handle("GET", "/projects/demo:desk/workspaces/elsewhere", null);
        assertEquals(true, Json.parseObject(view.body()).getObj("releasedFrom").getBool("changed"), view.body());
    }

    @Test
    void anElementWhoseSectionImportsWhatTheCompilerDoesNotIsRefusedByName() {
        ReleasedVersions r = new ReleasedVersions(new CoreGrammar());
        r.add("demo", "imports", "1.0.0", List.of(new ReleasedVersions.File("m.pure", """
                ###Pure
                import other::*;
                Class demo::A { b: B[1]; }
                """)));
        Sdlc sdlc = new Sdlc(new MemoryStorage(), "me", "Me", new CoreGrammar(), () -> 0L);
        sdlc.released(r);
        Sdlc.Response refused = sdlc.handle("POST", "/projects/demo:imports/workspaces/w", "{\"releasedVersion\":\"1.0.0\"}");
        assertEquals(400, refused.status(), refused.body());
        assertTrue(refused.body().contains("demo::A's section imports other::*"), refused.body());
        // the compiler's own imports are dropped, the element read the same
        r.add("demo", "imports", "2.0.0", List.of(new ReleasedVersions.File("m.pure", """
                ###Pure
                import meta::pure::metamodel::type::*;
                Class demo::A { b: String[1]; }
                """)));
        Sdlc.Response made = sdlc.handle("POST", "/projects/demo:imports/workspaces/w", "{\"releasedVersion\":\"2.0.0\"}");
        assertEquals(200, made.status(), made.body());
    }

    @Test
    void depotAnswersAReleasedVersionBesideThePersonsOwn_andThePersonsNeverTakesItsCoordinates() {
        ReleasedVersions r = released();
        Sdlc sdlc = new Sdlc(new MemoryStorage(), "me", "Me", new CoreGrammar(), () -> 0L);
        sdlc.released(r);
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
