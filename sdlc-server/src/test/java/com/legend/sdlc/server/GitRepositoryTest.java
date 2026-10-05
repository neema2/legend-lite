package com.legend.sdlc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.sdlc.CoreGrammar;
import com.legend.sdlc.Sdlc;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectChecker;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.ObjectWalk;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The repository sdlc-server writes by hand ({@code Git}, {@code GitStorage}) is git's: JGit -- the library
 * legend-sdlc's own file-system backend is built on -- reads every object, refs, history and files back, and its
 * strict {@link ObjectChecker} finds nothing wrong. JGit is the judge, not the machine's git: no host program in a
 * test (the Bazel program's ruling, 2026-10-04).
 */
class GitRepositoryTest {
    private static final String P = "org.finos.lite.git:judged";

    @TempDir
    Path dir;

    @Test
    void jgitReadsWhatTheServerWrote() throws IOException {
        AtomicLong clock = new AtomicLong(1_759_000_000_000L);
        Sdlc sdlc = new Sdlc(new GitStorage(dir), "local", "Local User", new CoreGrammar(), () -> clock.addAndGet(1000));
        String p = "/projects/" + URLEncoder.encode(P, StandardCharsets.UTF_8);
        ok(sdlc, "POST", "/projects", "{\"name\":\"Judged\",\"description\":\"\",\"groupId\":\"org.finos.lite.git\",\"artifactId\":\"judged\"}");
        ok(sdlc, "POST", p + "/workspaces/w1", null);
        save(sdlc, p, "add the party", "CREATE", "demo::party::Person", "// a person\\nClass demo::party::Person\\n{\\n  name: String[1];\\n}\\n");
        save(sdlc, p, "email", "CREATE", "demo::party::Email", "Class demo::party::Email\\n{\\n  address: String[1];\\n}\\n");
        // a review committed: a merge commit with two parents on the line, then a version: a tag
        ok(sdlc, "POST", p + "/workspaces/side", null);
        save(sdlc, p, "side", "side", "CREATE", "demo::party::Side", "Class demo::party::Side {}\\n");
        String review = Json.parseObject(ok(sdlc, "POST", p + "/reviews", "{\"workspaceId\":\"side\",\"title\":\"side\",\"description\":\"\"}")).getString("id");
        ok(sdlc, "POST", p + "/reviews/" + review + "/commit", "{\"message\":\"merge side\"}");
        ok(sdlc, "POST", p + "/versions", "{\"versionType\":\"MINOR\"}");

        try (Repository repo = new FileRepositoryBuilder().setGitDir(dir.toFile()).setMustExist(true).build()) {
            // GitStorage's layout: refs/heads/<group>/<artifact>/{master, workspace/<user>/<id>}, a version a tag
            // refs/tags/<group>/<artifact>/release-<v>; the review's workspace was deleted when it was committed
            String heads = Constants.R_HEADS + "org.finos.lite.git/judged/";
            String tags = Constants.R_TAGS + "org.finos.lite.git/judged/";
            List<Ref> refs = repo.getRefDatabase().getRefs();
            assertEquals(List.of(heads + "master", heads + "workspace/local/w1", tags + "release-0.1.0"),
                    refs.stream().map(Ref::getName).sorted().toList());

            // fsck: every object reachable from every ref is there, hashes to its id, and passes the strict checker
            ObjectChecker checker = new ObjectChecker();
            int objects = 0;
            try (ObjectWalk walk = new ObjectWalk(repo); ObjectInserter.Formatter format = new ObjectInserter.Formatter()) {
                for (Ref ref : refs) walk.markStart(walk.parseAny(ref.getObjectId()));
                List<RevObject> all = new ArrayList<>();
                for (RevCommit c; (c = walk.next()) != null; ) all.add(c);
                for (RevObject o; (o = walk.nextObject()) != null; ) all.add(o);
                for (RevObject o : all) {
                    ObjectLoader loader = repo.open(o, o.getType());
                    byte[] raw = loader.getCachedBytes();
                    assertEquals(o.getId(), format.idFor(o.getType(), raw), "object hashes to its id: " + o.name());
                    checker.check(o, o.getType(), raw);
                    objects++;
                }
            }
            assertTrue(objects > 10, "objects judged: " + objects);

            // history and files, as git log and git show read them
            Ref workspace = repo.exactRef(heads + "workspace/local/w1");
            try (RevWalk walk = new RevWalk(repo)) {
                walk.markStart(walk.parseCommit(workspace.getObjectId()));
                List<String> history = new ArrayList<>();
                for (RevCommit c : walk) history.add(c.getShortMessage());
                assertEquals(List.of("email", "add the party", "Build project structure"), history);

                RevCommit head = walk.parseCommit(workspace.getObjectId());
                assertEquals("local", head.getAuthorIdent().getName());
                try (TreeWalk file = TreeWalk.forPath(repo, "demo/party/Person.pure", head.getTree())) {
                    assertNotNull(file, "demo/party/Person.pure in the workspace's tree");
                    String text = new String(repo.open(file.getObjectId(0), Constants.OBJ_BLOB).getBytes(), StandardCharsets.UTF_8);
                    assertTrue(text.startsWith("// a person\nClass demo::party::Person"), text);
                }
            }

            // the review's commit on the line: a merge of the line and the workspace
            try (RevWalk walk = new RevWalk(repo)) {
                RevCommit merge = walk.parseCommit(repo.exactRef(heads + "master").getObjectId());
                assertEquals(2, merge.getParentCount(), merge.getFullMessage());
                assertEquals("merge side", merge.getShortMessage());
                ObjectId version = repo.exactRef(tags + "release-0.1.0").getObjectId();
                assertEquals(merge.getId(), version, "the version names the line's head");
            }
        }
    }

    private static void save(Sdlc sdlc, String p, String message, String type, String path, String code) {
        save(sdlc, p, "w1", message, type, path, code);
    }

    private static void save(Sdlc sdlc, String p, String workspace, String message, String type, String path, String code) {
        ok(sdlc, "POST", p + "/workspaces/" + workspace + "/pureChanges", "{\"message\":\"" + message + "\",\"changes\":[{\"type\":\""
                + type + "\",\"path\":\"" + path + "\",\"pureCode\":\"" + code + "\"}]}");
    }

    private static String ok(Sdlc sdlc, String method, String target, String body) {
        Sdlc.Response r = sdlc.handle(method, target, body);
        assertTrue(r.status() < 300, method + " " + target + ": " + r.status() + " " + r.body());
        return r.body() == null ? "" : r.body();
    }
}
