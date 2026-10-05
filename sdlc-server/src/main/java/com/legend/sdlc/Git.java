package com.legend.sdlc;

import com.legend.base.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Git's object model over a {@link Storage}: blobs, trees and commits, named by the SHA-1 git itself
 * would give them (`git hash-object` agrees), so history written here is real git history -- a page's
 * project can be pushed into a repository as it is (design S21, "moving up is pushing"). Kept as text
 * records: {@code obj/<id>} is {@code blob\n<text>}, {@code tree\n<mode> <name> <id>\n...} or
 * {@code commit\n<git's commit text>}.
 */
final class Git {
    private final Storage storage;

    Git(Storage storage) {
        this.storage = storage;
    }

    /** A commit: no parent (a project's first), one, or two (a review's merge); times in whole seconds, UTC. */
    record Commit(String tree, List<String> parents, String author, long authorSeconds,
                  String committer, long committerSeconds, String message) {}

    /**
     * {@code head} and every commit it reaches, each once, as {@code git log} lists them: a commit always
     * before its parents, and among those ready the newest (committer time; times are whole seconds, so
     * ties are common) first.
     */
    List<String> history(String head) {
        java.util.Map<String, Commit> commits = new java.util.LinkedHashMap<>();
        java.util.Map<String, Integer> children = new java.util.HashMap<>();
        java.util.ArrayDeque<String> todo = new java.util.ArrayDeque<>();
        todo.add(head);
        while (!todo.isEmpty()) {
            String id = todo.removeFirst();
            if (commits.containsKey(id)) continue;
            Commit c = readCommit(id);
            commits.put(id, c);
            for (String parent : c.parents()) {
                children.merge(parent, 1, Integer::sum);
                todo.add(parent);
            }
        }
        // discovery order breaks time ties: a child is always found before its parents
        java.util.Map<String, Integer> found = new java.util.HashMap<>();
        for (String id : commits.keySet()) found.put(id, found.size());
        java.util.function.Function<String, Commit> at = id -> java.util.Objects.requireNonNull(commits.get(id));
        java.util.PriorityQueue<String> ready = new java.util.PriorityQueue<>((a, b) -> {
            int byTime = Long.compare(at.apply(b).committerSeconds(), at.apply(a).committerSeconds());
            return byTime != 0 ? byTime : Integer.compare(found.getOrDefault(a, 0), found.getOrDefault(b, 0));
        });
        ready.add(head);
        List<String> out = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.poll();
            out.add(id);
            for (String parent : at.apply(id).parents()) {
                if (children.merge(parent, -1, Integer::sum) == 0) ready.add(parent);
            }
        }
        return out;
    }

    /** Whether {@code ancestor} is {@code of} or reached from it. */
    boolean reaches(String of, String ancestor) {
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.ArrayDeque<String> todo = new java.util.ArrayDeque<>();
        todo.add(of);
        while (!todo.isEmpty()) {
            String id = todo.removeFirst();
            if (id.equals(ancestor)) return true;
            if (seen.add(id)) todo.addAll(readCommit(id).parents());
        }
        return false;
    }

    // ---- blobs ----

    String writeBlob(String text) {
        byte[] content = text.getBytes(StandardCharsets.UTF_8);
        String id = hash("blob", content);
        storage.put("obj/" + id, "blob\n" + text);
        return id;
    }

    String readBlob(String id) {
        return body(id, "blob");
    }

    // ---- trees ----

    /** A tree for a flat map of file paths ({@code a/b/C.pure}) to blob ids; its sub-trees written too. */
    String writeTree(Map<String, String> files) {
        TreeMap<String, Object> root = new TreeMap<>();
        for (Map.Entry<String, String> f : files.entrySet()) {
            String[] parts = f.getKey().split("/");
            TreeMap<String, Object> dir = root;
            for (int i = 0; i < parts.length - 1; i++) {
                @SuppressWarnings("unchecked")
                TreeMap<String, Object> sub = (TreeMap<String, Object>) dir.computeIfAbsent(parts[i], k -> new TreeMap<String, Object>());
                dir = sub;
            }
            dir.put(parts[parts.length - 1], f.getValue());
        }
        return writeDirectory(root);
    }

    private String writeDirectory(TreeMap<String, Object> dir) {
        // git orders entries by name, a directory's name compared as if it ended in '/'
        List<String[]> entries = new ArrayList<>(); // {mode, name, id}
        for (Map.Entry<String, Object> e : dir.entrySet()) {
            if (e.getValue() instanceof String blob) {
                entries.add(new String[] {"100644", e.getKey(), blob});
            } else {
                @SuppressWarnings("unchecked")
                TreeMap<String, Object> sub = (TreeMap<String, Object>) e.getValue();
                entries.add(new String[] {"40000", e.getKey(), writeDirectory(sub)});
            }
        }
        entries.sort((x, y) -> sortName(x).compareTo(sortName(y)));
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        StringBuilder text = new StringBuilder("tree\n");
        for (String[] e : entries) {
            byte[] head = (e[0] + " " + e[1]).getBytes(StandardCharsets.UTF_8);
            bytes.write(head, 0, head.length);
            bytes.write(0);
            byte[] raw = Sha1.unhex(e[2]);
            bytes.write(raw, 0, raw.length);
            text.append(e[0]).append(' ').append(e[1]).append(' ').append(e[2]).append('\n');
        }
        String id = hash("tree", bytes.toByteArray());
        storage.put("obj/" + id, text.toString());
        return id;
    }

    private static String sortName(String[] entry) {
        return entry[0].equals("40000") ? entry[1] + "/" : entry[1];
    }

    /** A tree as a flat map of file paths to blob ids. */
    Map<String, String> readTree(String id) {
        TreeMap<String, String> out = new TreeMap<>();
        readInto(id, "", out);
        return out;
    }

    private void readInto(String id, String prefix, Map<String, String> out) {
        String body = body(id, "tree");
        for (String line : body.split("\n")) {
            if (line.isEmpty()) continue;
            int first = line.indexOf(' ');
            int last = line.lastIndexOf(' ');
            String mode = line.substring(0, first);
            String name = line.substring(first + 1, last);
            String child = line.substring(last + 1);
            if (mode.equals("40000")) readInto(child, prefix + name + "/", out);
            else out.put(prefix + name, child);
        }
    }

    // ---- commits ----

    String writeCommit(Commit c) {
        StringBuilder text = new StringBuilder();
        text.append("tree ").append(c.tree()).append('\n');
        for (String parent : c.parents()) text.append("parent ").append(parent).append('\n');
        text.append("author ").append(c.author()).append(" <> ").append(c.authorSeconds()).append(" +0000\n");
        text.append("committer ").append(c.committer()).append(" <> ").append(c.committerSeconds()).append(" +0000\n");
        text.append('\n').append(c.message()).append('\n');
        String id = hash("commit", text.toString().getBytes(StandardCharsets.UTF_8));
        storage.put("obj/" + id, "commit\n" + text);
        return id;
    }

    boolean exists(String id) {
        return storage.get("obj/" + id) != null;
    }

    boolean isCommit(String id) {
        String record = storage.get("obj/" + id);
        return record != null && record.startsWith("commit\n");
    }

    Commit readCommit(String id) {
        String text = body(id, "commit");
        String tree = "";
        List<String> parents = new ArrayList<>();
        String author = "";
        long authorSeconds = 0;
        String committer = "";
        long committerSeconds = 0;
        int blank = text.indexOf("\n\n");
        for (String line : text.substring(0, blank).split("\n")) {
            if (line.startsWith("tree ")) tree = line.substring(5);
            else if (line.startsWith("parent ")) parents.add(line.substring(7));
            else if (line.startsWith("author ") || line.startsWith("committer ")) {
                String rest = line.substring(line.indexOf(' ') + 1);
                String name = rest.substring(0, rest.indexOf(" <"));
                String[] tail = rest.substring(rest.indexOf("> ") + 2).split(" ");
                long seconds = Long.parseLong(tail[0]);
                if (line.startsWith("author ")) { author = name; authorSeconds = seconds; }
                else { committer = name; committerSeconds = seconds; }
            }
        }
        String message = text.substring(blank + 2);
        if (message.endsWith("\n")) message = message.substring(0, message.length() - 1);
        return new Commit(tree, List.copyOf(parents), author, authorSeconds, committer, committerSeconds, message);
    }

    // ----

    private String body(String id, String kind) {
        String record = storage.get("obj/" + id);
        if (record == null || !record.startsWith(kind + "\n")) {
            throw new IllegalStateException("the SDLC lost " + kind + " " + id);
        }
        return record.substring(kind.length() + 1);
    }

    /** Git's object id: SHA-1 of {@code "<kind> <length>\0"} then the content. */
    private static String hash(String kind, byte[] content) {
        byte[] head = (kind + " " + content.length).getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[head.length + 1 + content.length];
        System.arraycopy(head, 0, all, 0, head.length);
        all[head.length] = 0;
        System.arraycopy(content, 0, all, head.length + 1, content.length);
        return Sha1.hex(Sha1.digest(all));
    }
}
