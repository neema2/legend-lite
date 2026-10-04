package com.legend.sdlc.server;

import com.legend.base.Nullable;
import com.legend.sdlc.Storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * {@link Storage} as a real git repository on disk (design S16's local backend): the rules' objects are
 * git's own (their ids are git's), so they are written as git loose objects and the refs as git refs --
 * {@code git --git-dir=<dir> log} reads the history, {@code git push} sends it anywhere. No git binary and
 * no JGit: a loose object is a zlib stream of {@code "<kind> <length>\0<content>"}.
 *
 * <ul>
 *   <li>{@code obj/<id>} → {@code objects/<2>/<38>};</li>
 *   <li>{@code ref/<group>:<artifact>/<name>} → {@code refs/heads/<group>/<artifact>/<name>}, and a
 *       version ({@code version/<v>}) → {@code refs/tags/<group>/<artifact>/<v>};</li>
 *   <li>{@code project/<id>} → {@code legend/projects/<id>.json} (the SDLC's own record of a project,
 *       beside git's files).</li>
 * </ul>
 * A bare repository: {@code <dir>/HEAD}, {@code objects/}, {@code refs/}. One writer (the server
 * serializes calls); each file written to a temporary name, then moved into place.
 */
public final class GitStorage implements Storage {
    private final Path dir;

    public GitStorage(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir.resolve("objects"));
            Files.createDirectories(dir.resolve("refs/heads"));
            Files.createDirectories(dir.resolve("refs/tags"));
            Files.createDirectories(dir.resolve("legend/projects"));
            if (!Files.exists(dir.resolve("HEAD"))) write(dir.resolve("HEAD"), "ref: refs/heads/master\n".getBytes(StandardCharsets.UTF_8));
            if (!Files.exists(dir.resolve("config"))) {
                write(dir.resolve("config"), "[core]\n\trepositoryformatversion = 0\n\tbare = true\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public @Nullable String get(String key) {
        Path file = file(key);
        if (!Files.exists(file)) return null;
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (key.startsWith("obj/")) return fromLoose(inflate(bytes));
            String text = new String(bytes, StandardCharsets.UTF_8);
            return key.startsWith("ref/") ? text.trim() : text;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(String key, String value) {
        try {
            if (key.startsWith("obj/")) {
                Path file = file(key);
                if (!Files.exists(file)) write(file, deflate(toLoose(value)));
            } else if (key.startsWith("ref/")) {
                write(file(key), (value + "\n").getBytes(StandardCharsets.UTF_8));
            } else {
                write(file(key), value.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            Files.deleteIfExists(file(key));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<String> keys(String prefix) {
        // every key lives under a fixed directory per kind: walk it and keep those with the prefix
        List<String> out = new ArrayList<>();
        try {
            if (prefix.startsWith("project/")) {
                Path base = dir.resolve("legend/projects");
                try (Stream<Path> files = Files.walk(base, 2)) {
                    files.filter(Files::isRegularFile).forEach(f -> {
                        String name = f.getFileName().toString();
                        if (name.endsWith(".json") && f.getParent() != null && !f.getParent().equals(base)) {
                            out.add("project/" + f.getParent().getFileName() + ":" + name.substring(0, name.length() - 5));
                        }
                    });
                }
            } else if (prefix.startsWith("ref/")) {
                for (String root : List.of("refs/heads", "refs/tags")) {
                    Path base = dir.resolve(root);
                    try (Stream<Path> files = Files.walk(base)) {
                        files.filter(Files::isRegularFile).forEach(f -> {
                            String key = keyOfRef(root, base.relativize(f).toString().replace('\\', '/'));
                            if (key != null) out.add(key);
                        });
                    }
                }
            } else if (!prefix.startsWith("obj/")) {
                Path base = dir.resolve("legend/records");
                if (Files.isDirectory(base)) {
                    try (Stream<Path> files = Files.walk(base)) {
                        files.filter(Files::isRegularFile).forEach(f -> {
                            String rel = base.relativize(f).toString().replace('\\', '/');
                            if (rel.endsWith(".json")) out.add(rel.substring(0, rel.length() - 5).replace('~', ':'));
                        });
                    }
                }
            } else {
                try (Stream<Path> files = Files.walk(dir.resolve("objects"))) {
                    files.filter(Files::isRegularFile).forEach(f -> {
                        Path parent = f.getParent();
                        if (parent != null) out.add("obj/" + parent.getFileName() + f.getFileName());
                    });
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<String> kept = new ArrayList<>();
        for (String k : out) if (k.startsWith(prefix)) kept.add(k);
        Collections.sort(kept);
        return kept;
    }

    // ---- key ↔ file ----

    private Path file(String key) {
        if (key.startsWith("obj/")) {
            String id = key.substring(4);
            return dir.resolve("objects").resolve(id.substring(0, 2)).resolve(id.substring(2));
        }
        if (key.startsWith("ref/")) {
            String rest = key.substring(4);
            int slash = rest.indexOf('/');
            String project = rest.substring(0, slash).replace(':', '/');
            String name = rest.substring(slash + 1);
            // a version is a tag, named as upstream names it (`release-<v>`), under its project
            if (name.startsWith("version/")) return dir.resolve("refs/tags/" + project + "/release-" + name.substring(8));
            return dir.resolve("refs/heads/" + project + "/" + name);
        }
        if (key.startsWith("project/")) {
            // `<group>:<artifact>` as a directory and a file (`:` is not a file-name character everywhere)
            String id = key.substring(8);
            int colon = id.indexOf(':');
            return dir.resolve("legend/projects").resolve(id.substring(0, colon)).resolve(id.substring(colon + 1) + ".json");
        }
        // the SDLC's own records (reviews, version notes, counters): `<kind>/<group>:<artifact>/<name>`
        return dir.resolve("legend/records").resolve(key.replace(':', '~') + ".json");
    }

    /** {@code <group>/<artifact>/<name...>} under heads or tags back to its key, or null when not ours. */
    private static @Nullable String keyOfRef(String root, String relative) {
        String[] parts = relative.split("/", 3);
        if (parts.length < 3) return null;
        String project = parts[0] + ":" + parts[1];
        if (root.equals("refs/tags")) {
            return parts[2].startsWith("release-") ? "ref/" + project + "/version/" + parts[2].substring(8) : null;
        }
        return "ref/" + project + "/" + parts[2];
    }

    // ---- the rules' text records ↔ git's loose objects ----

    /** The rules' record ({@code blob\n...}, {@code tree\n<mode> <name> <id>\n...}, {@code commit\n...}) as git's bytes. */
    private static byte[] toLoose(String record) {
        int nl = record.indexOf('\n');
        String kind = record.substring(0, nl);
        String body = record.substring(nl + 1);
        byte[] content;
        if (kind.equals("tree")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (String line : body.split("\n")) {
                if (line.isEmpty()) continue;
                int first = line.indexOf(' ');
                int last = line.lastIndexOf(' ');
                byte[] head = (line.substring(0, first) + " " + line.substring(first + 1, last)).getBytes(StandardCharsets.UTF_8);
                out.write(head, 0, head.length);
                out.write(0);
                String id = line.substring(last + 1);
                for (int i = 0; i < 40; i += 2) out.write(Integer.parseInt(id.substring(i, i + 2), 16));
            }
            content = out.toByteArray();
        } else {
            content = body.getBytes(StandardCharsets.UTF_8);
        }
        byte[] header = (kind + " " + content.length).getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[header.length + 1 + content.length];
        System.arraycopy(header, 0, all, 0, header.length);
        System.arraycopy(content, 0, all, header.length + 1, content.length);
        return all;
    }

    private static String fromLoose(byte[] raw) {
        int zero = 0;
        while (raw[zero] != 0) zero++;
        String kind = new String(raw, 0, zero, StandardCharsets.UTF_8).split(" ")[0];
        int start = zero + 1;
        if (!kind.equals("tree")) return kind + "\n" + new String(raw, start, raw.length - start, StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder("tree\n");
        int i = start;
        while (i < raw.length) {
            int nul = i;
            while (raw[nul] != 0) nul++;
            String modeName = new String(raw, i, nul - i, StandardCharsets.UTF_8);
            StringBuilder id = new StringBuilder();
            for (int k = nul + 1; k < nul + 21; k++) id.append(String.format("%02x", raw[k] & 0xff));
            sb.append(modeName).append(' ').append(id).append('\n');
            i = nul + 21;
        }
        return sb.toString();
    }

    private static byte[] deflate(byte[] data) {
        Deflater d = new Deflater();
        d.setInput(data);
        d.finish();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        while (!d.finished()) out.write(buf, 0, d.deflate(buf));
        d.end();
        return out.toByteArray();
    }

    private static byte[] inflate(byte[] data) {
        Inflater inf = new Inflater();
        inf.setInput(data);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        try {
            while (!inf.finished()) {
                int n = inf.inflate(buf);
                if (n == 0 && inf.needsInput()) break;
                out.write(buf, 0, n);
            }
        } catch (DataFormatException e) {
            throw new IllegalStateException("a corrupt git object", e);
        } finally {
            inf.end();
        }
        return out.toByteArray();
    }

    private static void write(Path file, byte[] bytes) throws IOException {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, bytes);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
