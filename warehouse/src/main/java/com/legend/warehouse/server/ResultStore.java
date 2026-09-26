package com.legend.warehouse.server;

import com.legend.base.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where finished results wait to be fetched (a statement's chunks, JSON or Arrow, already written).
 * The server holds them in memory up to a budget shared by every result; past it, a chunk goes to a
 * file under the data directory and is read from there. A result is freed when its client says it is
 * done ({@code DELETE /sql/v1/statements/{id}}), else when it expires.
 */
public final class ResultStore {

    private final Path dir;
    private final long budget;
    private final AtomicLong inMemory = new AtomicLong();
    private final AtomicLong spilled = new AtomicLong();

    public ResultStore(Path dir, long budget) throws IOException {
        this.dir = dir;
        this.budget = budget;
        // results do not outlive the process: whatever a previous run spilled is gone
        if (Files.isDirectory(dir)) {
            try (var old = Files.walk(dir)) {
                for (Path p : old.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
            }
        }
        Files.createDirectories(dir);
    }

    public long inMemory() {
        return inMemory.get();
    }

    public long spilled() {
        return spilled.get();
    }

    /** A new result's chunks, filled in order as they are written. */
    public Stored open(String id) {
        return new Stored(dir.resolve(id));
    }

    /** One result's chunks: each in memory or in a file. */
    public final class Stored {
        private final Path files;
        private final List<byte @Nullable []> held = new ArrayList<>();
        private final List<Long> sizes = new ArrayList<>();
        private boolean freed;

        private Stored(Path files) {
            this.files = files;
        }

        /** The next chunk: kept in memory while the budget allows, else written to a file. */
        public synchronized void add(byte[] chunk) {
            if (freed) throw new IllegalStateException("the result was freed");
            long n = chunk.length;
            if (inMemory.addAndGet(n) <= budget) {
                held.add(chunk);
            } else {
                inMemory.addAndGet(-n);
                try {
                    Files.createDirectories(files);
                    Files.write(files.resolve(Integer.toString(held.size())), chunk);
                } catch (IOException e) {
                    throw new UncheckedIOException("could not spill a result to " + files, e);
                }
                spilled.addAndGet(n);
                held.add(null);
            }
            sizes.add(n);
        }

        public synchronized int count() {
            return held.size();
        }

        /** Chunk {@code i}, or null when there is no such chunk (or the result was freed). */
        public synchronized byte @Nullable [] get(int i) {
            if (freed || i < 0 || i >= held.size()) return null;
            byte[] b = held.get(i);
            if (b != null) return b;
            try {
                return Files.readAllBytes(files.resolve(Integer.toString(i)));
            } catch (IOException e) {
                throw new UncheckedIOException("a spilled chunk is unreadable", e);
            }
        }

        /** Gives the memory back and deletes the files. */
        public synchronized void free() {
            if (freed) return;
            freed = true;
            for (int i = 0; i < held.size(); i++) {
                if (held.get(i) != null) inMemory.addAndGet(-sizes.get(i));
                else spilled.addAndGet(-sizes.get(i));
            }
            held.clear();
            if (Files.isDirectory(files)) {
                try (var fs = Files.list(files)) {
                    for (Path p : fs.toList()) Files.deleteIfExists(p);
                    Files.deleteIfExists(files);
                } catch (IOException e) {
                    // a file left behind is removed at the next start
                }
            }
        }
    }
}
