// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import java.util.List;

/**
 * Classes PLANTED in the planner's package that write a file, for one test alone (2026-11-15, PR1's review):
 * {@code ArchitectureTest.thePlannerFileRuleBitesAPlantedWrite} shows {@code ArchitectureTest.thePlannerWritesNoFile}
 * fails on each. A test class: the rule's real check reads the product's classes, never this one.
 *
 * <p>ONE CLAUSE PER CLASS (the PR1 re-review's A3, 2026-11-16): the rule fails if any clause fires, so a violator that
 * writes two ways proves only that one of them is caught. Each nested class writes one way, so each clause of the rule
 * is shown to bite alone.
 */
public final class PlantedFileWrite {

    private PlantedFileWrite() {
    }

    /** Every planted violator, one per clause. */
    public static final List<Class<?>> EACH = List.of(FileOutputStreamWrite.class, FileWriterWrite.class,
            RandomAccessFileWrite.class, PrintWriterOverAFile.class, PrintWriterOverAName.class,
            PrintStreamOverAFile.class, PrintStreamOverAName.class, FilesWrite.class, FilesWriteString.class,
            FilesNewOutputStream.class, FilesNewBufferedWriter.class, FilesCopy.class, FilesMove.class,
            FilesCreateFile.class, FilesCreateTempFile.class, FilesNewByteChannel.class, FileChannelOpen.class);

    public static final class FileOutputStreamWrite {
        public static void wrote(String text) throws Exception {
            try (java.io.FileOutputStream out = new java.io.FileOutputStream("plan.txt")) {
                out.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
    }

    public static final class FileWriterWrite {
        public static void wrote(String text) throws Exception {
            try (java.io.FileWriter w = new java.io.FileWriter("plan.txt", java.nio.charset.StandardCharsets.UTF_8)) {
                w.write(text);
            }
        }
    }

    public static final class RandomAccessFileWrite {
        public static void wrote(String text) throws Exception {
            try (java.io.RandomAccessFile f = new java.io.RandomAccessFile("plan.txt", "rw")) {
                f.writeUTF(text);
            }
        }
    }

    public static final class PrintWriterOverAFile {
        public static void wrote(String text) throws Exception {
            try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.File("plan.txt"),
                    java.nio.charset.StandardCharsets.UTF_8)) {
                w.print(text);
            }
        }
    }

    public static final class PrintWriterOverAName {
        public static void wrote(String text) throws Exception {
            try (java.io.PrintWriter w = new java.io.PrintWriter("plan.txt", java.nio.charset.StandardCharsets.UTF_8)) {
                w.print(text);
            }
        }
    }

    public static final class PrintStreamOverAFile {
        public static void wrote(String text) throws Exception {
            try (java.io.PrintStream w = new java.io.PrintStream(new java.io.File("plan.txt"),
                    java.nio.charset.StandardCharsets.UTF_8)) {
                w.print(text);
            }
        }
    }

    public static final class PrintStreamOverAName {
        public static void wrote(String text) throws Exception {
            try (java.io.PrintStream w = new java.io.PrintStream("plan.txt", java.nio.charset.StandardCharsets.UTF_8)) {
                w.print(text);
            }
        }
    }

    public static final class FilesWrite {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.write(java.nio.file.Path.of("plan.txt"),
                    text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    public static final class FilesWriteString {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.writeString(java.nio.file.Path.of("plan.txt"), text);
        }
    }

    public static final class FilesNewOutputStream {
        public static void wrote(String text) throws Exception {
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(java.nio.file.Path.of("plan.txt"))) {
                out.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
    }

    public static final class FilesNewBufferedWriter {
        public static void wrote(String text) throws Exception {
            try (java.io.Writer w = java.nio.file.Files.newBufferedWriter(java.nio.file.Path.of("plan.txt"))) {
                w.write(text);
            }
        }
    }

    public static final class FilesCopy {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.copy(new java.io.ByteArrayInputStream(
                    text.getBytes(java.nio.charset.StandardCharsets.UTF_8)), java.nio.file.Path.of("plan.txt"));
        }
    }

    public static final class FilesMove {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.move(java.nio.file.Path.of(text), java.nio.file.Path.of("plan.txt"));
        }
    }

    public static final class FilesCreateFile {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.createFile(java.nio.file.Path.of(text));
        }
    }

    public static final class FilesCreateTempFile {
        public static void wrote(String text) throws Exception {
            java.nio.file.Files.createTempFile(text, ".txt");
        }
    }

    public static final class FilesNewByteChannel {
        public static void wrote(String text) throws Exception {
            try (var ch = java.nio.file.Files.newByteChannel(java.nio.file.Path.of(text),
                    java.nio.file.StandardOpenOption.WRITE)) {
                ch.write(java.nio.ByteBuffer.wrap(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            }
        }
    }

    public static final class FileChannelOpen {
        public static void wrote(String text) throws Exception {
            try (var ch = java.nio.channels.FileChannel.open(java.nio.file.Path.of(text),
                    java.nio.file.StandardOpenOption.WRITE)) {
                ch.write(java.nio.ByteBuffer.wrap(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            }
        }
    }
}
