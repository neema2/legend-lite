// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.planner.PlanningObserver;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * THE LEAN-SQL SETS' CAPTURE, a TEST HARNESS's (E-1 of the total-wall program, the user's ruling of 2026-11-12: the
 * capture OUT of the product; it replaced the planner's {@code capture-queries} switch). Loaded by the JUnit launcher
 * on every test JVM whose classpath holds {@code //core:query_capture} (core's suites, the corpus's, PCT's), it does
 * nothing unless the run names a file, {@code -Dlegend.capture.queries=FILE}: then it installs a
 * {@link PlanningObserver} for the run, and every product query planned from a model's TEXT and a query's TEXT is
 * appended to FILE as one JSON line, {@code {"model", "query", "runtime"}} -- the runtime its NAME, the model the text
 * the caller gave, which names a connection's credential and never holds one. The planner reads nothing back.
 */
public final class QueryCaptureListener implements TestExecutionListener {

    /** The property naming the file; unset, the listener installs nothing. */
    public static final String PROPERTY = "legend.capture.queries";

    @Override
    public void testPlanExecutionStarted(TestPlan plan) {
        String file = System.getProperty(PROPERTY);
        if (file == null) {
            return;
        }
        if (file.strip().isEmpty()) {
            throw new IllegalArgumentException("-D" + PROPERTY + "=FILE names the file it writes to, never a default");
        }
        PlanningObserver.install(new Capture(Path.of(file)));
    }

    @Override
    public void testPlanExecutionFinished(TestPlan plan) {
        if (PlanningObserver.installed() instanceof Capture) {
            PlanningObserver.install(null);
        }
    }

    /** The capturing observer: each model's and query's source text, weak and by identity, and a line per plan. */
    static final class Capture implements PlanningObserver {

        private final Path file;
        private final Map<Object, String> sources = Collections.synchronizedMap(new WeakHashMap<>());

        Capture(Path file) {
            this.file = file;
        }

        @Override
        public void modelCompiled(ModelContext model, String text) {
            sources.put(model, text);
        }

        @Override
        public void queryTyped(TypedQuery query, String text) {
            sources.put(query, text);
        }

        @Override
        public void planned(TypedQuery query, String runtime) {
            String m = sources.get(query.context());
            String q = sources.get(query);
            if (m == null || q == null) {
                return;
            }
            String line = "{\"model\":" + quoted(m) + ",\"query\":" + quoted(q) + ",\"runtime\":"
                    + quoted(runtime == null ? "" : runtime) + "}\n";
            synchronized (this) {
                try {
                    Files.writeString(file, line, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    /** {@code text} as a JSON string. */
    static String quoted(String text) {
        StringBuilder b = new StringBuilder(text.length() + 2).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }
}
