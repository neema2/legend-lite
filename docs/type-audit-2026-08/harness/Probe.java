import com.legend.Compiler;
import com.legend.Execution;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Type-system probe harness.
 *
 * Usage:
 *   java Probe <modelFile> <queryFile|-> [runtimeFqn] [ddlFile]
 *
 * Prints, in order:
 *   [G]    Phase-G typed HIR root type + full typed tree dump
 *   [PLAN] rendered SQL
 *   [EXEC] ExecutionResult: shape, returnType, columns (name/pureType/mult),
 *          rows with java runtime classes
 * Any phase that throws prints [<PHASE>-ERROR] <exception class>: <message>
 * and the probe continues to the next phase where possible.
 */
public final class Probe {

    public static void main(String[] args) throws Exception {
        String model = Files.readString(Path.of(args[0]));
        String query = args[1].equals("-")
                ? new String(System.in.readAllBytes())
                : Files.readString(Path.of(args[1]));
        String runtime = args.length > 2 && !args[2].equals("-") ? args[2] : null;
        String ddl = args.length > 3 && !args[3].equals("-") ? args[3] : null;

        // ---- Phase G ----
        try {
            TypedSpec root = Compiler.compileQuery(model, query);
            System.out.println("[G] rootClass=" + root.getClass().getSimpleName());
            System.out.println("[G] type=" + root.info().type().typeName()
                    + " mult=" + root.info().multiplicity().text());
            System.out.println("[G] typeRepr=" + root.info().type());
            System.out.println("[G-TREE]");
            dump(root, 1, new IdentityHashMap<>());
        } catch (Throwable t) {
            System.out.println("[G-ERROR] " + t.getClass().getName() + ": " + t.getMessage());
        }

        // ---- Plan ----
        String sql = null;
        try {
            var plan = Compiler.plan(model, query, runtime);
            sql = plan.sql();
            System.out.println("[PLAN] " + sql);
            System.out.println("[PLAN] rootType=" + plan.rootType().type().typeName()
                    + " mult=" + plan.rootType().multiplicity().text());
            System.out.println("[PLAN] shape=" + plan.shape());
        } catch (Throwable t) {
            System.out.println("[PLAN-ERROR] " + t.getClass().getName() + ": " + t.getMessage());
        }

        // ---- Execute ----
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            if (ddl != null) {
                for (String stmt : Files.readString(Path.of(ddl)).split(";\\s*\n")) {
                    if (!stmt.isBlank()) {
                        try (Statement s = c.createStatement()) { s.execute(stmt); }
                    }
                }
            }
            ExecutionResult r = Execution.execute(model, query, runtime, c);
            if (r == null) { System.out.println("[EXEC] null result"); return; }
            System.out.println("[EXEC] shape=" + r.getClass().getSimpleName()
                    + " returnType=" + r.returnType().typeName()
                    + " returnTypeRepr=" + r.returnType());
            for (var col : r.columns()) {
                System.out.println("[EXEC-COL] " + col.name() + " : "
                        + col.pureType().typeName() + " [" + col.pureType() + "]"
                        + " mult=" + (col.multiplicity() == null ? "null" : col.multiplicity().text()));
            }
            int n = 0;
            for (var row : r.rows()) {
                StringBuilder sb = new StringBuilder("[EXEC-ROW] ");
                for (Object v : row.values()) {
                    sb.append(v == null ? "null" : (v.getClass().getSimpleName() + "(" + v + ")")).append(" | ");
                }
                System.out.println(sb);
                if (++n >= 40) { System.out.println("[EXEC-ROW] ...truncated"); break; }
            }
            if (r instanceof ExecutionResult.Graph g) {
                System.out.println("[EXEC-JSON] " + g.json());
            }
        } catch (Throwable t) {
            System.out.println("[EXEC-ERROR] " + t.getClass().getName() + ": " + t.getMessage());
        }
    }

    /** Reflective structural dump of the typed HIR with per-node types. */
    private static void dump(Object node, int depth, IdentityHashMap<Object, Boolean> seen) {
        if (node == null || depth > 40) return;
        String pad = "  ".repeat(depth);
        if (node instanceof TypedSpec ts) {
            if (seen.put(ts, Boolean.TRUE) != null) {
                System.out.println(pad + "<cycle " + ts.getClass().getSimpleName() + ">");
                return;
            }
            String info;
            try {
                info = ts.info().type().typeName() + ts.info().multiplicity().text();
            } catch (Throwable t) {
                info = "<info threw " + t.getClass().getSimpleName() + ": " + t.getMessage() + ">";
            }
            System.out.println(pad + ts.getClass().getSimpleName() + " :: " + info);
            for (var comp : ts.getClass().getRecordComponents() == null
                    ? new java.lang.reflect.RecordComponent[0]
                    : ts.getClass().getRecordComponents()) {
                Object v;
                try { comp.getAccessor().setAccessible(true); v = comp.getAccessor().invoke(ts); }
                catch (Throwable t) { continue; }
                if (v instanceof TypedSpec || v instanceof List) {
                    System.out.println(pad + " ." + comp.getName() + ":");
                    dump(v, depth + 1, seen);
                } else if (v != null && !(v instanceof String) && !(v instanceof Number)
                        && !(v instanceof Boolean) && v.getClass().getName().startsWith("com.legend")) {
                    System.out.println(pad + " ." + comp.getName() + " = " + v);
                } else {
                    System.out.println(pad + " ." + comp.getName() + " = " + v);
                }
            }
        } else if (node instanceof List<?> l) {
            for (Object o : l) dump(o, depth, seen);
        }
    }
}
