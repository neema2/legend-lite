import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedSpec;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/** Phase 2b (scratch): boot whatever prelude.pure is first on the class path, compile each user module as the product
 *  does, and write every call's resolved overload (module, body id, callee id, in walk order) plus every wall. Run per
 *  world; the diff is what the world changes. */
public class CallIds2b {
    public static void main(String[] a) throws Exception {
        try {
            Compiler.buildModule(Compiler.parseSources(List.of()).model());
        } catch (Throwable e) {
            System.out.println("BOOT FAILED: " + e);
            for (Throwable c = e.getCause(); c != null; c = c.getCause()) System.out.println("  caused by " + c);
            System.exit(2);
        }
        int calls = 0, walls = 0;
        try (var out = Files.newBufferedWriter(Path.of(a[0]))) {
            for (String m : Files.readAllLines(Path.of(a[1]))) {
                if (m.isBlank()) continue;
                Path dir = Path.of(m);
                List<Path> files;
                try (Stream<Path> s = Files.walk(dir)) { files = s.filter(p -> p.toString().endsWith(".pure")).sorted().toList(); }
                List<Compiler.ModelSource> srcs = new ArrayList<>();
                for (Path f : files) srcs.add(new Compiler.ModelSource(f.toString(), Files.readString(f, StandardCharsets.UTF_8)));
                String mod = dir.getFileName().toString();
                try {
                    var bm = Compiler.buildModule(Compiler.parseSources(srcs, (k, v) -> {}).model());
                    for (var e : new TreeMap<>(bm.walls()).entrySet()) { out.write(mod + "\tWALL-build\t" + e.getKey() + "\t" + one(e.getValue()) + "\n"); walls++; }
                    ModelContext ctx = bm.context();
                    SpecCompiler specs = new SpecCompiler(ctx);
                    for (String fqn : new TreeSet<>(ctx.functionFqns())) {
                        List<TypedFunction> overloads;
                        try { overloads = ctx.findFunction(fqn); } catch (RuntimeException e) { out.write(mod + "\tWALL-find\t" + fqn + "\t" + one(e.getMessage()) + "\n"); walls++; continue; }
                        for (TypedFunction tf : overloads) {
                            if (tf.body().isEmpty() || !srcs.stream().anyMatch(s -> true)) continue;
                            if (!isUser(tf, files)) continue;
                            try {
                                var cf = specs.compile(tf);
                                List<String> ids = new ArrayList<>();
                                walk(cf.body(), ids, Collections.newSetFromMap(new IdentityHashMap<>()));
                                for (String id : ids) { out.write(mod + "\tCALL\t" + tf.id().qualified() + "\t" + id + "\n"); calls++; }
                            } catch (RuntimeException e) {
                                out.write(mod + "\tWALL-body\t" + tf.id().qualified() + "\t" + one(e.getMessage()) + "\n"); walls++;
                            }
                        }
                    }
                } catch (Throwable e) {
                    out.write(mod + "\tTHREW\t\t" + one(String.valueOf(e)) + "\n"); walls++;
                }
            }
        }
        System.out.printf("calls=%d walls=%d%n", calls, walls);
    }

    /** A body declared in this module's files (not the boot layer's): its source names one of them. */
    static boolean isUser(TypedFunction tf, List<Path> files) {
        return !tf.id().qualified().startsWith("meta::");
    }

    static void walk(Object o, List<String> ids, Set<Object> seen) {
        if (o == null || o instanceof String || o instanceof Number || o instanceof Boolean || o instanceof Enum<?>) return;
        if (o instanceof Collection<?> c) { for (Object x : c) walk(x, ids, seen); return; }
        if (o instanceof Map<?, ?> m) { for (Object x : m.values()) walk(x, ids, seen); return; }
        if (o instanceof Optional<?> op) { op.ifPresent(x -> walk(x, ids, seen)); return; }
        if (!seen.add(o)) return;
        if (o instanceof TypedSpec t) {
            var id = Calls.calleeIdOf(t);
            if (id != null) ids.add(id.qualified());
        } else if (!(o.getClass().getName().startsWith("com.legend.compiler.spec.typed"))) {
            return;   // only the typed tree: never a declaration's own fields
        }
        Class<?> k = o.getClass();
        if (k.isRecord()) {
            for (RecordComponent rc : k.getRecordComponents()) {
                try { Method m = rc.getAccessor(); m.setAccessible(true); walk(m.invoke(o), ids, seen); } catch (ReflectiveOperationException e) { }
            }
        }
    }

    static String one(String s) { return s == null ? "" : s.replace('\n', ' ').replace('\t', ' '); }
}
