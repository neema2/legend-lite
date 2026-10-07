import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.parser.Dialect;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/** Homework probe (not committed): every element of a world, name-resolved by OUR resolver, and every full name it
 *  references — split into DECL (signatures, supertypes, property types: outside any value specification) and BODY
 *  (inside function, derived-property and constraint bodies). One edge per (element, referenced name, context). */
public class ClosureProbe {
    static final Pattern FQN = Pattern.compile("(?:[A-Za-z_][A-Za-z0-9_]*::)+[A-Za-z_$][A-Za-z0-9_$]*");
    public static void main(String[] a) throws Exception {
        List<Compiler.ModelSource> srcs = new ArrayList<>();
        for (String f : Files.readAllLines(Path.of(a[0]))) {
            if (!f.isBlank()) srcs.add(new Compiler.ModelSource(f, Files.readString(Path.of(f), StandardCharsets.UTF_8)));
        }
        Map<String, String> pw = new LinkedHashMap<>(), rw = new LinkedHashMap<>();
        long t0 = System.nanoTime();
        var pm = Compiler.parseSources(srcs, pw::put, Dialect.LEGEND_PLATFORM);
        var resolved = NameResolver.resolve(pm.model(), rw);
        long t1 = System.nanoTime();
        System.out.printf("files=%d elements=%d resolved=%d parseWalls=%d resolveWalls=%d ms=%d%n", srcs.size(),
                pm.model().elements().size(), resolved.elements().size(), pw.size(), rw.size(), (t1 - t0) / 1_000_000);
        try (var out = Files.newBufferedWriter(Path.of(a[1]))) {
            for (var el : resolved.elements()) {
                String self = el.qualifiedName();
                Set<String> decl = new TreeSet<>(), body = new TreeSet<>();
                walk(el, false, decl, body, Collections.newSetFromMap(new IdentityHashMap<>()));
                out.write(self + "\t" + el.getClass().getSimpleName() + "\t\tself\n");
                for (String r : decl) if (!r.equals(self)) out.write(self + "\t" + el.getClass().getSimpleName() + "\t" + r + "\tdecl\n");
                for (String r : body) if (!r.equals(self) && !decl.contains(r)) out.write(self + "\t" + el.getClass().getSimpleName() + "\t" + r + "\tbody\n");
            }
        }
        try (var out = Files.newBufferedWriter(Path.of(a[1] + ".walls"))) {
            for (var e : pw.entrySet()) out.write("parse\t" + e.getKey() + "\t" + e.getValue().replace('\n', ' ') + "\n");
            for (var e : rw.entrySet()) out.write("resolve\t" + e.getKey() + "\t" + e.getValue().replace('\n', ' ') + "\n");
        }
    }
    static void walk(Object o, boolean inBody, Set<String> decl, Set<String> body, Set<Object> seen) {
        if (o == null || o instanceof Number || o instanceof Boolean || o instanceof Character || o instanceof Enum<?>) return;
        if (o instanceof String s) {
            if (s.contains("::") && s.length() < 400 && FQN.matcher(s).matches()) (inBody ? body : decl).add(s);
            return;
        }
        if (o instanceof Collection<?> c) { for (Object x : c) walk(x, inBody, decl, body, seen); return; }
        if (o instanceof Map<?, ?> m) { for (var e : m.entrySet()) { walk(e.getKey(), inBody, decl, body, seen); walk(e.getValue(), inBody, decl, body, seen); } return; }
        if (o instanceof Optional<?> op) { if (op.isPresent()) walk(op.get(), inBody, decl, body, seen); return; }
        if (o.getClass().isArray()) { if (!o.getClass().getComponentType().isPrimitive()) for (Object x : (Object[]) o) walk(x, inBody, decl, body, seen); return; }
        if (!seen.add(o)) return;
        boolean b = inBody || o instanceof com.legend.protocol.spec.ValueSpecification;
        Class<?> k = o.getClass();
        if (k.getName().startsWith("java.")) return;
        if (k.isRecord()) {
            for (RecordComponent rc : k.getRecordComponents()) {
                try { Method m = rc.getAccessor(); m.setAccessible(true); walk(m.invoke(o), b, decl, body, seen); } catch (ReflectiveOperationException e) { /* skip */ }
            }
            return;
        }
        for (Class<?> c = k; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                try { f.setAccessible(true); walk(f.get(o), b, decl, body, seen); } catch (ReflectiveOperationException | RuntimeException e) { /* skip */ }
            }
        }
    }
}
