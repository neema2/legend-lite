import java.nio.file.*;
import java.util.*;

/** Homework probe (not committed): every full name the BOOT itself references — the system metamodel's elements
 *  (its Pure text: classes, views, functions) and Pure.java's catalog signatures — split decl/body like ClosureProbe. */
public class BootDemandProbe {
    public static void main(String[] a) throws Exception {
        try (var out = Files.newBufferedWriter(Path.of(a[0]))) {
            Set<String> own = new HashSet<>();
            for (var el : com.legend.builtin.SystemMetamodel.elements()) own.add(el.qualifiedName());
            for (var el : com.legend.builtin.SystemMetamodel.elements()) emit(out, "system", el.qualifiedName(), el, own);
            for (var nf : com.legend.builtin.Pure.all()) emit(out, "catalog", nf.toString().length() > 0 ? name(nf) : "?", nf, own);
        }
    }
    static String name(Object nf) {
        for (var m : nf.getClass().getMethods()) {
            if (m.getName().equals("qualifiedName") || m.getName().equals("fqn") || m.getName().equals("name")) {
                try { Object v = m.invoke(nf); if (v instanceof String s) return s; } catch (Exception e) { /* next */ }
            }
        }
        return nf.getClass().getSimpleName();
    }
    static void emit(java.io.Writer out, String src, String self, Object o, Set<String> own) throws java.io.IOException {
        Set<String> decl = new TreeSet<>(), body = new TreeSet<>();
        ClosureProbe.walk(o, false, decl, body, Collections.newSetFromMap(new IdentityHashMap<>()));
        for (String r : decl) if (!own.contains(r) && !r.equals(self)) out.write(src + "\t" + self + "\t" + r + "\tdecl\n");
        for (String r : body) if (!own.contains(r) && !r.equals(self) && !decl.contains(r)) out.write(src + "\t" + self + "\t" + r + "\tbody\n");
    }
}
