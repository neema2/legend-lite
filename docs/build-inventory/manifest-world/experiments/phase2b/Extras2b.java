import com.legend.Compiler;
import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.SignatureMangle;
import com.legend.parser.Dialect;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Phase 2b (scratch): each upstream declaration at a catalog name, its signature id, and whether the catalog has
 *  that id. Writes the extras (ids the catalog lacks) as prelude sections, and a TSV of every candidate. */
public class Extras2b {
    public static void main(String[] a) throws Exception {
        Set<String> catalogIds = new HashSet<>();
        for (NativeFunctionDefinition n : Pure.all()) catalogIds.add(SignatureMangle.mangle(n));
        StringBuilder pure = new StringBuilder(), tsv = new StringBuilder();
        int extras = 0, extraNative = 0, extraBody = 0, inCatalog = 0, unparsed = 0;
        Map<String, Integer> byFqn = new TreeMap<>();
        for (String line : Files.readAllLines(Path.of(a[0]))) {
            String[] c = line.split("\t");
            String imports = new String(Base64.getDecoder().decode(c[3]), StandardCharsets.UTF_8);
            String text = new String(Base64.getDecoder().decode(c[4]), StandardCharsets.UTF_8);
            String src = "###Pure\n" + imports + "\n" + text;
            Map<String, String> walls = new LinkedHashMap<>();
            Function f = null;
            try {
                var pm = Compiler.parseSources(List.of(new Compiler.ModelSource(c[1], src)), walls::put, Dialect.LEGEND_PLATFORM);
                for (var el : pm.model().elements()) if (el instanceof Function fn && fn.qualifiedName().equals(c[0])) f = fn;
            } catch (RuntimeException e) { walls.put(c[0], e.toString()); }
            if (f == null) { unparsed++; tsv.append("UNPARSED\t").append(c[0]).append('\t').append(c[2]).append('\t').append(walls).append('\n'); continue; }
            String id = SignatureMangle.mangle(f);
            boolean have = catalogIds.contains(id);
            tsv.append(have ? "CATALOG" : "EXTRA").append('\t').append(id).append('\t').append(c[2]).append('\t').append(c[1].replaceAll(".*/src/main/resources/", "")).append('\n');
            if (have) { inCatalog++; continue; }
            extras++;
            if (c[2].equals("native function")) extraNative++; else extraBody++;
            byFqn.merge(c[0], 1, Integer::sum);
            pure.append("\n###Pure\n").append(imports).append('\n').append(text.strip()).append('\n');
        }
        Files.writeString(Path.of(a[1]), pure.toString());
        Files.writeString(Path.of(a[2]), tsv.toString());
        System.out.printf("candidates in the catalog by id: %d; EXTRA (ids the catalog lacks): %d (%d native, %d with a body) at %d names; unparsed: %d%n",
                inCatalog, extras, extraNative, extraBody, byFqn.size(), unparsed);
    }
}
