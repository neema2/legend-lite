import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * SPIKE (2026-10-10): the overlay's replacements answer exactly as String.format did -- the system metamodel's whole
 * source text (original jar against overlay, each in its own loader), and pad/hex4 over their ranges.
 * Arguments: <overlay classes dir> <jar>...
 */
public final class OverlayCheck {

    public static void main(String[] args) throws Exception {
        List<URL> jars = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            jars.add(Path.of(args[i]).toUri().toURL());
        }
        List<URL> overlaid = new ArrayList<>();
        overlaid.add(Path.of(args[0]).toUri().toURL());
        overlaid.addAll(jars);
        String before = source(new URLClassLoader(jars.toArray(URL[]::new), null));
        String after = source(new URLClassLoader(overlaid.toArray(URL[]::new), null));
        System.out.println("SystemMetamodel.SOURCE: " + before.length() + " chars, identical=" + before.equals(after));

        Class<?> f = new URLClassLoader(overlaid.toArray(URL[]::new), null).loadClass("com.legend.base.SpikeFormat");
        var pad = f.getMethod("pad", long.class, int.class);
        var hex4 = f.getMethod("hex4", int.class);
        int bad = 0;
        for (int width : new int[] {2, 3, 9, 10}) {
            for (long v = -100_000; v <= 2_000_000; v++) {
                if (!String.format(Locale.ROOT, "%0" + width + "d", v).equals(pad.invoke(null, v, width))) {
                    bad++;
                }
            }
        }
        for (int c = 0; c <= 0xFFFF; c++) {
            if (!String.format(Locale.ROOT, "%04x", c).equals(hex4.invoke(null, c))) {
                bad++;
            }
        }
        System.out.println("pad/hex4 mismatches: " + bad);
    }

    private static String source(ClassLoader loader) throws Exception {
        Field field = loader.loadClass("com.legend.builtin.SystemMetamodel").getDeclaredField("SOURCE");
        field.setAccessible(true);
        return (String) field.get(null);
    }
}
