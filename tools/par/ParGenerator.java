package com.legend.tools.par;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.finos.legend.pure.m3.generator.LogToSystemOut;
import org.finos.legend.pure.m3.generator.par.PureJarGenerator;

/**
 * Compiles one Pure code repository to its PAR ({@code pure-<repository>.par}) —
 * what legend-pure-maven-generation-par's {@code build-pure-jar} goal does, by the
 * same call ({@code PureJarGenerator.doGeneratePAR}), without Maven.
 *
 * <pre>
 *   ParGenerator &lt;repository&gt; &lt;definition.json&gt; &lt;source dir&gt; &lt;output pure-&lt;repository&gt;.par&gt;
 * </pre>
 *
 * <p>The repository's dependencies ({@code platform}, {@code core}, ...) are found
 * on this program's classpath, as the Maven goal finds them on its plugin's. The
 * Pure platform version written into the PAR is the version of the legend-pure
 * jar on the classpath, so it cannot disagree with the release that compiled it.
 * Every entry carries one constant time, so the same sources give the same bytes.
 */
public final class ParGenerator {

    /** The one time every entry carries: Bazel's own jar epoch (a DOS time: no zone, no extended field). */
    static final LocalDateTime ENTRY_TIME = LocalDateTime.of(2010, 1, 1, 0, 0);

    private ParGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "usage: ParGenerator <repository> <definition.json> <source dir> <output pure-<repository>.par>");
        }
        // the PAR file a build action declares (java_run's {OUT}); the generator writes it into its directory
        File par = new File(args[3]).getAbsoluteFile();
        if (!par.getName().equals("pure-" + args[0] + ".par")) {
            throw new IllegalArgumentException("the output must be named pure-" + args[0] + ".par, not " + par.getName());
        }
        PureJarGenerator.doGeneratePAR(
                Set.of(args[0]),
                null,
                Set.of(new File(args[1]).getAbsolutePath()),
                platformVersion(),
                null,
                new File(args[2]),
                par.getParentFile(),
                ParGenerator.class.getClassLoader(),
                new LogToSystemOut());
        if (!par.isFile()) {
            throw new IllegalStateException("PureJarGenerator wrote no " + par);
        }
        fixEntryTimes(par.toPath());
    }

    /**
     * legend-pure stamps every entry with the clock (PureRepositoryJarBuilder makes each JarEntry with no time, and
     * ZipOutputStream then takes the current one), so two builds of the same PAR differed in those bytes alone, and
     * every PCT test downstream ran again. This writes the PAR again: the same entries, in the same order, with the
     * same contents, each at ENTRY_TIME.
     */
    static void fixEntryTimes(Path par) throws IOException {
        record Entry(String name, byte[] bytes) {}
        List<Entry> entries = new ArrayList<>();
        try (ZipFile zip = new ZipFile(par.toFile())) {
            for (Enumeration<? extends ZipEntry> e = zip.entries(); e.hasMoreElements(); ) {
                ZipEntry entry = e.nextElement();
                try (InputStream in = zip.getInputStream(entry)) {
                    entries.add(new Entry(entry.getName(), in.readAllBytes()));
                }
            }
        }
        // the manifest stays first, where JarInputStream looks for it; JarOutputStream marks the first entry a jar's
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(par))) {
            for (Entry e : entries) {
                JarEntry entry = new JarEntry(e.name());
                entry.setTimeLocal(ENTRY_TIME);
                out.putNextEntry(entry);
                out.write(e.bytes());
                out.closeEntry();
            }
        }
    }

    private static String platformVersion() throws IOException {
        String resource = "META-INF/maven/org.finos.legend.pure/legend-pure-m3-core/pom.properties";
        try (InputStream in = PureJarGenerator.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("no " + resource + " on the classpath");
            }
            Properties p = new Properties();
            p.load(in);
            return p.getProperty("version");
        }
    }
}
