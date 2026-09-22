package com.legend.tools.par;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import org.finos.legend.pure.m3.generator.LogToSystemOut;
import org.finos.legend.pure.m3.generator.par.PureJarGenerator;

/**
 * Compiles one Pure code repository to its PAR ({@code pure-<repository>.par}) —
 * what legend-pure-maven-generation-par's {@code build-pure-jar} goal does, by the
 * same call ({@code PureJarGenerator.doGeneratePAR}), without Maven.
 *
 * <pre>
 *   ParGenerator &lt;repository&gt; &lt;definition.json&gt; &lt;source dir&gt; &lt;output dir&gt;
 * </pre>
 *
 * <p>The repository's dependencies ({@code platform}, {@code core}, ...) are found
 * on this program's classpath, as the Maven goal finds them on its plugin's. The
 * Pure platform version written into the PAR is the version of the legend-pure
 * jar on the classpath, so it cannot disagree with the release that compiled it.
 */
public final class ParGenerator {

    private ParGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "usage: ParGenerator <repository> <definition.json> <source dir> <output dir>");
        }
        PureJarGenerator.doGeneratePAR(
                Set.of(args[0]),
                null,
                Set.of(new File(args[1]).getAbsolutePath()),
                platformVersion(),
                null,
                new File(args[2]),
                new File(args[3]),
                ParGenerator.class.getClassLoader(),
                new LogToSystemOut());
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
