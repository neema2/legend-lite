// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.warehouse.server.duck;

import java.io.IOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.PaddingLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * THE NATIVE IMAGE'S REACHABILITY METADATA, generated whole (Bazel workplan P2-09, A23): no section is recorded by
 * hand or by a tracing agent on some host (an agent's output names the recording host's locale bundles and platform
 * providers, so it cannot be one golden for three platforms).
 * <ul>
 *   <li>{@code foreign}: every downcall signature in {@link Duck#DOWNCALLS} (and an Arrow release callback's) and
 *       every callback in {@link AuthenticatedUser#UPCALLS}, in GraalVM's notation ({@code void*}, {@code jlong},
 *       {@code struct(jlong,jlong)}, {@code padding(6)});</li>
 *   <li>{@code reflection}: the callbacks' methods, from the same table; and the JDK services the server looks up by
 *       name, declared below, each with why;</li>
 *   <li>{@code resources}: the JDK service-provider files and data the image reads, declared below.</li>
 * </ul>
 * {@code //warehouse:reachability_metadata} runs it; {@code bazel run //warehouse:update_reachability_metadata} writes
 * the committed file, and {@code //:generated} fails when an FFM signature or a declared service changed without it.
 *
 * <pre>
 *   ReachabilityMetadata &lt;output&gt;
 * </pre>
 */
public final class ReachabilityMetadata {

    private ReachabilityMetadata() {}

    /** A JDK class the server reaches by name (a JCA provider, a resource bundle), with the constructor or method it
     *  calls ({@code null}: the type alone), and why. */
    private record Service(String type, String member, List<String> parameterTypes, boolean jni, String why) {
    }

    private static final List<Service> SERVICES = List.of(
            // Identity: tokens are HMAC-SHA256 signatures, passwords PBKDF2 with HMAC-SHA256 (JCA, by algorithm name)
            new Service("com.sun.crypto.provider.HmacCore$HmacSHA256", "<init>", List.of(), false, "token signatures"),
            new Service("com.sun.crypto.provider.PBKDF2Core$HmacSHA256", "<init>", List.of(), false, "password hashes"),
            // the JDK reads a boolean system property through JNI while bringing up its native networking
            new Service("java.lang.Boolean", "getBoolean", List.of("java.lang.String"), true, "native networking"),
            // SecureRandom and MessageDigest providers (salts, token keys; the JCA by name)
            new Service("sun.security.provider.NativePRNG", "<init>", List.of("java.security.SecureRandomParameters"),
                    false, "secure random"),
            new Service("sun.security.provider.SHA", "<init>", List.of(), false, "SHA-1 digest"),
            new Service("sun.security.provider.SHA2$SHA256", "<init>", List.of(), false, "SHA-256 digest"),
            // the en_US locale data (the image formats dates and zone names in the pinned locale, P0-10)
            new Service("sun.text.resources.FormatData", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.FormatData_en", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.FormatData_en_US", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.JavaTimeSupplementary", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.cldr.FormatData", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.cldr.FormatData_en", null, List.of(), false, "locale data"),
            new Service("sun.text.resources.cldr.FormatData_en_US", null, List.of(), false, "locale data"),
            new Service("sun.util.resources.cldr.TimeZoneNames", null, List.of(), false, "zone names"),
            new Service("sun.util.resources.cldr.TimeZoneNames_en", null, List.of(), false, "zone names"),
            new Service("sun.util.resources.cldr.TimeZoneNames_en_US", null, List.of(), false, "zone names"));

    /** Resources the image reads: {module or null, glob}. The JDK's service-provider files (the HTTP server, URL
     *  handlers, selectors, zone rules) and ICU's normalization data (String.normalize, in java.base). */
    private static final List<String[]> RESOURCES = List.of(
            new String[] {null, "META-INF/services/com.sun.net.httpserver.spi.HttpServerProvider"},
            new String[] {null, "META-INF/services/java.net.spi.URLStreamHandlerProvider"},
            new String[] {null, "META-INF/services/java.nio.channels.spi.SelectorProvider"},
            new String[] {null, "META-INF/services/java.time.zone.ZoneRulesProvider"},
            new String[] {"java.base", "jdk/internal/icu/impl/data/icudt76b/nfc.nrm"});

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: ReachabilityMetadata <output>");
        }
        // distinct signatures, in a stable order (GraalVM keys a downcall by its signature, not its symbol)
        TreeSet<String> downcalls = new TreeSet<>();
        for (FunctionDescriptor d : Duck.DOWNCALLS.values()) {
            downcalls.add(entry(null, null, d));
        }
        downcalls.add(entry(null, null, Duck.RELEASE));
        List<String> upcalls = new ArrayList<>();
        for (AuthenticatedUser.Upcall u : AuthenticatedUser.UPCALLS) {
            upcalls.add(entry(AuthenticatedUser.class.getName(), u.method(), u.descriptor()));
        }
        upcalls.sort(null);
        // reflection: the callbacks' methods (findStatic), then the declared services
        List<String> reflection = new ArrayList<>();
        List<String> methods = new ArrayList<>();
        for (AuthenticatedUser.Upcall u : AuthenticatedUser.UPCALLS.stream()
                .sorted(java.util.Comparator.comparing(AuthenticatedUser.Upcall::method)).toList()) {
            List<String> ps = new ArrayList<>();
            for (Class<?> c : u.descriptor().toMethodType().parameterArray()) {
                ps.add("\"" + c.getName() + "\"");
            }
            methods.add("{\"name\": \"" + u.method() + "\", \"parameterTypes\": [" + String.join(", ", ps) + "]}");
        }
        reflection.add("    {\"type\": \"" + AuthenticatedUser.class.getName() + "\", \"methods\": ["
                + String.join(", ", methods) + "]}");
        for (Service sv : SERVICES) {
            StringBuilder sb = new StringBuilder("    {\"type\": \"").append(sv.type()).append('"');
            if (sv.jni()) {
                sb.append(", \"jniAccessible\": true");
            }
            if (sv.member() != null) {
                List<String> ps = new ArrayList<>();
                sv.parameterTypes().forEach(t -> ps.add("\"" + t + "\""));
                sb.append(", \"methods\": [{\"name\": \"").append(sv.member()).append("\", \"parameterTypes\": [")
                        .append(String.join(", ", ps)).append("]}]");
            }
            reflection.add(sb.append('}').toString());
        }
        List<String> resources = new ArrayList<>();
        for (String[] r : RESOURCES) {
            resources.add("    {" + (r[0] == null ? "" : "\"module\": \"" + r[0] + "\", ") + "\"glob\": \"" + r[1] + "\"}");
        }
        String json = "{\n  \"reflection\": [\n" + String.join(",\n", reflection)
                + "\n  ],\n  \"resources\": [\n" + String.join(",\n", resources)
                + "\n  ],\n  \"foreign\": {\n    \"downcalls\": [\n" + String.join(",\n", downcalls)
                + "\n    ],\n    \"directUpcalls\": [\n" + String.join(",\n", upcalls) + "\n    ]\n  }\n}\n";
        Files.writeString(Path.of(args[0]), json, StandardCharsets.UTF_8);
    }

    private static String entry(String cls, String method, FunctionDescriptor d) {
        StringBuilder sb = new StringBuilder("      {");
        if (cls != null) {
            sb.append("\"class\": \"").append(cls).append("\", \"method\": \"").append(method).append("\", ");
        }
        sb.append("\"returnType\": \"").append(d.returnLayout().map(ReachabilityMetadata::type).orElse("void"))
                .append("\", \"parameterTypes\": [");
        List<String> ps = new ArrayList<>();
        for (MemoryLayout l : d.argumentLayouts()) {
            ps.add("\"" + type(l) + "\"");
        }
        return sb.append(String.join(", ", ps)).append("]}").toString();
    }

    /** A layout in GraalVM's notation. */
    static String type(MemoryLayout l) {
        if (l instanceof AddressLayout) {
            return "void*";
        }
        if (l instanceof ValueLayout v) {
            Class<?> c = v.carrier();
            if (c == int.class) return "jint";
            if (c == long.class) return "jlong";
            if (c == byte.class) return "jbyte";
            if (c == short.class) return "jshort";
            if (c == char.class) return "jchar";
            if (c == float.class) return "jfloat";
            if (c == double.class) return "jdouble";
            if (c == boolean.class) return "jboolean";
        }
        if (l instanceof PaddingLayout p) {
            return "padding(" + p.byteSize() + ")";
        }
        if (l instanceof StructLayout s) {
            List<String> members = new ArrayList<>();
            for (MemoryLayout m : s.memberLayouts()) {
                members.add(type(m));
            }
            return "struct(" + String.join(",", members) + ")";
        }
        throw new IllegalStateException("no GraalVM notation for the layout " + l);
    }
}
