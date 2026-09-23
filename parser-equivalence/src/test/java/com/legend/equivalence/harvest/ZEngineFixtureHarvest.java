package com.legend.equivalence.harvest;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** HARVEST BY EXECUTION (tier 1): run every test class from the engine's
 *  published grammar/compiler tests-jars under the recording shims — the
 *  fixtures they assemble at runtime land in target/engine-fixtures.jsonl.
 *  Assertion outcomes are irrelevant here (the shims record, not judge);
 *  adjudication happens downstream when the dump becomes corpus tier C6.
 *  Run on demand; the dump is committed as a resource. */
class ZEngineFixtureHarvest {

    @Test
    void harvest() throws Exception {
        List<String> testJars = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path")
                .split(java.io.File.pathSeparator)) {
            if (entry.endsWith("-tests.jar")
                    && (entry.contains("legend-engine-language-pure-grammar")
                    || entry.contains("legend-engine-language-pure-compiler"))) {
                testJars.add(entry);
            }
        }
        System.out.println("@@ tests-jars: " + testJars);
        java.nio.file.Files.deleteIfExists(
                Repo.out("engine-fixtures.jsonl"));
        System.out.println("@@ " + FixtureHarvest.tier1(testJars, getClass().getClassLoader()));
        long lines = java.nio.file.Files.exists(Repo.out("engine-fixtures.jsonl"))
                ? java.nio.file.Files.lines(Repo.out("engine-fixtures.jsonl")).count()
                : 0;
        System.out.println("@@ fixtures dumped: " + lines);
    }
}
