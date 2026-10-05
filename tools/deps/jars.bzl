"""THE PRODUCT'S JARS, fetched: the product_jars extension makes each jar of jars_table.bzl an http_jar repository.

http_jar (rules_java's) downloads each jar from Maven Central, checks its sha256, and makes it a java_import
(`@<name>//jar`).
  * Compiling uses Bazel's ijar interface jar: class signatures only, with the target's label stamped in, so
    strict-deps names the target to add.
  * Running uses the downloaded jar unchanged.
  * Nothing rewrites a jar, so DuckDB's 77 MB carry no stamping or copying step on the critical path. With
    rules_jvm_external that was 17.4 s of a 19.7 s critical path (docs/BUILD_REBUILD_DESIGN_2026_10_05.md, B1).
"""

load("@rules_java//java:http_jar.bzl", "http_jar")
load(":jars_table.bzl", "JARS")

def _url(coordinate):
    group, artifact, version = coordinate.split(":")
    return "https://repo1.maven.org/maven2/%s/%s/%s/%s-%s.jar" % (group.replace(".", "/"), artifact, version, artifact, version)

def _product_jars_impl(module_ctx):
    for name, jar in JARS.items():
        _, artifact, version = jar.coordinate.split(":")
        http_jar(
            name = name,
            urls = [_url(jar.coordinate)],
            sha256 = jar.sha256,
            downloaded_file_name = "%s-%s.jar" % (artifact, version),
        )
    return module_ctx.extension_metadata(reproducible = True)

product_jars = module_extension(
    implementation = _product_jars_impl,
    doc = "The product's jars (JARS), each an http_jar repository: @<name>//jar.",
)
