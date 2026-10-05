"""THE PRODUCT'S JARS: the third-party jars our programs run with, fetched by Bazel's own http_jar.

docs/BUILD_REBUILD_DESIGN_2026_10_05.md, section 5b. These are leaf jars: each is a JDBC driver with no dependency it
needs at run time. So no Maven resolution is wanted. rules_jvm_external stays for the pools that resolve trees
(pools.bzl): upstream, the engine runner, TeaVM, test tooling and compiler plugins.

http_jar downloads each jar from Maven Central, checks its sha256, and makes it a java_import (`@<name>//jar`).
  * Compiling uses Bazel's ijar interface jar: class signatures only, with the target's label stamped in, so
    strict-deps names the target to add.
  * Running uses the downloaded jar unchanged.
  * Nothing rewrites a jar, so DuckDB's 77 MB carry no stamping or copying step on the critical path. With
    rules_jvm_external that was 17.4 s of a 19.7 s critical path (B1).

JARS is the one source of truth. The product_jars extension below makes the repositories, pools.bzl's
check_pool_use limits who may use each, and //tools/guards' classpath report reads each jar's coordinate (G11).
Bumping a jar is one edit here: the coordinate, and the sha256 of the new file. Adding a user is a reviewed edit
here, with the reason.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_jar")

# name -> struct(coordinate, sha256, users): users are packages, or single targets "package:name".
JARS = {
    # core's DuckDB, through JDBC: //core:drivers, and //core:duckdb_load compiles its bulk loader against it
    "duckdb_jdbc": struct(
        coordinate = "org.duckdb:duckdb_jdbc:1.4.4.0",
        sha256 = "43f0cc93c892699162d46e8a45e2cbb92d92f74b933832f84c78ed551a2608df",
        users = ["core", "spec"],
    ),
    # THE WAREHOUSE's DuckDB (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md): 1.5.x for chunked fetching and host functions.
    # The warehouse loads its native library through FFM (//warehouse:duckdb_library extracts it). It moves ahead of
    # core's 1.4.4, which upgrades in its own leg.
    "duckdb_jdbc_warehouse": struct(
        coordinate = "org.duckdb:duckdb_jdbc:1.5.5.1",
        sha256 = "22343dd258db1b0b51d37afc776c8dff5b19282829fa5b47f7d5d6fe02b3377a",
        users = ["datacube", "warehouse"],
    ),
    # core's H2
    "h2": struct(
        coordinate = "com.h2database:h2:2.1.214",
        sha256 = "d623cdc0f61d218cf549a8d09f1c391ff91096116b22e2475475fce4fbe72bd0",
        users = ["core", "spec"],
    ),
    # THE MODERN H2, for the one PCT lane that runs on it (gate 7: PCT relation on H2 2.4.240, the engine's own PCT
    # H2 profile). An ALTERNATIVE to core's 2.1.214, never beside it.
    "h2_modern": struct(
        coordinate = "com.h2database:h2:2.4.240",
        sha256 = "29b70e427cc1c40cdc376283adbb0cc62853073797bb5fe5761f81fe73d57ce0",
        users = ["pct"],
    ),
    # The server's Postgres arm (ConnectionResolver: jdbc:postgresql://). 42.7.13, the latest release (2026-07-06).
    # Its POM declares checker-qual (annotations only) and waffle-jna (optional: Windows single sign-on). Neither is
    # fetched: annotations whose classes are missing are ignored at run time, and the Postgres tests prove it.
    "postgresql": struct(
        coordinate = "org.postgresql:postgresql:42.7.13",
        sha256 = "6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93",
        users = ["core", "spec"],
    ),
    "sqlite_jdbc": struct(
        coordinate = "org.xerial:sqlite-jdbc:3.47.1.0",
        sha256 = "4164d95347aeab42b754cabb7945b93e54f29ddb2eded967dbe979b4045cb685",
        users = ["core", "spec"],
    ),
}

# The canonical repository name's marker: +product_jars+<name> (the extension's repositories).
REPOSITORY_MARKER = "+product_jars+"

def jar_of_label(label):
    """The JARS name a label's repository is, or None."""
    repo = label.repo_name
    if REPOSITORY_MARKER not in repo:
        return None
    return repo.split(REPOSITORY_MARKER)[-1]

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
