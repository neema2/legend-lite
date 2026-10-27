"""THE PRODUCT'S JARS, the table: the third-party jars our programs run with, each fetched by http_jar (jars.bzl).

docs/BUILD_REBUILD_DESIGN_2026_10_05.md, section 5b. These are leaf jars: each is a JDBC driver with no dependency it
needs at run time, so no Maven resolution is wanted. One is test-only (h2_modern, a PCT lane's H2). rules_jvm_external
stays for the pools that resolve trees (pools.bzl): upstream, the engine runner, TeaVM, test tooling and compiler
plugins.

JARS is the one source of truth. The product_jars extension (jars.bzl) makes the repositories, pools.bzl's
check_pool_use limits who may use each, and //tools/guards' classpath report reads each jar's coordinate (G11).
Bumping a jar is one edit here: the coordinate, and the sha256 of the new file. Adding a user is a reviewed edit
here, with the reason. This file is data, so a BUILD file that checks its users (pools.bzl) loads no repository rule.
"""

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
        # datacube's one use, the catalog-facts generator, went with DataCube's TypeScript model writer (2026-10-08)
        users = ["warehouse"],
    ),
    # core's H2
    "h2": struct(
        coordinate = "com.h2database:h2:2.1.214",
        sha256 = "d623cdc0f61d218cf549a8d09f1c391ff91096116b22e2475475fce4fbe72bd0",
        users = ["core"],
    ),
    # THE MODERN H2, for the one PCT lane that runs on it (gate 7: PCT relation on H2 2.4.240, the engine's own PCT
    # H2 profile). An ALTERNATIVE to core's 2.1.214, never beside it. Test-only: no product runs it.
    "h2_modern": struct(
        coordinate = "com.h2database:h2:2.4.240",
        sha256 = "29b70e427cc1c40cdc376283adbb0cc62853073797bb5fe5761f81fe73d57ce0",
        users = ["pct"],
    ),
    # The server's Postgres arm (ConnectionResolver: jdbc:postgresql://). 42.7.13, released 2026-07-06.
    # Its POM declares checker-qual (annotations only) and waffle-jna (optional: Windows single sign-on). Neither is
    # fetched: annotations whose classes are missing are ignored at run time, and the Postgres tests prove it.
    # testing (2026-10-26): EmbeddedPostgres creates pgcrypto through a client connection once its server is up, and
    # the driver is that client on every JVM that starts the server (the warehouse's live tests carry no other).
    "postgresql": struct(
        coordinate = "org.postgresql:postgresql:42.7.13",
        sha256 = "6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93",
        users = ["core", "testing"],
    ),
    # THE CLOUD PCT LANES' drivers (docs/CLOUD_BACKENDS_PCT_2026_10_11.md): the PCT suites on Databricks, Snowflake
    # and BigQuery, on real accounts. Test-only until the server opens those databases; each is one self-contained
    # jar (Snowflake's and Databricks' POMs declare no dependency; BigQuery's is its shaded "all" jar). Apache-2.0,
    # each checked against Maven Central's sha1 on 2026-10-11.
    "databricks_jdbc": struct(
        coordinate = "com.databricks:databricks-jdbc:3.4.3",
        sha256 = "191917c5a45629d33f4734ff599f9a4d4f4bdfc82f159ed7dc9f2b285a1d35d2",
        users = ["pct"],
    ),
    "snowflake_jdbc": struct(
        coordinate = "net.snowflake:snowflake-jdbc:4.4.0",
        sha256 = "eeabc114d2fd429365f8bdde0cb5dd00f0cf529b22aab754751bab1b09fc65c0",
        users = ["pct"],
    ),
    # group:artifact:version:classifier -- the shaded jar; the plain one declares 109 runtime dependencies
    "bigquery_jdbc": struct(
        coordinate = "com.google.cloud:google-cloud-bigquery-jdbc:1.6.0:all",
        sha256 = "b9d3368ae2d48721a053734745b2ef4d1cdf62f7e704e4d8a41b2a7126ca80a6",
        users = ["pct"],
    ),
    # THE MARIADB PCT LANES' driver (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): MariaDB Connector/J, which also
    # speaks to MySQL servers. Test-only; one jar, every POM dependency optional or provided. LGPL-2.1-or-later (a
    # test's runtime dependency, never shipped). Checked against Maven Central's sha1 on 2026-10-11.
    "mariadb_jdbc": struct(
        coordinate = "org.mariadb.jdbc:mariadb-java-client:3.5.10",
        sha256 = "919b8c1c771d9ee3465811462f242c9543ab401e140c64988ddbf1d8abcb18b2",
        users = ["pct"],
    ),
    # Trino's JDBC driver (2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18): the PCT Trino lanes' sessions on the
    # embedded server (@embedded_trino). Test-only; one shaded jar with no dependencies. Apache-2.0. Checked against
    # Maven Central's sha1 on 2026-10-12.
    "trino_jdbc": struct(
        coordinate = "io.trino:trino-jdbc:483",
        sha256 = "518062369fdfa937b35f11aa45276daa31cccaaad3c8c05eb51e674e23a419e3",
        users = ["pct"],
    ),
    # ClickHouse's JDBC driver (2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §19): the PCT ClickHouse lanes'
    # sessions on the embedded server (@embedded_clickhouse), over its HTTP port. Test-only;
    # group:artifact:version:classifier -- the shaded jar with every dependency inside. Apache-2.0. Checked against
    # Maven Central's sha1 on 2026-10-12.
    "clickhouse_jdbc": struct(
        coordinate = "com.clickhouse:clickhouse-jdbc:0.10.0:all-dependencies",
        sha256 = "ba4325c59a8ff5ec6cb7cf85b206e4317e6be5d056512aadc7a6e6a4abbd3e81",
        users = ["pct"],
    ),
    "sqlite_jdbc": struct(
        coordinate = "org.xerial:sqlite-jdbc:3.47.1.0",
        sha256 = "4164d95347aeab42b754cabb7945b93e54f29ddb2eded967dbe979b4045cb685",
        users = ["core"],
    ),
    # MongoDB's Java driver (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12): the MongoDB connector's client
    # (//core:mongodb_connector, which rides with //core:drivers), and the embedded server's user setup (//testing
    # EmbeddedMongoDb). Four jars, the driver's required closure (sync, core, bson, and bson-record-codec, core's one
    # non-optional runtime dependency); everything else its POM declares is optional. Apache-2.0. Each checked
    # against Maven Central's download on 2026-10-17.
    "mongodb_driver_sync": struct(
        coordinate = "org.mongodb:mongodb-driver-sync:5.13.0",
        sha256 = "b1042221132552cdf56f95bcd61d524b745923d7bc3ace1aaa882a7fd51fb94e",
        users = ["core", "testing"],
    ),
    "mongodb_driver_core": struct(
        coordinate = "org.mongodb:mongodb-driver-core:5.13.0",
        sha256 = "44f0bc54cba2ef1cbfff8ad5f84e0f1bfccf4886661a93b0e4882ed9c4fdb81d",
        users = ["core", "testing"],
    ),
    "bson": struct(
        coordinate = "org.mongodb:bson:5.13.0",
        sha256 = "51f583ee96fda9bc0897e73321c5ae1485102bfd8f95d0d34d7d57919cbc650a",
        users = ["core", "testing"],
    ),
    "bson_record_codec": struct(
        coordinate = "org.mongodb:bson-record-codec:5.13.0",
        sha256 = "0f6aca2c3c4b3c63e0c1fea65147c51af02be5d8d940f298abb3dc0906ca1ba0",
        users = ["core", "testing"],
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
