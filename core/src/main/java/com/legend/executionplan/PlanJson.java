// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.Format;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Node;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Relation;
import com.legend.executionplan.ExecutionPlan.ResultType;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.SetupStep;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsColumn;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.executionplan.ExecutionPlan.TextResult;
import com.legend.executionplan.ExecutionPlan.Value;
import com.legend.json.Json;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE LITE PLAN FORMAT: an {@link ExecutionPlan} as JSON and back. Lite's own, clean shape — legend-engine's protocol is a
 * separate, later serialisation of the same plan (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8). Every {@code _type} is an
 * enum constant shared by the writer and the reader, read by an exhaustive switch; an unknown tag is refused by name. The typed SQL tree is not written yet: a plan read back has none.
 */
public final class PlanJson {

    /** The format's name and version, the first two fields of every plan. */
    public static final String FORMAT = "legend-lite-plan";
    /** 2 since 2026-10-08: step 2's records (enum values by name, {@code textResult}, setup steps, no identity); 3 since
     *  2026-10-09: the planner's plans (step 2's landing 2) — a target's database (declared or the platform's), its
     *  server versions and session statements, and a text result's columns without a SQL type. */
    public static final int VERSION = 3;

    private PlanJson() {
    }

    /** The {@code _type} tags of the lite format, one spelling for the writer and the reader: an exhaustive switch over
     *  each family, an unknown tag refused by name ({@link #tag}). */
    enum NodeTag { sequence, tdsResult, textResult }

    enum ResultTypeTag { relation, value }

    enum SetupTag { statement, rows }

    enum DatabaseTag { declared, platform }

    enum ServersTag { every, versions }

    enum SpecificationTag { InMemory, LocalFile, LocalH2, EmbeddedH2, StaticDatasource, Snowflake, Spanner, Databricks,
        BigQuery }

    enum AuthenticationTag { NoAuth, DefaultH2, TestAuth, GCPApplicationDefaultCredentials, UsernamePassword,
        DelegatedKerberos, VaultUserNamePassword, SnowflakePublic, ApiToken, MiddleTierUserNamePassword, OAuth,
        GcpWorkloadIdentityFederation }

    private static <E extends Enum<E>> E tag(Class<E> family, Json.Obj o, String what) {
        return named(family, o.getString("_type"), what + " _type");
    }

    private static <E extends Enum<E>> E named(Class<E> family, String name, String what) {
        for (E e : family.getEnumConstants()) {
            if (e.name().equals(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " '" + name + "'");
    }

    // ---- write ------------------------------------------------------------------------------------------------------

    public static String write(ExecutionPlan plan) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", FORMAT);
        out.put("version", VERSION);
        out.put("parameters", plan.parameters().stream().map(PlanJson::parameter).toList());
        out.put("root", node(plan.root()));
        return Json.toCompact(out);
    }

    private static Map<String, Object> parameter(Parameter p) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", p.name());
        o.put("type", p.type());
        o.put("multiplicity", multiplicity(p.multiplicity()));
        o.put("enumValues", p.enumValues());
        return o;
    }

    private static Map<String, Object> multiplicity(Multiplicity m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("lower", m.lower());
        if (m.upper() != null) {
            o.put("upper", m.upper());
        }
        return o;
    }

    private static Map<String, Object> node(Node n) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (n) {
            case Sequence s -> {
                o.put("_type", NodeTag.sequence.name());
                o.put("steps", s.steps().stream().map(PlanJson::node).toList());
            }
            case TdsResult t -> {
                o.put("_type", NodeTag.tdsResult.name());
                o.put("columns", columns(t.columns()));
                o.put("sql", sql(t.sql()));
            }
            case TextResult x -> {
                o.put("_type", NodeTag.textResult.name());
                o.put("format", x.format().name());
                o.put("type", resultType(x.type()));
                o.put("sql", sql(x.sql()));
            }
        }
        return o;
    }

    private static List<Object> columns(List<TdsColumn> columns) {
        List<Object> cols = new ArrayList<>();
        for (TdsColumn c : columns) {
            Map<String, Object> co = new LinkedHashMap<>();
            co.put("name", c.name());
            co.put("type", c.type());
            co.put("sqlType", c.sqlType());
            cols.add(co);
        }
        return cols;
    }

    private static Map<String, Object> resultType(ResultType type) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (type) {
            case Relation r -> {
                o.put("_type", ResultTypeTag.relation.name());
                List<Object> cols = new ArrayList<>();
                for (ExecutionPlan.Column c : r.columns()) {
                    Map<String, Object> co = new LinkedHashMap<>();
                    co.put("name", c.name());
                    co.put("type", c.type());
                    cols.add(co);
                }
                o.put("columns", cols);
            }
            case Value v -> {
                o.put("_type", ResultTypeTag.value.name());
                o.put("type", v.type());
                o.put("multiplicity", multiplicity(v.multiplicity()));
            }
        }
        return o;
    }

    private static Map<String, Object> sql(Sql s) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("statement", s.statement());
        List<Object> slots = new ArrayList<>();
        for (Slot slot : s.slots()) {
            Map<String, Object> so = new LinkedHashMap<>();
            so.put("parameter", slot.parameter());
            if (slot.arrayElementSqlType() != null) {
                so.put("arrayElementSqlType", slot.arrayElementSqlType());
            }
            slots.add(so);
        }
        o.put("slots", slots);
        o.put("target", target(s.target()));
        return o;
    }

    /** {@code target} in the lite format, alone: an unambiguous spelling of its whole content (a runner keys a shared
     *  database by it, decision A). */
    public static String writeTarget(Target target) {
        return Json.toCompact(target(target));
    }

    private static Map<String, Object> target(Target t) {
        Map<String, Object> to = new LinkedHashMap<>();
        to.put("database", database(t.database()));
        to.put("servers", servers(t.servers()));
        to.put("session", t.session());
        to.put("setup", t.setup().stream().map(PlanJson::setupStep).toList());
        return to;
    }

    private static Map<String, Object> database(ExecutionPlan.Database database) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (database) {
            case ExecutionPlan.Database.Declared d -> {
                o.put("_type", DatabaseTag.declared.name());
                o.put("connection", connection(d.connection()));
            }
            case ExecutionPlan.Database.Platform p -> {
                o.put("_type", DatabaseTag.platform.name());
                o.put("databaseType", p.type().name());
            }
        }
        return o;
    }

    private static Map<String, Object> servers(ExecutionPlan.Servers servers) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (servers) {
            case ExecutionPlan.Servers.Every e -> o.put("_type", ServersTag.every.name());
            case ExecutionPlan.Servers.Versions v -> {
                o.put("_type", ServersTag.versions.name());
                o.put("prefixes", v.prefixes());
            }
        }
        return o;
    }

    private static Map<String, Object> setupStep(SetupStep step) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (step) {
            case SetupStep.Statement s -> {
                o.put("_type", SetupTag.statement.name());
                o.put("sql", s.sql());
            }
            case SetupStep.Rows r -> {
                o.put("_type", SetupTag.rows.name());
                o.put("stagingTable", r.stagingTable());
                o.put("createStaging", r.createStaging());
                o.put("copy", r.copy());
                o.put("dropStaging", r.dropStaging());
                o.put("rows", r.rows());
            }
        }
        return o;
    }

    private static Map<String, Object> connection(ConnectionDefinition c) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", c.qualifiedName());
        if (c.storeName() != null) {
            o.put("store", c.storeName());
        }
        o.put("databaseType", c.databaseType().name());
        o.put("specification", specification(c.specification()));
        o.put("authentication", authentication(c.authentication()));
        return o;
    }

    private static Map<String, Object> specification(ConnectionSpecification spec) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (spec) {
            case ConnectionSpecification.InMemory x -> o.put("_type", SpecificationTag.InMemory.name());
            case ConnectionSpecification.LocalFile x -> {
                o.put("_type", SpecificationTag.LocalFile.name());
                o.put("path", x.path());
            }
            case ConnectionSpecification.LocalH2 x -> {
                o.put("_type", SpecificationTag.LocalH2.name());
                putIfSet(o, "url", x.url());
                putIfSet(o, "testDataSetupCsv", x.testDataSetupCsv());
                if (x.testDataSetupSqls() != null) {
                    o.put("testDataSetupSqls", x.testDataSetupSqls());
                }
            }
            case ConnectionSpecification.EmbeddedH2 x -> {
                o.put("_type", SpecificationTag.EmbeddedH2.name());
                o.put("databaseName", x.databaseName());
                o.put("directory", x.directory());
                o.put("autoServerMode", x.autoServerMode());
            }
            case ConnectionSpecification.StaticDatasource x -> {
                o.put("_type", SpecificationTag.StaticDatasource.name());
                o.put("host", x.host());
                o.put("port", x.port());
                o.put("database", x.database());
            }
            case ConnectionSpecification.Snowflake x -> {
                o.put("_type", SpecificationTag.Snowflake.name());
                o.put("databaseName", x.databaseName());
                o.put("accountName", x.accountName());
                o.put("warehouseName", x.warehouseName());
                o.put("region", x.region());
                putIfSet(o, "accountType", x.accountType());
                putIfSet(o, "cloudType", x.cloudType());
                if (x.enableQueryTags() != null) {
                    o.put("enableQueryTags", x.enableQueryTags());
                }
                putIfSet(o, "organization", x.organization());
                putIfSet(o, "role", x.role());
            }
            case ConnectionSpecification.Spanner x -> {
                o.put("_type", SpecificationTag.Spanner.name());
                o.put("projectId", x.projectId());
                o.put("instanceId", x.instanceId());
                o.put("databaseId", x.databaseId());
            }
            case ConnectionSpecification.Databricks x -> {
                o.put("_type", SpecificationTag.Databricks.name());
                o.put("hostname", x.hostname());
                o.put("port", x.port());
                o.put("protocol", x.protocol());
                o.put("httpPath", x.httpPath());
            }
            case ConnectionSpecification.BigQuery x -> {
                o.put("_type", SpecificationTag.BigQuery.name());
                o.put("projectId", x.projectId());
                o.put("defaultDataset", x.defaultDataset());
            }
        }
        return o;
    }

    private static Map<String, Object> authentication(AuthenticationSpec auth) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (auth) {
            case AuthenticationSpec.NoAuth x -> o.put("_type", AuthenticationTag.NoAuth.name());
            case AuthenticationSpec.DefaultH2 x -> o.put("_type", AuthenticationTag.DefaultH2.name());
            case AuthenticationSpec.TestAuth x -> o.put("_type", AuthenticationTag.TestAuth.name());
            case AuthenticationSpec.GCPApplicationDefaultCredentials x ->
                    o.put("_type", AuthenticationTag.GCPApplicationDefaultCredentials.name());
            case AuthenticationSpec.UsernamePassword x -> {
                o.put("_type", AuthenticationTag.UsernamePassword.name());
                o.put("username", x.username());
                o.put("passwordVaultRef", x.passwordVaultRef());
            }
            case AuthenticationSpec.DelegatedKerberos x -> {
                o.put("_type", AuthenticationTag.DelegatedKerberos.name());
                putIfSet(o, "serverPrincipal", x.serverPrincipal());
                putIfSet(o, "kerberosRemoteServiceName", x.kerberosRemoteServiceName());
                if (x.kerberosUseCanonicalHostname() != null) {
                    o.put("kerberosUseCanonicalHostname", x.kerberosUseCanonicalHostname());
                }
            }
            case AuthenticationSpec.VaultUserNamePassword x -> {
                o.put("_type", AuthenticationTag.VaultUserNamePassword.name());
                putIfSet(o, "baseVaultReference", x.baseVaultReference());
                o.put("userNameVaultReference", x.userNameVaultReference());
                o.put("passwordVaultReference", x.passwordVaultReference());
            }
            case AuthenticationSpec.SnowflakePublic x -> {
                o.put("_type", AuthenticationTag.SnowflakePublic.name());
                o.put("publicUserName", x.publicUserName());
                o.put("privateKeyVaultReference", x.privateKeyVaultReference());
                o.put("passPhraseVaultReference", x.passPhraseVaultReference());
            }
            case AuthenticationSpec.ApiToken x -> {
                o.put("_type", AuthenticationTag.ApiToken.name());
                o.put("apiToken", x.apiToken());
            }
            case AuthenticationSpec.MiddleTierUserNamePassword x -> {
                o.put("_type", AuthenticationTag.MiddleTierUserNamePassword.name());
                o.put("vaultReference", x.vaultReference());
            }
            case AuthenticationSpec.OAuth x -> {
                o.put("_type", AuthenticationTag.OAuth.name());
                o.put("oauthKey", x.oauthKey());
                o.put("scopeName", x.scopeName());
            }
            case AuthenticationSpec.GcpWorkloadIdentityFederation x -> {
                o.put("_type", AuthenticationTag.GcpWorkloadIdentityFederation.name());
                o.put("serviceAccountEmail", x.serviceAccountEmail());
                o.put("additionalGcpScopes", x.additionalGcpScopes());
            }
        }
        return o;
    }

    private static void putIfSet(Map<String, Object> o, String key, @com.legend.base.Nullable String value) {
        if (value != null) {
            o.put(key, value);
        }
    }

    // ---- read -------------------------------------------------------------------------------------------------------

    public static ExecutionPlan read(String json) {
        Json.Obj o = Json.parseObject(json);
        String format = o.getStringOr("format", null);
        if (!FORMAT.equals(format) || o.getIntOr("version", -1) != VERSION) {
            throw new IllegalArgumentException("not a " + FORMAT + " v" + VERSION + " plan (format " + format
                    + ", version " + o.getIntOr("version", -1) + ")");
        }
        List<Parameter> parameters = new ArrayList<>();
        for (Json.Node n : o.getArr("parameters").items()) {
            parameters.add(readParameter((Json.Obj) n));
        }
        return new ExecutionPlan(parameters, readNode(o.getObj("root")));
    }

    private static Parameter readParameter(Json.Obj o) {
        return new Parameter(o.getString("name"), o.getString("type"), readMultiplicity(o.getObj("multiplicity")),
                o.getStringArray("enumValues"));
    }

    private static Multiplicity readMultiplicity(Json.Obj o) {
        return new Multiplicity(o.getInt("lower"), o.has("upper") ? o.getInt("upper") : null);
    }

    private static Node readNode(Json.Obj o) {
        return switch (tag(NodeTag.class, o, "plan node")) {
            case sequence -> {
                List<Node> steps = new ArrayList<>();
                for (Json.Node n : o.getArr("steps").items()) {
                    steps.add(readNode((Json.Obj) n));
                }
                yield new Sequence(steps);
            }
            case tdsResult -> new TdsResult(readColumns(o.getArr("columns")), readSql(o.getObj("sql")));
            case textResult -> new TextResult(named(Format.class, o.getString("format"), "text result format"),
                    readResultType(o.getObj("type")), readSql(o.getObj("sql")));
        };
    }

    private static List<ExecutionPlan.Column> readRelationColumns(Json.Arr columns) {
        List<ExecutionPlan.Column> cols = new ArrayList<>();
        for (Json.Node n : columns.items()) {
            Json.Obj c = (Json.Obj) n;
            cols.add(new ExecutionPlan.Column(c.getString("name"), c.getString("type")));
        }
        return cols;
    }

    private static List<TdsColumn> readColumns(Json.Arr columns) {
        List<TdsColumn> cols = new ArrayList<>();
        for (Json.Node n : columns.items()) {
            Json.Obj c = (Json.Obj) n;
            cols.add(new TdsColumn(c.getString("name"), c.getString("type"), c.getString("sqlType")));
        }
        return cols;
    }

    private static ResultType readResultType(Json.Obj o) {
        return switch (tag(ResultTypeTag.class, o, "result type")) {
            case relation -> new Relation(readRelationColumns(o.getArr("columns")));
            case value -> new Value(o.getString("type"), readMultiplicity(o.getObj("multiplicity")));
        };
    }

    private static Sql readSql(Json.Obj o) {
        List<Slot> slots = new ArrayList<>();
        for (Json.Node n : o.getArr("slots").items()) {
            Json.Obj s = (Json.Obj) n;
            slots.add(new Slot(s.getString("parameter"), s.getStringOr("arrayElementSqlType", null)));
        }
        Json.Obj t = o.getObj("target");
        List<SetupStep> setup = new ArrayList<>();
        for (Json.Node n : t.getArr("setup").items()) {
            setup.add(readSetupStep((Json.Obj) n));
        }
        return new Sql(o.getString("statement"), slots, new Target(readDatabase(t.getObj("database")),
                readServers(t.getObj("servers")), t.getStringArray("session"), setup), null);
    }

    private static ExecutionPlan.Database readDatabase(Json.Obj o) {
        return switch (tag(DatabaseTag.class, o, "database")) {
            case declared -> new ExecutionPlan.Database.Declared(readConnection(o.getObj("connection")));
            case platform -> new ExecutionPlan.Database.Platform(
                    named(ConnectionDefinition.DatabaseType.class, o.getString("databaseType"), "database type"));
        };
    }

    private static ExecutionPlan.Servers readServers(Json.Obj o) {
        return switch (tag(ServersTag.class, o, "servers")) {
            case every -> new ExecutionPlan.Servers.Every();
            case versions -> new ExecutionPlan.Servers.Versions(o.getStringArray("prefixes"));
        };
    }

    private static SetupStep readSetupStep(Json.Obj o) {
        return switch (tag(SetupTag.class, o, "setup step")) {
            case statement -> new SetupStep.Statement(o.getString("sql"));
            case rows -> {
                List<List<String>> rows = new ArrayList<>();
                for (Json.Node r : o.getArr("rows").items()) {
                    if (!(r instanceof Json.Arr cells)) {
                        throw new IllegalArgumentException("rows for " + o.getString("stagingTable")
                                + ": a row is an array of cells");
                    }
                    List<String> row = new ArrayList<>();
                    for (Json.Node cell : cells.items()) {
                        row.add(switch (cell) {
                            case Json.Str s -> s.value();
                            case Json.Null x -> null;
                            case Json.Num x -> throw notTextCell(o);
                            case Json.Bool x -> throw notTextCell(o);
                            case Json.Obj x -> throw notTextCell(o);
                            case Json.Arr x -> throw notTextCell(o);
                        });
                    }
                    rows.add(row);
                }
                yield new SetupStep.Rows(o.getString("stagingTable"), o.getString("createStaging"), o.getString("copy"),
                        o.getString("dropStaging"), rows);
            }
        };
    }

    private static IllegalArgumentException notTextCell(Json.Obj rows) {
        return new IllegalArgumentException("rows for " + rows.getString("stagingTable")
                + ": a cell is text or null (the database types it)");
    }

    private static ConnectionDefinition readConnection(Json.Obj o) {
        return new ConnectionDefinition(o.getString("name"), o.getStringOr("store", null),
                ConnectionDefinition.DatabaseType.valueOf(o.getString("databaseType")),
                readSpecification(o.getObj("specification")), readAuthentication(o.getObj("authentication")));
    }

    private static ConnectionSpecification readSpecification(Json.Obj o) {
        return switch (tag(SpecificationTag.class, o, "connection specification")) {
            case InMemory -> new ConnectionSpecification.InMemory();
            case LocalFile -> new ConnectionSpecification.LocalFile(o.getString("path"));
            case LocalH2 -> new ConnectionSpecification.LocalH2(o.getStringOr("url", null),
                    o.getStringOr("testDataSetupCsv", null),
                    o.has("testDataSetupSqls") ? o.getStringArray("testDataSetupSqls") : null);
            case EmbeddedH2 -> new ConnectionSpecification.EmbeddedH2(o.getString("databaseName"),
                    o.getString("directory"), o.getBool("autoServerMode"));
            case StaticDatasource -> new ConnectionSpecification.StaticDatasource(o.getString("host"),
                    o.getInt("port"), o.getString("database"));
            case Snowflake -> new ConnectionSpecification.Snowflake(o.getString("databaseName"),
                    o.getString("accountName"), o.getString("warehouseName"), o.getString("region"),
                    o.getStringOr("accountType", null), o.getStringOr("cloudType", null),
                    o.has("enableQueryTags") ? o.getBool("enableQueryTags") : null,
                    o.getStringOr("organization", null), o.getStringOr("role", null));
            case Spanner -> new ConnectionSpecification.Spanner(o.getString("projectId"), o.getString("instanceId"),
                    o.getString("databaseId"));
            case Databricks -> new ConnectionSpecification.Databricks(o.getString("hostname"), o.getString("port"),
                    o.getString("protocol"), o.getString("httpPath"));
            case BigQuery -> new ConnectionSpecification.BigQuery(o.getString("projectId"),
                    o.getString("defaultDataset"));
        };
    }

    private static AuthenticationSpec readAuthentication(Json.Obj o) {
        return switch (tag(AuthenticationTag.class, o, "authentication")) {
            case NoAuth -> new AuthenticationSpec.NoAuth();
            case DefaultH2 -> new AuthenticationSpec.DefaultH2();
            case TestAuth -> new AuthenticationSpec.TestAuth();
            case GCPApplicationDefaultCredentials -> new AuthenticationSpec.GCPApplicationDefaultCredentials();
            case UsernamePassword -> new AuthenticationSpec.UsernamePassword(o.getString("username"),
                    o.getString("passwordVaultRef"));
            case DelegatedKerberos -> new AuthenticationSpec.DelegatedKerberos(o.getStringOr("serverPrincipal", null),
                    o.getStringOr("kerberosRemoteServiceName", null),
                    o.has("kerberosUseCanonicalHostname") ? o.getBool("kerberosUseCanonicalHostname") : null);
            case VaultUserNamePassword -> new AuthenticationSpec.VaultUserNamePassword(
                    o.getStringOr("baseVaultReference", null), o.getString("userNameVaultReference"),
                    o.getString("passwordVaultReference"));
            case SnowflakePublic -> new AuthenticationSpec.SnowflakePublic(o.getString("publicUserName"),
                    o.getString("privateKeyVaultReference"), o.getString("passPhraseVaultReference"));
            case ApiToken -> new AuthenticationSpec.ApiToken(o.getString("apiToken"));
            case MiddleTierUserNamePassword -> new AuthenticationSpec.MiddleTierUserNamePassword(
                    o.getString("vaultReference"));
            case OAuth -> new AuthenticationSpec.OAuth(o.getString("oauthKey"), o.getString("scopeName"));
            case GcpWorkloadIdentityFederation -> new AuthenticationSpec.GcpWorkloadIdentityFederation(
                    o.getString("serviceAccountEmail"), o.getStringArray("additionalGcpScopes"));
        };
    }
}
