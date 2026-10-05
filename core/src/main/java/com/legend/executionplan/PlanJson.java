// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.EnumValue;
import com.legend.executionplan.ExecutionPlan.JsonResult;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Node;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsColumn;
import com.legend.executionplan.ExecutionPlan.TdsResult;
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
    public static final int VERSION = 1;

    private PlanJson() {
    }

    /** The {@code _type} tags of the lite format, one spelling for the writer and the reader: an exhaustive switch over
     *  each family, an unknown tag refused by name ({@link #tag}). */
    enum NodeTag { sequence, tdsResult, jsonResult }

    enum SpecificationTag { InMemory, LocalFile, LocalH2, EmbeddedH2, StaticDatasource, Snowflake, Spanner, Databricks,
        BigQuery }

    enum AuthenticationTag { NoAuth, DefaultH2, TestAuth, GCPApplicationDefaultCredentials, UsernamePassword,
        DelegatedKerberos, VaultUserNamePassword, SnowflakePublic, ApiToken, MiddleTierUserNamePassword, OAuth,
        GcpWorkloadIdentityFederation }

    private static <E extends Enum<E>> E tag(Class<E> family, Json.Obj o, String what) {
        String t = o.getString("_type");
        for (E e : family.getEnumConstants()) {
            if (e.name().equals(t)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " _type '" + t + "'");
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
        if (!p.enumValues().isEmpty()) {
            List<Object> values = new ArrayList<>();
            for (EnumValue v : p.enumValues()) {
                Map<String, Object> e = new LinkedHashMap<>();
                e.put("name", v.name());
                e.put("databaseValues", v.databaseValues());
                values.add(e);
            }
            o.put("enumValues", values);
        }
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
                List<Object> cols = new ArrayList<>();
                for (TdsColumn c : t.columns()) {
                    Map<String, Object> co = new LinkedHashMap<>();
                    co.put("name", c.name());
                    co.put("type", c.type());
                    co.put("sqlType", c.sqlType());
                    cols.add(co);
                }
                o.put("columns", cols);
                o.put("sql", sql(t.sql()));
            }
            case JsonResult j -> {
                o.put("_type", NodeTag.jsonResult.name());
                o.put("type", j.type());
                o.put("multiplicity", multiplicity(j.multiplicity()));
                o.put("sql", sql(j.sql()));
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
        Target t = s.target();
        Map<String, Object> to = new LinkedHashMap<>();
        to.put("connection", connection(t.connection()));
        to.put("setup", t.setup());
        to.put("identity", t.identity());
        o.put("target", to);
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
        List<EnumValue> values = new ArrayList<>();
        Json.Arr ev = o.getArrOr("enumValues", null);
        if (ev != null) {
            for (Json.Node n : ev.items()) {
                Json.Obj e = (Json.Obj) n;
                List<Object> db = new ArrayList<>();
                for (Json.Node v : e.getArr("databaseValues").items()) {
                    db.add(switch (v) {
                        case Json.Str s -> s.value();
                        case Json.Num num when num.isInteger() -> num.longValue();
                        case Json.Num num -> throw new IllegalArgumentException("enum value '" + e.getString("name")
                                + "': a database value is a String or an integer, not " + num.doubleValue());
                        case Json.Obj x -> throw notADatabaseValue(e);
                        case Json.Arr x -> throw notADatabaseValue(e);
                        case Json.Bool x -> throw notADatabaseValue(e);
                        case Json.Null x -> throw notADatabaseValue(e);
                    });
                }
                values.add(new EnumValue(e.getString("name"), db));
            }
        }
        return new Parameter(o.getString("name"), o.getString("type"), readMultiplicity(o.getObj("multiplicity")),
                values);
    }

    private static IllegalArgumentException notADatabaseValue(Json.Obj e) {
        return new IllegalArgumentException("enum value '" + e.getString("name")
                + "': a database value is a String or an integer");
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
            case tdsResult -> {
                List<TdsColumn> cols = new ArrayList<>();
                for (Json.Node n : o.getArr("columns").items()) {
                    Json.Obj c = (Json.Obj) n;
                    cols.add(new TdsColumn(c.getString("name"), c.getString("type"), c.getString("sqlType")));
                }
                yield new TdsResult(cols, readSql(o.getObj("sql")));
            }
            case jsonResult -> new JsonResult(o.getString("type"), readMultiplicity(o.getObj("multiplicity")),
                    readSql(o.getObj("sql")));
        };
    }

    private static Sql readSql(Json.Obj o) {
        List<Slot> slots = new ArrayList<>();
        for (Json.Node n : o.getArr("slots").items()) {
            Json.Obj s = (Json.Obj) n;
            slots.add(new Slot(s.getString("parameter"), s.getStringOr("arrayElementSqlType", null)));
        }
        Json.Obj t = o.getObj("target");
        Target target = new Target(readConnection(t.getObj("connection")), t.getStringArray("setup"),
                t.getString("identity"));
        return new Sql(o.getString("statement"), slots, target, null);
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
