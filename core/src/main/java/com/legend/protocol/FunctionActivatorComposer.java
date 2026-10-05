// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * The function activators as upstream prints them: {@code ###Snowflake}'s app and M2M UDF, {@code ###BigQuery}'s
 * function, {@code ###MemSql}'s function, {@code ###HostedService} and {@code ###FunctionJar}
 * ({@code SnowflakeGrammarComposer}, {@code BigQueryFunctionGrammarComposer}, {@code MemSqlFunctionGrammarComposer},
 * {@code HostedServiceGrammarComposer}, {@code FunctionJarGrammarComposer}). Their paths print unquoted, as upstream's do.
 */
final class FunctionActivatorComposer {

    private static final String DEPLOYMENT_OWNER = "DeploymentOwner";

    private FunctionActivatorComposer() {
    }

    /** The {@code ###Snowflake} section's two kinds. */
    static String snowflake(Json.Obj e) {
        return "snowflakeM2MUdf".equals(Composing.type(e)) ? snowflakeM2MUdf(e) : snowflakeApp(e);
    }

    private static String head(String keyword, Json.Obj e) {
        return DomainComposer.declarationPrefix(keyword, "", e) + Composing.path(e) + "\n{\n";
    }

    private static String function(Json.Obj e) {
        return "   function : " + e.getObj("function").getString("path") + ";\n";
    }

    /** {@code ((DeploymentOwner) ownership).id}: any other owner fails upstream's cast. */
    private static String deploymentId(Json.Obj e) {
        Json.Obj owner = e.getObj("ownership");
        if (!DEPLOYMENT_OWNER.equals(Composing.type(owner))) {
            throw Composing.refused("an activator of _type '" + Composing.type(e) + "' owned by a '" + Composing.type(owner)
                    + "' (upstream's printer casts to a deployment owner)");
        }
        return owner.getString("id");
    }

    private static String quotedLine(String key, @com.legend.base.Nullable String value) {
        return value == null ? "" : "   " + key + " : '" + value + "';\n";
    }

    private static String activation(Json.Obj e) {
        Json.Obj config = objOr(e, "activationConfiguration");
        return config == null ? "" : "   activationConfiguration : " + config.getObj("activationConnection").getString("connection") + ";\n";
    }

    private static String snowflakeApp(Json.Obj app) {
        String permission = str(app, "permissionScheme");
        return head("SnowflakeApp", app)
                + "   applicationName : '" + app.getString("applicationName") + "';\n"
                + function(app)
                + "   ownership : Deployment { identifier: '" + deploymentId(app) + "'};\n"
                + quotedLine("description", str(app, "description"))
                + quotedLine("usageRole", str(app, "usageRole"))
                + (permission == null ? "" : "   permissionScheme : " + permission + ";\n")
                + quotedLine("deploymentSchema", str(app, "deploymentSchema"))
                + activation(app)
                + "}";
    }

    private static String snowflakeM2MUdf(Json.Obj udf) {
        return head("SnowflakeM2MUdf", udf)
                + "   udfName : '" + udf.getString("udfName") + "';\n"
                + function(udf)
                + "   ownership : Deployment { identifier: '" + deploymentId(udf) + "'};\n"
                + "   deploymentSchema : '" + str(udf, "deploymentSchema") + "';\n"
                + "   deploymentStage : '" + str(udf, "deploymentStage") + "';\n"
                + quotedLine("description", str(udf, "description"))
                + activation(udf)
                + "}";
    }

    /** {@code ###BigQuery}'s and {@code ###MemSql}'s functions: the same shape under their own keyword. */
    static String bigQueryFunction(Json.Obj f) {
        return namedFunction("BigQueryFunction", f);
    }

    static String memSqlFunction(Json.Obj f) {
        return namedFunction("MemSqlFunction", f);
    }

    private static String namedFunction(String keyword, Json.Obj f) {
        return head(keyword, f)
                + "   functionName : '" + f.getString("functionName") + "';\n"
                + function(f)
                + "   ownership : Deployment { identifier: '" + deploymentId(f) + "' };\n"
                + quotedLine("description", str(f, "description"))
                + activation(f)
                + "}";
    }

    static String hostedService(Json.Obj s) {
        requireNoActions(s);
        return head("HostedService", s)
                + "   pattern : " + convertString(s.getString("pattern"), true) + ";\n"
                + "   ownership : " + owner(s)
                + function(s)
                + quotedLine("documentation", str(s, "documentation"))
                + "   autoActivateUpdates : " + s.getBoolOr("autoActivateUpdates", false) + ";\n"
                + "}";
    }

    private static String owner(Json.Obj s) {
        Json.Obj owner = s.getObj("ownership");
        if ("userList".equals(Composing.type(owner))) {
            List<String> users = new ArrayList<>();
            for (String u : owner.getStringArrayOr("users", List.of())) {
                users.add(tab(2) + convertString(u, true));
            }
            return "UserList { users: [\n" + String.join(",\n", users) + "\n" + tab(2) + "] };\n";
        }
        return "Deployment { identifier: '" + deploymentId(s) + "' };\n";
    }

    static String functionJar(Json.Obj j) {
        return head("FunctionJar", j)
                + "   ownership : Deployment { identifier: '" + deploymentId(j) + "' };\n"
                + function(j)
                + quotedLine("documentation", str(j, "documentation"))
                + "}";
    }

    /** Post-deployment actions print through extensions none of which the reference engine registers. */
    private static void requireNoActions(Json.Obj e) {
        if (!items(e, "actions").isEmpty()) {
            throw Composing.refused("an activator of _type '" + Composing.type(e) + "' with post-deployment actions");
        }
    }
}
