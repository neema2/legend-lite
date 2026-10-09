// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * The function activators as upstream prints them: {@code ###Snowflake}'s app and M2M UDF, {@code ###BigQuery}'s
 * function, {@code ###MemSql}'s function, {@code ###HostedService} and {@code ###FunctionJar}
 * ({@code SnowflakeGrammarComposer}, {@code BigQueryFunctionGrammarComposer}, {@code MemSqlFunctionGrammarComposer},
 * {@code HostedServiceGrammarComposer}, {@code FunctionJarGrammarComposer}) -- over the record
 * ({@link Protocol.PFunctionActivator}; the protocol program's leg 2, step 3). Their paths print unquoted, as
 * upstream's do. Post-deployment actions (printed through extensions the reference engine does not register) have
 * no reader rule: refused when read.
 */
final class FunctionActivatorComposer {

    private FunctionActivatorComposer() {
    }

    /** The {@code ###Snowflake} section's two kinds. */
    static String snowflake(Protocol.PFunctionActivator e) {
        return "SnowflakeM2MUdf".equals(e.kind()) ? snowflakeM2MUdf(e) : snowflakeApp(e);
    }

    static String snowflake(Json.Obj e) {
        return snowflake(read(e));
    }

    /** The activator record the JSON reads as. */
    private static Protocol.PFunctionActivator read(Json.Obj e) {
        return Composing.element(e, Protocol.PFunctionActivator.class);
    }

    private static String head(String keyword, Protocol.PFunctionActivator e) {
        return DomainComposer.declarationPrefix(keyword, "", e.stereotypes(), e.taggedValues())
                + (e.pkg().isEmpty() ? e.name() : e.pkg() + "::" + e.name()) + "\n{\n";
    }

    private static String function(Protocol.PFunctionActivator e) {
        return "   function : " + e.functionPath() + ";\n";
    }

    /** {@code ((DeploymentOwner) ownership).id}: any other owner fails upstream's cast. */
    private static String deploymentId(Protocol.PFunctionActivator e) {
        if (e.ownerId() == null) {
            throw Composing.refused("a " + e.kind() + " " + (e.userListUsers() != null ? "owned by a 'userList'" : "with no owner")
                    + " (upstream's printer casts to a deployment owner)");
        }
        return e.ownerId();
    }

    private static String quotedLine(String key, @com.legend.base.Nullable String value) {
        return value == null ? "" : "   " + key + " : '" + value + "';\n";
    }

    private static String activation(Protocol.PFunctionActivator e) {
        return e.activationConnection() == null ? "" : "   activationConfiguration : " + e.activationConnection() + ";\n";
    }

    /** A required string slot, refused when absent (upstream's printer reads it unconditionally). */
    private static String required(Protocol.PFunctionActivator e, String key) {
        String v = e.scalars().get(key);
        if (v == null) {
            throw Composing.refused("a " + e.kind() + " without its " + key);
        }
        return v;
    }

    private static String snowflakeApp(Protocol.PFunctionActivator app) {
        String permission = app.scalars().get("permissionScheme");
        return head("SnowflakeApp", app)
                + "   applicationName : '" + required(app, "applicationName") + "';\n"
                + function(app)
                + "   ownership : Deployment { identifier: '" + deploymentId(app) + "'};\n"
                + quotedLine("description", app.scalars().get("description"))
                + quotedLine("usageRole", app.scalars().get("usageRole"))
                + (permission == null ? "" : "   permissionScheme : " + permission + ";\n")
                + quotedLine("deploymentSchema", app.scalars().get("deploymentSchema"))
                + activation(app)
                + "}";
    }

    private static String snowflakeM2MUdf(Protocol.PFunctionActivator udf) {
        return head("SnowflakeM2MUdf", udf)
                + "   udfName : '" + required(udf, "udfName") + "';\n"
                + function(udf)
                + "   ownership : Deployment { identifier: '" + deploymentId(udf) + "'};\n"
                // upstream prints these two whether or not they are set
                + "   deploymentSchema : '" + udf.scalars().get("deploymentSchema") + "';\n"
                + "   deploymentStage : '" + udf.scalars().get("deploymentStage") + "';\n"
                + quotedLine("description", udf.scalars().get("description"))
                + activation(udf)
                + "}";
    }

    /** {@code ###BigQuery}'s and {@code ###MemSql}'s functions: the same shape under their own keyword. */
    static String bigQueryFunction(Json.Obj f) {
        return namedFunction("BigQueryFunction", read(f));
    }

    static String memSqlFunction(Json.Obj f) {
        return namedFunction("MemSqlFunction", read(f));
    }

    static String namedFunction(String keyword, Protocol.PFunctionActivator f) {
        return head(keyword, f)
                + "   functionName : '" + required(f, "functionName") + "';\n"
                + function(f)
                + "   ownership : Deployment { identifier: '" + deploymentId(f) + "' };\n"
                + quotedLine("description", f.scalars().get("description"))
                + activation(f)
                + "}";
    }

    static String hostedService(Protocol.PFunctionActivator s) {
        return head("HostedService", s)
                + "   pattern : " + convertString(required(s, "pattern"), true) + ";\n"
                + "   ownership : " + owner(s)
                + function(s)
                + quotedLine("documentation", s.scalars().get("documentation"))
                + "   autoActivateUpdates : " + s.booleans().getOrDefault("autoActivateUpdates", false) + ";\n"
                + "}";
    }

    static String hostedService(Json.Obj s) {
        return hostedService(read(s));
    }

    private static String owner(Protocol.PFunctionActivator s) {
        List<String> users = s.userListUsers();
        if (users != null) {
            List<String> out = new ArrayList<>();
            for (String u : users) {
                out.add(tab(2) + convertString(u, true));
            }
            return "UserList { users: [\n" + String.join(",\n", out) + "\n" + tab(2) + "] };\n";
        }
        return "Deployment { identifier: '" + deploymentId(s) + "' };\n";
    }

    static String functionJar(Protocol.PFunctionActivator j) {
        return head("FunctionJar", j)
                + "   ownership : Deployment { identifier: '" + deploymentId(j) + "' };\n"
                + function(j)
                + quotedLine("documentation", j.scalars().get("documentation"))
                + "}";
    }

    static String functionJar(Json.Obj j) {
        return functionJar(read(j));
    }
}
