// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.Locale;

import static com.legend.protocol.Composing.tab;

/**
 * An authentication specification ({@code # UserPassword { ... }#}) and its credential vault secrets as
 * upstream prints them ({@code AuthenticationSpecificationComposer} and its secret composer), for the
 * store connections that carry one (Elasticsearch, MongoDB, Deephaven).
 */
final class AuthenticationComposer {

    private AuthenticationComposer() {
    }

    /** {@code renderAuthentication(spec, indentLevel, context)}: {@code i} is the context's indentation. */
    static String authentication(Json.Obj spec, int level, String i) {
        String type = Composing.type(spec);
        if ("userPassword".equals(type)) {
            return "# UserPassword {\n"
                    + i + tab(level + 1) + "username: '" + spec.getString("username") + "';\n"
                    + i + tab(level + 1) + "password: " + secret(spec.getObj("password"), level + 1) + ";\n"
                    + i + tab(level) + "}#";
        }
        if ("PSK".equals(type)) {
            return "# PSK {\n"
                    + i + tab(level + 1) + "psk: '" + spec.getString("psk") + "';\n"
                    + i + tab(level) + "}#";
        }
        if ("kerberos".equals(type)) {
            return "# Kerberos {\n" + i + tab(level) + "}#";
        }
        if ("apiKey".equals(type)) {
            return "# ApiKey {\n"
                    + i + tab(level + 1) + "location: '" + spec.getString("location").toLowerCase(Locale.ROOT) + "';\n"
                    + i + tab(level + 1) + "keyName: '" + spec.getString("keyName") + "';\n"
                    + i + tab(level + 1) + "value: " + secret(spec.getObj("value"), level + 1) + ";\n"
                    + i + tab(level) + "}#";
        }
        if ("encryptedPrivateKey".equals(type)) {
            return "# EncryptedPrivateKey {\n"
                    + i + tab(level + 1) + "userName: '" + spec.getString("userName") + "';\n"
                    + i + tab(level + 1) + "privateKey: " + secret(spec.getObj("privateKey"), level + 1) + ";\n"
                    + i + tab(level + 1) + "passphrase: " + secret(spec.getObj("passphrase"), level + 1) + ";\n"
                    + i + tab(level) + "}#";
        }
        throw Composing.refused("no composer rule for an authentication specification of _type '" + type + "'");
    }

    /** A credential vault secret: its keyword, then its one field in a block at {@code level}. */
    private static String secret(Json.Obj secret, int level) {
        String type = Composing.type(secret);
        String keyword;
        String field;
        if ("properties".equals(type)) {
            keyword = "PropertiesFileSecret";
            field = "propertyName";
        } else if ("environment".equals(type)) {
            keyword = "EnvironmentSecret";
            field = "envVariableName";
        } else if ("systemproperties".equals(type)) {
            keyword = "SystemPropertiesSecret";
            field = "systemPropertyName";
        } else {
            throw Composing.refused("no composer rule for a credential vault secret of _type '" + type + "'");
        }
        return keyword + "\n" + tab(level) + "{\n" + tab(level + 1) + field + ": '" + secret.getString(field) + "';\n" + tab(level) + "}";
    }
}
