// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.Locale;

import static com.legend.protocol.Composing.tab;

/**
 * An authentication specification ({@code # UserPassword { ... }#}) and its credential vault secrets as
 * upstream prints them ({@code AuthenticationSpecificationComposer} and its secret composer), for the
 * store connections that carry one (Elasticsearch, MongoDB, Deephaven) -- over the records
 * ({@link Protocol.PAuthSpecValue}; the protocol program's leg 2, step 3).
 */
final class AuthenticationComposer {

    private AuthenticationComposer() {
    }

    /** {@code renderAuthentication(spec, indentLevel, context)}: {@code i} is the context's indentation. */
    static String authentication(Protocol.PAuthSpecValue spec, int level, String i) {
        return switch (spec) {
            case Protocol.PMongoAuth up -> "# UserPassword {\n"
                    + i + tab(level + 1) + "username: '" + up.username() + "';\n"
                    + i + tab(level + 1) + "password: " + secret(up.password(), level + 1) + ";\n"
                    + i + tab(level) + "}#";
            case Protocol.PPskAuth psk -> "# PSK {\n"
                    + i + tab(level + 1) + "psk: '" + psk.psk() + "';\n"
                    + i + tab(level) + "}#";
            case Protocol.PKerberosAuth k -> "# Kerberos {\n" + i + tab(level) + "}#";
            case Protocol.PApiKeyAuth key -> "# ApiKey {\n"
                    + i + tab(level + 1) + "location: '" + key.location().toLowerCase(Locale.ROOT) + "';\n"
                    + i + tab(level + 1) + "keyName: '" + key.keyName() + "';\n"
                    + i + tab(level + 1) + "value: " + secret(key.value(), level + 1) + ";\n"
                    + i + tab(level) + "}#";
            case Protocol.PEpkAuth epk -> "# EncryptedPrivateKey {\n"
                    + i + tab(level + 1) + "userName: '" + epk.userName() + "';\n"
                    + i + tab(level + 1) + "privateKey: " + secret(epk.privateKey(), level + 1) + ";\n"
                    + i + tab(level + 1) + "passphrase: " + secret(epk.passphrase(), level + 1) + ";\n"
                    + i + tab(level) + "}#";
            case Protocol.PGcpWifIslandAuth g -> throw Composing.refused("no composer rule for an authentication"
                    + " specification of _type 'gcpWithAWSIdP'");
        };
    }

    /** {@link #authentication(Protocol.PAuthSpecValue, int, String)} of the JSON, read first. */
    static String authentication(Json.Obj spec, int level, String i) {
        return authentication(ConnectionReader.authSpec(spec), level, i);
    }

    /** A credential vault secret: its keyword, then its one field in a block at {@code level}. */
    private static String secret(Protocol.PVaultSecret secret, int level) {
        if (!(secret instanceof Protocol.PMongoSecret s)) {
            throw Composing.refused("no composer rule for an AWS credential vault secret");
        }
        String keyword;
        String field;
        switch (s.kind()) {
            case "properties" -> {
                keyword = "PropertiesFileSecret";
                field = "propertyName";
            }
            case "environment" -> {
                keyword = "EnvironmentSecret";
                field = "envVariableName";
            }
            case "systemproperties" -> {
                keyword = "SystemPropertiesSecret";
                field = "systemPropertyName";
            }
            default -> throw Composing.refused("no composer rule for a credential vault secret of _type '" + s.kind()
                    + "'");
        }
        if (!field.equals(s.fieldKey())) {
            throw Composing.refused("a '" + s.kind() + "' secret whose value is in '" + s.fieldKey() + "', not '"
                    + field + "'");
        }
        return keyword + "\n" + tab(level) + "{\n" + tab(level + 1) + field + ": '" + s.value() + "';\n" + tab(level) + "}";
    }
}
