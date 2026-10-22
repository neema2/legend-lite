// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * legend-engine's MongoDB store and connection as the model reads them (docs/MONGODB_PLAN_2026_10_16.md, D3, D4
 * and D6; 2026-10-16). A {@code ###MongoDB} database is a store whose collections are tables and whose columns are
 * the declared JSON schema's top-level properties, each typed by its {@code bsonType}: a nested object or array is a
 * Variant ({@link RelationalDataType.SemiStructured}), an objectId is read as its hex text
 * ({@link RelationalDataType.Other}: a type Pure cannot name, read as text), a required property is NOT NULL, and
 * {@code _id}, where the schema declares it, is the key. Nothing is inferred from documents (invariant 4). A
 * {@code MongoDBConnection} declares {@link ConnectionDefinition.DatabaseType#MongoDB}: its one server as a static
 * datasource, its authentication as the vault reference it names. This is the model language's vocabulary: it
 * carries the store's declarations and decides nothing by the database.
 */
public final class MongoDeclarations {

    private MongoDeclarations() {
    }

    /** The text length a string property declares none for: a BSON string is bounded by the document's own size. */
    private static final int UNBOUNDED_TEXT = 16777216;

    /** The one server the connection names, as a static datasource; its authentication as the model's. */
    static ConnectionDefinition toConnection(String qualifiedName, Protocol.PMongoDbConnection c) {
        if (c.serverUrls().size() != 1) {
            throw new UnsupportedOperationException("MongoDBConnection '" + qualifiedName + "' names "
                    + c.serverUrls().size() + " servers: one server is supported (a replica set's list is not yet)");
        }
        Protocol.PMongoServerUrl url = c.serverUrls().get(0);
        ConnectionSpecification spec = new ConnectionSpecification.StaticDatasource(url.baseUrl(), (int) url.port(),
                c.databaseName());
        AuthenticationSpec auth = switch (c.auth()) {
            case Protocol.PMongoAuth up -> new AuthenticationSpec.UsernamePassword(up.username(),
                    vaultReference(qualifiedName, up.password()));
            case Protocol.PKerberosAuth k -> new AuthenticationSpec.DelegatedKerberos(null, null, null);
            case Protocol.PApiKeyAuth a -> refusedAuthentication(qualifiedName, "an API key");
            case Protocol.PEpkAuth e -> refusedAuthentication(qualifiedName, "an encrypted private key");
            case Protocol.PPskAuth p -> refusedAuthentication(qualifiedName, "a pre-shared key");
            case Protocol.PGcpWifIslandAuth g -> refusedAuthentication(qualifiedName, "GCP workload identity");
        };
        return new ConnectionDefinition(qualifiedName, c.element(),
                ConnectionDefinition.DatabaseType.valueOf(c.databaseType()), spec, auth);
    }

    /** A password's vault reference as the model spells one: the secret's kind and the key it names
     *  ({@code systemproperties:legend.mongodb.password}); a secret of another kind is refused by name. */
    private static String vaultReference(String qualifiedName, Protocol.PVaultSecret secret) {
        return switch (secret) {
            case Protocol.PMongoSecret s -> s.kind() + ":" + s.value();
            case Protocol.PAwsSecret s -> throw new UnsupportedOperationException("MongoDBConnection '"
                    + qualifiedName + "' keeps its password in AWS Secrets Manager, which is not read yet");
        };
    }

    private static AuthenticationSpec refusedAuthentication(String qualifiedName, String what) {
        throw new UnsupportedOperationException("MongoDBConnection '" + qualifiedName
                + "' authenticates with " + what + ", which is not implemented");
    }

    /** The store: each collection a table, its columns the schema's top-level properties. */
    static DatabaseDefinition toDatabaseDefinition(Protocol.PMongoDatabase db) {
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        for (Protocol.PMongoDatabase.PMongoCollection collection : db.collections()) {
            tables.add(new DatabaseDefinition.TableDefinition(collection.name(),
                    columns(db.qualifiedName(), collection)));
        }
        return new DatabaseDefinition(db.qualifiedName(), List.of(), List.of(), tables, List.of(), List.of(),
                List.of(), List.of(), List.of());
    }

    private static List<DatabaseDefinition.ColumnDefinition> columns(String store,
            Protocol.PMongoDatabase.PMongoCollection collection) {
        Protocol.PBsonSchema schema = collection.schema();
        java.util.Set<String> required = new java.util.HashSet<>(schema.required());
        List<DatabaseDefinition.ColumnDefinition> out = new ArrayList<>();
        for (java.util.Map.Entry<String, Protocol.PBsonSchema> property : schema.properties()) {
            String name = property.getKey();
            boolean key = "_id".equals(name);
            out.add(new DatabaseDefinition.ColumnDefinition(name,
                    columnType(store, collection.name(), name, property.getValue()), key,
                    key || required.contains(name)));
        }
        return out;
    }

    /** A property's declared type as a store column's: by its {@code bsonType}, each named, none guessed. */
    private static RelationalDataType columnType(String store, String collection, String property,
            Protocol.PBsonSchema s) {
        return switch (s.wireType()) {
            case "stringType" -> new RelationalDataType.Varchar(
                    s.maxLength() == null ? UNBOUNDED_TEXT : s.maxLength().intValue());
            case "longType" -> new RelationalDataType.BigInt();
            case "intType" -> new RelationalDataType.Integer_();
            case "boolType" -> new RelationalDataType.Bit();
            case "doubleType" -> new RelationalDataType.Double_();
            // a nested object or array is a Variant, as a relational SEMISTRUCTURED column is
            case "schema", "objectType", "arrayType" -> new RelationalDataType.SemiStructured();
            // an objectId is read as its hex text: a type Pure cannot name, so OTHER, a String read as text
            case "objectIdType" -> new RelationalDataType.Other();
            case "decimalType" -> throw new UnsupportedOperationException("collection '" + collection
                    + "' of " + store + ": property '" + property + "' is a decimal, whose scale the schema does"
                    + " not declare: not typed yet (docs/MONGODB_PLAN_2026_10_16.md §11)");
            default -> throw new IllegalStateException("collection '" + collection + "' of " + store
                    + ": property '" + property + "' has the BSON schema type '" + s.wireType()
                    + "', which the parser does not make");
        };
    }
}
