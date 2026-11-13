// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/**
 * A class PLANTED in exec's package to send a MongoDB command of its own, for one test alone (the C final audit's J3,
 * the user's ruling of 2026-11-12): {@code ArchitectureTest.theMongoDbRuleBitesAPlantedCommand} shows
 * {@code ArchitectureTest.theMongoDbDriverSendsAtOneChokePoint} fails on it -- the version check
 * {@code MongoDbStatements} sent until then, written outside the one method that sends. A test class, compiled against
 * the driver in its own library ({@code //core:mongodb_planted}): the rule's real check reads the product's classes.
 */
public final class PlantedMongoCommand {

    private PlantedMongoCommand() {
    }

    /** The server's version, read by a command exec wrote: the defect the rule refuses. */
    public static BsonDocument version(MongoDatabase database) {
        return database.runCommand(new BsonDocument("buildInfo", new BsonInt32(1)), BsonDocument.class);
    }
}
