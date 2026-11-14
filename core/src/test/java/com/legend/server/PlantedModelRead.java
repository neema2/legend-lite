// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.model.ModelFromProtocol;
import com.legend.model.ParsedModel;
import com.legend.protocol.Protocol;

/**
 * A class PLANTED in the server's package to read a model's records past the doors that ask the run's wall, for one
 * test alone (the final audit of 9af16fbe58's J5, 2026-11-14): {@code ArchitectureTest
 * .MODEL_RECORDS_BY_THE_CHECKED_DOORS} fails on it, by a call and by a reference. A test class: the rule's real check
 * reads the product's classes, never this one.
 */
public final class PlantedModelRead {

    private PlantedModelRead() {
    }

    /** A model's records read outside the checked doors: the defect the rule refuses. */
    public static ParsedModel read(Protocol.PureModelContextData records) {
        return ModelFromProtocol.of(records);
    }

    /** The same, by the method's reference, never called. */
    public static final class ByReference {
        private ByReference() {
        }

        public static java.util.function.Function<Protocol.PureModelContextData, ParsedModel> reading() {
            return ModelFromProtocol::of;
        }
    }
}
