// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

/**
 * The service store's embedded data ({@code ServiceStore #{ ... }#}) as upstream prints it.
 */
final class ServiceStoreDataComposer {

    private ServiceStoreDataComposer() {
    }

    static EmbeddedDataComposer.Kind kind(Json.Obj data) {
        throw Composing.refused("no composer rule for embedded data of _type '" + Composing.type(data) + "'");
    }
}
