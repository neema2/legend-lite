// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// PLANTED (census B2): an edge module, the one user of the product's tdsColumnsOf.
import { tdsColumnsOf } from '../product/relation-reader.ts';

export function legendEngineRoutes(): string {
  return `/api/server/v1/currentUser ${tdsColumnsOf.length}`;
}
