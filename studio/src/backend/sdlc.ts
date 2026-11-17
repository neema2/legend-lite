// Where Studio's projects and published versions live (design S21): the model home every app here shares
// (depot-client/src/model-home.ts) -- in this page, or on servers. The rest of Studio sees one SdlcClient and one
// DepotClient either way.

import { connectModelHome, type ModelHome, type ModelHomeConfig } from '../../../depot-client/src/model-home.ts';

export interface StudioConfig extends ModelHomeConfig {
  /**
   * The session's engine (plan A1): absent, the in-tab one (legend-lite's planner in a worker); else a legend-lite
   * server's API root (`http://127.0.0.1:8080/api`), asked on its own /api/lite/v1 (the edge program's H7, 2026-11-17).
   */
  readonly engine?: string;
}
export type Connection = ModelHome;

export const connect = (config: StudioConfig): Promise<Connection> => connectModelHome(config);
