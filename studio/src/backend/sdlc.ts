// Where Studio's projects and published versions live (design S21): the model home every app here shares
// (depot-client/src/model-home.ts) -- in this page, or on servers. The rest of Studio sees one SdlcClient and one
// DepotClient either way.

import { connectModelHome, type ModelHome, type ModelHomeConfig } from '../../../depot-client/src/model-home.ts';

export type StudioConfig = ModelHomeConfig;
export type Connection = ModelHome;

export const connect = (config: StudioConfig): Promise<Connection> => connectModelHome(config);
