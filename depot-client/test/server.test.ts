// The model home's Depot over HTTP (`//sdlc-server:server`, Depot at /depot/api beside the SDLC, over a
// git repository on disk) -- held to the one suite.

import { after, before } from 'node:test';

import { startSdlcServer, type RunningServer } from '../../sdlc-client/test/sdlc-server.ts';
import { conformance } from './conformance.ts';

let server: RunningServer | undefined;
let base = '';

before(async () => {
  server = await startSdlcServer('depot');
  base = server.base;
});

after(() => server?.stop());

const network = globalThis.fetch.bind(globalThis);
conformance('the model home over HTTP', () => ({
  sdlc: { api: `${base}/sdlc/api`, fetch: network },
  depot: { api: `${base}/depot/api`, fetch: network },
}));
