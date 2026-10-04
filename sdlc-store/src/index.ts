// The SDLC store (README.md): upstream legend-sdlc's records and lite's text routes, one typed client,
// and the same API answered in the page.

export * from './wire.ts';
export * from './client.ts';
export { classifierPathOf } from './classifiers.ts';
export { BrowserRecords, MemoryRecords, DATABASE, type Records } from './records.ts';
export { localSdlcServer, LOCAL_API, type LocalSdlc, type LocalSdlcOptions, type ModelReader } from './local-server.ts';
