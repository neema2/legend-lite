// The SDLC client (README.md): upstream legend-sdlc's records and lite's text routes, one typed client,
// and the page's SDLC (sdlc-server's rules, compiled to WebAssembly) behind a fetch.

export * from './wire.ts';
export * from './client.ts';
export { BrowserRecords, MemoryRecords, DATABASE, type Records } from './records.ts';
export { wasmSdlcServer, WASM_API, WASM_DEPOT_API, type SdlcModule, type WasmSdlc } from './wasm-server.ts';
