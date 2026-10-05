// The page: read the config (`?config=<file>`, default config.json; `?sdlc=<url>` overrides where
// projects live) and start Studio. With `"sdlc": "page"` nothing runs on a server: the SDLC and the
// compiler are WebAssembly in this tab and projects stay in this browser (design S21, level 0).

import { start } from '../src/app/app.ts';
import type { StudioConfig } from '../src/backend/sdlc.ts';

const params = new URLSearchParams(location.search);
const res = await fetch(params.get('config') ?? './config.json', { cache: 'no-cache' });
const file = (await res.json()) as StudioConfig & { worker: string };
const config: StudioConfig = { ...file, sdlc: params.get('sdlc') ?? file.sdlc };
await start(document.getElementById('app')!, config, file.worker);
