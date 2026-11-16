# @legend-lite/datacube

Embed Legend DataCube in your application with planning and query execution entirely in the user's browser.
The package includes the existing Legend planner compiled to WebAssembly, DuckDB-WASM, their workers, and the
local saved-page/query store. No Legend server, JVM, or database service is needed by your application.

## Install and serve the runtime assets

```sh
npm install @legend-lite/datacube
npx legend-datacube-assets public/datacube
```

The second command copies the installed package's version-matched runtime files into your application's static
assets directory. Run it as part of your application build and after upgrading the package. Use a dedicated
directory; the command copies and replaces matching files without deleting unrelated files. It makes no network
requests and does not compile anything. Node 22.14 or newer is needed for this build-time command.

Serve that directory from your own web origin, for example at `/datacube/`. For an application deployed under a
subpath, use the corresponding full path, such as `/my-app/datacube/`. The WASM files should be served with the
`application/wasm` content type and worker scripts with a JavaScript content type. Configure your deployment's
content security policy to permit its own workers and WebAssembly. Nothing needs to be downloaded from a CDN.

## Mount DataCube

```ts
import { createBrowserDataCube } from '@legend-lite/datacube';
import '@legend-lite/datacube/styles.css';

const host = document.querySelector<HTMLElement>('#cube')!;
const cube = await createBrowserDataCube({
  host,
  assetsUrl: '/datacube/',
  storageNamespace: 'my-application',
});

// A browser File from an <input type="file"> or drag-and-drop.
await cube.openFile(file);

// When the component is unmounted or its page is closed:
await cube.dispose();
```

Give the host a nonzero size, for example `height: 600px; width: 100%`. DataCube's stylesheet is scoped to its own
classes; the host application supplies its page layout and fonts. The existing font stack prefers Roboto when the
host provides it and otherwise uses the system sans-serif font.

The public API is framework independent. In React, Vue, or another UI framework, initialize it after the host
element mounts and dispose it when that element unmounts. Initialize in the browser only, not during server-side
rendering. For a component that can unmount while initialization is running, pass an `AbortSignal` using the
`signal` option. Use `onStatus` to report startup progress in your own UI.

Each instance owns its planner and DuckDB runtime. One instance can contain multiple grids, charts, and flows;
prefer that arrangement when they use the same data. `dispose()` releases its workers and database resources.

## Data and browser support

Use `openFile` to open the same supported files as DataCube's Open command, including CSV and Parquet. Parsing,
planning, filtering, grouping, and pivoting use the local planner and DuckDB. Saved pages use browser storage,
scoped by `storageNamespace`; saving a page does not imply that its original data files will remain available after
a browser restart. Browser storage availability and quotas still apply.

A current browser with WebAssembly GC, WebAssembly exception handling, and Web Workers is required. Unsupported
browsers or missing assets produce an error; this package does not switch to a remote compiler or database.
Serve your application over HTTPS (or localhost during development) for browser features such as the clipboard.
The package does not need a backend, but its static files still need to be served. Offline reloads additionally
require your application to cache those files, for example with its existing service worker.

The browser app retains its tools for explicitly opening remote files. Such a file is fetched from the location
the user chooses and must permit browser access. Local file analysis does not send its contents to a Legend server.

## Building and releasing from the repository

The source repository's package manifests remain private. Bazel constructs the publishable package, including
JavaScript, its generated public declarations, styles, runtime assets, and license notices:

```sh
bazel build --lockfile_mode=error //datacube:npm_archive
bazel test --lockfile_mode=error //datacube:npm_consumer_test
```

The unpacked package is `bazel-bin/datacube/npm-package/`; the npm-generated tarball is
`bazel-bin/datacube/legend-lite-datacube.tgz`. To try it before publication:

```sh
npm install /absolute/path/to/legend-lite-datacube.tgz
```

Release procedure:

1. Set the intended version in `datacube/npm/package.json` and record the API changes.
2. Run the repository's required gates and the package consumer test, which installs the tarball into an isolated
   project and exercises the real browser runtimes using only static assets.
3. Build the archive from the tested commit. Inspect it with `npm publish --dry-run` against that tarball.
4. With the maintainer's npm scope and publishing credentials configured, publish the tested tarball. Use
   `npm publish <tarball> --access public --tag next` for a prerelease; publish stable versions under `latest`.

The name `@legend-lite/datacube` requires ownership of that npm scope; this repository does not claim that it has
been registered or published. Automated releases can use npm's trusted publishing with GitHub Actions. Publishing
is a separate maintainer action and never runs as part of the Bazel build or tests.

Apache-2.0. See `LICENSE`, `NOTICE`, and `THIRD_PARTY_NOTICES.md` in the installed package.
