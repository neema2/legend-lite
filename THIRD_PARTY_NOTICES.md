# Third-party notices

legend-lite is licensed under the Apache License 2.0 ([LICENSE](LICENSE)); its attributions are in [NOTICE](NOTICE).
This file lists the third-party software that legend-lite's products redistribute, by product. Build and test tools
(Bazel and its rules, esbuild, TypeScript, the JDK used by `bazel run`, LLVM, Chromium, Playwright, the embedded
Postgres used by tests, the `org.finos.legend` jars the equivalence tests run against) are not redistributed and are
not listed.

## FINOS Legend

| Project | Licence | What is used | Shipped in |
|---|---|---|---|
| [legend-pure](https://github.com/finos/legend-pure), [legend-engine](https://github.com/finos/legend-engine) | Apache-2.0 | Pure declarations copied verbatim (`prelude.pure`); signatures, registry names and imports extracted by `spec/`'s generators; test fixtures | core server, Python wheel, WebAssembly planner (all apps) |
| [legend-studio](https://github.com/finos/legend-studio) @821c74c | Apache-2.0 | Colour tokens, type icons, Pure Monarch tokenizer and editor themes, look and layout | Query, Studio, DataCube |

See [NOTICE](NOTICE) for the files and their notice.

## Java and native

| Component | Version | Licence | Shipped in |
|---|---|---|---|
| [DuckDB](https://github.com/duckdb/duckdb) (JDBC driver and native library) | 1.4.4.0 (core), 1.5.5.1 (warehouse) | MIT, © Stichting DuckDB Foundation; DuckDB includes third-party code under its own licences (see its `third_party/` directory) | core server; warehouse; DataCube app |
| DuckDB [postgres extension](https://github.com/duckdb/duckdb-postgres) | 1.5.5 | MIT; links libpq (PostgreSQL Licence) | warehouse; DataCube app |
| [H2 Database Engine](https://h2database.com) | 2.1.214 | MPL-2.0 or EPL-1.0 (source: https://github.com/h2database/h2database) | core server |
| [PostgreSQL JDBC Driver](https://jdbc.postgresql.org) | 42.7.13 | BSD-2-Clause | core server |
| [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc) | 3.47.1.0 | Apache-2.0; includes code under the Zentus BSD-style licence; SQLite itself is public domain | core server |
| [GraalVM Community Edition](https://www.graalvm.org) (Substrate VM and the JDK class library compiled into native images) | 25.0.2 | GPL-2.0 with the Classpath Exception; GraalVM's third-party licences | warehouse native server; DataCube app; `libcompiler`; Python wheel (licence files ship in its `.dist-info`) |
| [zlib](https://zlib.net) | 1.3.2 | Zlib | native images (Linux) |
| [TeaVM](https://teavm.org) class library, JSO and runtime | 0.15.0 | Apache-2.0 | WebAssembly planner and the SDLC page (`classes.wasm`, `wasm-gc-module-runtime.js`) |

## JavaScript, WebAssembly, fonts and icons

| Component | Version | Licence | Shipped in |
|---|---|---|---|
| [DuckDB-Wasm](https://github.com/duckdb/duckdb-wasm) (with DuckDB compiled in) | 1.33.1-dev57.0 | MIT; DuckDB's third-party code as above | Query, Studio, DataCube |
| [Apache Arrow](https://arrow.apache.org) (JavaScript) | 17.0.0 | Apache-2.0 (NOTICE below) | Query, Studio, DataCube, Python wheel's site |
| [FlatBuffers](https://github.com/google/flatbuffers) | 24.12.23 | Apache-2.0 | as Arrow |
| [tslib](https://github.com/microsoft/tslib) | 2.8.1, 2.3.0 | 0BSD | as Arrow; DataCube |
| [fzstd](https://github.com/101arrowz/fzstd) | 0.1.1 | MIT | Query, Studio, DataCube |
| [Apache ECharts](https://echarts.apache.org) | 6.1.0 | Apache-2.0 (NOTICE in [NOTICE](NOTICE)); includes code from d3 (BSD-3-Clause) | DataCube |
| [ZRender](https://github.com/ecomfe/zrender) | 6.1.0 | BSD-3-Clause | DataCube |
| [fflate](https://github.com/101arrowz/fflate) | 0.8.3 | MIT | DataCube |
| [Monaco Editor](https://github.com/microsoft/monaco-editor) | 0.57.0 | MIT; its ThirdPartyNotices; includes DOMPurify 3.4.15 (used under Apache-2.0, of MPL-2.0 or Apache-2.0), marked 14.0.0 (MIT) and the Codicons font (CC BY 4.0) | Studio |
| [react-icons](https://github.com/react-icons/react-icons) SVG paths, generated into `legend-art/src/icons.ts` | 5.5.0 | MIT; per icon set: Font Awesome Free (CC BY 4.0), Codicons (CC BY 4.0), Material Design icons (Apache-2.0), Remix Icon 4.2.0 (Apache-2.0), GitHub Octicons, Feather, Ionicons 5, Tabler, Bootstrap Icons (MIT), Simple Icons (CC0) | Query, Studio, DataCube |
| [Roboto, Roboto Mono, Raleway](https://fontsource.org) (via @fontsource) | 5.3.0 | SIL Open Font License 1.1 (licence files ship beside the fonts) | Query, Studio, DataCube, Python wheel's site |
| GitHub VS Code Theme (light) colours, via legend-studio | 6.3.4 | MIT, © GitHub | Studio |

## Licence texts

Apache-2.0 components are under the licence in [LICENSE](LICENSE). GraalVM's licences (GPL-2.0 with the Classpath
Exception) ship beside the products built with it (the wheel's `.dist-info`, the DataCube app's `licenses/`). The fonts'
SIL Open Font License 1.1 texts ship beside the fonts (`vendor/fonts/LICENSE-*.txt`). The other licences:

### MIT

Each of these components is copyright its holder, as below, and is licensed under the MIT terms that follow:

- DuckDB, DuckDB-Wasm, DuckDB postgres extension: Copyright 2018-2025 Stichting DuckDB Foundation
- Monaco Editor: Copyright (c) 2016 - present Microsoft Corporation
- marked: Copyright (c) 2018+, MarkedJS (https://github.com/markedjs/); Copyright (c) 2011-2018, Christopher Jeffrey
  (https://github.com/chjj/)
- fzstd: Copyright (c) 2020 Arjun Barrett
- fflate: Copyright (c) 2026 Arjun Barrett
- react-icons: Copyright 2018 kamijin_fanta <kamijin@live.jp>
- GitHub Octicons: Copyright (c) 2026 GitHub Inc.
- Feather: Copyright (c) 2013-2023 Cole Bemis
- Ionicons: Copyright (c) 2015-present Ionic (http://ionic.io/)
- Tabler Icons: Copyright (c) 2020-2026 Paweł Kuna
- Bootstrap Icons: Copyright (c) 2019-2024 The Bootstrap Authors
- GitHub VS Code Theme: Copyright (c) 2020 Primer

```
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### BSD-3-Clause: ZRender

```
BSD 3-Clause License

Copyright (c) 2017, Baidu Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### BSD-3-Clause: d3 (code included in Apache ECharts)

```
Copyright 2010-2016 Mike Bostock
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the author nor the names of contributors may be used to
  endorse or promote products derived from this software without specific prior
  written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### BSD-2-Clause: PostgreSQL JDBC Driver

```
Copyright (c) 1997, PostgreSQL Global Development Group
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```

### sqlite-jdbc: code under the Zentus licence

```
Copyright (c) 2006, David Crawshaw.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
```

### PostgreSQL Licence: libpq (linked by the DuckDB postgres extension)

```
PostgreSQL Database Management System
(also known as Postgres, formerly known as Postgres95)

Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group

Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
```

### 0BSD: tslib

```
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
```

### Others, by reference

- H2 Database Engine: MPL-2.0 (https://www.mozilla.org/MPL/2.0/) or EPL-1.0
  (https://www.eclipse.org/legal/epl-v10.html), at your option; its source is at https://github.com/h2database/h2database.
- DOMPurify (in Monaco Editor): used under Apache-2.0, of its MPL-2.0 or Apache-2.0.
- Font Awesome Free icons and Codicons: CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); attribution in
  [NOTICE](NOTICE).
- Simple Icons: CC0 1.0 (https://creativecommons.org/publicdomain/zero/1.0/).
- zlib: the zlib licence (https://zlib.net/zlib_license.html).
- SQLite: public domain.

## Apache Arrow NOTICE

```
Apache Arrow
Copyright 2016-2024 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

This product includes software from the SFrame project (BSD, 3-clause).
* Copyright (C) 2015 Dato, Inc.
* Copyright (c) 2009 Carnegie Mellon University.

This product includes software from the Feather project (Apache 2.0)
https://github.com/wesm/feather

This product includes software from the DyND project (BSD 2-clause)
https://github.com/libdynd

This product includes software from the LLVM project
 * distributed under the University of Illinois Open Source

This product includes software from the google-lint project
 * Copyright (c) 2009 Google Inc. All rights reserved.

This product includes software from the mman-win32 project
 * Copyright https://code.google.com/p/mman-win32/
 * Licensed under the MIT License;

This product includes software from the LevelDB project
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * Moved from Kudu http://github.com/cloudera/kudu

This product includes software from the CMake project
 * Copyright 2001-2009 Kitware, Inc.
 * Copyright 2012-2014 Continuum Analytics, Inc.
 * All rights reserved.

This product includes software from https://github.com/matthew-brett/multibuild (BSD 2-clause)
 * Copyright (c) 2013-2016, Matt Terry and Matthew Brett; all rights reserved.

This product includes software from the Ibis project (Apache 2.0)
 * Copyright (c) 2015 Cloudera, Inc.
 * https://github.com/cloudera/ibis

This product includes software from Dremio (Apache 2.0)
  * Copyright (C) 2017-2018 Dremio Corporation
  * https://github.com/dremio/dremio-oss

This product includes software from Google Guava (Apache 2.0)
  * Copyright (C) 2007 The Guava Authors
  * https://github.com/google/guava

This product include software from CMake (BSD 3-Clause)
  * CMake - Cross Platform Makefile Generator
  * Copyright 2000-2019 Kitware, Inc. and Contributors

The web site includes files generated by Jekyll.

--------------------------------------------------------------------------------

This product includes code from Apache Kudu, which includes the following in
its NOTICE file:

  Apache Kudu
  Copyright 2016 The Apache Software Foundation

  This product includes software developed at
  The Apache Software Foundation (http://www.apache.org/).

  Portions of this software were developed at
  Cloudera, Inc (http://www.cloudera.com/).

--------------------------------------------------------------------------------

This product includes code from Apache ORC, which includes the following in
its NOTICE file:

  Apache ORC
  Copyright 2013-2019 The Apache Software Foundation

  This product includes software developed by The Apache Software
  Foundation (http://www.apache.org/).

  This product includes software developed by Hewlett-Packard:
  (c) Copyright [2014-2015] Hewlett-Packard Development Company, L.P
```
