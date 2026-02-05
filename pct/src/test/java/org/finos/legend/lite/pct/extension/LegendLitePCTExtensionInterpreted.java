// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.lite.pct.extension;

import org.finos.legend.pure.runtime.java.interpreted.extension.BaseInterpretedExtension;
import org.finos.legend.pure.runtime.java.interpreted.extension.InterpretedExtension;

/**
 * Interpreted extension that registers Legend-Lite native functions
 * for the PCT framework.
 * 
 * This extension is discovered via SPI (ServiceLoader) and registers
 * the executeLegendLiteQuery native function which bridges PCT tests
 * to Legend-Lite's QueryService execution engine.
 */
public class LegendLitePCTExtensionInterpreted extends BaseInterpretedExtension {

    public LegendLitePCTExtensionInterpreted() {
        super("executeLegendLiteQuery_String_1__Any_1_", ExecuteLegendLiteQuery::new);
    }

    public static InterpretedExtension extension() {
        return new LegendLitePCTExtensionInterpreted();
    }
}
