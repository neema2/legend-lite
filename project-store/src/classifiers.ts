// An element's protocol `_type` → its `classifierPath`, the field every SDLC `Entity` carries.
//
// `data/classifier-paths.json` is legend-engine 4.145.0's own answer to
// `GET /api/pure/v1/protocol/pure/getClassifierPathMap`, kept verbatim (sorted by type). That route
// lists only the classes registered through subtype-info collectors, so it leaves out four core
// elements the engine's serializer does map (`CorePureProtocolExtension.getExtraProtocolToClassifierPathMap`,
// legend-engine-protocol-pure `CorePureProtocolExtension.java:194-210`; their `_type`s as the engine's
// `grammarToJson` writes them, asked 2026-10-04). legend-sdlc builds entities from that serializer
// map (`PureToEntityConverter.java:30`), so an SDLC answers with these too.

import engineAnswer from '../data/classifier-paths.json' with { type: 'json' };

const CORE_NOT_IN_THE_ROUTE: readonly { readonly type: string; readonly classifierPath: string }[] = [
  { type: 'mapping', classifierPath: 'meta::pure::mapping::Mapping' },
  { type: 'connection', classifierPath: 'meta::pure::runtime::PackageableConnection' },
  { type: 'runtime', classifierPath: 'meta::pure::runtime::PackageableRuntime' },
  { type: 'dataElement', classifierPath: 'meta::pure::data::DataElement' },
];

const BY_TYPE: ReadonlyMap<string, string> = new Map(
  [...engineAnswer, ...CORE_NOT_IN_THE_ROUTE].map((e) => [e.type, e.classifierPath]),
);

/** The classifier path of an element written with this `_type`, or undefined when no SDLC stores it. */
export function classifierPathOf(type: string): string | undefined {
  return BY_TYPE.get(type);
}
