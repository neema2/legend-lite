// The model as a query app reads it: an index over a PureModelContextData.
//
// Display only: what a class's properties are, their docs, subclasses. Anything that needs the
// compiler -- which properties a mapping maps, what a data space offers -- is asked of the engine
// (`analytics/mapping/modelCoverage`, `analytics/dataSpace/render`) and is not worked out here;
// legend-lite does not serve those yet, so the app shows what the model declares.

import type { GenericType, Multiplicity } from '../../../pure-protocol/src/index.ts';
import {
  pathOf,
  type Annotated, type PAssociation, type PClass, type PDataSpace, type PElement, type PEnumeration,
  type PFunction, type PMapping, type PProfile, type PProperty, type PQualifiedProperty, type PRuntime,
  type PService, type PureModelContextData, type TaggedValue,
} from '../../../engine-client/src/legend/pmcd.ts';

export const DOC_PROFILE = 'meta::pure::profiles::doc';
const TEMPORAL_PROFILE = 'meta::pure::profiles::temporal';

/**
 * A profile's path as written: a simple name (`doc`, `temporal`) resolves through Pure's
 * auto-imported `meta::pure::profiles` package -- the model's JSON keeps the name as written.
 */
export function profilePath(name: string): string {
  return name.includes('::') ? name : `meta::pure::profiles::${name}`;
}

/** A class's milestoning (its `temporal` stereotype): the dates its `all()` takes. */
export type Temporal = 'businesstemporal' | 'processingtemporal' | 'bitemporal';

/** The dates a temporal class's `all()` (or a property into it) takes, in order: processing first. */
export function milestoningDates(t: Temporal): readonly ('processingDate' | 'businessDate')[] {
  return t === 'bitemporal' ? ['processingDate', 'businessDate'] : t === 'businesstemporal' ? ['businessDate'] : ['processingDate'];
}

/** The primitive types, by the name the grammar writes. */
const PRIMITIVES = new Set([
  'String', 'Boolean', 'Integer', 'Float', 'Decimal', 'Number', 'Date', 'StrictDate', 'DateTime',
  'StrictTime', 'LatestDate', 'Byte', 'Binary',
]);

/** Precise primitives (store-shaped types) and the standard primitive a query treats them as. */
const PRECISE: Readonly<Record<string, string>> = {
  Varchar: 'String', Char: 'String', Text: 'String',
  Int: 'Integer', TinyInt: 'Integer', UTinyInt: 'Integer', SmallInt: 'Integer', USmallInt: 'Integer',
  UInt: 'Integer', BigInt: 'Integer', UBigInt: 'Integer',
  Double: 'Float', Real: 'Float', Numeric: 'Decimal',
  Timestamp: 'DateTime', Time: 'StrictTime',
};

export type PrimitiveFamily = 'string' | 'boolean' | 'integer' | 'float' | 'decimal' | 'number' | 'date' | 'time' | 'other';

/** The standard primitive for a type name (`Varchar` is `String`), or the name itself. */
export function standardPrimitive(name: string): string {
  return PRECISE[name] ?? name;
}

/** May a value of type `from` go where `to` is expected? Pure's primitive subtyping: StrictDate and DateTime are Dates; Integer, Float and Decimal are Numbers. */
export function assignable(from: string, to: string): boolean {
  const f = standardPrimitive(from), t = standardPrimitive(to);
  if (f === t) return true;
  if (t === 'Date') return f === 'StrictDate' || f === 'DateTime';
  if (t === 'Number') return f === 'Integer' || f === 'Float' || f === 'Decimal';
  return false;
}

export function isPrimitive(name: string): boolean {
  return PRIMITIVES.has(standardPrimitive(name));
}

export function primitiveFamily(name: string): PrimitiveFamily {
  switch (standardPrimitive(name)) {
    case 'String': return 'string';
    case 'Boolean': return 'boolean';
    case 'Integer': return 'integer';
    case 'Float': return 'float';
    case 'Decimal': return 'decimal';
    case 'Number': return 'number';
    case 'Date': case 'StrictDate': case 'DateTime': case 'LatestDate': return 'date';
    case 'StrictTime': return 'time';
    default: return 'other';
  }
}

export function isNumericFamily(f: PrimitiveFamily): boolean {
  return f === 'integer' || f === 'float' || f === 'decimal' || f === 'number';
}

/** A type's path, from a generic type (`String`, `my::Class`). */
export function typePath(t: GenericType): string {
  const raw = t.rawType;
  if (raw._type === 'packageableType') return raw.fullPath;
  return 'meta::pure::metamodel::relation::Relation';
}

export function isToMany(m: Multiplicity): boolean {
  return m.upperBound === undefined || m.upperBound > 1;
}

export function isOptional(m: Multiplicity): boolean {
  return m.lowerBound === 0;
}

export function multiplicityText(m: Multiplicity): string {
  if (m.upperBound === undefined) return m.lowerBound === 0 ? '[*]' : `[${m.lowerBound}..*]`;
  if (m.lowerBound === m.upperBound) return `[${m.lowerBound}]`;
  return `[${m.lowerBound}..${m.upperBound}]`;
}

/** The last segment of a path: `my::pkg::Thing` is `Thing`; a function's signature suffix is dropped. */
export function simpleName(path: string): string {
  const i = path.lastIndexOf('::');
  const name = i < 0 ? path : path.slice(i + 2);
  const sig = name.indexOf('__');
  return sig > 0 ? name.slice(0, sig) : name;
}

export function packageOf(path: string): string {
  const i = path.lastIndexOf('::');
  return i < 0 ? '' : path.slice(0, i);
}

/** `legalName` is `Legal Name`; `firmID` is `Firm ID`; `FIRM_ID` is `Firm Id`. */
export function humanize(name: string): string {
  if (/^[A-Z0-9_]+$/.test(name)) {
    return name.toLowerCase().split('_').filter(Boolean).map((w) => w[0]!.toUpperCase() + w.slice(1)).join(' ');
  }
  const spaced = name
    .replace(/_/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2');
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function docOf(a: Annotated | undefined): string | undefined {
  const docs = (a?.taggedValues ?? []).filter((t) => profilePath(t.tag.profile) === DOC_PROFILE && t.tag.value === 'doc');
  return docs.length === 0 ? undefined : docs.map((d) => d.value).join('\n');
}

export function otherTaggedValues(a: Annotated | undefined): readonly TaggedValue[] {
  return (a?.taggedValues ?? []).filter((t) => !(profilePath(t.tag.profile) === DOC_PROFILE && t.tag.value === 'doc'));
}

export type TypeKind = 'primitive' | 'enumeration' | 'class' | 'unknown';

/** A property of a class, as the explorer shows it: its own, inherited, or from an association. */
export interface PropertyInfo {
  readonly name: string;
  readonly owner: string;
  readonly type: string;
  readonly kind: TypeKind;
  readonly multiplicity: Multiplicity;
  readonly derived: boolean;
  /** A derived property's parameters (beyond `$this`); empty for a plain one. */
  readonly parameters: readonly { readonly name: string; readonly type: string; readonly multiplicity: Multiplicity }[];
  readonly doc: string | undefined;
  readonly taggedValues: readonly TaggedValue[];
  readonly stereotypes: readonly { readonly profile: string; readonly value: string }[];
  /** Declared by an association rather than the class. */
  readonly association: string | undefined;
}

export class ModelGraph {
  readonly elements: ReadonlyMap<string, PElement>;
  readonly classes: ReadonlyMap<string, PClass>;
  readonly enumerations: ReadonlyMap<string, PEnumeration>;
  readonly associations: ReadonlyMap<string, PAssociation>;
  readonly profiles: ReadonlyMap<string, PProfile>;
  readonly mappings: ReadonlyMap<string, PMapping>;
  readonly runtimes: ReadonlyMap<string, PRuntime>;
  readonly dataSpaces: ReadonlyMap<string, PDataSpace>;
  readonly services: ReadonlyMap<string, PService>;
  readonly functions: ReadonlyMap<string, PFunction>;

  readonly #subclasses = new Map<string, string[]>();
  readonly #associationProps = new Map<string, { prop: PProperty; association: string }[]>();
  readonly #properties = new Map<string, readonly PropertyInfo[]>();

  constructor(pmcd: PureModelContextData) {
    const all = new Map<string, PElement>();
    const classes = new Map<string, PClass>();
    const enumerations = new Map<string, PEnumeration>();
    const associations = new Map<string, PAssociation>();
    const profiles = new Map<string, PProfile>();
    const mappings = new Map<string, PMapping>();
    const runtimes = new Map<string, PRuntime>();
    const dataSpaces = new Map<string, PDataSpace>();
    const services = new Map<string, PService>();
    const functions = new Map<string, PFunction>();
    for (const e of pmcd.elements) {
      if (e._type === 'sectionIndex') continue;
      const path = pathOf(e);
      all.set(path, e);
      switch (e._type) {
        case 'class': classes.set(path, e as PClass); break;
        case 'Enumeration': enumerations.set(path, e as PEnumeration); break;
        case 'association': associations.set(path, e as PAssociation); break;
        case 'profile': profiles.set(path, e as PProfile); break;
        case 'mapping': mappings.set(path, e as PMapping); break;
        case 'runtime': runtimes.set(path, e as PRuntime); break;
        case 'dataSpace': dataSpaces.set(path, e as PDataSpace); break;
        case 'service': services.set(path, e as PService); break;
        case 'function': functions.set(path, e as PFunction); break;
        default: break;
      }
    }
    this.elements = all;
    this.classes = classes;
    this.enumerations = enumerations;
    this.associations = associations;
    this.profiles = profiles;
    this.mappings = mappings;
    this.runtimes = runtimes;
    this.dataSpaces = dataSpaces;
    this.services = services;
    this.functions = functions;

    for (const [path, c] of classes) {
      for (const s of this.superTypesOf(c)) {
        const list = this.#subclasses.get(s) ?? [];
        list.push(path);
        this.#subclasses.set(s, list);
      }
    }
    for (const [path, a] of associations) {
      const [p0, p1] = a.properties;
      if (p0 === undefined || p1 === undefined) continue;
      // Each end is a property of the OTHER end's type.
      this.#addAssociationProp(typePath(p1.genericType), p0, path);
      this.#addAssociationProp(typePath(p0.genericType), p1, path);
    }
  }

  #addAssociationProp(owner: string, prop: PProperty, association: string): void {
    const list = this.#associationProps.get(owner) ?? [];
    list.push({ prop, association });
    this.#associationProps.set(owner, list);
  }

  superTypesOf(c: PClass): string[] {
    return (c.superTypes ?? []).map((s) => (typeof s === 'string' ? s : (s.fullPath ?? s.path ?? '')))
      .filter((s) => s.length > 0 && s !== 'meta::pure::metamodel::type::Any' && s !== 'Any');
  }

  /** Every class this one extends, nearest first. */
  ancestors(path: string): string[] {
    const out: string[] = [];
    const seen = new Set<string>([path]);
    const queue = [path];
    while (queue.length > 0) {
      const c = this.classes.get(queue.shift()!);
      if (c === undefined) continue;
      for (const s of this.superTypesOf(c)) {
        if (seen.has(s)) continue;
        seen.add(s);
        out.push(s);
        queue.push(s);
      }
    }
    return out;
  }

  /** A class's milestoning, from its own `temporal` stereotype (or an ancestor's); undefined when it has none. */
  temporalOf(path: string): Temporal | undefined {
    for (const c of [path, ...this.ancestors(path)]) {
      const s = (this.classes.get(c)?.stereotypes ?? []).find((t) => profilePath(t.profile) === TEMPORAL_PROFILE);
      if (s && (s.value === 'businesstemporal' || s.value === 'processingtemporal' || s.value === 'bitemporal')) return s.value;
    }
    return undefined;
  }

  /**
   * What a property takes when written with arguments: a derived property its parameters; a
   * property into a temporal class its dates (Date[1] each, processing first); any other nothing.
   */
  parametersOf(p: PropertyInfo): PropertyInfo['parameters'] {
    if (p.derived) return p.parameters;
    const t = p.kind === 'class' ? this.temporalOf(p.type) : undefined;
    return t === undefined ? [] : milestoningDates(t).map((name) => ({ name, type: 'Date', multiplicity: { lowerBound: 1, upperBound: 1 } }));
  }

  /** The direct subclasses of a class. */
  subclasses(path: string): readonly string[] {
    return this.#subclasses.get(path) ?? [];
  }

  kindOf(type: string): TypeKind {
    if (isPrimitive(type)) return 'primitive';
    if (this.enumerations.has(type)) return 'enumeration';
    if (this.classes.has(type)) return 'class';
    return 'unknown';
  }

  /**
   * A class's properties: its own, its ancestors', those associations give it, and derived
   * (qualified) ones. A subclass's property shadows an ancestor's of the same name.
   */
  properties(classPath: string): readonly PropertyInfo[] {
    const cached = this.#properties.get(classPath);
    if (cached) return cached;
    const byName = new Map<string, PropertyInfo>();
    for (const owner of [classPath, ...this.ancestors(classPath)]) {
      const c = this.classes.get(owner);
      if (c === undefined) continue;
      for (const p of c.properties ?? []) this.#put(byName, this.#plain(p, owner, undefined));
      for (const q of c.qualifiedProperties ?? []) this.#put(byName, this.#derived(q, owner));
      for (const { prop, association } of this.#associationProps.get(owner) ?? []) {
        this.#put(byName, this.#plain(prop, owner, association));
      }
    }
    const list = [...byName.values()];
    this.#properties.set(classPath, list);
    return list;
  }

  /** Only the properties a class declares itself (a subtype node's children). */
  ownProperties(classPath: string): readonly PropertyInfo[] {
    return this.properties(classPath).filter((p) => p.owner === classPath);
  }

  property(classPath: string, name: string): PropertyInfo | undefined {
    return this.properties(classPath).find((p) => p.name === name);
  }

  #put(byName: Map<string, PropertyInfo>, p: PropertyInfo): void {
    if (!byName.has(p.name)) byName.set(p.name, p);
  }

  #plain(p: PProperty, owner: string, association: string | undefined): PropertyInfo {
    const type = typePath(p.genericType);
    return {
      name: p.name, owner, type, kind: this.kindOf(type), multiplicity: p.multiplicity, derived: false,
      parameters: [], doc: docOf(p), taggedValues: otherTaggedValues(p), stereotypes: p.stereotypes ?? [],
      association,
    };
  }

  #derived(q: PQualifiedProperty, owner: string): PropertyInfo {
    const type = typePath(q.returnGenericType);
    return {
      name: q.name, owner, type, kind: this.kindOf(type), multiplicity: q.returnMultiplicity, derived: true,
      parameters: q.parameters.filter((p) => p.name !== 'this').map((p) => ({
        name: p.name,
        type: p.genericType ? typePath(p.genericType) : 'Any',
        multiplicity: p.multiplicity ?? { lowerBound: 1, upperBound: 1 },
      })),
      doc: docOf(q), taggedValues: otherTaggedValues(q), stereotypes: q.stereotypes ?? [], association: undefined,
    };
  }

  /**
   * The classes a mapping's class mappings name, as the mapping element declares them (its
   * includes' too) -- what a source offers to query. Which PROPERTIES are mapped is not worked
   * out here: that is the engine's mapping analysis, which legend-lite does not serve yet.
   */
  mappedClasses(mappingPath: string): string[] {
    const out = new Set<string>();
    const visit = (path: string, seen: Set<string>): void => {
      if (seen.has(path)) return;
      seen.add(path);
      const m = this.mappings.get(path);
      for (const cm of m?.classMappings ?? []) out.add(cm.class);
      for (const inc of m?.includedMappings ?? []) if (inc.includedMapping) visit(inc.includedMapping, seen);
    };
    visit(mappingPath, new Set());
    return [...out].sort((a, b) => simpleName(a).localeCompare(simpleName(b)));
  }

  /** The mappings whose class mappings name a class (or one of its subclasses), as the elements declare. */
  mappingsFor(classPath: string): string[] {
    const wanted = new Set([classPath, ...this.subclasses(classPath)]);
    return [...this.mappings.entries()]
      .filter(([, m]) => (m.classMappings ?? []).some((cm) => wanted.has(cm.class)))
      .map(([p]) => p)
      .sort();
  }

  /** The runtimes that declare a mapping. */
  runtimesFor(mappingPath: string): string[] {
    return [...this.runtimes.entries()]
      .filter(([, r]) => (r.runtimeValue.mappings ?? []).some((m) => m.path === mappingPath))
      .map(([p]) => p)
      .sort();
  }

  /** Every element a person can document-search: classes, enumerations, associations. */
  documentedElements(): { path: string; kind: 'class' | 'enumeration' | 'association'; doc: string | undefined }[] {
    const out: { path: string; kind: 'class' | 'enumeration' | 'association'; doc: string | undefined }[] = [];
    for (const [p, c] of this.classes) out.push({ path: p, kind: 'class', doc: docOf(c) });
    for (const [p, e] of this.enumerations) out.push({ path: p, kind: 'enumeration', doc: docOf(e) });
    for (const [p, a] of this.associations) out.push({ path: p, kind: 'association', doc: docOf(a) });
    return out.sort((a, b) => a.path.localeCompare(b.path));
  }
}
