// PureModelContextData as legend-engine writes it (`grammar/grammarToJson/model`, or Depot's
// entities): the element shapes a query app reads. Types only, and only the fields it reads --
// the wire carries more, which is kept (these are views, never re-serialized from here).

import type { GenericType, Lambda, Multiplicity, ValueSpecification } from '../../../pure-protocol/src/index.ts';

export interface TagRef {
  readonly profile: string;
  readonly value: string;
}

export interface TaggedValue {
  readonly tag: TagRef;
  readonly value: string;
}

export interface Annotated {
  readonly stereotypes?: readonly TagRef[];
  readonly taggedValues?: readonly TaggedValue[];
}

export interface ElementBase extends Annotated {
  readonly _type: string;
  readonly name: string;
  readonly package: string;
}

export interface PProperty extends Annotated {
  readonly name: string;
  readonly genericType: GenericType;
  readonly multiplicity: Multiplicity;
}

export interface PQualifiedProperty extends Annotated {
  readonly name: string;
  readonly parameters: readonly { readonly name: string; readonly genericType?: GenericType; readonly multiplicity?: Multiplicity }[];
  readonly returnGenericType: GenericType;
  readonly returnMultiplicity: Multiplicity;
  readonly body: readonly ValueSpecification[];
}

export interface PClass extends ElementBase {
  readonly _type: 'class';
  readonly superTypes?: readonly (string | { readonly path?: string; readonly fullPath?: string })[];
  readonly properties?: readonly PProperty[];
  readonly qualifiedProperties?: readonly PQualifiedProperty[];
  readonly originalMilestonedProperties?: readonly PProperty[];
}

export interface PEnumeration extends ElementBase {
  readonly _type: 'Enumeration';
  readonly values: readonly (Annotated & { readonly value: string })[];
}

export interface PAssociation extends ElementBase {
  readonly _type: 'association';
  readonly properties: readonly PProperty[];
  readonly qualifiedProperties?: readonly PQualifiedProperty[];
}

export interface PProfile extends ElementBase {
  readonly _type: 'profile';
  readonly stereotypes?: readonly TagRef[] & readonly { readonly value: string }[];
  readonly tags?: readonly { readonly value: string }[];
}

export interface PPropertyMapping {
  readonly _type: string;
  readonly property: { readonly class?: string; readonly property: string };
  readonly target?: string;
}

export interface PClassMapping {
  readonly _type: string;
  readonly class: string;
  readonly id?: string;
  readonly root?: boolean;
  readonly propertyMappings?: readonly PPropertyMapping[];
}

export interface PMapping extends ElementBase {
  readonly _type: 'mapping';
  readonly classMappings?: readonly PClassMapping[];
  readonly associationMappings?: readonly { readonly association?: string; readonly propertyMappings?: readonly PPropertyMapping[] }[];
  readonly enumerationMappings?: readonly { readonly enumeration: string }[];
  readonly includedMappings?: readonly { readonly includedMapping?: string; readonly _type?: string }[];
}

export interface PRuntime extends ElementBase {
  readonly _type: 'runtime';
  readonly runtimeValue: {
    readonly _type: string;
    readonly mappings?: readonly { readonly path: string }[];
    readonly connections?: readonly { readonly store: { readonly path: string } }[];
  };
}

export interface PElementRef {
  readonly path: string;
  readonly type?: string;
}

export interface PDataSpaceExecutionContext {
  readonly name: string;
  readonly title?: string;
  readonly description?: string;
  readonly mapping?: PElementRef;
  readonly defaultRuntime?: PElementRef;
}

export type PDataSpaceExecutable =
  | {
    readonly _type: 'dataSpaceTemplateExecutable';
    readonly id?: string;
    readonly title: string;
    readonly description?: string;
    readonly executionContextKey?: string;
    readonly query: Lambda;
  }
  | {
    readonly _type: 'dataSpacePackageableElementExecutable';
    readonly id?: string;
    readonly title: string;
    readonly description?: string;
    readonly executionContextKey?: string;
    readonly executable: PElementRef;
  };

export interface PDataSpaceSupportInfo {
  readonly _type: string;
  readonly documentationUrl?: string;
  readonly address?: string;
  readonly emails?: readonly string[];
  readonly website?: string;
  readonly faqUrl?: string;
  readonly supportUrl?: string;
}

export interface PDataSpace extends ElementBase {
  readonly _type: 'dataSpace';
  readonly title?: string;
  readonly description?: string;
  readonly executionContexts: readonly PDataSpaceExecutionContext[];
  readonly defaultExecutionContext: string;
  readonly elements?: readonly (PElementRef & { readonly exclude?: boolean })[];
  readonly executables?: readonly PDataSpaceExecutable[];
  readonly diagrams?: readonly { readonly title: string; readonly description?: string; readonly diagram: PElementRef }[];
  readonly supportInfo?: PDataSpaceSupportInfo;
}

export interface PService extends ElementBase {
  readonly _type: 'service';
  readonly pattern: string;
  readonly documentation?: string;
  readonly execution: {
    readonly _type: string;
    readonly func?: Lambda;
    readonly mapping?: string;
    readonly runtime?: { readonly _type: string; readonly runtime?: string };
    readonly executionParameters?: unknown;
    readonly executions?: readonly {
      readonly key: string;
      readonly mapping: string;
      readonly runtime: { readonly _type: string; readonly runtime?: string };
    }[];
  };
}

export interface PFunction extends ElementBase {
  readonly _type: 'function';
  readonly parameters: readonly { readonly name: string; readonly genericType?: GenericType; readonly multiplicity?: Multiplicity }[];
  readonly returnGenericType: GenericType;
  readonly returnMultiplicity: Multiplicity;
  readonly body: readonly ValueSpecification[];
}

export type PElement =
  | PClass | PEnumeration | PAssociation | PProfile | PMapping | PRuntime | PDataSpace | PService | PFunction
  | (ElementBase & { readonly _type: 'relational' | 'connection' | 'sectionIndex' | 'diagram' | string });

export interface PureModelContextData {
  readonly _type: 'data';
  readonly elements: readonly PElement[];
}

export function pathOf(e: { readonly package: string; readonly name: string }): string {
  return e.package ? `${e.package}::${e.name}` : e.name;
}
