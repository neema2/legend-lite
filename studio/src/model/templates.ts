// The text a new element starts as, by kind (upstream Studio's "New Element" kinds that text mode needs
// first). The user picks a kind and a full path; everything after that is text.

export interface ElementKind {
  readonly label: string;
  /** The element's text, for `pkg::Name`. */
  template(pkg: string, name: string): string;
}

export const ELEMENT_KINDS: readonly ElementKind[] = [
  { label: 'Class', template: (p, n) => `Class ${p}::${n}\n{\n  name: String[1];\n}\n` },
  { label: 'Enumeration', template: (p, n) => `Enum ${p}::${n}\n{\n  VALUE_ONE,\n  VALUE_TWO\n}\n` },
  { label: 'Association', template: (p, n) => `Association ${p}::${n}\n{\n  left: ${p}::Left[1];\n  right: ${p}::Right[*];\n}\n` },
  { label: 'Profile', template: (p, n) => `Profile ${p}::${n}\n{\n  stereotypes: [important];\n  tags: [doc];\n}\n` },
  { label: 'Function', template: (p, n) => `function ${p}::${n}(): String[1]\n{\n  'hello'\n}\n` },
  { label: 'Mapping', template: (p, n) => `###Mapping\nMapping ${p}::${n}\n(\n)\n` },
  { label: 'Database', template: (p, n) => `###Relational\nDatabase ${p}::${n}\n(\n  Table T\n  (\n    ID INTEGER PRIMARY KEY\n  )\n)\n` },
  { label: 'Runtime', template: (p, n) => `###Runtime\nRuntime ${p}::${n}\n{\n  mappings:\n  [\n  ];\n}\n` },
  { label: 'Service', template: (p, n) => `###Service\nService ${p}::${n}\n{\n  pattern: '/${n}';\n  documentation: '';\n  execution: Single\n  {\n    query: |${p}::Thing.all()->project(~[name: x | $x.name]);\n    mapping: ${p}::Mapping;\n    runtime: ${p}::Runtime;\n  }\n}\n` },
];

/** `a::b::C` as its package and name, or undefined when it is not a full path. */
export function splitPath(path: string): { pkg: string; name: string } | undefined {
  const m = /^((?:[A-Za-z0-9_]+::)+)([A-Za-z0-9_$]+)$/.exec(path.trim());
  if (!m) return undefined;
  return { pkg: m[1]!.slice(0, -2), name: m[2]! };
}
