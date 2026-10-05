// Pure's grammar as Monaco colours it: section headers, element keywords, primitive types, strings,
// numbers, comments, multiplicities. Colour only; what the text means is the compiler's.

import type * as Monaco from 'monaco-editor/editor/editor.api';

export const PURE = 'pure';

const KEYWORDS = [
  'Class', 'Enum', 'Association', 'Profile', 'Primitive', 'Measure', 'Unit', 'function', 'native',
  'Mapping', 'Runtime', 'SingleConnectionRuntime', 'Database', 'Schema', 'Table', 'View', 'Join', 'Filter',
  'RelationalDatabaseConnection', 'Service', 'Data', 'DataSpace', 'Diagram', 'import', 'extends', 'let',
  'stereotypes', 'tags', 'mappings', 'connections', 'store', 'type', 'specification', 'auth', 'include',
  'Pure', 'Relational', 'Operation', 'EnumerationMapping', 'AssociationMapping', 'XStore', 'true', 'false',
];
const TYPES = ['String', 'Integer', 'Float', 'Decimal', 'Number', 'Boolean', 'Date', 'StrictDate', 'DateTime', 'Any', 'Nil'];

export function registerPure(monaco: typeof Monaco): void {
  monaco.languages.register({ id: PURE, extensions: ['.pure'] });
  monaco.languages.setLanguageConfiguration(PURE, {
    comments: { lineComment: '//', blockComment: ['/*', '*/'] },
    brackets: [['{', '}'], ['[', ']'], ['(', ')']],
    autoClosingPairs: [
      { open: '{', close: '}' }, { open: '[', close: ']' }, { open: '(', close: ')' }, { open: "'", close: "'", notIn: ['string'] },
    ],
  });
  monaco.languages.setMonarchTokensProvider(PURE, {
    keywords: KEYWORDS,
    typeKeywords: TYPES,
    tokenizer: {
      root: [
        [/^###\w+/, 'keyword.section'],
        [/\/\/.*$/, 'comment'],
        [/\/\*/, 'comment', '@comment'],
        [/'([^'\\]|\\.)*'/, 'string'],
        [/\[\s*(\d+|\*)(\s*\.\.\s*(\d+|\*))?\s*\]/, 'number.multiplicity'],
        [/%\d{4}(-\d{2}(-\d{2})?)?(T[\d:.]+)?/, 'number.date'],
        [/\d+(\.\d+)?/, 'number'],
        [/<<|>>/, 'delimiter.stereotype'],
        [/[A-Za-z_$][\w$]*(::[A-Za-z_$][\w$]*)+/, 'type.identifier.path'],
        [/[A-Za-z_$][\w$]*/, { cases: { '@keywords': 'keyword', '@typeKeywords': 'type', '@default': 'identifier' } }],
        [/\$[A-Za-z_][\w]*/, 'variable'],
        [/->|\||=>|==|!=|<=|>=|&&|\|\|/, 'operator'],
      ],
      comment: [
        [/[^/*]+/, 'comment'],
        [/\*\//, 'comment', '@pop'],
        [/[/*]/, 'comment'],
      ],
    },
  });
}
