// Pure as upstream Studio colours it (census UPSTREAM_STUDIO_LOOK.md 5.2-5.3): legend-studio @821c74c's Monarch
// tokenizer and language configuration (packages/legend-code-editor/src/PureLanguageService.ts, PureLanguage.ts),
// ported rule for rule, and its two editor themes (CodeEditorTheme.ts): `default-dark` -- Monaco's vs-dark plus
// the Pure token colours -- and `github-light`. Colour only; what the text means is the compiler's.

import type * as Monaco from 'monaco-editor/editor/editor.api';

import { GITHUB_LIGHT_COLORS } from './github-light-colors.ts';

export const PURE = 'pure';

/** upstream's theme names (CODE_EDITOR_THEME) */
export const DEFAULT_DARK = 'default-dark';
export const GITHUB_LIGHT = 'github-light';

/** The editor theme for the app's theme (getCodeEditorThemeForAppTheme). */
export const editorTheme = (light: boolean): string => (light ? GITHUB_LIGHT : DEFAULT_DARK);

/** PURE_GRAMMAR_TOKEN */
const T = {
  WHITESPACE: '', KEYWORD: 'keyword', IDENTIFIER: 'identifier', OPERATOR: 'operator', DELIMITER: 'delimiter',
  PARSER: 'parser', NUMBER: 'number', DATE: 'date', COLOR: 'color', PACKAGE: 'package', STRING: 'string',
  COMMENT: 'comment', LANGUAGE_STRUCT: 'language-struct', MULTIPLICITY: 'multiplicity', GENERICS: 'generics',
  PROPERTY: 'property', PARAMETER: 'parameter', VARIABLE: 'variable', TYPE: 'type',
} as const;

/** What upstream Studio's plugins add (getExtraPureGrammarKeywords): diagram, data space, external format, text,
 *  persistence, service store, snowflake app. */
const EXTRA_KEYWORDS = [
  'Diagram', 'DataSpace', 'Binding', 'SchemaSet', 'Text', 'Persistence', 'PersistenceContext', 'ServiceStore',
  'ServiceStoreConnection', 'ServiceGroup', 'SnowflakeApp',
];

const CONFIGURATION: Monaco.languages.LanguageConfiguration = {
  // a Pure identifier includes $ but not first (that is a variable)
  wordPattern: /(-?\d*\.\d\w*)|([^`~!@#%^$&*()\-=+[{\]}\\|;:'",.<>/?\s][^`~!@#%^&*()\-=+[{\]}\\|;:'",.<>/?\s]*)/,
  comments: { lineComment: '//', blockComment: ['/*', '*/'] },
  brackets: [['{', '}'], ['[', ']'], ['(', ')']],
  autoClosingPairs: [
    { open: '{', close: '}' }, { open: '[', close: ']' }, { open: '(', close: ')' }, { open: '"', close: '"' }, { open: "'", close: "'" },
  ],
  surroundingPairs: [
    { open: '{', close: '}' }, { open: '[', close: ']' }, { open: '(', close: ')' }, { open: '"', close: '"' },
    { open: "'", close: "'" }, { open: '<', close: '>' }, { open: '<<', close: '>>' },
  ],
  folding: {
    markers: {
      start: /^\s*\/\/\s*(?:(?:#?region\b)|(?:<editor-fold\b))/,
      end: /^\s*\/\/\s*(?:(?:#?endregion\b)|(?:<\/editor-fold>))/,
    },
  },
};

const MONARCH = {
  defaultToken: 'invalid',
  tokenPostfix: '.pure',
  keywords: [
    ...EXTRA_KEYWORDS,
    // relational
    'Schema', 'Table', 'Join', 'View', 'primaryKey', 'groupBy', 'mainTable',
    // native
    'let', 'extends', 'true', 'false', 'projects',
    // elements (PURE_ELEMENT_NAME)
    'Class', 'Association', 'Enum', 'Measure', 'Profile', 'function', 'Mapping', 'Runtime', 'Connection',
    'FileGeneration', 'GenerationSpecification', 'Data',
    // connections (PURE_CONNECTION_NAME)
    'JsonModelConnection', 'ModelChainConnection', 'XmlModelConnection',
    // mapping
    'include', 'EnumerationMapping', 'Pure', 'AssociationMapping', 'XStore', 'AggregationAware',
    'Service', 'FlatData', 'Database', 'FlatDataConnection', 'RelationalDatabaseConnection', 'Relational',
  ],
  operators: [
    '=', '>', '<', '!', '~', '?', ':', '==', '<=', '>=', '&&', '||', '++', '--', '+', '-', '*', '/', '&', '|', '^',
    '%', '->', '#{', '}#', '@', '<<', '>>',
  ],
  languageStructs: ['import', 'native'],
  identifier: /[a-zA-Z_$][\w$]*/,
  symbols: /[=><!~?:&|+\-*/^%#@]+/,
  escapes: /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,
  digits: /\d+(_+\d+)*/,
  octaldigits: /[0-7]+(_+[0-7]+)*/,
  binarydigits: /[0-1]+(_+[0-1]+)*/,
  hexdigits: /[[0-9a-fA-F]+(_+[0-9a-fA-F]+)*/,
  multiplicity: /\[(?:[a-zA-Z0-9]+(?:\.\.(?:[a-zA-Z0-9]+|\*|))?|\*)\]/,
  package: /(?:[\w_]+::)+/,
  generics: /(?:(?:<\w+>)|(?:<[^:.@^()]+[^-]>))/,
  date: /%-?\d+(?:-\d+(?:-\d+(?:T(?:\d+(?::\d+(?::\d+(?:.\d+)?)?)?)(?:[+-][0-9]{4})?)))/,
  time: /%\d+(?::\d+(?::\d+(?:.\d+)?)?)?/,
  tokenizer: {
    root: [
      { include: '@pure' },
      { include: '@date' },
      { include: '@color' },
      // parser markers (leading whitespace before a section header is invalid)
      [/^\s*###[\w]+/, T.PARSER],
      // identifiers and keywords
      [/(@identifier)/, {
        cases: {
          '@languageStructs': T.LANGUAGE_STRUCT,
          '@keywords': `${T.KEYWORD}.$0`,
          // a function descriptor
          '([a-zA-Z_$][\\w$]*)_((\\w+_(([a-zA-Z0-9]+)|(\\$[a-zA-Z0-9]+_[a-zA-Z0-9]+\\$)))__)*(\\w+_(([a-zA-Z0-9]+)|(\\$[a-zA-Z0-9]+_[a-zA-Z0-9]+\\$)))_': T.TYPE,
          '@default': T.IDENTIFIER,
        },
      }],
      { include: '@whitespace' },
      // delimiters and operators
      [/[{}()[\]]/, '@brackets'],
      [/[<>](?!@symbols)/, '@brackets'],
      [/@symbols/, { cases: { '@operators': T.OPERATOR, '@default': T.IDENTIFIER } }],
      { include: '@number' },
      // delimiter: after number because of .\d floats
      [/[;,.]/, T.DELIMITER],
      // strings (an unterminated one shows as one while it is typed)
      [/'([^'\\]|\\.)*$/, `${T.STRING}.invalid`],
      [/'/, T.STRING, '@string'],
      { include: '@characters' },
    ],
    pure: [
      // type
      [/(@package\*)/, [T.PACKAGE]], // import path
      [/(@package?)(@identifier)(@generics?)(\s*)(@multiplicity)/, [T.PACKAGE, T.TYPE, T.GENERICS, T.WHITESPACE, T.MULTIPLICITY]],
      [/(@package)(@identifier)(@generics?)/, [T.PACKAGE, T.TYPE, T.GENERICS]],
      // special operators that use a type (constructor, cast)
      [/([@^])(\s*)(@package?)(@identifier)(@generics?)(@multiplicity?)/, [`${T.TYPE}.operator`, T.WHITESPACE, T.PACKAGE, T.TYPE, T.GENERICS, T.MULTIPLICITY]],
      // property / parameter
      [/(\.\s*)(@identifier)/, [T.DELIMITER, T.PROPERTY]],
      [/(@identifier)(\s*=)/, [T.PROPERTY, T.OPERATOR]],
      [/(@identifier)(\.)(@identifier)/, [T.TYPE, T.OPERATOR, T.PROPERTY]], // a property chain, a profile tag or stereotype
      [/(@identifier)(\s*:)/, [T.PARAMETER, T.OPERATOR]],
      // variables
      [/(let)(\s+)(@identifier)(\s*=)/, [T.KEYWORD, T.WHITESPACE, T.VARIABLE, T.OPERATOR]],
      [/(\$@identifier)/, [`${T.VARIABLE}.reference`]],
    ],
    date: [
      [/(%latest)/, [`${T.DATE}.latest`]],
      [/(@date)/, [T.DATE]],
      [/(@time)/, [`${T.DATE}.time`]],
    ],
    color: [[/(#[0-9a-fA-F]{6})/, [T.COLOR]]],
    number: [
      [/(@digits)[eE]([-+]?(@digits))?[fFdD]?/, `${T.NUMBER}.float`],
      [/(@digits)\.(@digits)([eE][-+]?(@digits))?[fFdD]?/, `${T.NUMBER}.float`],
      [/0[xX](@hexdigits)[Ll]?/, `${T.NUMBER}.hex`],
      [/0(@octaldigits)[Ll]?/, `${T.NUMBER}.octal`],
      [/0[bB](@binarydigits)[Ll]?/, `${T.NUMBER}.binary`],
      [/(@digits)[fFdD]/, `${T.NUMBER}.float`],
      [/(@digits)[lL]?/, T.NUMBER],
    ],
    whitespace: [
      [/[ \t\r\n]+/, T.WHITESPACE],
      [/\/\*\*(?!\/)/, `${T.COMMENT}.doc`, '@doc'],
      [/\/\*/, T.COMMENT, '@comment'],
      [/\/\/.*$/, T.COMMENT],
    ],
    comment: [
      [/[^/*]+/, T.COMMENT],
      [/\*\//, T.COMMENT, '@pop'],
      [/[/*]/, T.COMMENT],
    ],
    doc: [
      [/[^/*]+/, `${T.COMMENT}.doc`],
      [/\/\*/, `${T.COMMENT}.doc.invalid`],
      [/\*\//, `${T.COMMENT}.doc`, '@pop'],
      [/[/*]/, `${T.COMMENT}.doc`],
    ],
    string: [
      [/[^\\']+/, T.STRING],
      [/@escapes/, `${T.STRING}.escape`],
      [/\\./, `${T.STRING}.escape.invalid`],
      [/'/, T.STRING, '@pop'],
    ],
    characters: [
      [/'[^\\']'/, T.STRING],
      [/(')(@escapes)(')/, [T.STRING, `${T.STRING}.escape`, T.STRING]],
      [/'/, `${T.STRING}.invalid`],
    ],
  },
} as unknown as Monaco.languages.IMonarchLanguage;

/** upstream's BASE_PURE_LANGUAGE_COLOR_TOKENS (Monaco takes hex, not CSS variables) */
const PURE_TOKEN_COLORS: Monaco.editor.ITokenThemeRule[] = [
  { token: T.IDENTIFIER, foreground: 'dcdcaa' },
  { token: T.NUMBER, foreground: 'b5cea8' },
  { token: T.DATE, foreground: 'b5cea8' },
  { token: T.COLOR, foreground: 'b5cea8' },
  { token: T.PACKAGE, foreground: '808080' },
  { token: T.PARSER, foreground: 'c586c0' },
  { token: T.LANGUAGE_STRUCT, foreground: 'c586c0' },
  { token: T.MULTIPLICITY, foreground: '2d796b' },
  { token: T.GENERICS, foreground: '2d796b' },
  { token: T.PROPERTY, foreground: '9cdcfe' },
  { token: T.PARAMETER, foreground: '9cdcfe' },
  { token: T.VARIABLE, foreground: '4fc1ff' },
  { token: T.TYPE, foreground: '3dc9b0' },
  { token: `${T.STRING}.escape`, foreground: 'd7ba7d' },
];

export function registerPure(monaco: typeof Monaco): void {
  monaco.languages.register({ id: PURE, extensions: ['.pure'] });
  monaco.languages.setLanguageConfiguration(PURE, CONFIGURATION);
  monaco.languages.setMonarchTokensProvider(PURE, MONARCH);
  monaco.editor.defineTheme(DEFAULT_DARK, {
    base: 'vs-dark',
    inherit: true,
    colors: {},
    rules: [
      ...PURE_TOKEN_COLORS,
      // SQL's strings, as upstream corrects them
      { token: 'string.sql', foreground: 'ce9178' },
      { token: 'white.sql', foreground: 'd4d4d4' },
      { token: 'identifier.sql', foreground: 'd4d4d4' },
      { token: 'operator.sql', foreground: 'd4d4d4' },
    ],
  });
  monaco.editor.defineTheme(GITHUB_LIGHT, { base: 'vs', inherit: true, colors: GITHUB_LIGHT_COLORS, rules: [] });
}
