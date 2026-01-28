# Legend-Engine Raw Grammar Files

This directory contains the original ANTLR grammar files copied from the Legend-Engine repository.
These files serve as the source of truth and reference for creating the merged `PureLexer.g4` and `PureParser.g4`.

## Source

Copied from: `legend-engine-grammar/` (git submodule pointing to legend-engine)

## Structure

```
antlr4-raw/
├── core/                    # Core language foundation
│   ├── CoreFragmentGrammar.g4    # Fragment rules (String, Integer, etc.)
│   ├── CoreLexerGrammar.g4       # Basic tokens (operators, delimiters)
│   ├── CoreParserGrammar.g4      # qualifiedName, packagePath
│   ├── M3LexerGrammar.g4         # M3 keywords (all, let, etc.)
│   └── M3ParserGrammar.g4        # Expressions, lambdas, types
├── domain/                  # Class, Association, Enum, Function
│   ├── DomainLexerGrammar.g4
│   └── DomainParserGrammar.g4
├── mapping/                 # Mapping definitions
│   ├── MappingLexerGrammar.g4
│   └── MappingParserGrammar.g4
├── runtime/                 # Runtime definitions
│   ├── RuntimeLexerGrammar.g4
│   └── RuntimeParserGrammar.g4
├── connection/              # Connection definitions
│   ├── ConnectionLexerGrammar.g4
│   ├── ConnectionParserGrammar.g4
│   ├── RelationalDatabaseConnectionLexerGrammar.g4
│   └── RelationalDatabaseConnectionParserGrammar.g4
└── relational/              # Database/Store definitions
    ├── RelationalLexerGrammar.g4
    └── RelationalParserGrammar.g4
```

## Import Hierarchy

```
CoreFragmentGrammar (fragments)
    └── CoreLexerGrammar (basic tokens)
        └── M3LexerGrammar (M3 keywords)
            └── DomainLexerGrammar (domain keywords)

CoreParserGrammar (qualifiedName)
    └── M3ParserGrammar (expressions)
        └── DomainParserGrammar (class, enum, etc.)
```

## Notes

- Each grammar uses ISLAND modes for embedded DSLs (mapping body, connection config)
- These raw files are NOT processed by ANTLR - only for reference
- The processed grammars are in `src/main/antlr4/org/finos/legend/pure/dsl/antlr/`
