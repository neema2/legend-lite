execution samples: 1969

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| parse | 1749 | 88.8% | 1750 | 88.9% |
| other | 219 | 11.1% | 0 | 0.0% |
| exec/result | 1 | 0.1% | 1 | 0.1% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `com.legend.lexer.Lexer.<init>` | 469 | 23.8% |
| `java.lang.String.equals` | 192 | 9.8% |
| `com.legend.parser.TokenStreamCursor.peek` | 132 | 6.7% |
| `com.legend.lexer.TokenStream.lineStarts` | 74 | 3.8% |
| `com.legend.model.ModelFromProtocol.lineStarts` | 57 | 2.9% |
| `com.legend.lexer.TokenStream.tokenPositions` | 34 | 1.7% |
| `java.lang.StringUTF16.charAt` | 31 | 1.6% |
| `com.legend.lexer.Lexer.scanIslandToken` | 30 | 1.5% |
| `java.lang.StringUTF16.length` | 29 | 1.5% |
| `com.legend.lexer.Lexer.atSectionBoundary` | 24 | 1.2% |
| `java.lang.StringUTF16.compress` | 23 | 1.2% |
| `com.legend.parser.TokenStreamCursor.spanOf` | 17 | 0.9% |
| `java.lang.StringLatin1.newString` | 16 | 0.8% |
| `java.lang.StringLatin1.charAt` | 16 | 0.8% |
| `java.lang.String.isLatin1` | 16 | 0.8% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.Compiler.parseSources` | 1969 | 100.0% |
| `com.legend.integration.PerfProbe.main` | 1969 | 100.0% |
| `com.legend.door.ModelFromText.parse` | 1927 | 97.9% |
| `com.legend.parser.PmcdParser.parse` | 1750 | 88.9% |
| `com.legend.parser.PmcdParser.parseSections` | 1554 | 78.9% |
| `com.legend.parser.PmcdParser.sectionElements` | 1260 | 64.0% |
| `com.legend.parser.PmcdParser.strictWalk` | 1248 | 63.4% |
| `com.legend.parser.PmcdParser.parseOneAt` | 1245 | 63.2% |
| `com.legend.parser.section.ServiceSectionGrammar.parseService` | 1004 | 51.0% |
| `com.legend.parser.section.ServiceSectionGrammar.parseElement` | 1004 | 51.0% |
| `com.legend.parser.section.ServiceSectionGrammar.parseOne` | 1004 | 51.0% |
| `com.legend.parser.section.ServiceSectionGrammar.parseTestSuites` | 618 | 31.4% |
| `com.legend.lexer.Lexer.tokenizeRange` | 572 | 29.1% |
| `com.legend.lexer.TokenStream.lexRange` | 572 | 29.1% |
| `com.legend.parser.MappingProtocolParser.readIsland` | 553 | 28.1% |
| `com.legend.lexer.Lexer.<init>` | 470 | 23.9% |
| `com.legend.parser.section.ServiceSectionGrammar.parseSuiteTests` | 421 | 21.4% |
| `com.legend.parser.MappingProtocolParser.assertionValue` | 413 | 21.0% |
| `com.legend.parser.MappingProtocolParser.parseTestAssertion` | 413 | 21.0% |
| `com.legend.parser.MappingProtocolParser.parseTestAssertionAt` | 401 | 20.4% |
| `com.legend.parser.section.ServiceSectionGrammar.parseExecution` | 333 | 16.9% |
| `com.legend.lexer.Lexer.run` | 312 | 15.8% |
| `com.legend.parser.section.ServiceSectionGrammar.parseQuery` | 288 | 14.6% |
| `com.legend.parser.SpecParser.parseProgramLine` | 256 | 13.0% |
| `com.legend.parser.SpecParser.parseCombinedExpression` | 255 | 13.0% |
allocation (sampled weight): 3.61 GB
| phase | share |
|---|---|
| parse | 97.8% |
| other | 2.2% |
| allocating legend frame (innermost) | share |
| `com.legend.lexer.Lexer.<init>` | 77.9% |
| `com.legend.lexer.TokenStream.text` | 3.8% |
| `com.legend.lexer.TokenStream.tokenPositions` | 2.6% |
| `com.legend.lexer.Lexer.scanIdentifierOrKeyword` | 2.4% |
| `com.legend.parser.TokenStreamCursor.spanOf` | 1.6% |
| `com.legend.parser.SpecParser.parseSingleParamLambda` | 0.8% |
| `com.legend.model.ModelFromProtocol.lineStarts` | 0.8% |
| `com.legend.parser.TokenStreamCursor.parseQualifiedName` | 0.7% |
| `com.legend.lexer.TokenStream.slice` | 0.6% |
| `com.legend.parser.SpecParser.parseCombinedParts` | 0.6% |
| `com.legend.parser.TokenStreamCursor.unquoteAndUnescape` | 0.5% |
| `com.legend.protocol.spec.LambdaFunction.<init>` | 0.5% |
| `com.legend.parser.SpecParser.parseColSpecTail` | 0.4% |
| `com.legend.parser.SpecParser.parseDotPostfix` | 0.4% |
| `com.legend.parser.SpecParser.parseArrowPostfix` | 0.4% |
| `com.legend.protocol.Escapes.unescapeJavaLike` | 0.3% |
| `com.legend.model.ModelFromProtocol.of` | 0.3% |
| `com.legend.parser.SpecParser.parseAllCall` | 0.3% |
| `com.legend.parser.SpecParser.parseArgListBody` | 0.3% |
| `com.legend.parser.section.ServiceSectionGrammar.parseExecution` | 0.2% |
