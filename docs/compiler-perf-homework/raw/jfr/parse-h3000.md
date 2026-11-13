execution samples: 17573

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| parse | 12726 | 72.4% | 12726 | 72.4% |
| names | 2533 | 14.4% | 2556 | 14.5% |
| normalize | 1087 | 6.2% | 3392 | 19.3% |
| model | 740 | 4.2% | 1208 | 6.9% |
| other | 465 | 2.6% | 0 | 0.0% |
| lower | 15 | 0.1% | 15 | 0.1% |
| type | 7 | 0.0% | 1114 | 6.3% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `java.util.ArrayList.indexOfRange` | 11718 | 66.7% |
| `jdk.internal.util.ArraysSupport.hashCodeOfUnsigned` | 694 | 3.9% |
| `java.util.HashMap.getNode` | 687 | 3.9% |
| `java.lang.StringLatin1.hashCode` | 409 | 2.3% |
| `java.util.HashMap.resize` | 155 | 0.9% |
| `com.legend.lexer.Lexer.scanIdentifierOrKeyword` | 134 | 0.8% |
| `java.util.ImmutableCollections$SetN.contains` | 131 | 0.7% |
| `java.lang.StringLatin1.newString` | 118 | 0.7% |
| `com.legend.parser.TokenStreamCursor.peek` | 116 | 0.7% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 110 | 0.6% |
| `com.legend.compiler.BareNames.catalogTiered` | 105 | 0.6% |
| `java.util.HashMap.putVal` | 91 | 0.5% |
| `java.util.JumboEnumSet.contains` | 87 | 0.5% |
| `java.lang.StringLatin1.lastIndexOf` | 77 | 0.4% |
| `java.util.ArrayList.grow` | 75 | 0.4% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.integration.PerfProbe.main` | 17565 | 100.0% |
| `com.legend.door.ModelFromText.parse` | 13094 | 74.5% |
| `com.legend.Compiler.parseSources` | 13020 | 74.1% |
| `com.legend.parser.PmcdParser.parse` | 12726 | 72.4% |
| `com.legend.Compiler.buildModel` | 4536 | 25.8% |
| `com.legend.Compiler.normalizeWithSystem` | 3677 | 20.9% |
| `com.legend.Compiler.normalizeLayer` | 3517 | 20.0% |
| `com.legend.normalizer.ModelNormalizer.normalize` | 3392 | 19.3% |
| `com.legend.normalizer.MappingNormalizer.normalize` | 2062 | 11.7% |
| `com.legend.normalizer.MappingNormalizer.withElement` | 2041 | 11.6% |
| `com.legend.normalizer.MappingNormalizer.normalizeMapping` | 1795 | 10.2% |
| `com.legend.normalizer.MappingNormalizer.lambda$normalize$0` | 1795 | 10.2% |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 1262 | 7.2% |
| `com.legend.compiler.NameResolver.resolveList` | 1249 | 7.1% |
| `com.legend.normalizer.ModelNormalizer.resolveSynthesized` | 1237 | 7.0% |
| `com.legend.compiler.NameResolver.resolveVs` | 1235 | 7.0% |
| `com.legend.compiler.NameResolver.resolveQueryIn` | 1189 | 6.8% |
| `com.legend.compiler.NameResolver.resolveVsList` | 1161 | 6.6% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 1159 | 6.6% |
| `com.legend.compiler.spec.Calls.platform` | 1113 | 6.3% |
| `com.legend.compiler.NameResolver.builtCallNames` | 1107 | 6.3% |
| `com.legend.compiler.NameResolver.decide` | 998 | 5.7% |
| `com.legend.parser.PmcdParser.parseSections` | 972 | 5.5% |
| `com.legend.compiler.BareNames.referents` | 957 | 5.4% |
| `com.legend.compiler.BareNames.catalogTiered` | 839 | 4.8% |
| `com.legend.normalizer.AssociationSynthesis.synthesizeAssociationMapping` | 831 | 4.7% |
| `com.legend.normalizer.MappingNormalizer.synthesizeClassMapping` | 823 | 4.7% |
| `com.legend.normalizer.MappingNormalizer.synthRelational` | 808 | 4.6% |
| `com.legend.compiler.NameResolver.callTierMatches` | 805 | 4.6% |
| `com.legend.compiler.NameResolver.addKnown` | 795 | 4.5% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedMapping` | 792 | 4.5% |
| `com.legend.parser.PmcdParser.sectionElements` | 674 | 3.8% |
| `com.legend.parser.PmcdParser.strictWalk` | 670 | 3.8% |
| `com.legend.parser.PmcdParser.parseOneAt` | 648 | 3.7% |
| `com.legend.normalizer.ViewRelation.sourceRefFor` | 620 | 3.5% |
| `com.legend.compiler.element.PureModelContext.from` | 558 | 3.2% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedParts` | 537 | 3.1% |
| `com.legend.compiler.NameResolver.resolveLambda` | 513 | 2.9% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 488 | 2.8% |
| `com.legend.compiler.element.ModelIntegrity.check` | 449 | 2.6% |
allocation (sampled weight): 5.94 GB
| phase | share |
|---|---|
| names | 65.2% |
| normalize | 12.1% |
| parse | 11.3% |
| model | 7.2% |
| other | 3.9% |
| type | 0.3% |
| lower | 0.2% |
| allocating legend frame (innermost) | share |
| `com.legend.compiler.NameResolver.callTierMatches` | 20.1% |
| `com.legend.compiler.BareNames.tiered` | 16.4% |
| `com.legend.compiler.BareNames.catalogTiered` | 11.5% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 11.3% |
| `com.legend.compiler.BareNames.referents` | 3.8% |
| `com.legend.lexer.Lexer.<init>` | 2.1% |
| `com.legend.lexer.Lexer.scanIdentifierOrKeyword` | 1.8% |
| `com.legend.lexer.TokenStream.text` | 1.8% |
| `com.legend.model.RelationalOperation.descendants` | 1.5% |
| `com.legend.lexer.TokenStream.tokenPositions` | 1.3% |
| `com.legend.protocol.SetIds.defaultFor` | 1.2% |
| `com.legend.parser.TokenStreamCursor.parseQualifiedName` | 1.1% |
| `com.legend.parser.TokenStreamCursor.spanOf` | 0.8% |
| `com.legend.compiler.ModelBuilder.findTableDefinition` | 0.7% |
| `com.legend.compiler.NameResolver.resolveList` | 0.7% |
| `com.legend.integration.PerfProbe.main` | 0.6% |
| `com.legend.model.SignatureMangle.mangle` | 0.6% |
| `com.legend.model.SignatureMangle.typeId` | 0.6% |
| `com.legend.compiler.KnowledgeLayer.propertyType` | 0.6% |
| `com.legend.model.SignatureMangle.segment` | 0.5% |
