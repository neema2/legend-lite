execution samples: 2963

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| names | 1805 | 60.9% | 1824 | 61.6% |
| normalize | 800 | 27.0% | 1609 | 54.3% |
| model | 251 | 8.5% | 575 | 19.4% |
| parse | 47 | 1.6% | 47 | 1.6% |
| other | 44 | 1.5% | 0 | 0.0% |
| lower | 12 | 0.4% | 12 | 0.4% |
| type | 4 | 0.1% | 190 | 6.4% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `jdk.internal.util.ArraysSupport.hashCodeOfUnsigned` | 362 | 12.2% |
| `java.util.HashMap.getNode` | 231 | 7.8% |
| `java.lang.String.getBytes` | 123 | 4.2% |
| `com.legend.compiler.NameResolver.resolveList` | 99 | 3.3% |
| `java.util.HashMap.hash` | 92 | 3.1% |
| `java.util.Collections$UnmodifiableMap.get` | 76 | 2.6% |
| `com.legend.compiler.NameResolver.decide` | 59 | 2.0% |
| `java.lang.String.equals` | 57 | 1.9% |
| `java.util.HashMap.resize` | 55 | 1.9% |
| `com.legend.compiler.NameResolver.callTierMatches` | 47 | 1.6% |
| `java.util.stream.ReferencePipeline$2$1.accept` | 47 | 1.6% |
| `java.util.HashMap.putVal` | 44 | 1.5% |
| `java.lang.String.hashCode` | 41 | 1.4% |
| `com.legend.compiler.BareNames.tiered` | 39 | 1.3% |
| `java.lang.String.length` | 34 | 1.1% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.Compiler.buildModel` | 2963 | 100.0% |
| `com.legend.integration.PerfProbe.main` | 2962 | 100.0% |
| `com.legend.Compiler.normalizeWithSystem` | 1768 | 59.7% |
| `com.legend.Compiler.normalizeLayer` | 1640 | 55.3% |
| `com.legend.normalizer.ModelNormalizer.normalize` | 1609 | 54.3% |
| `com.legend.compiler.NameResolver.resolveVs` | 1488 | 50.2% |
| `com.legend.compiler.NameResolver.resolveList` | 1385 | 46.7% |
| `com.legend.compiler.NameResolver.resolveVsList` | 1335 | 45.1% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 1317 | 44.4% |
| `com.legend.compiler.NameResolver.resolve` | 973 | 32.8% |
| `com.legend.compiler.NameResolver.resolveAlongside` | 940 | 31.7% |
| `com.legend.compiler.NameResolver.resolveElement` | 930 | 31.4% |
| `com.legend.normalizer.MappingNormalizer.normalize` | 899 | 30.3% |
| `com.legend.normalizer.MappingNormalizer.withElement` | 867 | 29.3% |
| `com.legend.compiler.NameResolver.resolveService` | 829 | 28.0% |
| `com.legend.compiler.NameResolver.decide` | 771 | 26.0% |
| `com.legend.normalizer.MappingNormalizer.normalizeMapping` | 702 | 23.7% |
| `com.legend.normalizer.MappingNormalizer.lambda$normalize$0` | 702 | 23.7% |
| `com.legend.normalizer.ModelNormalizer.resolveSynthesized` | 668 | 22.5% |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 660 | 22.3% |
| `com.legend.compiler.NameResolver.resolveQueryIn` | 618 | 20.9% |
| `com.legend.compiler.BareNames.catalogTiered` | 608 | 20.5% |
| `com.legend.normalizer.MappingNormalizer.synthesizeClassMapping` | 601 | 20.3% |
| `com.legend.normalizer.MappingNormalizer.synthRelational` | 585 | 19.7% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedMapping` | 577 | 19.5% |
| `com.legend.compiler.NameResolver.resolveLambda` | 556 | 18.8% |
| `com.legend.compiler.NameResolver.callTierMatches` | 545 | 18.4% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedParts` | 484 | 16.3% |
| `com.legend.compiler.NameResolver.addKnown` | 373 | 12.6% |
| `com.legend.compiler.BareNames.tiered` | 273 | 9.2% |
| `com.legend.compiler.BareNames.referents` | 252 | 8.5% |
| `com.legend.Compiler.boot` | 228 | 7.7% |
| `com.legend.normalizer.JoinChainEmission.emitJoinChain` | 222 | 7.5% |
| `com.legend.normalizer.JoinChainEmission.emitHopsForStructuralPm` | 220 | 7.4% |
| `com.legend.cache.ContentStore.getOrCompute` | 205 | 6.9% |
| `com.legend.Compiler.lambda$boot$0` | 204 | 6.9% |
| `com.legend.cache.ContentStore.lambda$getOrCompute$0` | 204 | 6.9% |
| `com.legend.compiler.NameResolver.resolveKeyBindings` | 204 | 6.9% |
| `com.legend.compiler.element.PureModelContext.from` | 204 | 6.9% |
| `com.legend.compiler.spec.Calls.platform` | 188 | 6.3% |
allocation (sampled weight): 2.97 GB
| phase | share |
|---|---|
| names | 67.3% |
| normalize | 22.5% |
| parse | 4.3% |
| model | 4.0% |
| other | 1.4% |
| lower | 0.3% |
| type | 0.2% |
| allocating legend frame (innermost) | share |
| `com.legend.compiler.NameResolver.callTierMatches` | 20.4% |
| `com.legend.compiler.BareNames.tiered` | 19.1% |
| `com.legend.compiler.BareNames.catalogTiered` | 10.3% |
| `com.legend.compiler.NameResolver.decide` | 7.4% |
| `com.legend.compiler.KnowledgeLayer.propertyType` | 6.1% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 5.1% |
| `com.legend.normalizer.AssociationSynthesis.injectMultiHopAssociationPMs` | 4.1% |
| `com.legend.parser.SpecParser.parseAndPutKeyExpression` | 2.8% |
| `com.legend.model.RelationalOperation.descendants` | 1.7% |
| `com.legend.compiler.ModelBuilder.indexedView` | 1.7% |
| `com.legend.compiler.BareNames.referents` | 1.6% |
| `com.legend.compiler.NameResolver.resolveList` | 1.6% |
| `com.legend.model.SignatureMangle.typeId` | 1.6% |
| `com.legend.lexer.Lexer.<init>` | 1.2% |
| `com.legend.model.SignatureMangle.mangle` | 0.8% |
| `com.legend.compiler.KnowledgeLayer.propertyDef` | 0.7% |
| `com.legend.compiler.KnowledgeLayer.columnKind` | 0.5% |
| `com.legend.compiler.KnowledgeLayer.superClassFqns` | 0.5% |
| `com.legend.compiler.KnowledgeLayer.lambda$column$0` | 0.4% |
| `com.legend.compiler.ModelBuilder.findTableDefinition` | 0.4% |
