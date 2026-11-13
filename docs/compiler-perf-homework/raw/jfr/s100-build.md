execution samples: 7680

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| names | 4108 | 53.5% | 4152 | 54.1% |
| normalize | 2390 | 31.1% | 6252 | 81.4% |
| model | 992 | 12.9% | 2753 | 35.8% |
| parse | 115 | 1.5% | 115 | 1.5% |
| other | 51 | 0.7% | 0 | 0.0% |
| lower | 15 | 0.2% | 15 | 0.2% |
| type | 9 | 0.1% | 1179 | 15.4% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `java.lang.String.getBytes` | 1391 | 18.1% |
| `java.util.ImmutableCollections$AbstractImmutableList.iterator` | 560 | 7.3% |
| `java.util.HashMap.newNode` | 493 | 6.4% |
| `jdk.internal.util.ArraysSupport.hashCodeOfUnsigned` | 423 | 5.5% |
| `java.util.HashMap.resize` | 298 | 3.9% |
| `java.util.HashMap.getNode` | 288 | 3.8% |
| `com.legend.compiler.NameResolver.callTierMatches` | 287 | 3.7% |
| `com.legend.compiler.BareNames.tiered` | 245 | 3.2% |
| `java.lang.String.equals` | 186 | 2.4% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 158 | 2.1% |
| `java.util.Arrays.copyOf` | 151 | 2.0% |
| `com.legend.compiler.ModelBuilder.indexedView` | 148 | 1.9% |
| `java.lang.Integer.valueOf` | 134 | 1.7% |
| `java.lang.StringLatin1.hashCode` | 106 | 1.4% |
| `java.util.Map.entry` | 105 | 1.4% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.Compiler.buildModel` | 7680 | 100.0% |
| `com.legend.integration.PerfProbe.main` | 7656 | 99.7% |
| `com.legend.Compiler.normalizeWithSystem` | 6482 | 84.4% |
| `com.legend.Compiler.normalizeLayer` | 6331 | 82.4% |
| `com.legend.normalizer.ModelNormalizer.normalize` | 6251 | 81.4% |
| `com.legend.normalizer.MappingNormalizer.normalize` | 3418 | 44.5% |
| `com.legend.normalizer.MappingNormalizer.withElement` | 3392 | 44.2% |
| `com.legend.normalizer.MappingNormalizer.normalizeMapping` | 3130 | 40.8% |
| `com.legend.normalizer.MappingNormalizer.lambda$normalize$0` | 3130 | 40.8% |
| `com.legend.normalizer.ModelNormalizer.resolveSynthesized` | 2732 | 35.6% |
| `com.legend.compiler.NameResolver.resolveVs` | 2719 | 35.4% |
| `com.legend.compiler.NameResolver.resolveQueryIn` | 2681 | 34.9% |
| `com.legend.compiler.NameResolver.resolveList` | 2677 | 34.9% |
| `com.legend.compiler.NameResolver.resolveVsList` | 2570 | 33.5% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 2563 | 33.4% |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 2068 | 26.9% |
| `com.legend.normalizer.AssociationSynthesis.synthesizeAssociationMapping` | 1973 | 25.7% |
| `com.legend.compiler.NameResolver.decide` | 1710 | 22.3% |
| `com.legend.compiler.BareNames.referents` | 1680 | 21.9% |
| `com.legend.compiler.BareNames.catalogTiered` | 1441 | 18.8% |
| `com.legend.compiler.NameResolver.callTierMatches` | 1373 | 17.9% |
| `com.legend.compiler.spec.Calls.platform` | 1175 | 15.3% |
| `com.legend.compiler.NameResolver.builtCallNames` | 1170 | 15.2% |
| `com.legend.normalizer.ViewRelation.sourceRefFor` | 1056 | 13.8% |
| `com.legend.compiler.NameResolver.resolveLambda` | 1029 | 13.4% |
| `com.legend.normalizer.MappingNormalizer.synthesizeClassMapping` | 988 | 12.9% |
| `com.legend.normalizer.MappingNormalizer.synthRelational` | 980 | 12.8% |
| `com.legend.compiler.ModelBuilder.findView` | 973 | 12.7% |
| `com.legend.compiler.ModelBuilder.indexedView` | 948 | 12.3% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedMapping` | 870 | 11.3% |
| `com.legend.normalizer.AssociationSynthesis.buildAssocPredicateBody` | 869 | 11.3% |
| `com.legend.compiler.element.PureModelContext.from` | 851 | 11.1% |
| `com.legend.compiler.BareNames.tiered` | 778 | 10.1% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 740 | 9.6% |
| `com.legend.compiler.element.ModelIntegrity.check` | 701 | 9.1% |
| `com.legend.compiler.element.PureModelContext.<init>` | 701 | 9.1% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedParts` | 638 | 8.3% |
| `com.legend.compiler.element.ModelIntegrity.withElement` | 472 | 6.1% |
| `com.legend.compiler.NameResolver.resolveKeyBindings` | 426 | 5.5% |
| `com.legend.compiler.NameResolver.addKnown` | 373 | 4.9% |
allocation (sampled weight): 6.25 GB
| phase | share |
|---|---|
| names | 62.6% |
| normalize | 28.8% |
| model | 6.9% |
| parse | 1.1% |
| type | 0.3% |
| lower | 0.2% |
| other | 0.1% |
| allocating legend frame (innermost) | share |
| `com.legend.compiler.NameResolver.callTierMatches` | 19.5% |
| `com.legend.compiler.BareNames.tiered` | 16.6% |
| `com.legend.compiler.ModelBuilder.indexedView` | 14.2% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 10.6% |
| `com.legend.compiler.BareNames.catalogTiered` | 10.3% |
| `com.legend.compiler.ModelBuilder.findTableDefinition` | 4.3% |
| `com.legend.compiler.BareNames.referents` | 3.6% |
| `com.legend.model.RelationalOperation.descendants` | 1.3% |
| `com.legend.model.SignatureMangle.mangle` | 0.8% |
| `com.legend.lexer.Lexer.<init>` | 0.8% |
| `com.legend.compiler.ModelBuilder.findOwnTableDefinition` | 0.8% |
| `com.legend.compiler.ModelBuilder.findJoin` | 0.8% |
| `com.legend.compiler.KnowledgeLayer.propertyType` | 0.7% |
| `com.legend.model.SignatureMangle.typeId` | 0.6% |
| `com.legend.model.SignatureMangle.segment` | 0.5% |
| `com.legend.protocol.SetIds.defaultFor` | 0.5% |
| `com.legend.compiler.NameResolver.resolveList` | 0.4% |
| `com.legend.normalizer.ResolvedMapping.<init>` | 0.4% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedParts` | 0.4% |
| `com.legend.compiler.KnowledgeLayer.propertyDef` | 0.4% |
