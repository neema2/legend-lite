execution samples: 3070

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| normalize | 1941 | 63.2% | 2551 | 83.1% |
| names | 731 | 23.8% | 745 | 24.3% |
| model | 246 | 8.0% | 498 | 16.2% |
| parse | 90 | 2.9% | 90 | 2.9% |
| other | 37 | 1.2% | 0 | 0.0% |
| lower | 23 | 0.7% | 23 | 0.7% |
| type | 2 | 0.1% | 284 | 9.3% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `java.util.LinkedHashMap.newNode` | 299 | 9.7% |
| `jdk.internal.util.ArraysSupport.hashCodeOfUnsigned` | 219 | 7.1% |
| `java.util.HashMap.putVal` | 219 | 7.1% |
| `java.util.HashMap.resize` | 152 | 5.0% |
| `java.util.HashMap.hash` | 149 | 4.9% |
| `java.util.HashMap.getNode` | 112 | 3.6% |
| `java.util.LinkedHashMap$LinkedHashIterator.nextNode` | 63 | 2.1% |
| `java.util.HashMap.putMapEntries` | 61 | 2.0% |
| `java.lang.String.hashCode` | 61 | 2.0% |
| `java.util.HashMap.newNode` | 60 | 2.0% |
| `java.util.ImmutableCollections$AbstractImmutableList.iterator` | 58 | 1.9% |
| `java.util.LinkedHashMap.linkNodeAtEnd` | 54 | 1.8% |
| `com.legend.compiler.BareNames.referents` | 51 | 1.7% |
| `com.legend.normalizer.AssociationSynthesis.injectMultiHopAssociationPMs` | 48 | 1.6% |
| `java.lang.StringLatin1.replace` | 41 | 1.3% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.Compiler.buildModel` | 3070 | 100.0% |
| `com.legend.integration.PerfProbe.main` | 3060 | 99.7% |
| `com.legend.Compiler.normalizeWithSystem` | 2707 | 88.2% |
| `com.legend.Compiler.normalizeLayer` | 2574 | 83.8% |
| `com.legend.normalizer.ModelNormalizer.normalize` | 2551 | 83.1% |
| `com.legend.normalizer.MappingNormalizer.normalize` | 2163 | 70.5% |
| `com.legend.normalizer.MappingNormalizer.withElement` | 2052 | 66.8% |
| `com.legend.normalizer.MappingPrePass.run` | 1242 | 40.5% |
| `com.legend.normalizer.MappingPrePass.lambda$run$0` | 1238 | 40.3% |
| `com.legend.normalizer.MappingPrePass.prePass` | 1126 | 36.7% |
| `com.legend.normalizer.MappingNormalizer.normalizeMapping` | 814 | 26.5% |
| `com.legend.normalizer.MappingNormalizer.lambda$normalize$0` | 814 | 26.5% |
| `com.legend.normalizer.ImplicitInheritance.apply` | 780 | 25.4% |
| `com.legend.normalizer.ResolvedMapping.visibleSets` | 698 | 22.7% |
| `com.legend.normalizer.MappingClosures$Closure.sets` | 640 | 20.8% |
| `com.legend.normalizer.MappingClosures$Closure.walkSets` | 639 | 20.8% |
| `com.legend.compiler.NameResolver.resolveList` | 382 | 12.4% |
| `com.legend.normalizer.ModelNormalizer.resolveSynthesized` | 358 | 11.7% |
| `com.legend.compiler.NameResolver.resolveVs` | 356 | 11.6% |
| `com.legend.compiler.NameResolver.resolveVsList` | 341 | 11.1% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 338 | 11.0% |
| `com.legend.normalizer.AssociationSynthesis.synthesizeAssociationMapping` | 335 | 10.9% |
| `com.legend.compiler.NameResolver.resolveQueryIn` | 325 | 10.6% |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 315 | 10.3% |
| `com.legend.normalizer.MappingNormalizer.synthesizeClassMapping` | 302 | 9.8% |
| `com.legend.normalizer.MappingNormalizer.synthRelational` | 292 | 9.5% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedMapping` | 286 | 9.3% |
| `com.legend.compiler.spec.Calls.platform` | 283 | 9.2% |
| `com.legend.compiler.NameResolver.builtCallNames` | 282 | 9.2% |
| `com.legend.compiler.NameResolver.decide` | 272 | 8.9% |
| `com.legend.compiler.BareNames.referents` | 249 | 8.1% |
| `com.legend.Compiler.boot` | 244 | 7.9% |
| `com.legend.Compiler.lambda$boot$0` | 218 | 7.1% |
| `com.legend.cache.ContentStore.lambda$getOrCompute$0` | 218 | 7.1% |
| `com.legend.cache.ContentStore.getOrCompute` | 218 | 7.1% |
| `com.legend.normalizer.ResolvedMapping.<init>` | 216 | 7.0% |
| `com.legend.compiler.NameResolver.callTierMatches` | 209 | 6.8% |
| `com.legend.normalizer.MappingNormalizer.synthTableBackedParts` | 200 | 6.5% |
| `com.legend.compiler.BareNames.catalogTiered` | 197 | 6.4% |
| `com.legend.compiler.NameResolver.addKnown` | 173 | 5.6% |
allocation (sampled weight): 3.64 GB
| phase | share |
|---|---|
| normalize | 70.8% |
| names | 24.1% |
| model | 3.1% |
| parse | 1.6% |
| lower | 0.3% |
| other | 0.1% |
| type | 0.1% |
| allocating legend frame (innermost) | share |
| `com.legend.normalizer.MappingClosures$Closure.walkSets` | 27.5% |
| `com.legend.compiler.NameResolver.callTierMatches` | 8.8% |
| `com.legend.normalizer.MappingLedger.mappedInClosure` | 8.7% |
| `com.legend.model.MappingDefinition$ClassBindings.lambda$new$1` | 7.1% |
| `com.legend.compiler.BareNames.tiered` | 7.0% |
| `com.legend.model.RelationalOperation.descendants` | 5.0% |
| `com.legend.protocol.SetIds.defaultFor` | 4.7% |
| `com.legend.normalizer.ResolvedMapping.<init>` | 4.4% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 3.2% |
| `com.legend.compiler.BareNames.catalogTiered` | 2.7% |
| `com.legend.model.SignatureMangle.mangle` | 2.0% |
| `com.legend.normalizer.ModelNormalizer.resolveSynthesized` | 2.0% |
| `com.legend.normalizer.ImplicitInheritance.apply` | 1.7% |
| `com.legend.normalizer.MappingClosures$Closure.relationalSets` | 1.6% |
| `com.legend.compiler.BareNames.referents` | 1.3% |
| `com.legend.normalizer.MappingClosures$Closure.walkIds` | 1.1% |
| `com.legend.lexer.Lexer.<init>` | 1.1% |
| `com.legend.normalizer.ResolvedMapping.visibleSets` | 1.0% |
| `com.legend.normalizer.ImplicitInheritance.implicitOpsForRoutedTargets` | 1.0% |
| `com.legend.normalizer.AssociationSynthesis.injectMultiHopAssociationPMs` | 0.7% |
