execution samples: 30153

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| store-resolve | 10071 | 33.4% | 13513 | 44.8% |
| type | 7359 | 24.4% | 15703 | 52.1% |
| lower | 3422 | 11.3% | 3650 | 12.1% |
| names | 2349 | 7.8% | 2349 | 7.8% |
| plan | 2306 | 7.6% | 22395 | 74.3% |
| json | 2095 | 6.9% | 2764 | 9.2% |
| render | 1218 | 4.0% | 1218 | 4.0% |
| other | 806 | 2.7% | 0 | 0.0% |
| inline | 522 | 1.7% | 905 | 3.0% |
| exec/result | 3 | 0.0% | 3 | 0.0% |
| model | 2 | 0.0% | 5053 | 16.8% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `java.util.HashMap.getNode` | 3901 | 12.9% |
| `java.lang.String.equals` | 1428 | 4.7% |
| `java.util.HashMap.putVal` | 799 | 2.6% |
| `java.util.HashMap.resize` | 684 | 2.3% |
| `java.util.ImmutableCollections$ListItr.next` | 413 | 1.4% |
| `com.legend.compiler.spec.typed.TypedSpec.mapChildren` | 371 | 1.2% |
| `java.util.stream.ReferencePipeline$2$1.accept` | 353 | 1.2% |
| `java.lang.String.hashCode` | 336 | 1.1% |
| `com.legend.json.Json.escapeTo` | 324 | 1.1% |
| `java.lang.AbstractStringBuilder.append` | 306 | 1.0% |
| `java.util.ArrayList.add` | 274 | 0.9% |
| `com.legend.json.Json.writeValue` | 260 | 0.9% |
| `com.legend.model.MappingDefinition.collectIncludes` | 246 | 0.8% |
| `com.legend.sql.SqlRewriter.mapList` | 232 | 0.8% |
| `java.util.ImmutableCollections$AbstractImmutableList.iterator` | 224 | 0.7% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.integration.PerfProbe.one` | 30153 | 100.0% |
| `com.legend.integration.PerfProbe.main` | 29965 | 99.4% |
| `com.legend.TypedQuery.planned` | 21735 | 72.1% |
| `com.legend.TypedQuery.executionPlan` | 21735 | 72.1% |
| `com.legend.planner.PlanMaker.plan` | 21726 | 72.1% |
| `com.legend.planner.ProductStatements.prepare` | 15207 | 50.4% |
| `com.legend.resolver.StoreResolver.resolve` | 13444 | 44.6% |
| `com.legend.resolver.StoreResolver.resolveNode` | 12638 | 41.9% |
| `com.legend.resolver.StoreResolver.anchoredNode` | 12591 | 41.8% |
| `com.legend.resolver.StoreResolver.resolveChain` | 12381 | 41.1% |
| `com.legend.resolver.StoreResolver.resolveObject` | 12374 | 41.0% |
| `com.legend.compiler.spec.typed.TypedSpec.mapChildren` | 10986 | 36.4% |
| `com.legend.resolver.StoreResolver.structural` | 10121 | 33.6% |
| `com.legend.resolver.StoreResolver.lambda$structural$0` | 10093 | 33.5% |
| `com.legend.compiler.spec.Typer.typeBody` | 5066 | 16.8% |
| `com.legend.compiler.spec.Typer.synth` | 5059 | 16.8% |
| `com.legend.compiler.spec.Typer.applyFunction` | 5049 | 16.7% |
| `com.legend.resolver.ClassSources.get` | 4478 | 14.9% |
| `com.legend.resolver.ClassSources.build` | 4430 | 14.7% |
| `com.legend.compiler.spec.Overloads.checkGeneric` | 4364 | 14.5% |
| `com.legend.resolver.StoreResolver.collectOpChain` | 4054 | 13.4% |
| `com.legend.compiler.spec.Typer.applyCore` | 3996 | 13.3% |
| `com.legend.planner.ProductStatements.lower` | 3765 | 12.5% |
| `com.legend.compiler.spec.Typer.checkGeneric` | 3716 | 12.3% |
| `com.legend.lowering.Lowerer.lower` | 3476 | 11.5% |
| `com.legend.compiler.spec.Overloads.checkWithDeferred` | 3449 | 11.4% |
| `com.legend.compiler.spec.Overloads.synth` | 3088 | 10.2% |
| `com.legend.resolver.StoreResolver.registerNavigations` | 2993 | 9.9% |
| `com.legend.model.MappingDefinition.classBindingsWithIncludes` | 2898 | 9.6% |
| `com.legend.compiler.spec.SpecCompiler.compile` | 2819 | 9.3% |
| `com.legend.compiler.spec.SpecCompiler.check` | 2797 | 9.3% |
| `com.legend.compiler.spec.Typer.inFunctionScope` | 2795 | 9.3% |
| `com.legend.compiler.spec.TypeAnnotations.inFrame` | 2789 | 9.2% |
| `com.legend.compiler.spec.InferenceKernel.withRigid` | 2788 | 9.2% |
| `com.legend.compiler.spec.Typer.lambda$inFunctionScope$0` | 2788 | 9.2% |
| `com.legend.compiler.spec.SpecCompiler.lambda$check$0` | 2773 | 9.2% |
| `com.legend.compiler.spec.SpecCompiler.checkBody` | 2770 | 9.2% |
| `com.legend.executionplan.PlanJson.write` | 2764 | 9.2% |
| `com.legend.compiler.spec.SpecCompiler.typeBody` | 2632 | 8.7% |
| `com.legend.TypedQuery.body` | 2498 | 8.3% |
| `com.legend.compiler.spec.SpecCompiler.typeQueryBody` | 2494 | 8.3% |
| `com.legend.lowering.Lowerer.relation` | 2463 | 8.2% |
| `com.legend.Compiler.resolveQuery` | 2396 | 7.9% |
| `com.legend.resolver.AssociationJoins.associationJoin` | 2283 | 7.6% |
| `com.legend.lowering.Sorts.sort` | 2185 | 7.2% |
| `com.legend.model.MappingDefinition.withIncludes` | 2100 | 7.0% |
| `com.legend.model.MappingDefinition.collectIncludes` | 2078 | 6.9% |
| `com.legend.Compiler.resolveNames` | 2052 | 6.8% |
| `com.legend.compiler.NameResolver.resolveQueryIn` | 2039 | 6.8% |
| `com.legend.compiler.NameResolver.resolveVs` | 2036 | 6.8% |
| `com.legend.compiler.NameResolver.resolveLambda` | 2034 | 6.7% |
| `com.legend.compiler.NameResolver.resolveList` | 2032 | 6.7% |
| `com.legend.resolver.NavMaterializer.navTargetMaterialized` | 2028 | 6.7% |
| `com.legend.compiler.NameResolver.resolveVsList` | 2027 | 6.7% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 2009 | 6.7% |
| `com.legend.resolver.RelationalRootForm.primaryKeyColumns` | 1979 | 6.6% |
| `com.legend.compiler.spec.SortChecker.check` | 1862 | 6.2% |
| `com.legend.lowering.Lowerer.filter` | 1695 | 5.6% |
| `com.legend.planner.PlanMaker.typed` | 1668 | 5.5% |
| `com.legend.compiler.spec.Overloads.bindDeferredAndBuild` | 1618 | 5.4% |
| `com.legend.compiler.spec.MapChecker.check` | 1603 | 5.3% |
| `com.legend.json.Json.toCompact` | 1596 | 5.3% |
| `com.legend.json.Json.writeValue` | 1579 | 5.2% |
| `com.legend.resolver.StoreResolver.lambda$resolveObject$8` | 1563 | 5.2% |
| `com.legend.resolver.StoreResolver.lambda$resolveObject$5` | 1562 | 5.2% |
| `com.legend.resolver.StoreResolver.substitution` | 1527 | 5.1% |
| `com.legend.compiler.SymbolTable.resolveId` | 1464 | 4.9% |
| `com.legend.compiler.ModelBuilder.findMapping` | 1451 | 4.8% |
| `com.legend.compiler.element.PureModelContext.findMapping` | 1435 | 4.8% |
| `com.legend.compiler.spec.ProjectChecker.check` | 1354 | 4.5% |
allocation (sampled weight): 23.45 GB
| phase | share |
|---|---|
| store-resolve | 36.1% |
| type | 21.6% |
| plan | 11.0% |
| names | 9.5% |
| json | 9.0% |
| lower | 7.7% |
| other | 2.2% |
| render | 1.5% |
| inline | 1.2% |
| model | 0.0% |
| allocating legend frame (innermost) | share |
| `com.legend.model.MappingDefinition.collectIncludes` | 9.9% |
| `com.legend.resolver.ClassSources.findBinding` | 5.3% |
| `com.legend.compiler.BareNames.tiered` | 3.1% |
| `com.legend.executionplan.MirJson.outputCol` | 2.4% |
| `com.legend.compiler.BareNames.catalogTiered` | 2.3% |
| `com.legend.compiler.NameResolver.callTierMatches` | 2.2% |
| `com.legend.json.Json.escapeTo` | 2.2% |
| `com.legend.executionplan.MirJson.type` | 2.1% |
| `com.legend.model.MappingDefinition.classBindingsWithIncludes` | 2.0% |
| `com.legend.integration.PerfProbe.one` | 2.0% |
| `com.legend.compiler.spec.typed.TypedSpec.mapChildren` | 2.0% |
| `com.legend.compiler.ModelBuilder.findMapping` | 1.9% |
| `com.legend.sql.SqlExpr$Column.of` | 1.7% |
| `com.legend.json.Json$Writer.toString` | 1.5% |
| `com.legend.compiler.spec.typed.TypedPropertyAccess.children` | 1.5% |
| `com.legend.json.Json$Writer.append` | 1.4% |
| `com.legend.executionplan.MirJson.columnType` | 1.3% |
| `com.legend.compiler.element.type.Type$RelationType.<init>` | 1.2% |
| `com.legend.resolver.ClassSources.bindsIn` | 1.2% |
| `com.legend.resolver.StoreEscapees.check` | 1.1% |
