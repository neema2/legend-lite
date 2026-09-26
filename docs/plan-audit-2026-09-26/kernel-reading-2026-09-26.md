# Kernel reading: legend-pure function matching / inference (pinned tree)

Read-only reading of the twelve reference methods, 2026-09-26, from
`M3 = /Users/neemsandv/Library/Caches/bazel/_bazel_neemsandv/646bc514b2e63fa36f2af3ea828e232c/external/+http_archive+legend_pure_src/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3`
and `RES = …/legend-pure-m3-core/src/main/resources/platform/pure`.

Abbreviations: FEP = `M3/compiler/postprocessing/processor/valuespecification/FunctionExpressionProcessor.java`,
FEM = `M3/compiler/postprocessing/functionmatch/FunctionExpressionMatcher.java`,
FM = `M3/compiler/postprocessing/functionmatch/FunctionMatch.java`,
GTM = `M3/navigation/generictype/match/GenericTypeMatch.java`, TM = `…/match/TypeMatch.java`,
MM = `M3/navigation/multiplicity/MultiplicityMatch.java`, TI = `M3/compiler/postprocessing/inference/TypeInference.java`,
TIC = `…/inference/TypeInferenceContext.java`, IVP = `…/valuespecification/InstanceValueProcessor.java`,
IS = `M3/navigation/importstub/ImportStub.java`, C3 = `M3/navigation/linearization/C3Linearization.java`.
"GT" = generic type, "ctx" = the current `TypeInferenceContext`.

Helper facts used below (each checked in source):
- `Multiplicity.isToOne(m, strict)` (`M3/navigation/multiplicity/Multiplicity.java:78-83`): concrete AND (`!strict` OR lower==1) AND upper==1. With `strict=false` (the only way FEP calls it) `[0..1]` IS to-one; a multiplicity parameter `m` is NOT.
- `Multiplicity.isMultiplicityConcrete(m)` (:40-43): non-null and `multiplicityParameter == null`.
- `Type.isTopType`/`isBottomType` (`M3/navigation/type/Type.java:123-138`): identity with `type_TopType()` (Any) / `type_BottomType()` (Nil).
- `Type.getGeneralizationResolutionOrder` (:150-153) = `C3Linearization.getTypeGeneralizationLinearization`.
- `Type.getDirectGeneralizations` (:169-173): the `generalizations` property in declaration order, each `.general.rawType`.
- `_Package.SPECIAL_TYPES` (`M3/navigation/_package/_Package.java:39`) = primitive type names + `Package`.
- `_Package.findInPackage(pkg, name)` (:123-126) = the child of that exact name.
- `FunctionDefinitionProcessor.shouldInferTypesForFunctionParameters(ft)` (`…/processor/FunctionDefinitionProcessor.java:171-174`): any parameter with `genericType == null`.
- `NullMatchBehavior` = {MATCH_ANYTHING, MATCH_NOTHING, ERROR}; `ParameterMatchBehavior` = {MATCH_ANYTHING, MATCH_CAUTIOUSLY, MATCH_NOTHING, ERROR}; a null behaviour is treated as ERROR (GTM:310-318, MM:304-312).
- `Automap.AUTOMAP_LAMBDA_VARIABLE_NAME = "v_automap"` (`…/processor/Automap.java:25`).
- `GenericType.makeTypeArgumentAsConcreteAsPossible(null, …)` returns null (`M3/navigation/generictype/GenericType.java:127-130`).
- `ProcessorState.pushTypeInferenceContext` (`…/postprocessing/ProcessorState.java:160-172`): if the current ctx is "ahead" and not yet consumed, it is consumed instead of pushing; otherwise a child ctx is pushed. `pushTypeInferenceContextAhead` (:174-179) pushes then marks ahead; `popTypeInferenceContextAhead` (:181-189) pops to the parent.

---

## A. The twelve items

### A1. FEP `process` (FEP:120-265) and `processLambda` (FEP:618-679)

Read: FEP:96-265 (`process`), :394-489 (`manageMagicColumnFunctions`), :491-523, :526-565, :567-594, :596-616, :618-679, :681-701, :703-784, :796-821 (`firstPassTypeInference`), :823-853 (`cleanProcess`), :855-912 (`isInferenceSuccess` & co).

```
process(fe):
  observer = state.observer; state.pushTypeInferenceContext()                      // :123-124
  mr = matchFunction(fe)                                                            // :126 (see A2)
        -> mr.foundFunctions (ordered list), mr.parametersRequiringTypeInference (IntSet),
           mr.functionName (null unless found via repository search), mr.parametersValues
  finalFunction = null; someInferenceFailed = false                                // :128-129
  FOR foundFunction IN mr.foundFunctions (in list order):                          // :131
      fe.func := foundFunction                                                     // :133-134 (remove then set)
      ctx.setScope(foundFunction); fft = function_getFunctionType(foundFunction)   // :136-137
      paramsType = fft.parameters; parametersValues = fe.parametersValues (RE-READ) // :142-144
      lambdaOK = true; columnOK = true                                             // :146-147
      IF mr.parametersRequiringTypeInference not empty:                            // :149
          potentiallyUpdateTypeInferenceContextUsingFunctionSignature:             // :153 -> :567-584
              for each (instance z): if isInferenceSuccess(instance):
                  ctx.register(paramsType[z].GT, instance.GT, ctx.topContext, merge=false)   // :576
                  ctx.registerMul(paramsType[z].mult, instance.mult, ctx.topContext)          // :577
              else observer.paramInferenceFailed(z)
          for z in 0..parametersValues.size-1:                                     // :156
              templateVariable = (foundFunction.classifierGenericType.typeArguments[0].rawType as FunctionType).parameters[z]  // :161
              templateGT = templateVariable.GT                                     // :162
              if isColumnWithEmptyType(instance):        columnOK = processEmptyColumnType(...)          // :164-167
              elif isLambdaWithEmptyParamType(instance): lambdaOK = processLambda(..., lambdaOK, templateGT)  // :168-171
              elif z in mr.parametersRequiringTypeInference: handleParameter(...)  // :172-175 (reverse inference, below)
      ELSE:                                                                        // :180
          updateTypeInferenceContextUsingFunctionSignature: for each z:
              ctx.register(paramsType[z].GT, instance.GT, ctx.topContext, merge=TRUE)        // :591
              ctx.registerMul(paramsType[z].mult, instance.mult, ctx.topContext)             // :592
          returnGT = makeTypeArgumentAsConcreteAsPossible(fft.returnType, ctx maps)          // :187
          if !concrete(returnGT) && !ctx.isTop(typeParamName(returnGT)):
              updateTypeInferenceContextSoThatReturnTypeIsConcrete(...)            // :189-192 -> :491-523
                  for each (instance z): resolvedGT/resolvedMult = template made concrete with ctx maps (:498-501)
                      state.pushTypeInferenceContextAhead(); scope := instance.func if FunctionExpression (:503-504)
                      ctx.register(instance.GT, resolvedGT, ctx.parent); ctx.registerMul(instance.mult, resolvedMult, ctx.parent)  // :507-508
                      cleanProcess(instance); PostProcessor.processElement(instance)   // :513-514  (REPROCESS the argument)
                      state.popTypeInferenceContextAhead()                              // :518
      columnOK = manageMagicColumnFunctions(fe, foundFunction, columnOK, ctx)      // :196 (funcColSpec*/aggColSpec* only, :394-489)
      updateFunctionExpressionReturnTypeAndMultiplicity(fe, foundFunction, success = lambdaOK && columnOK)   // :198 -> :526-565
          success: TypeInference.storeInferredTypeParametersInFunctionExpression (:530; TI:68-106, see A9 note)
                   returnGT = makeConcrete(fft.returnType, ctx type map MINUS entries whose value is a GenericTypeOperation-equal, ctx mult map)  // :533
                   returnMult = makeMultiplicityAsConcreteAsPossible(fft.returnMultiplicity, ctx mult map)                        // :534
                   if !concrete(returnGT) && !ctx.isTop(name(returnGT)):
                       THROW PureCompilationException(fe.src, "The system is not capable of inferring the return type (<GT>) of the <function|property|qualified property> '<name>'. Check your signatures!")   // :536-542
                   fe.GT := copyAsInferred(returnGT); fe.mult := copy(returnMult)     // :545-550
          failure: fe.GT := copy(fft.returnType) (raw, may contain T); fe.mult := copy(fft.returnMultiplicity)   // :556-562
      // ACCEPT TEST                                                                // :200-215
      IF mr.functionName == null:            finalFunction = foundFunction          // :200-203  (UNCONDITIONAL accept: pre-resolved func, property, relation column, qualified property)
      ELIF !lambdaOK || !columnOK:            someInferenceFailed = true             // :204-207  (candidate skipped, no best-match test)
      ELSE: best = FEM.getBestFunctionMatch(mr.foundFunctions /*ALL*/, parametersValues, mr.functionName, fe.src, lenient=false)  // :210
            if best == foundFunction: finalFunction = foundFunction                  // :211-214  (best==null or another candidate -> not accepted)
      IF finalFunction != null: BREAK                                                // :217-220
      // RETRY CLEANUP                                                               // :223-227
      IF mr.foundFunctions.size() > 1:
          for pv in parametersValues: cleanProcess(pv)      // :225  (Unbinder unbinds pv and everything it visited; marks not processed; for new/copy with 3 params also unmarks the key values, :823-853)
          mr.parametersRequiringTypeInference = firstPassTypeInference(fe, parametersValues)   // :226 (re-run first pass)
  END FOR
  IF finalFunction != null:                                                          // :231
      finalFunction.applications += fe                                               // :233
      if name in {new_Class_1__String_1__KeyExpression_MANY__T_1_, new_Class_1__String_1__T_1_, copy_T_1__String_1__KeyExpression_MANY__T_1_, copy_T_1__String_1__T_1_}: addTraceForKeyExpressions  // :236-244
      if name == letFunction_String_1__T_m__T_m_:                                    // :246
          state.variableContext.PARENT.registerValue(name of params[0] (InstanceValue).values.any, params[1])  // :250
          VariableNameConflictException -> PureCompilationException(fe.src, e.message)  // :252-255
  ELIF !someInferenceFailed: throwNoMatchException(fe)                               // :258-261 (see below)
  // else: silently leave fe with the LAST tried func and its return type            // (no code — fallthrough)
  state.popTypeInferenceContext()                                                    // :264
```

`throwNoMatchException(fe)` (FEP:1183-1228): message `"The system can't find a match for the function: " + printFunctionSignatureFromExpression(fe)`. If `function_getFunctionsForName(bareName)` is non-empty and `< 20` → `PureUnmatchedFunctionException` carrying candidates partitioned by (package is Root or in the section's `_imports` paths) × (package path in coreImport's paths), filtered by `Visibility.isVisibleInSource` (:1247). Else `PureCompilationException(fe.src, message)`.

`firstPassTypeInference` (FEP:796-821): for each argument i: `PostProcessor.processElement(arg)` inside a milestoning date context (:806-808); `success = isInferenceSuccess(arg)`; unsuccessful indices collected; each arg gets a `ParameterValueSpecificationContext(offset=i, fe)` usageContext if it has none (:816, :926-934).

`isInferenceSuccess` (FEP:855-880): column-with-empty-type → false; lambda-with-empty-param-type → false; InstanceValue → all values recursively; FunctionExpression whose func is not an AbstractProperty → `fType.typeParameters.isEmpty() || (resolvedTypeParameters non-empty && none is an empty-typed RelationType)` (:876); anything else → true.
`isLambdaWithEmptyParamType` (:882-886): InstanceValue with ANY value that is a FunctionDefinition with a parameter whose genericType is null.
`isColumnWithEmptyType` (:888-906): InstanceValue whose GT rawType is a RelationType ALL of whose columns have a null column type.

`handleParameter` (FEP:596-616) — reverse (top-down) inference for a non-lambda argument that failed the first pass: resolvedGT/mult = template param made concrete with ctx maps (:600-603); `pushTypeInferenceContextAhead`; scope := arg's func if SimpleFunctionExpression; `ctx.register(arg.GT, resolvedGT, ctx.parent)`, `registerMul` likewise (:609-610); `cleanProcess(arg)`; `PostProcessor.processElement(arg)` (:611-612); pop.

**`processLambda`** (FEP:618-679):
```
processLambda(fe, ..., foundFunction, z, instance /*InstanceValue*/, lambdaOK, templateGT):
  templateToMatchLambdaTo = (foundFunction.classifierGenericType.typeArguments[0].rawType as FunctionType).parameters[z]  // :620
  FOR val IN instance.values:                                                        // :623
    IF val is LambdaFunction:                                                        // :625
      within a new milestoning date context (:627):
        lambdaOK = lambdaOK && !TI.processParamTypesOfLambdaUsedAsAFunctionExpressionParamValue(instance, val, templateToMatchLambdaTo, ...)  // :629
        // NB: Java && short-circuits: once lambdaOK is false, later lambdas (in this value list AND in later parameters,
        //     because the caller threads lambdaOK through :170) are NOT typed at all.
      // "Manage return type in any case" (runs even if the lambda typing failed)    // :632
      IF concrete(templateGT) && rawType(templateGT) subTypeOf Function:            // :633
        templateGenFunctionType = templateGT.typeArguments[0]                       // :635
        IF concrete(templateGenFunctionType) && its rawType != Any:                 // :636
          templateReturnType = (rawType as FunctionType).returnType                 // :638
          lambdaCtx = ctx.topContext   // "Generics in lambdas are relative to their environment"   // :641
          IF templateReturnType != null:
            lambdaFT = val's FunctionType; concreteGT = makeConcrete(lambdaFT.returnType, lambdaCtx maps)  // :645-646 (null if lambda unprocessed)
            lambdaFT.returnType := concreteGT                                       // :647-648
            IF !concrete(templateReturnType): ctx.register(templateReturnType, concreteGT, ctx.parent)   // :649-653 (register(…,null) is a no-op, TIC:336-340)
            ELIF concreteGT != null: handleTypeArgumentTypeInference(templateReturnType, concreteGT)     // :654-657 -> :681-701: zip type args; non-concrete template arg -> register in parent; else recurse. NO recursion into Relation/Function templates (TODO :700)
          templateReturnMult = (rawType as FunctionType).returnMultiplicity         // :660
          IF templateReturnMult != null:
            concreteMult = makeMultiplicityAsConcreteAsPossible(lambdaFT.returnMultiplicity, lambdaCtx mult map)  // :664
            lambdaFT.returnMultiplicity := concreteMult                              // :666-667
            IF concreteMult != null: ctx.registerMul(templateReturnMult, concreteMult, ctx.parent)   // :668-672
  RETURN lambdaOK                                                                    // :678
```

### A2. FEP `matchFunction` (FEP:267-392)

Read: FEP:267-392, :954-1026 (`findFunctionForPropertyBasedOnMultiplicity`), :1045-1057, :1059-1067, :1069-1094, :1096-1106, :1108-1181, :1230-1241.

```
matchFunction(fe):
  parametersValues = fe.parametersValues
  for each vs: resolve import stubs in vs.multiplicity and vs.genericType        // :270-275
  parametersRequiringTypeInference = firstPassTypeInference(fe, parametersValues)  // :278  (FIRST PASS: every argument processed)
  foundFunctions = []; functionName = null                                        // :281-282
  IF fe.func != null:  foundFunctions = [resolve(fe.func)]                        // :283-286 (pre-resolved; NO matching at all)
  ELSE:
    IF fe.propertyName != null:                                                   // :290-291   ($x.p)
      source = parametersValues[0]; propertyName = name of the propertyName InstanceValue's value   // :293-295
      sourceGT = source.GT; if !concrete(sourceGT): THROW "The type '<T>' can't be inferred yet. Please specify it. (Property:'<p>')" at source.src   // :296, :1059-1067
      IF sourceGT subTypeOf Enumeration (raw-type check only, GenericType.java:396-399):       // :299
         reprocessEnumValueInExtractEnumValue: append a String literal 'propertyName' as parameter 1 (marked processed),
             fe.functionName := "extractEnumValue", fe.propertyName removed, param 1 gets usageContext offset 1   // :301, :1096-1106
         (foundFunctions stays empty -> falls to the repository search at :384)
      ELIF rawType(sourceGT) is a RelationType:                                   // :303
         IF isToOne(source.mult, strict=false): foundFunctions += _RelationType.findColumn(rawType, name, fe.src)   // :306-310 (the Column IS the function)
         ELSE: AUTOMAP (below) with M3Properties.propertyName; parametersValues re-read; first pass re-run   // :311-320
      ELSE (class-like receiver):
         IF isToOne(source.mult, strict=false):                                   // :325
            propertyFunc = findFunctionForPropertyBasedOnMultiplicity(fe, sourceGT)   // :327 -> :954-1026:
                property = class_findPropertyUsingGeneralization(sourceType, name)          // :959
                if null and sourceType is ClassProjection: process the projection, retry    // :962-966
                if still null: qps = _Class.findQualifiedPropertiesUsingGeneralization(sourceType, name)   // :969 (+ projection retry :970-974)
                    property = first qp whose FunctionType.parameters.size() == sourceType.typeVariables.size() + 1   // :975, :1045-1057
                    if null: THROW at propertyName.src (:1020-1021) with message by qps.size():
                        0 -> "Can't find the property '<p>' in the class <path>"                                // :983
                        1 -> "The property '<p>' " + (milestoned generated ? "is milestoned with stereotypes: [ … ] and requires date parameters: [ … ]" (or "… requires 2 date parameters : [start, end]" for allVersionsInRange) : "requires some parameters.")   // :986-1005
                        n -> all milestoned ? same milestoning text : "There are <n> properties named '<p>' and all require additional parameters."  // :1007-1018
            IF MilestoningFunctions.isGeneratedMilestonedQualifiedPropertyWithMissingDates(propertyFunc):
                propertyFunc = MilestoningDatesPropagationFunctions.getMilestoningQualifiedPropertyWithAllDatesSupplied(...)   // :328-331 (adds date params to fe from the propagated context; returns the original if no dates found, MilestoningDatesPropagationFunctions.java:147-157)
            foundFunctions += propertyFunc                                          // :332
         ELSE: AUTOMAP with propertyName; re-read; first pass re-run              // :334-343
    ELIF fe.qualifiedPropertyName != null:                                        // :349-350   ($x.q(args))
      source = parametersValues[0]; name; sourceGT concrete-or-throw (same text)  // :352-355
      IF isToOne(source.mult, strict=false):                                      // :359
         qps = findFunctionsForQualifiedPropertyBasedOnMultiplicity(...)          // :361 -> :1069-1094:
              process a ClassProjection receiver first (:1075-1078)
              firstParam = synthetic VariableExpression(GT = parametersValues[0].GT, mult = PureOne)   // :1080-1082
              params = [firstParam] ++ sourceGT.typeVariableValues ++ tail(parametersValues)          // :1084
              properties = _Class.findQualifiedPropertiesUsingGeneralization(sourceRawType, name)     // :1085
              found = FEM.getFunctionMatches(properties, params, name, qualifiedPropertyName.src, lenient=TRUE)   // :1087
              empty -> THROW "The system can't find a match for the function: <name(sig)>" at qualifiedPropertyName.src  // :1089-1092, :1230-1235
         IF qps.size()==1 && isGeneratedMilestonedQualifiedPropertyWithMissingDates(qps[0]): foundFunctions += datesSupplied(qps[0])   // :362-366
         ELSE foundFunctions += ALL qps (lenient order)                            // :367-370
      ELSE: AUTOMAP with qualifiedPropertyName; re-read; first pass re-run        // :372-380
    IF foundFunctions.isEmpty():                                                  // :384
       foundFunctions += FEM.findMatchingFunctionsInTheRepository(fe, lenient=TRUE)   // :387  (LENIENT SEARCH; see A3/A4)
       functionName = getFunctionName(fe) = fe.functionName after the last ':'    // :388, :1237-1241
  RETURN (foundFunctions, parametersRequiringTypeInference, functionName, parametersValues)   // :391
```

AUTOMAP = `reprocessPropertyForManySources` (FEP:1108-1130) + `buildLambdaForMapWithProperty` (:1132-1174): builds `LambdaFunction` named after the property-name instance (:1156/:1161), classifier `LambdaFunction<FunctionType(parameters=[v_automap : copyAsInferred(sourceGT) [1]])>` with NO return type (:1134-1147), body = one `SimpleFunctionExpression` carrying the SAME `propertyName`/`qualifiedPropertyName` InstanceValue, the same importGroup, parameters `[v_automap] ++ tail(parametersValues)` (:1149-1169). Wraps it in a fresh `InstanceValue` (NOT via wrapValueSpecification, comment :1112), sets `fe.parametersValues = [source, lambdaIV]`, `fe.functionName = "map"`, removes the property-name property (:1113-1129). The lambda has typed params, so `isLambdaWithEmptyParamType` is false and the re-run first pass (:318/:341/:379) processes the body's property access normally.

### A3. FEM `getValidPackages`, `getFunctionsWithMatchingName`; `Imports.getImportGroupPackages`; the core import group

Read: FEM:42-56 (`findMatchingFunctionsInTheRepository`), :153-170; `M3/navigation/imports/Imports.java:52-65`; `RES/grammar/m3.pure:175-211`.

```
findMatchingFunctionsInTheRepository(fe, lenient):                                  // FEM:42-56
  split = PackageableElement.splitUserPath(fe.functionName)  (on "::")             // :46
  name = last segment; pkgPath = all but last (empty if bare)                       // :47-49
  fns = getFunctionsWithMatchingName(name, pkgPath, fe)                             // :51
  return getFunctionMatches(fns, fe.parametersValues, name, fe.src, lenient)        // :55

getFunctionsWithMatchingName(name, pkgPath, fe):                                     // :153-157
  packages = getValidPackages(pkgPath, fe)
  return function_getFunctionsForName(name).collectIf(f -> packages.contains(f._package()))   // :156 (source order = the name index's SET iteration order)

getValidPackages(pkgPath, fe):                                                       // :159-170
  IF pkgPath non-empty: pkg = _Package.getByUserPath(pkgPath)  (ABSOLUTE; imports never consulted)
      return pkg == null ? {} : {pkg}                                                // :163-164 (unknown package -> zero candidates -> "can't find a match", never "unknown package")
  packages = Imports.getImportGroupPackages(fe.importGroup).toSet()                  // :167
  packages += repository_getTopLevel("Root")                                         // :168
  return packages    // a SET: no ordering, no own-package entry

Imports.getImportGroupPackages(importGroup):                                         // Imports.java:52-65
  packages = {}                                                                      // :54
  coreImport = package_getByUserPath("system::imports::coreImport")                  // :57
  packages += coreImport.imports.map(imp -> package_getByUserPath(imp.path))          // :58
  packages += importGroup.imports.map(imp -> package_getByUserPath(imp.path))         // :61
  return packages.without(null)                                                       // :64 (unknown import paths vanish silently)
```
Core import group (`m3.pure:175-211`, element `coreImport` in package `system::imports`, :175-178): 29 paths at :181-209 —
`meta::pure::metamodel`, `::type`, `::type::generics`, `::relationship`, `::valuespecification`, `::multiplicity`, `::function`, `::function::property`, `::extension`, `::import`; `meta::pure::functions::{date,string,collection,meta,constraints,lang,boolean,tools,relation,io,math,asserts,test,multiplicity}`; `meta::pure::router`, `meta::pure::service`, `meta::pure::tds`, `meta::pure::tools`, `meta::pure::profiles`.

### A4. FEM `getFunctionMatches` (FEM:63-88) and `getBestFunctionMatch` (FEM:90-151)

```
getFunctionMatches(fns, args, name, src, lenient):                                   // :63
  try:
    byMatch: Map<FunctionMatch, MutableList<T>>                                       // :67
    for f in fns (source order): m = FunctionMatch.newFunctionMatch(f, name, args, lenient)   // :70
        if m != null: byMatch[m] (created on first sight) += f                        // :71-74 (EQUAL keys share a bucket; insertion order kept)
    return byMatch.keyValues.toSortedListBy(key).flatCollect(bucket)                  // :76 (keys sorted by FunctionMatch.compareTo, ascending = best first)
  catch RuntimeException e: THROW PureCompilationException(src, "Error finding match for function '<name>'" + (": " + e.message)?)   // :78-87

getBestFunctionMatch(fns, args, name, src, lenient):                                  // :90
  best = null; bestFns = []
  for f in fns: m = newFunctionMatch(...)                                              // :97-99
      if m != null:
          if best == null: best = m; bestFns = [f]                                     // :102-106
          else c = m.compareTo(best); c == 0 -> bestFns += f; c < 0 -> best = m; bestFns = [f]   // :109-119
  (same RuntimeException wrapping :124-133)
  if best == null: return null                                                          // :135-138
  if bestFns.size() > 1: THROW PureCompilationException(src, "Too many matches for <name(sig)>:\n\t<f1>\n\t<f2>…" (printed, sorted))   // :140-148
  return bestFns[0]                                                                     // :150
```

### A5. `FunctionMatch.newFunctionMatch` (FM:114-157), `compareTo` (FM:69-103)

```
newFunctionMatch(function, name, givenParams, lenient):
  if name != function._functionName(): return null                                       // :116-119 (bare-name equality re-checked)
  params = function_getFunctionType(function).parameters; if count != givenParams.size(): return null   // :121-127
  nullBehaviour  = lenient ? MATCH_ANYTHING : MATCH_NOTHING                              // :131
  valueParamBeh  = lenient ? MATCH_ANYTHING : MATCH_CAUTIOUSLY                           // :132
  for i: paramGT = params[i].genericType (import-stub resolved); valueGT = given[i].genericType   // :138-139
         tm = GTM.newGenericTypeMatch(paramGT, valueGT, covariant=TRUE, nullBehaviour, target=MATCH_ANYTHING, value=valueParamBeh)   // :140
         null -> return null                                                             // :141-144
         mm = MM.newMultiplicityMatch(params[i].mult, given[i].mult, covariant=TRUE, nullBehaviour, target=MATCH_ANYTHING, value=MATCH_CAUTIOUSLY /*ALWAYS*/)   // :147-149
         null -> return null
  return FunctionMatch(typeMatches[], multiplicityMatches[])                             // :156

compareTo(other):                                                                        // :69-103
  identical -> 0; different arity -> compare arity (cannot happen for one call)          // :71-80
  for i: c = typeMatches[i].compareTo(other.typeMatches[i]); c != 0 -> return c          // :83-90  (ALL types, left to right)
  for i: c = multiplicityMatches[i].compareTo(other…[i]); c != 0 -> return c             // :93-100 (THEN all multiplicities, left to right)
  return 0                                                                               // :102
equals/hashCode: Arrays.equals / Arrays.hashCode of both arrays                          // :45-66
```
Tie handling: `compareTo == 0` between two different functions ⇒ they are one bucket in `getFunctionMatches` (adjacent, source order) and ⇒ `bestFns.size() > 1` in `getBestFunctionMatch` ⇒ "Too many matches".

### A6. `GenericTypeMatch.newGenericTypeMatch` (GTM:135-308), `compareTo` (GTM:80-99), `compareMatchLists` (GTM:101-114)

```
newGenericTypeMatch(targetGT, valueGT, covariant, nullBeh, targetBeh, valueBeh):
  targetGT == null -> IllegalArgumentException("Target generic type may not be null")     // :138-141
  valueGT == null: MATCH_ANYTHING -> NULL_MATCH(TypeMatch NULL); MATCH_NOTHING -> null; ERROR -> RuntimeException("Value generic type may not be null")   // :142-159
  GenericType.genericTypesEqual(target, value) -> EXACT_MATCH (rawTypeMatch = Simple(0), no arg lists)   // :162-165 (identity or structural incl. type args)
  targetRaw = target.rawType (resolved); valueRaw = value.rawType                         // :167-168
  IF targetRaw == null (target is a type parameter):                                      // :171
      MATCH_ANYTHING -> NON_CONCRETE_MATCH                                                // :175-177  (the only behaviour FM uses for targets)
      MATCH_CAUTIOUSLY -> valueRaw == null ? (same parameter NAME ? NON_CONCRETE : null) : (covariant ? isBottom(valueRaw) : isTop(valueRaw)) ? NON_CONCRETE : null   // :179-189
      MATCH_NOTHING -> null; ERROR -> RuntimeException("Target generic type must be concrete, got: …")   // :190-197
  IF valueRaw == null (value is a type parameter):                                        // :206
      MATCH_ANYTHING -> NON_CONCRETE_MATCH                                                // :210-213 (lenient)
      MATCH_CAUTIOUSLY -> (covariant ? isTop(targetRaw) : isBottom(targetRaw)) ? NON_CONCRETE : null   // :214-218 (strict: a T-typed arg only matches Any)
      MATCH_NOTHING -> null; ERROR -> RuntimeException("Value generic type must be concrete, got: …")   // :219-226
  rawMatch = TM.newTypeMatch(targetRaw, valueRaw, covariant, nullBeh, targetBeh, valueBeh); null -> null   // :231-235
  IF isBottom(covariant ? valueRaw : targetRaw) || isTop(covariant ? targetRaw : valueRaw): return GTM(rawMatch) with EMPTY arg lists   // :236-239
  targetTypeArgs = target.typeArguments; n = size                                          // :241-242
  IF n > 0:
      hom = GenericType.resolveClassTypeParameterUsingInheritance(valueGT, targetGT)  (value's args expressed at target's raw type); null -> null   // :247-251
      if n != hom.argumentsByParameterName.size(): null                                    // :253-256
      for i: tp = targetRaw.typeParameters[i]; paramCovariant = tp.contravariant ? !covariant : covariant   // :258-263
             m = newGenericTypeMatch(targetTypeArgs[i], hom[tp.name], paramCovariant, same behaviours); null -> null   // :264-269
  targetMultArgs = target.multiplicityArguments; k = size                                   // :274-275
  IF k > 0: homM = resolveClassMultiplicityParameterUsingInheritance(valueGT, targetRaw); null -> null; size mismatch -> null   // :280-289
      for i: MM.newMultiplicityMatch(targetMultArgs[i], valueMultArgs[i], covariant /*not flipped*/, same behaviours); null -> null   // :291-299
  IF !ExtendedPrimitiveType.testTypeVariableValuesCompatible(target, value): null           // :302-305
  return GTM(rawMatch, typeArgMatches, multArgMatches)                                      // :307

compareTo(other): rawTypeMatch.compareTo; then compareMatchLists(typeArgumentMatches); then compareMatchLists(multiplicityArgumentMatches)   // :80-99
compareMatchLists(a, b): lexicographic over min(len) by element compareTo, then Integer.compare(len a, len b)   // :101-114 (shorter list wins a tie — EXACT/NON_CONCRETE/NULL have empty lists)
```

### A7. `TypeMatch.newTypeMatch` (TM:350-434) and every `compareTo`

Singletons: `NULL_MATCH` (:36-49), `NON_CONCRETE_MATCH` (:52-75), `BOTTOM_TYPE_MATCH` (:78-101); classes `SimpleTypeMatch(typeDistance)` (:107-156, `EXACT_MATCH = Simple(0)` :109), `RelationTypeMatch` (:158-230), `FunctionTypeMatch` (:232-328).

compareTo table (this vs other → result):
- `NULL.compareTo(x)`: `x==this ? 0 : 1` (:39-42) — NULL is last.
- `NON_CONCRETE.compareTo(x)`: 0 if same; `1` if x is Simple; else `-1` (:55-68).
- `BOTTOM.compareTo(x)`: 0 if same; `-1` if x == NULL; else `1` (:81-94).
- `Simple.compareTo(x)`: 0 if same; `-1` if x == NON_CONCRETE; `-1` if x not Simple; else `Integer.compare(distance)` (:131-149).
- `Relation.compareTo(x)`: 0; `1` if x is NON_CONCRETE or Simple; `-1` if x not Relation; else compareMatchLists(columnsTypeMatches) then compareMatchLists(columnsMultiplicityMatches) (:204-229).
- `Function.compareTo(x)`: 0; `1` if x is NON_CONCRETE or Simple; `-1` if x not Function; else parameterTypeMatches, parameterMultiplicityMatches, returnTypeMatch, returnMultiplicityMatch (:288-327).
Resulting total-ish order: `Simple(0) < Simple(1) < … < NON_CONCRETE < {Relation, Function} < BOTTOM < NULL`. Relation vs Function is `-1` BOTH ways (:216-218 / :300-302) — not antisymmetric.

```
newTypeMatch(targetType, valueType, covariant, nullBeh, targetBeh, valueBeh):
  targetType == null -> IllegalArgumentException("Target type cannot be null")             // :353-356
  valueType == null: MATCH_ANYTHING -> NULL_MATCH; MATCH_NOTHING -> null; ERROR -> RuntimeException("Value type may not be null")   // :358-379
  targetType.equals(valueType) -> EXACT (Simple(0))                                         // :381-384
  superType = covariant ? target : value; subType = covariant ? value : target             // :386-387
  both RelationTypes -> newRelationTypeMatch(sub, super, …)                                  // :389-392 -> :436-488:
        equalRelationType -> EXACT; candidate(sub).columns.size < signature(super).columns.size -> null; alignColumnSets; count mismatch -> null;
        per column: GTM.newGenericTypeMatch(signatureColType, candidateColType, covariant), MM.newMultiplicityMatch(signatureMult, candidateMult, covariant); any null -> null
  isBottom(subType) -> BOTTOM_TYPE_MATCH                                                    // :394-397 (value is Nil when covariant; note: checked BEFORE the FunctionType branches)
  IF isFunctionType(superType):                                                             // :399
      !isFunctionType(subType) -> null; functionTypesEqual -> EXACT;                        // :401-408
      else newFunctionTypeMatch(target, value, covariant, …)                                // :409 -> :490-544:
          parameter counts differ -> null; per parameter: GTM(paramGT_target, paramGT_value, !covariant), MM(…, !covariant); return GTM(returnGT_target, returnGT_value, covariant); MM(return mults, covariant); any null -> null
  IF isFunctionType(subType): return isTop(superType) ? Simple(1) : null                    // :413-416 ("hack: FunctionTypes have no generalizations")
  distance = getGeneralizationResolutionOrder(subType).indexOf(superType)                   // :418 (C3 index of the super type in the SUB type's linearization)
  -1 -> null; 0 -> EXACT; n -> Simple(n)                                                    // :419-433
```

### A8. `MultiplicityMatch.newMultiplicityMatch` (MM:160-302) and `compareTo`s

Singletons `NULL_MATCH` (:23-36), `NON_CONCRETE_MATCH` (:38-62); class `SimpleMultiplicityMatch(lowerBoundDistance, upperBoundDistance)` (:68-133), `EXACT_MATCH = Simple(0,0)` (:70).

compareTo:
- `NULL.compareTo(x)`: `x==this ? 0 : 1` (:26-29).
- `NON_CONCRETE.compareTo(x)`: 0 if same; `-1` if x == NULL; else cast to Simple: `x is (0,0) ? 1 : -1` (:41-55) — NON_CONCRETE sits between EXACT and every inexact Simple.
- `Simple.compareTo(x)`: 0 if same; `-1` if x == NULL; if x == NON_CONCRETE: `this is (0,0) ? -1 : 1`; else `Integer.compare(upperDist)` then `Integer.compare(lowerDist)` (:106-126). UPPER first, then lower.
Order: `EXACT(0,0) < NON_CONCRETE < Simple(upper asc, then lower asc) < NULL`. equals = both distances equal (:88-103).

```
newMultiplicityMatch(target, value, covariant, nullBeh, targetBeh, valueBeh):
  target == null -> IllegalArgumentException("Target multiplicity cannot be null")          // :163-166
  value == null: MATCH_ANYTHING -> NULL; MATCH_NOTHING -> null; ERROR -> RuntimeException("Value multiplicity may not be null")   // :167-184
  target == value (IDENTITY) -> EXACT                                                       // :187-190
  IF !concrete(target) (target is `m`):                                                     // :193
      MATCH_ANYTHING -> NON_CONCRETE                                                        // :197-200 (FM always)
      MATCH_CAUTIOUSLY -> concrete(value) ? (!covariant && target is [*] ? NON_CONCRETE : null) : (same parameter name ? NON_CONCRETE : null)   // :201-219 [sic: reads lower/upper of a NON-concrete target]
      MATCH_NOTHING -> null; ERROR -> RuntimeException("Target multiplicity must be concrete, got: …")   // :220-227
  IF !concrete(value) (value is `m`):                                                       // :232
      MATCH_ANYTHING -> NON_CONCRETE                                                        // :236-239 (never from FM: value beh is MATCH_CAUTIOUSLY)
      MATCH_CAUTIOUSLY -> (covariant && target lower==0 && target upper==-1 i.e. [*]) ? Simple(MAX_VALUE, MAX_VALUE) : null   // :240-247
      MATCH_NOTHING -> null; ERROR -> RuntimeException("Value multiplicity must be concrete, got: …")   // :248-255
  large = covariant ? target : value; small = covariant ? value : target                    // :260-271
  lowerDist = small.lower - large.lower; < 0 -> null                                        // :273-279
  IF large.upper < 0 (`*`): upperDist = small.upper < 0 ? 0 : Integer.MAX_VALUE             // :284-287
  ELIF small.upper < 0: null                                                                // :288-291  ([*] arg never fits a bounded param, incl. [1..*])
  ELSE upperDist = large.upper - small.upper; < 0 -> null                                   // :292-299
  return (0,0) ? EXACT : Simple(lowerDist, upperDist)                                        // :301
```

### A9. `TypeInference.processParamTypesOfLambdaUsedAsAFunctionExpressionParamValue` (TI:108-148)

```
(instanceValueContainer, lambda, templateToMatchLambdaTo /*VariableExpression*/):
  templateGT = template.genericType                                                          // :110
  templateFunctionType = templateGT.typeArguments non-empty ? typeArguments[0].rawType : null   // :111
  lambdaFT = lambda.classifierGenericType.typeArguments[0].rawType                            // :112
  IF !concrete(templateGT) || rawType(templateGT) !subTypeOf Function:
      THROW PureCompilationException(lambda.src, "Can't infer the parameters' types for the lambda. Please specify it in the signature.")   // :114-117  (HARD error, not a retry)
  FOR j, param IN lambdaFT.parameters:                                                        // :119-120
     IF param.genericType == null:                                                            // :123 (already-typed params untouched)
        IF isBottom(templateFunctionType) || isTop(templateFunctionType): THROW same message   // :125-128
        templateParam = (templateFunctionType as FunctionType).parameters[j]                 // :129 (no arity check: IndexOutOfBounds if the lambda has more params)
        gt = makeTypeArgumentAsConcreteAsPossible(templateParam.GT, ctx.typeMap, ctx.multMap)   // :130
        IF !ctx.isTypeParameterResolved(gt): RETURN true   // = FAILURE (caller negates)      // :131-134 (TIC:137-157: concrete, or bound terminal/concrete in this ctx's last state, or in the TOP ctx)
        mult = makeMultiplicityAsConcreteAsPossible(templateParam.mult, ctx.multMap)         // :135
        param.GT := copyAsInferred(ctx.resolve(gt)); param.mult := copy(mult)                 // :136-137 (resolve: TIC:606-627 — value from this ctx's last state, else the TOP ctx, else null)
  in a NEW variable context: FunctionDefinitionProcessor.process(lambda); LambdaFunctionProcessor.process(lambda)   // :140-144 (body typed now, under THIS candidate)
  container.genericType removed; InstanceValueProcessor.updateInstanceValue(container)        // :145-146 (container re-typed from the now-typed lambda)
  RETURN false  // success                                                                    // :147
```
Side note on `storeInferredTypeParametersInFunctionExpression` (TI:68-106), invoked by FEP:530: skipped for QualifiedProperty (:71); for each type parameter of the function type, `value = ctx.getTypeParameterValue(name)`; non-null → copied into `fe.resolvedTypeParameters` (:80-84); null AND `ctx.getParent() == null` → THROW "The type parameter T was not resolved (<fn> / <FunctionType>)!" (:85-90); null with a parent → silently nothing. Every multiplicity parameter unresolved → THROW "The multiplicity parameter m was not resolved!" (:92-104) regardless of parent.

### A10. `TypeInferenceContext.register` (TIC:325-580), `registerMul` (TIC:261-313)

State: a stack of `TypeInferenceContextState`s (`states`; last = current), `parent`, `tops` (names of the owner Class/FunctionType's type parameters, registered terminal at construction, :63-84), `scope`.

```
registerMul(templateMul, valueMul, targetCtx):                                                // :261
  name = multiplicityParameter(templateMul); null -> nothing                                  // :265-266
  existing = last state[name]
  existing == null                                  -> put(name, valueMul, targetCtx)          // :271-275
  concrete(existing) && concrete(valueMul)          -> put(name, minSubsumingMultiplicity([valueMul, existing]))   // :276-281 (MERGE, never error)
  states.size() > 1 (collection element)            -> put(name, valueMul)                     // :282-287
  concrete(existing) (value non-concrete)           -> put(name, valueMul); forward = (targetCtx, valueMul, existing)   // :288-293
  concrete(valueMul) (existing non-concrete)        -> forward = (existing.targetCtx, existing, valueMul)   // :294-298
  else (both non-concrete)                          -> forward = (existing.targetCtx, existing, valueMul)   // :299-303
  observer; if forward != null && forward.ctx != this: forward.ctx.registerMul(forward.template, forward.value, targetCtx)   // :304-311

register(templateGT, genericType, targetCtx, merge=false)                                     // :325-328
register(templateGT, genericType, targetCtx, merge):                                          // :330
  genericType == null -> return                                                                // :336-340
  copy = copyGenericType(genericType, true)                                                    // :342
  IF copy is a GenericTypeOperation EQUAL && targetCtx.parent != null: parent.register(copy.left, copy.right, targetCtx.parent, merge)   // :344-350
  IF templateGT is EQUAL-operation: register(template.left, copy, targetCtx, merge)            // :352-357
  IF both are GenericTypeOperations of the same operation type: register(left,left); register(right,right)   // :359-370
  name = typeParameterName(templateGT)                                                          // :372
  IF name != null:                                                                              // :373
     existing = last state[name]; forwards = []
     existing == null -> put(name, copy, targetCtx)                                             // :379-383 (FIRST binding)
     concrete(existing) && concrete(copy):                                                       // :384
        both RelationTypes -> put(canConcatenate ? _RelationType.merge(existing, copy, isCovariant(templateGT)) : Any)   // :388-401
        else: if both are Function subtypes with equal type-arg counts: forward each (existing.targetCtx, existingArg, copyArg)   // :404-420
              merged = findBestCommonGenericType([existing, copy], isCovariant(templateGT)); put(name, merged)   // :423-424 (LUB by variance — NEVER an error)
              if existing.rawType == merged.rawType: for each type arg pair (existing non-concrete, replacement concrete): forward (existing.targetCtx, existingArg, replArg)   // :427-442
     states.size() > 1 -> put(name, copy)  (collection element state)                            // :445-450
     concrete(existing) (copy non-concrete) -> put(name, copy); forward (targetCtx, copy, existing)   // :451-456
     concrete(copy) (existing non-concrete):                                                      // :457
        existing is GenericTypeOperation -> put(name, copy)                                        // :459-462
        existing.targetCtx != this -> forward (existing.targetCtx, existing, copy)                 // :463-466
        !merge -> put(name, copy)   ("propagating the inference UP")                               // :467-471
        merge  -> NOTHING (commented-out LUB; existing non-concrete stays)                          // :472-480
     both non-concrete: existing.targetCtx != this -> forward (existing.targetCtx, existing, copy)  // :482-489
     observer; each forward: ctx.register(template, value, targetCtx, merge)                        // :490-494
  IF concrete(templateGT) && concrete(copy) && neither raw is Nil nor Any:                          // :497-505 (structural descent)
     both RelationTypes -> alignColumnSets(valueCols, templateCols); register(templateCol.type, valueCol.type) per aligned pair   // :506-516
     both FunctionTypes -> processFunctionType(targetCtx, merge=false, template, copy)              // :517-523 -> :582-604: if param counts equal: register/registerMul per param, then return type/mult (all COVARIANT, no flip)
     else class generics:                                                                            // :524
        templateRaw subTypeOf copyRaw -> templates = template's args homogenised UP to copy's raw; values = copy's own args   // :531-537
        elif either not fully concrete -> templates = template's own args; values = copy's args homogenised UP to template's raw   // :538-545
        else nothing                                                                                 // :546-552
        registerMul per mult arg; per type arg: both concrete FunctionTypes -> processFunctionType(…, merge) else register(template, value, targetCtx, merge)   // :554-576
```

### A11. `InstanceValueProcessor.updateInstanceValue` (IVP:112-132), `updateCompositeInstanceValue` (IVP:196-214)

Also read `process` (:56-95), `updateSingleInstanceValue` (:134-146), `getGenericType` (:148-185), `getMultiplicity` (:187-194).

```
process(iv):                                                                                  // :57
  values = iv.values with import stubs by-passed; isCollection = size > 1                       // :60-61
  for (child, i): if isCollection: ctx.addStateForCollectionElement()  (copy of the FIRST state pushed, TIC:101-104)   // :64-67
      ValueSpecification child -> usageContext InstanceValueSpecificationContext(offset i, iv)   // :68-74
      ImportStub child -> processImportStub; else if not a Class -> PostProcessor.processElement(child)   // :80-88
  if isCollection: TI.potentiallyUpdateParentTypeParamForInstanceValueWithManyElements(iv, ctx)  // :90-93 (TI:150-198: drop the n element states; for each non-concrete type param in the base state, LUB the element bindings by the parameter's variance and register in the PARENT if concrete; mults folded by minSubsumingMultiplicity and registered in the parent)
  updateInstanceValue(iv)                                                                       // :94

updateInstanceValue(iv): if !Measure.isUnitInstance(iv): updateInstanceValue(iv, bound=null)     // :112-118
updateInstanceValue(iv, bound):                                                                  // :120
  isExecutable = !isNonExecutableValueSpecification(iv)                                          // :122
  values = iv.values (resolved)
  size == 1 -> updateSingleInstanceValue(iv, isExecutable, values[0])                            // :124-127
  else       -> updateCompositeInstanceValue(iv, isExecutable, values, bound)                    // :128-131 (size 0 AND size >= 2)

updateSingleInstanceValue: if iv.GT == null: iv.GT := copy(getGenericType(value))                // :136-140
                           if iv.mult == null: iv.mult := copy(getMultiplicity(value))            // :141-145
getGenericType(value):                                                                            // :148
  executable && value is a ValueSpecification -> value.genericType                                // :150-153
  value is a Class with type or multiplicity parameters -> Class<X<Any|Nil per variance, mults ZeroMany>>   // :154-182 (contravariant param -> Nil, covariant -> Any :167-168; every mult param -> [*] :175)
  else Instance.extractGenericTypeFromInstance(value) = classifierGenericType or wrap(classifier)   // :184 (Instance.java:84-88)
getMultiplicity(value): executable && ValueSpecification -> value.multiplicity; else [1]           // :187-194

updateCompositeInstanceValue(iv, isExecutable, values, bound):                                    // :196
  if iv.GT == null:
     set = values.map(v -> gt = getGenericType(v); concrete(gt) ? gt : Any)                       // :200-204 (a non-concrete element counts as Any)
     iv.GT := findBestCommonCovariantNonFunctionTypeGenericType(set, bound, iv.src)               // :206 (GenericType.java:1420-1437: 0 -> Nil; 1 -> copy; n -> findBestCommonGenericType(set, bound, covariant=true, …))
  if iv.mult == null: iv.mult := newMultiplicity(values.size())  i.e. EXACTLY [n]                 // :209-213 (so [] is Nil[0]; [a,b] is [2] whatever a and b's multiplicities are)
```

### A12. `ImportStub.resolvePackageableElement` (IS:181-235); `C3Linearization.getGeneralizationLinearization` (C3:192-211) with `getLinearization`/`calculateLinearization`/`merge` (C3:47-151)

```
resolvePackageableElement(idOrPath, stub, repository):
  IF idOrPath in _Package.SPECIAL_TYPES (primitive names + "Package"): return repository.getTopLevel(idOrPath)   // :184-187
  importGroup = stub.importGroup                                                                    // :189
  IF idOrPath contains ':':  node = package_getByUserPath(idOrPath) (ABSOLUTE)                     // :191-194
      null -> THROW PureUnresolvedIdentifierException(stub.src, idOrPath, id=last segment, …)      // :195-199 (message "<idOrPath> has not been defined!" + optional " The system found N possible matches:\n    <path>…", PureUnresolvedIdentifierException.java:76-81)
      return node
  results = {}                                                                                      // :204
  for pkg in Imports.getImportGroupPackages(importGroup) (core ∪ section, ONE set): found = pkg.children[idOrPath]; add if non-null   // :205-212
  results.size():
     0 -> node = package_getByUserPath(idOrPath) (a Root-level element); null -> THROW PureUnresolvedIdentifierException(…, idOrPath, idOrPath, …)   // :216-225 ("important to do that last")
     1 -> results.any                                                                               // :226-229
     n -> THROW PureCompilationException(stub.src, "<idOrPath> has been found more than one time in the imports: [<path1>, <path2>…]" (paths sorted))   // :230-233
```
No own-package tier; no precedence between section imports and core imports; the Root fallback is only tried on zero hits.

```
getGeneralizationLinearization(type, typeSupport):                                                  // C3:192
  try: new C3Linearization(typeSupport).getLinearization(type)
  catch InconsistentGeneralizationHierarchyException e:
      path = e.getInconsistentTypePath(); root = path.last                                          // :200-201
      path.size()==1 -> THROW PureCompilationException(root.src, e.message)                         // :202-205  ("Inconsistent generalization hierarchy for <type>")
      else -> THROW PureCompilationException(root.src, e.message + "; root inconsistent class: " + root + "; path to root class: " + path)   // :206-209

getLinearization(type):                                                                              // :47
  type on the recursion stack -> Inconsistent(type)  (cycle)                                        // :49-52
  push; lin = typeSupport.getGeneralizations(type, this)  (= processorSupport.type_getTypeGeneralizations -> cached per Context, calls back valueOf -> calculateLinearization); pop   // :53-56

calculateLinearization(type):                                                                        // :67
  gens = typeSupport.getDirectGeneralizations(type)  (declaration order)                            // :69
  empty -> [type]                                                                                    // :70-73
  queues = [[type]] ++ [getLinearization(g) for g in gens] (Inconsistent from a parent -> rethrown as Inconsistent(type, cause)) ++ [gens]   // :76-91
  return merge(queues)  (C3LinearizationConflictException -> Inconsistent(type))                    // :92-100

merge(queues):                                                                                        // :104
  while queues non-empty:
     candidate = first queue head (in queue order) that appears in NO other queue at a position after that queue's head   // :110-128
     none -> C3LinearizationConflictException                                                          // :129-132
     result += candidate; pop it from every queue whose head equals it (identity for Type, TypeTypeSupport:45-48); drop emptied queues   // :133-148
  return result
```
Result: `[type, …, Any]`; distance in A7 = index in this list. With single inheritance it is the plain super chain; with multiple inheritance it is C3 order (own generalizations' linearizations first in declaration order, the direct-generalization list last as a tie-breaker).

---

## B. Discrepancies with `reference-matching.md`

Checked each numbered finding of `/Users/neemsandv/legend/legend-lite/.claude/worktrees/build-audit/docs/plan-audit-2026-09-26/reference-matching.md` against the lines above.

1. **Finding 5(b) — "equal keys grouped in a HashMap → hash order within a tie": mechanism misattributed.** Within a bucket the order is the bucket list's insertion order (FEM:73), which is the iteration order of `function_getFunctionsForName(name)` filtered at FEM:156 — a set. So the nondeterminism, if any, comes from the name index's set iteration order, not from the map of matches. Practical effect is the same (unspecified order among exact ties); the fix location differs.

2. **Finding 5 omits the unconditional-accept path.** FEP:200-203: when `mr.functionName == null` the FIRST candidate is accepted with no `getBestFunctionMatch` call and regardless of `lambdaOK/columnOK`. That path covers: a pre-resolved `func` (FEP:283-286), a simple property (:332), a relation column (:309) and — importantly — ALL qualified-property overloads (:369): they are ordered by `getFunctionMatches(lenient=true)` (:1087) and the first is taken. So finding 11's "Qualified-property overloads go through getFunctionMatches(lenient=true)" is true but incomplete: there is never a strict re-rank and never a "Too many matches" for qualified properties; a lenient tie is resolved by source order.

3. **Finding 4 — "If every candidate's lambda/column inference failed" is too narrow.** FEP:204-207 sets `someInferenceFailed` when ANY candidate's inference failed; FEP:258 suppresses the no-match error whenever that flag is set. So: candidate A fails lambda inference, candidate B infers fine but is not strict-best → no error, and the expression is left with `func = B` and B's SUCCESSFULLY inferred return type (FEP:545-550), while its arguments have been unbound by the retry cleanup (:223-227). The report's "keeps the last tried func, gets the raw signature return type" only describes the case where the last candidate itself failed.

4. **Finding 5/15 omit that untyped-lambda typing can throw, not just fail.** `TI:114-117` throws "Can't infer the parameters' types for the lambda. Please specify it in the signature." when the candidate's corresponding parameter is not a concrete `Function<…>` (e.g. `T[1]`, `Any[1]`), and `TI:125-128` throws when the template FunctionType is Any/Nil. Nothing in FEP:131-228 catches it, so this is a compile error thrown by the first candidate reached whose parameter at that position is not Function-typed — not a "retry later". The report presents lambda-typing failure as always recoverable.

5. **Finding 5 omits the short-circuit.** FEP:629 `lambdaOK && !process…` and the threading of `lambdaOK` through FEP:170: after the first lambda that cannot be typed under a candidate, no further lambda (same InstanceValue or later parameter) is typed for that candidate.

6. **Finding 15 — "first binding wins; a second concrete binding is MERGED" is right for concrete×concrete but wrong in `merge` mode for non-concrete existing.** TIC:467-480: when the existing value is non-concrete, the new one concrete, same target ctx and `merge == true` (the FEP:591 path, all args inferred), NOTHING is recorded — the concrete value is dropped, not merged, not "first wins" in the sense of erroring. Also `registerMul` with two concrete values takes `minSubsumingMultiplicity` (TIC:276-281) — the report has this right.

7. **Finding 11 — automap trigger is stated as "NOT to-one receiver"; precise rule is `isToOne(m, strict=false)`** (Multiplicity.java:78-83): concrete AND upper == 1. So `[0..1]` is to-one (no automap), `[0]` is NOT, and a receiver whose multiplicity is a multiplicity PARAMETER `m` is NOT to-one → automap. The report's `relaxed=true` gloss hides the parameter case.

8. **Finding 9 — TypeMatch order line ranges are right but the report does not say the Nil check precedes the FunctionType branch** (TM:394-397 before :399): a Nil-typed value against a `Function<…>` target is BOTTOM (a match), not "not a function type → null".

9. **Finding 13 — collection multiplicity claim is right but incomplete:** `updateCompositeInstanceValue` handles size 0 too (IVP:128-131) and the `[n]` multiplicity is `values.size()` irrespective of element multiplicities (IVP:211-212); a non-concrete element type becomes `Any` before the LUB (IVP:203).

10. **Finding 8 — "type args skipped when target raw is Any or value raw is Nil"** is the covariant reading; GTM:236 flips both roles under contravariance (`isBottom(targetRaw)` / `isTop(valueRaw)`). Matters inside FunctionType parameter positions (TM:510).

11. **Finding 1(c) "two overloads … tie → Too many matches, never nearest import wins"** — correct, but only on the strict re-rank path (item 2 above): for qualified properties there is no such error.

12. **Not in the report at all:** (a) `storeInferredTypeParametersInFunctionExpression` throws "The type parameter T was not resolved" ONLY when `ctx.getParent() == null` (TI:85-90) and always throws for an unresolved multiplicity parameter (TI:100-103); (b) the `let` variable is registered in the PARENT variable context (FEP:250 `getParent()`); (c) the retry cleanup runs only when `foundFunctions.size() > 1` (FEP:223) — a single-candidate failure leaves the arguments bound; (d) the accept test at FEP:210 evaluates `getBestFunctionMatch` over ALL candidates against arguments typed under the CURRENT candidate, so "Too many matches" can be raised by the first successfully-inferred candidate even when the tying candidate would have typed the lambda differently; (e) `getFunctionMatches` wraps any RuntimeException from matching into "Error finding match for function '<name>': …" (FEM:78-87); (f) `Imports.getImportGroupPackages` drops unknown import paths silently (`without(null)`, Imports.java:64) — an `import a::b::*;` of a non-existent package is not an error at this point.

Everything else in findings 1, 2, 3, 6, 7, 10, 12, 14, 17, 18, 20 is supported by the lines cited above (lines re-verified: FEM:159-170; Imports.java:52-65; m3.pure:175-211; IS:181-235; FM:114-157; FM:69-103; GTM:80-114, 236-239; MM:187-301).

---

## C. Traps for an implementer

1. **T-parameter placement (target side).** A parameter typed `T` is ALWAYS `NON_CONCRETE` (GTM:171-177, target behaviour MATCH_ANYTHING at FM:140), which ranks BELOW every `Simple(n)` including `Any` at distance n (TM:138-149 vs :55-68). `f(Any[1])` beats `f(T[1])` for any concrete argument.
2. **T-parameter placement (value side, strict).** A `T`-typed ARGUMENT in strict mode (FM:132 MATCH_CAUTIOUSLY) matches only an `Any` target covariantly (GTM:214-218); leniently it is NON_CONCRETE against anything (GTM:210-213). Against a `T` target it is NON_CONCRETE in both modes because the target check comes first (GTM:171).
3. **Nil / bottom.** `[]` is `Nil[0]` (IVP:206 → GenericType.java:1426; IVP:212). Against a concrete target Nil scores `BOTTOM` (TM:394-397), which ranks BELOW `NON_CONCRETE` (TM:88-94) — so `f(T[*])` beats `f(String[*])` for `[]`. Type arguments are not compared when the value is Nil (GTM:236). Nil is checked BEFORE the FunctionType branch (TM:394 vs :399).
4. **Any / top.** Type arguments are ignored when the target raw type is Any (GTM:236-239); `List<String>` vs `Any` is just `Simple(distance)`. A FunctionType value vs `Any` target is `Simple(1)` (TM:413-416) — `Function<…>` targets score `Simple(1)` for a LambdaFunction too (C3 index of Function in LambdaFunction's linearization) PLUS type-argument matches, and the shorter-list rule (GTM:113) makes the arg-less Any match lose only on the raw distance.
5. **`[1..*]` vs `[*]`.** `[1..*]` REJECTS a `[*]` argument (lowerDist = 0−1 < 0, MM:275-279). For a `[3]` literal, `[1..*]` = (2, MAX) and `[*]` = (3, MAX): equal MAX uppers, lower tiebreak → `[1..*]` wins (MM:124-125).
6. **MAX upper arithmetic.** Bounded argument vs `*` parameter → upperDist = `Integer.MAX_VALUE` (MM:286); `*` argument vs bounded parameter → no match (MM:288-291); `*` vs `*` → 0 (MM:286). A non-concrete argument multiplicity vs `[*]` → `Simple(MAX, MAX)` (MM:242-245), which loses to every concrete match.
7. **Multiplicity identity.** EXACT at MM:187 is Java identity; two structurally equal but distinct multiplicity instances still get EXACT via (0,0) at MM:301 — fine, but only after both bounds are read, so do not shortcut on identity alone.
8. **Ordering inside GenericTypeMatch.** raw type first, then type args lexicographically, then LENGTH (GTM:101-114): EXACT/NON_CONCRETE/NULL carry empty arg lists and win a raw-type tie against any populated list.
9. **FunctionMatch ordering.** All parameter TYPES left to right, then all MULTIPLICITIES left to right (FM:83-100). An exact multiplicity never compensates for a worse type at any position.
10. **Relation vs Function TypeMatch is not antisymmetric** (TM:216-218 and :300-302 both return −1); a sort with such keys is order-dependent. Do not assume a total order.
11. **Lenient vs strict.** Lenient (FEP:387) turns null argument types/mults (untyped lambda params, empty column types) into NULL matches (ranked last) and T-typed args into NON_CONCRETE; strict (FEP:210) rejects nulls (FM:131) — an untyped lambda can NEVER strict-match, which is why the loop types it under the candidate first (TI:119-146).
12. **The accept rule is "first candidate, in lenient order, that is strict-best over ALL candidates once its own lambda typing is applied"** (FEP:131-228). Inference-failed candidates are skipped WITHOUT the strict test (:204-207); pre-resolved/property/qualified-property candidates skip the strict test entirely (:200-203).
13. **"Too many matches" is raised inside the loop** (FEP:210 → FEM:140-148) by the first candidate whose inference succeeded, comparing all candidates against the arguments as typed under it.
14. **Retry cleanup is conditional** (FEP:223): with exactly one candidate nothing is unbound. With >1 the whole first pass re-runs (:226) — so first-pass side effects (usage contexts, import stub resolution) must be idempotent.
15. **Silent survival.** If any candidate failed inference and none was accepted, NO error is thrown (FEP:258); the expression keeps the last candidate's `func` and either its raw return type (:556-562) or its inferred one (:545-550). Later reverse inference from the parent (`handleParameter` :596-616 / :491-523) is expected to reprocess it; if it never does, the bad binding reaches validation.
16. **Lambda typing throws, not fails, when the parameter is not `Function<…>`** (TI:114-117, :125-128) — order candidates so a Function-typed overload precedes an `Any`/`T`-typed one, or expect a compile error the reference produces.
17. **Lambda parameter type is looked up in the TOP context** as a fallback (TIC:150-155, :617-625): an unbound `T` that is a type parameter of the ENCLOSING function is "resolved" (terminal) and the lambda param gets type `T` of the enclosing function. Lambda return types are made concrete with the TOP context's maps (FEP:641-646), not the current one.
18. **Lambda return registration happens even when parameter typing failed** (FEP:632 "in any case") and is a no-op only because `register(…, null)` returns early (TIC:336-340); the lambda's FunctionType.returnType is overwritten with null (FEP:647-648).
19. **Short-circuit across lambdas** (FEP:629 `&&`, :170): after one failure no further lambda is typed in that candidate pass.
20. **`handleTypeArgumentTypeInference` does not descend into Relation/Function templates** (FEP:681-701, TODO at :700); `TypeInferenceContext.register` DOES descend into them (TIC:506-523, :562-568) — the two paths bind different things for `Function<{T[1]->Relation<X>[1]}>`-shaped returns.
21. **Merge mode drops a concrete value over a non-concrete existing binding** (TIC:467-480) — `if<T|m>` is the cited case; do not "fix" it by LUB-ing or your results will diverge from the reference.
22. **Return type must be concrete or an enclosing type parameter** (FEP:536-542, `isTop` TIC:127-130); otherwise "The system is not capable of inferring the return type (…) of the function '…'. Check your signatures!". Type params unresolved → error only in a root context (TI:85-90); multiplicity params unresolved → always an error (TI:100-103).
23. **Automap.** Trigger is `!isToOne(receiverMult, strict=false)` = NOT (concrete AND upper==1) (Multiplicity.java:78-83): `[0..1]` does NOT automap; `[0]`, `[2]`, `[*]`, `[1..*]` and a multiplicity PARAMETER do. The rewrite (FEP:1108-1174) produces `map($src, {v_automap | $v_automap.p(args…)})` with the lambda param typed as a copy of the receiver's generic type at `[1]`, the lambda NAMED after the property-name instance, no return type, the SAME propertyName InstanceValue reused in the body, and then a bare repository search for `map` (:384-388). The typed tree therefore contains a `map` the source never spelled; the receiver's `[0..1]` keeps the plain property call.
24. **Enum value extraction.** `$Enum.VALUE` is recognised by `subTypeOf(receiverGT, Enumeration)` on the RAW type only (FEP:299, GenericType.java:396-399), rewritten to `extractEnumValue(<enum>, 'VALUE')` with the name appended as a processed String literal at parameter index 1 (FEP:1096-1106) and then matched from the repository (:384-388) — `extractEnumValue_Enumeration_1__String_1__T_1_`. Reference usages are added only for that exact name (FEP:786-794).
25. **Milestoning injection.** A generated milestoned property/qualified property with missing dates is swapped for its dated variant and the date arguments are APPENDED to the call from the propagated context (FEP:328-331, :362-366; MilestoningDatesPropagationFunctions.java:147-157); when no dates are in scope the original stays and the later property lookup produces the "is milestoned with stereotypes … and requires date parameters" text (FEP:986-1005). For simple properties this happens only on the to-one path; on the qualified path only when exactly one qualified property was found (:362).
26. **Single-argument qualified property via `$x.q`** (no parentheses): resolved as a property only if a qualified property with `typeVariables.size()+1` parameters exists (FEP:975, :1045-1057); otherwise the size-dependent errors at :979-1019.
27. **Qualified property matching uses a synthetic receiver** typed with the receiver's generic type at `[1]` (FEP:1080-1082) and prepends the receiver's `typeVariableValues` before the explicit arguments (:1084); overloads are ordered leniently and the first is taken (item B.2).
28. **Function candidate scope.** Qualified call → exactly that absolute package, unknown → zero candidates → "can't find a match" (FEM:161-164). Bare call → core imports ∪ section imports ∪ Root as a SET (FEM:167-168, Imports.java:52-65); the call's own package is NOT searched unless imported; Root-level functions are visible everywhere; missing import paths are dropped silently.
29. **Element scope.** SPECIAL_TYPES (primitives + Package) first (IS:184-187); qualified → absolute (IS:191-201); bare → all import packages as ONE set (IS:204-212) with `>1 hits` → "<id> has been found more than one time in the imports: [a, b]" (IS:230-233), `0 hits` → Root-level element or "<id> has not been defined!" (IS:216-225). Never own-package, never section-before-core.
30. **`let` is a function** (`letFunction_String_1__T_m__T_m_`) whose variable is registered in the PARENT variable context after acceptance (FEP:246-256), so the binding is visible to siblings, not to the let's own arguments.
31. **C3 distance, not "steps up".** Distance is the index in the C3 linearization of the VALUE type (TM:418, C3:104-151); under multiple inheritance the linearization interleaves parents' chains (own parents' linearizations first in declaration order, then the direct-parent list as tiebreaker). Cycles and merge conflicts throw "Inconsistent generalization hierarchy for X" with the path to the root (C3:198-210).
32. **`function_getFunctionType` vs `classifierGenericType`.** FEP reads parameters for inference from `foundFunction.classifierGenericType.typeArguments[0].rawType` (FEP:161, :620, :914-924) but from `function_getFunctionType` for registration (:142) — they are the same FunctionType instance for concrete/native functions but the code paths differ; keep one source of truth or make the two agree.
33. **First-pass side effects on arguments.** Every argument gets `usageContext = ParameterValueSpecificationContext(offset, fe)` (FEP:926-934) and each element of a collection literal gets `InstanceValueSpecificationContext(offset, iv)` (IVP:70-73); milestoning date contexts wrap each argument (FEP:806); import stubs in argument GT/mult are resolved before the first pass (FEP:270-275).
34. **Collection literal binding.** Each element of a `[a,b]` gets its own inference state (IVP:64-67, TIC:101-104); `T` bindings are LUB-ed across elements by the parameter's variance and pushed to the PARENT context only if the LUB is concrete (TI:161-177, :195); the literal's multiplicity is `[n]` (IVP:212) regardless of element multiplicities; a Class literal with type parameters is `Class<X<Any|Nil>>` with `[*]` mult args (IVP:154-182).
35. **Column-spec magic.** `funcColSpec*`/`aggColSpec*`/`*Array` names get post-match column-type fix-ups that write the lambda's return type into the column and register the relation type (FEP:394-489) — keyed by exact function names (:396, :414, :447, :470).
