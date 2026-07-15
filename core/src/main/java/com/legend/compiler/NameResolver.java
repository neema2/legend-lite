package com.legend.compiler;

import com.legend.builtin.Pure;
import com.legend.model.ImportScope;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationDefinition.AssociationEndDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassDefinition.ConstraintDefinition;
import com.legend.model.ClassDefinition.DerivedPropertyDefinition;
import com.legend.model.ClassDefinition.ParameterDefinition;
import com.legend.model.ClassDefinition.PropertyDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.ConnectionDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.DatabaseDefinition.FilterDefinition;
import com.legend.model.DatabaseDefinition.JoinDefinition;
import com.legend.model.DatabaseDefinition.SchemaDefinition;
import com.legend.model.DatabaseDefinition.ViewDefinition;
import com.legend.model.DatabaseDefinition.ViewDefinition.ViewColumnMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.Realization;
import com.legend.model.MappingInclude;
import com.legend.model.MappingInclude.StoreSubstitution;
import com.legend.model.LegacyMappingDefinition.TableReference;
import com.legend.model.EnumDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.EnumerationMapping.EnumValueMapping;
import com.legend.model.EnumerationMapping.SourceValue;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ProfileDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.model.StereotypeApplication;
import com.legend.model.TaggedValue;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CDate;
import com.legend.model.spec.CDecimal;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CLatestDate;
import com.legend.model.spec.CString;
import com.legend.model.spec.CTime;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Name-resolution pass: rewrite simple names to fully-qualified names
 * using an {@link ImportScope} and the universe of known FQNs.
 *
 * <h2>Pipeline position (AGENTS.md)</h2>
 *
 * <pre>
 * ElementParser / SpecParser  &rarr;  <strong>NameResolver</strong>  &rarr;  PureModelBuilder / TypeChecker
 * </pre>
 *
 * <h2>Contract</h2>
 *
 * <ul>
 *   <li><strong>Owns:</strong> {@code imports} &rarr; FQN as a pure
 *       data {@code AST} &rarr; {@code AST} transform.</li>
 *   <li><strong>Forbidden:</strong> consulting the model; type
 *       checking; primitive/platform-class promotion. Classification
 *       of {@code NameRef} into {@code ClassType}/{@code LClass}/
 *       {@code Primitive} is {@code PureModelBuilder}'s job.</li>
 * </ul>
 *
 * <h2>Resolution rule</h2>
 *
 * <ol>
 *   <li>Already qualified (contains {@code ::}) &rarr; pass through.</li>
 *   <li>Specific import match &rarr; use that FQN.</li>
 *   <li>Wildcard imports: collect candidates {@code pkg::name} that
 *       exist in {@code knownFqns}.
 *       0 matches &rarr; pass through (likely a primitive). 1 match
 *       &rarr; use it. &gt;1 matches &rarr; {@link IllegalStateException}.</li>
 * </ol>
 *
 * <h2>Coverage</h2>
 *
 * <p>Covers every element and nested record the current
 * {@link ElementParser} / {@link SpecParser} emits:
 * Class/Association/Function/NativeFunction/Mapping/Database/Runtime/
 * Service/Connection/Enum/Profile; all
 * {@link PropertyMapping} variants, all
 * {@link RelationalOperation} variants, all
 * {@link ValueSpecification} variants, the full
 * {@link TypeExpression} grammar, stereotype/tagged-value profile
 * references, and mapping {@link FilterMapping} +
 * {@link JoinChainElement} chains.
 *
 * <h2>Reference-equality discipline</h2>
 *
 * <p>Every helper returns the input instance ({@code ==}) when
 * nothing changed, allowing skipped allocations to bubble through
 * outer wrappers. A fully-resolved input graph round-trips through
 * {@code resolve(...)} without allocating new records.
 */
public final class NameResolver {

    private NameResolver() {}

    // =================================================================
    // Public entry points
    // =================================================================

    /**
     * Resolve {@code parsed} against the user's imports <em>plus the platform
     * prelude</em>. This is the frontend entry point.
     *
     * <p>The prelude is the bootstrap import set (every {@link Pure} native
     * class/enum) that is <strong>always in scope</strong> &mdash; the same way
     * {@code java.lang} is auto-imported in Java, {@code scala.Predef} in Scala,
     * or {@code std::prelude} is injected into every Rust module. A bare
     * {@code String} therefore resolves to
     * {@code meta::pure::metamodel::type::String} with no user import.
     *
     * <p>The prelude lives <strong>here</strong>, in the name-resolution layer
     * that legitimately knows the builtin catalog &mdash; not in the pipeline
     * driver. Knowing the fixed platform catalog is not "consulting the model"
     * (AGENTS.md): the model is the user's compiled elements; the prelude is
     * bootstrap data.
     */
    public static ParsedModel resolve(ParsedModel parsed) {
        Objects.requireNonNull(parsed, "parsed");
        java.util.Map<String, ImportScope> perElement = new java.util.HashMap<>();
        parsed.elementImports().forEach((fqn, sc) -> perElement.put(fqn, withPrelude(sc)));
        ParsedModel scoped = new ParsedModel(parsed.elements(), withPrelude(parsed.imports()),
                parsed.source(), parsed.elementOffsets(), perElement);
        return resolve(scoped, knownFqns(parsed.elements()));
    }

    /**
     * Resolve every simple name reference in {@code model} to its FQN
     * using {@code model.imports()} and {@code knownFqns}. Lower-level entry:
     * the caller supplies the full import scope and FQN universe (the
     * prelude-aware {@link #resolve(ParsedModel)} is the usual entry).
     */
    public static ParsedModel resolve(ParsedModel model, Set<String> knownFqns) {
        Scope globalScope = Scope.of(model.imports(), knownFqns);
        // SECTION-scoped resolution (real pure): each element resolves in
        // ITS OWN section's imports when recorded; the union scope is the
        // fallback (single-source models, synthesized elements).
        List<PackageableElement> resolved = new ArrayList<>(model.elements().size());
        boolean changed = false;
        for (PackageableElement el : model.elements()) {
            ImportScope own = model.elementImports().get(el.qualifiedName());
            Scope scope = own == null ? globalScope : Scope.of(own, knownFqns);
            PackageableElement r = resolveElement(el, scope);
            resolved.add(r);
            changed |= r != el;
        }
        return !changed ? model
                : new ParsedModel(resolved, model.imports(),
                        model.source(), model.elementOffsets(), model.elementImports());
    }

    /** User imports merged with the always-in-scope platform prelude. */
    private static ImportScope withPrelude(ImportScope user) {
        ImportScope.Builder b = new ImportScope.Builder();
        for (String wildcard : user.wildcards()) {
            b.add(wildcard + "::*");
        }
        for (String fqn : user.typeImports().values()) {
            b.add(fqn);
        }
        for (String fqn : Pure.nativeClassFqns()) {
            b.add(fqn);
        }
        for (String fqn : Pure.nativeEnumFqns()) {
            b.add(fqn);
        }
        return b.build();
    }

    /** Declared element FQNs + platform FQNs: the wildcard-disambiguation universe. */
    private static Set<String> knownFqns(List<PackageableElement> elements) {
        Set<String> known = new HashSet<>();
        for (PackageableElement el : elements) {
            known.add(el.qualifiedName());
        }
        known.addAll(Pure.nativeClassFqns());
        known.addAll(Pure.nativeEnumFqns());
        return known;
    }

    private static PackageableElement resolveElement(
            PackageableElement element, Scope scope) {
        return switch (element) {
            case com.legend.model.PrimitiveExtensionDefinition pe -> {
                // base primitives (String, Integer...) pass through resolveName
                // unchanged; an extension-of-extension base resolves via imports
                String base = resolveName(pe.baseTypeName(), scope);
                yield base.equals(pe.baseTypeName()) ? pe
                        : new com.legend.model.PrimitiveExtensionDefinition(
                                pe.qualifiedName(), base);
            }
            case ClassDefinition cd -> resolveClass(cd, scope);
            case AssociationDefinition ad -> resolveAssociation(ad, scope);
            case FunctionDefinition fd -> resolveFunction(fd, scope);
            case NativeFunctionDefinition nfd -> resolveNativeFunction(nfd, scope);
            case LegacyMappingDefinition md -> resolveMapping(md, scope);
            case DatabaseDefinition db -> resolveDatabase(db, scope);
            case RuntimeDefinition rd -> resolveRuntime(rd, scope);
            case ServiceDefinition sd -> resolveService(sd, scope);
            case ConnectionDefinition cd -> resolveConnection(cd, scope);
            // Explicit pass-through so adding a new PackageableElement variant
            // surfaces as an unhandled-case compile error rather than silently
            // skipping resolution.
            case EnumDefinition ed -> ed;
            case ProfileDefinition pd -> pd;
            // The canonical (binding-table) MappingDefinition is produced
            // directly by Door 1 (clean-sheet text), so it reaches the resolver
            // and its binding FQNs must be resolved like any other element's.
            case MappingDefinition md -> resolveCanonicalMapping(md, scope);
        };
    }

    /**
     * Resolve a query / expression AST in isolation. Callers that
     * parse a single expression rather than a whole element go
     * through this overload, constructing a {@link Scope} via
     * {@link Scope#of(ImportScope, Set)}.
     */
    public static ValueSpecification resolve(
            ValueSpecification vs, Scope scope) {
        return resolveVs(vs, scope);
    }

    /**
     * Resolve a <strong>standalone query</strong> expression &mdash; real
     * legend-engine's SECTIONLESS-lambda scope ({@code CompileContext.META_IMPORTS}):
     * the platform prelude is always in scope (&ldquo;system elements will always
     * be resolved no matter what&rdquo;), so {@code JoinKind.INNER} or
     * {@code SortDirection.DESC} resolve bare; user elements require full paths,
     * exactly like an ad-hoc lambda with no import-bearing section. An unresolved
     * bare name passes through and fails loudly in Phase G.
     */
    public static ValueSpecification resolveQuery(ValueSpecification query) {
        return resolveVs(query, QUERY_SCOPE);
    }

    /**
     * Resolve a query under a SECTION import scope — the real-pure shape
     * for a query written inside an import-bearing section (a test file,
     * a notebook cell): the section's imports plus the prelude, with the
     * MODEL's element universe as the wildcard-candidate set. An
     * unresolved bare name passes through and fails loudly in Phase G.
     */
    public static ValueSpecification resolveQuery(ValueSpecification query,
            ImportScope imports, Set<String> modelFqns) {
        Set<String> known = new HashSet<>(Pure.nativeClassFqns());
        known.addAll(Pure.nativeEnumFqns());
        known.addAll(modelFqns);
        return resolveVs(query, Scope.of(withPrelude(imports), Set.copyOf(known)));
    }

    /** The sectionless-query scope: prelude imports only; the native FQN universe. */
    private static final Scope QUERY_SCOPE = querycope();

    private static Scope querycope() {
        Set<String> known = new HashSet<>(Pure.nativeClassFqns());
        known.addAll(Pure.nativeEnumFqns());
        return Scope.of(withPrelude(new ImportScope.Builder().build()), Set.copyOf(known));
    }

    private static TypeExpression resolveType(
            TypeExpression t, Scope scope) {
        if (t == null) return null;
        return switch (t) {
            case TypeExpression.NameRef nr -> {
                String r = resolveName(nr.name(), scope);
                yield r.equals(nr.name()) ? nr : new TypeExpression.NameRef(r);
            }
            case TypeExpression.Generic g -> {
                String r = resolveName(g.name(), scope);
                List<TypeExpression> args = resolveTypeList(g.arguments(), scope);
                yield (r.equals(g.name()) && args == g.arguments()) ? g
                        : new TypeExpression.Generic(r, args);
            }
            case TypeExpression.FunctionType ft -> {
                List<TypeExpression.TypedParameter> params = resolveList(
                        ft.parameters(), NameResolver::resolveTypedParameter, scope);
                TypeExpression.TypedParameter result = resolveTypedParameter(ft.result(), scope);
                yield (params == ft.parameters() && result == ft.result()) ? ft
                        : new TypeExpression.FunctionType(params, result);
            }
            case TypeExpression.RelationType rt -> {
                List<TypeExpression.Column> cols = resolveList(
                        rt.columns(), NameResolver::resolveColumn, scope);
                yield cols == rt.columns() ? rt : new TypeExpression.RelationType(cols);
            }
            case TypeExpression.SchemaAlgebra sa -> {
                TypeExpression l = resolveType(sa.left(), scope);
                TypeExpression r = resolveType(sa.right(), scope);
                yield (l == sa.left() && r == sa.right()) ? sa
                        : new TypeExpression.SchemaAlgebra(l, sa.op(), r);
            }
        };
    }

    private static TypeExpression.TypedParameter resolveTypedParameter(
            TypeExpression.TypedParameter p, Scope scope) {
        TypeExpression t = resolveType(p.type(), scope);
        return t == p.type() ? p
                : new TypeExpression.TypedParameter(t, p.multiplicity());
    }

    private static TypeExpression.Column resolveColumn(
            TypeExpression.Column c, Scope scope) {
        TypeExpression t = resolveType(c.type(), scope);
        return t == c.type() ? c
                : new TypeExpression.Column(c.name(), t, c.multiplicity());
    }

    /**
     * Platform-qualified native calls normalize to the catalog's BARE name at
     * RESOLUTION time (real Pure writes {@code meta::pure::functions::date::
     * adjust(...)}; our catalog registers natives bare — an engine-lite
     * convention). This is the INVERSE of the eventual fix (an FQN-keyed
     * catalog with prelude function imports, LEGEND_SQL_VISION-adjacent);
     * until then, ONE normalization here keeps every later stage a dumb
     * lookup. Only {@code meta::pure::}-prefixed names with a registered bare
     * native normalize — user FQNs are untouched.
     */
    private static String normalizePlatformFunction(String fn) {
        // FQN-keyed catalog era (FQN_MIGRATION step 1c): both spellings
        // resolve DIRECTLY against the catalog (FQN via the primary index,
        // bare via the bare-name union index) — the old blind prefix-strip
        // silently CAPTURED user functions whose last segment collided with
        // a native (meta::pure::custom::map -> native map). A non-catalog
        // platform FQN now resolves (or fails loudly) as a user function.
        return fn;
    }

    /** Core lookup. Private; callers go through {@link #resolveType} etc. */
    private static String resolveName(String name, Scope scope) {
        if (name == null || name.isEmpty()) return name;
        // Type-parameter shadowing: a NameRef matching an in-scope type
        // parameter (e.g. T inside Class Foo<T>) is a parameter
        // reference, not a Pure FQN. Skip import resolution.
        if (scope.typeParams().contains(name)) return name;
        if (name.contains("::")) return name;
        Map<String, String> typeImports = scope.imports().typeImports();
        if (typeImports.containsKey(name)) return typeImports.get(name);
        List<String> matches = new ArrayList<>(0);
        for (String pkg : scope.imports().wildcards()) {
            String candidate = pkg + "::" + name;
            if (scope.knownFqns().contains(candidate)) matches.add(candidate);
        }
        if (matches.size() == 1) return matches.get(0);
        if (matches.size() > 1) {
            throw new com.legend.error.ResolutionException(
                    "ambiguous reference '" + name + "' \u2014 matches via imports: "
                    + matches + ". Use a fully qualified name.");
        }
        return name;
    }

    // =================================================================
    // Class / Property / Association / Function
    // =================================================================

    private static ClassDefinition resolveClass(
            ClassDefinition cd, Scope outer) {
        // Class type parameters (e.g. `Class Foo<T,U>`) shadow imports
        // inside the body. Push them onto the scope before resolving
        // anything that could mention them.
        Scope scope = outer.withTypeParams(cd.typeParams());
        List<TypeExpression> superClasses = resolveTypeList(cd.superClasses(), scope);
        List<PropertyDefinition> properties = resolvePropertyList(cd.properties(), scope);
        List<DerivedPropertyDefinition> derived = resolveDerivedList(cd.derivedProperties(), scope);
        List<ConstraintDefinition> constraints = resolveConstraintList(cd.constraints(), scope);
        List<StereotypeApplication> stereotypes = resolveStereotypes(cd.stereotypes(), scope);
        List<TaggedValue> taggedValues = resolveTaggedValues(cd.taggedValues(), scope);
        if (superClasses == cd.superClasses() && properties == cd.properties()
                && derived == cd.derivedProperties() && constraints == cd.constraints()
                && stereotypes == cd.stereotypes() && taggedValues == cd.taggedValues()) {
            return cd;
        }
        return new ClassDefinition(cd.qualifiedName(), cd.typeParams(),
                superClasses, properties, derived, constraints,
                stereotypes, taggedValues, cd.isNative());
    }

    private static PropertyDefinition resolveProperty(
            PropertyDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        List<StereotypeApplication> stereotypes = resolveStereotypes(p.stereotypes(), scope);
        List<TaggedValue> taggedValues = resolveTaggedValues(p.taggedValues(), scope);
        if (type == p.type() && stereotypes == p.stereotypes() && taggedValues == p.taggedValues()) {
            return p;
        }
        return new PropertyDefinition(p.name(), type, p.multiplicity(), stereotypes, taggedValues);
    }

    private static DerivedPropertyDefinition resolveDerivedProperty(
            DerivedPropertyDefinition dp, Scope scope) {
        List<ParameterDefinition> params = resolveParameterList(dp.parameters(), scope);
        Realization realization = resolveRealization(dp.realization(), scope);
        TypeExpression type = resolveType(dp.type(), scope);
        if (params == dp.parameters() && realization == dp.realization() && type == dp.type()) {
            return dp;
        }
        return new DerivedPropertyDefinition(dp.name(), params, realization, type, dp.multiplicity());
    }

    private static ParameterDefinition resolveParameter(
            ParameterDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        return type == p.type() ? p
                : new ParameterDefinition(p.name(), type, p.multiplicity());
    }

    private static ConstraintDefinition resolveConstraint(
            ConstraintDefinition c, Scope scope) {
        Realization realization = resolveRealization(c.realization(), scope);
        return realization == c.realization() ? c : new ConstraintDefinition(c.name(), realization);
    }

    private static AssociationDefinition resolveAssociation(
            AssociationDefinition ad, Scope scope) {
        AssociationEndDefinition p1 = resolveAssocEnd(ad.property1(), scope);
        AssociationEndDefinition p2 = resolveAssocEnd(ad.property2(), scope);
        List<DerivedPropertyDefinition> derived =
                resolveDerivedList(ad.derivedProperties(), scope);
        if (p1 == ad.property1() && p2 == ad.property2()
                && derived == ad.derivedProperties()) {
            return ad;
        }
        return new AssociationDefinition(ad.qualifiedName(), p1, p2, derived);
    }

    private static AssociationEndDefinition resolveAssocEnd(
            AssociationEndDefinition end, Scope scope) {
        TypeExpression target = resolveType(end.targetClass(), scope);
        return target == end.targetClass() ? end
                : new AssociationEndDefinition(end.propertyName(), target, end.multiplicity());
    }

    private static FunctionDefinition resolveFunction(
            FunctionDefinition fd, Scope outer) {
        // Function type parameters shadow imports throughout the
        // signature and body.
        Scope scope = outer.withTypeParams(fd.typeParameters());
        List<FunctionDefinition.ParameterDefinition> params =
                resolveFunctionParams(fd.parameters(), scope);
        TypeExpression returnType = resolveType(fd.returnType(), scope);
        List<ValueSpecification> body = resolveVsList(fd.body(), scope);
        List<StereotypeApplication> stereotypes = resolveStereotypes(fd.stereotypes(), scope);
        List<TaggedValue> taggedValues = resolveTaggedValues(fd.taggedValues(), scope);
        if (params == fd.parameters() && returnType == fd.returnType() && body == fd.body()
                && stereotypes == fd.stereotypes() && taggedValues == fd.taggedValues()) {
            return fd;
        }
        return new FunctionDefinition(fd.qualifiedName(), fd.typeParameters(),
                fd.multiplicityParameters(), params, returnType, fd.returnMultiplicity(),
                body, stereotypes, taggedValues);
    }

    private static NativeFunctionDefinition resolveNativeFunction(
            NativeFunctionDefinition nfd, Scope outer) {
        Scope scope = outer.withTypeParams(nfd.typeParameters());
        List<FunctionDefinition.ParameterDefinition> params =
                resolveFunctionParams(nfd.parameters(), scope);
        TypeExpression returnType = resolveType(nfd.returnType(), scope);
        List<StereotypeApplication> stereotypes = resolveStereotypes(nfd.stereotypes(), scope);
        List<TaggedValue> taggedValues = resolveTaggedValues(nfd.taggedValues(), scope);
        if (params == nfd.parameters() && returnType == nfd.returnType()
                && stereotypes == nfd.stereotypes() && taggedValues == nfd.taggedValues()) {
            return nfd;
        }
        return new NativeFunctionDefinition(nfd.qualifiedName(), nfd.typeParameters(),
                nfd.multiplicityParameters(), params, returnType, nfd.returnMultiplicity(),
                stereotypes, taggedValues);
    }

    private static FunctionDefinition.ParameterDefinition resolveFunctionParam(
            FunctionDefinition.ParameterDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        return type == p.type() ? p
                : new FunctionDefinition.ParameterDefinition(p.name(), type, p.multiplicity());
    }

    // =================================================================
    // Mapping
    // =================================================================

    private static LegacyMappingDefinition resolveMapping(
            LegacyMappingDefinition md, Scope scope) {
        List<MappingInclude> includes = resolveMappingIncludes(md.includes(), scope);
        List<ClassMapping> classMappings = resolveClassMappings(md.classMappings(), scope);
        List<AssociationMapping> assocMappings = resolveAssociationMappings(
                md.associationMappings(), scope);
        List<EnumerationMapping> enumMappings = resolveEnumerationMappings(
                md.enumerationMappings(), scope);
        if (includes == md.includes() && classMappings == md.classMappings()
                && assocMappings == md.associationMappings()
                && enumMappings == md.enumerationMappings()) {
            return md;
        }
        return new LegacyMappingDefinition(md.qualifiedName(), includes, classMappings,
                assocMappings, enumMappings, md.testSuitesSource());
    }

    /**
     * Resolve a canonical (binding-table) {@link MappingDefinition} produced by
     * Door 1. Every binding references types/functions by FQN; a clean-sheet
     * author writes those as simple names under {@code import}, so each must be
     * resolved here exactly like the legacy form's class/association mappings.
     * {@code setId}/{@code extendsSetId}/{@code root}/{@code kind} are not names
     * (set ids are local; kind/root are flags) and pass through unchanged.
     * Reference-equality preserved when nothing resolved.
     */
    private static MappingDefinition resolveCanonicalMapping(
            MappingDefinition md, Scope scope) {
        List<MappingInclude> includes = resolveMappingIncludes(md.includes(), scope);
        List<MappingDefinition.ClassBinding> classBindings =
                resolveList(md.classBindings(), NameResolver::resolveClassBinding, scope);
        List<MappingDefinition.AssociationBinding> assocBindings =
                resolveList(md.associationBindings(), NameResolver::resolveAssociationBinding, scope);
        List<EnumerationMapping> enumMappings = resolveEnumerationMappings(
                md.enumerationMappings(), scope);
        if (includes == md.includes() && classBindings == md.classBindings()
                && assocBindings == md.associationBindings()
                && enumMappings == md.enumerationMappings()) {
            return md;
        }
        return new MappingDefinition(md.qualifiedName(), includes, classBindings,
                assocBindings, enumMappings, md.testSuitesSource());
    }

    private static MappingDefinition.ClassBinding resolveClassBinding(
            MappingDefinition.ClassBinding b, Scope scope) {
        String classFqn = resolveName(b.classFqn(), scope);
        Realization realization = resolveRealization(b.realization(), scope);
        if (classFqn.equals(b.classFqn()) && realization == b.realization()) return b;
        return new MappingDefinition.ClassBinding(
                classFqn, b.kind(), b.setId(), b.extendsSetId(), b.root(), realization);
    }

    private static MappingDefinition.AssociationBinding resolveAssociationBinding(
            MappingDefinition.AssociationBinding b, Scope scope) {
        String assocFqn = resolveName(b.associationFqn(), scope);
        Realization realization = resolveRealization(b.realization(), scope);
        if (assocFqn.equals(b.associationFqn()) && realization == b.realization()) return b;
        return new MappingDefinition.AssociationBinding(assocFqn, realization);
    }

    /**
     * Resolve a binding realization: a {@code Ref} resolves its function FQN; an
     * {@code Inline} resolves the names inside its expression body (it is an
     * ordinary {@link ValueSpecification}). Reference-equality preserved.
     */
    private static Realization resolveRealization(
            Realization r, Scope scope) {
        return switch (r) {
            case Realization.Ref ref -> {
                String fqn = resolveName(ref.functionFqn(), scope);
                yield fqn.equals(ref.functionFqn()) ? ref
                        : new Realization.Ref(fqn);
            }
            case Realization.Inline inl -> {
                List<ValueSpecification> body = resolveList(inl.body(), NameResolver::resolveVs, scope);
                yield body == inl.body() ? inl
                        : new Realization.Inline(body);
            }
        };
    }

    private static List<EnumerationMapping> resolveEnumerationMappings(
            List<EnumerationMapping> mappings, Scope scope) {
        return resolveList(mappings, NameResolver::resolveEnumerationMapping, scope);
    }

    private static EnumerationMapping resolveEnumerationMapping(
            EnumerationMapping em, Scope scope) {
        String enumName = resolveName(em.enumName(), scope);
        List<EnumValueMapping> values = resolveEnumValueMappings(
                em.valueMappings(), scope);
        if (enumName.equals(em.enumName()) && values == em.valueMappings()) return em;
        return new EnumerationMapping(enumName, em.mappingId(), values);
    }

    private static EnumValueMapping resolveEnumValueMapping(
            EnumValueMapping evm, Scope scope) {
        List<SourceValue> sources = resolveSourceValues(evm.sourceValues(), scope);
        return sources == evm.sourceValues() ? evm
                : new EnumValueMapping(evm.enumValue(), sources);
    }

    private static List<EnumValueMapping> resolveEnumValueMappings(
            List<EnumValueMapping> values, Scope scope) {
        return resolveList(values, NameResolver::resolveEnumValueMapping, scope);
    }

    private static List<SourceValue> resolveSourceValues(
            List<SourceValue> sources, Scope scope) {
        return resolveList(sources, NameResolver::resolveSourceValue, scope);
    }

    private static SourceValue resolveSourceValue(
            SourceValue sv, Scope scope) {
        return switch (sv) {
            case SourceValue.StringValue s -> s;
            case SourceValue.IntegerValue i -> i;
            case SourceValue.EnumRef ref -> {
                String path = resolveName(ref.enumPath(), scope);
                yield path.equals(ref.enumPath()) ? ref
                        : new SourceValue.EnumRef(path, ref.enumValueName());
            }
        };
    }

    private static MappingInclude resolveMappingInclude(
            MappingInclude inc, Scope scope) {
        String resolved = resolveName(inc.mappingPath(), scope);
        List<StoreSubstitution> subs = resolveStoreSubstitutions(inc.substitutions(), scope);
        return (resolved.equals(inc.mappingPath()) && subs == inc.substitutions()) ? inc
                : new MappingInclude(resolved, subs);
    }

    private static List<MappingInclude> resolveMappingIncludes(
            List<MappingInclude> includes, Scope scope) {
        return resolveList(includes, NameResolver::resolveMappingInclude, scope);
    }

    private static StoreSubstitution resolveStoreSubstitution(
            StoreSubstitution sub, Scope scope) {
        String orig = resolveName(sub.originalStore(), scope);
        String repl = resolveName(sub.replacementStore(), scope);
        return (orig.equals(sub.originalStore()) && repl.equals(sub.replacementStore())) ? sub
                : new StoreSubstitution(orig, repl);
    }

    private static List<StoreSubstitution> resolveStoreSubstitutions(
            List<StoreSubstitution> subs, Scope scope) {
        return resolveList(subs, NameResolver::resolveStoreSubstitution, scope);
    }

    private static List<ClassMapping> resolveClassMappings(
            List<ClassMapping> mappings, Scope scope) {
        return resolveList(mappings, NameResolver::resolveClassMapping, scope);
    }

    private static ClassMapping resolveClassMapping(
            ClassMapping cm, Scope scope) {
        return switch (cm) {
            case ClassMapping.Relational r -> {
                String className = resolveName(r.className(), scope);
                TableReference mainTable = resolveTableReference(r.mainTable(), scope);
                FilterMapping filter = resolveFilterMapping(r.filter(), scope);
                List<RelationalOperation> groupBy = resolveRelOpList(r.groupBy(), scope);
                List<RelationalOperation> primaryKey = resolveRelOpList(r.primaryKey(), scope);
                List<PropertyMapping> props = resolvePropertyMappingList(
                        r.propertyMappings(), scope);
                if (className.equals(r.className()) && mainTable == r.mainTable()
                        && filter == r.filter() && groupBy == r.groupBy()
                        && primaryKey == r.primaryKey() && props == r.propertyMappings()) {
                    yield r;
                }
                yield new ClassMapping.Relational(className, r.setId(), r.extendsSetId(),
                        r.root(), mainTable, filter, r.distinct(),
                        groupBy, primaryKey, props, r.sourceUrl(),
                        r.propertyTargetSets());
            }
            case ClassMapping.Union u -> {
                String className = resolveName(u.className(), scope);
                yield className.equals(u.className()) ? u
                        : new ClassMapping.Union(className, u.setId(),
                                u.extendsSetId(), u.root(), u.memberSetIds());
            }
            case ClassMapping.Inheritance ih -> {
                String className = resolveName(ih.className(), scope);
                yield className.equals(ih.className()) ? ih
                        : new ClassMapping.Inheritance(className, ih.setId(),
                                ih.extendsSetId(), ih.root());
            }
            case ClassMapping.RelationFunction rf -> {
                String className = resolveName(rf.className(), scope);
                String funcRef = resolveName(rf.funcRef(), scope);
                yield className.equals(rf.className()) && funcRef.equals(rf.funcRef())
                        ? rf
                        : new ClassMapping.RelationFunction(className, rf.setId(),
                                rf.extendsSetId(), rf.root(), funcRef, rf.columns());
            }
            case ClassMapping.Pure p -> {
                String className = resolveName(p.className(), scope);
                String sourceClass = p.sourceClass() == null
                        ? null : resolveName(p.sourceClass(), scope);
                ValueSpecification filter = resolveVs(p.filter(), scope);
                List<ClassMapping.Pure.PropertyBinding> bindings = resolvePropertyBindings(
                        p.propertyBindings(), scope);
                if (className.equals(p.className())
                        && Objects.equals(sourceClass, p.sourceClass())
                        && filter == p.filter()
                        && bindings == p.propertyBindings()) {
                    yield p;
                }
                yield new ClassMapping.Pure(className, p.setId(), p.extendsSetId(),
                        p.root(), sourceClass, filter, bindings);
            }
        };
    }

    private static ClassMapping.Pure.PropertyBinding resolvePropertyBinding(
            ClassMapping.Pure.PropertyBinding b, Scope scope) {
        ValueSpecification expr = resolveVs(b.expression(), scope);
        return expr == b.expression() ? b
                : new ClassMapping.Pure.PropertyBinding(b.propertyName(), expr);
    }

    private static List<ClassMapping.Pure.PropertyBinding> resolvePropertyBindings(
            List<ClassMapping.Pure.PropertyBinding> bindings, Scope scope) {
        return resolveList(bindings, NameResolver::resolvePropertyBinding, scope);
    }

    private static List<AssociationMapping> resolveAssociationMappings(
            List<AssociationMapping> mappings, Scope scope) {
        return resolveList(mappings, NameResolver::resolveAssociationMapping, scope);
    }

    private static AssociationMapping resolveAssociationMapping(
            AssociationMapping am, Scope scope) {
        return switch (am) {
            case AssociationMapping.Relational r -> {
                String name = resolveName(r.associationName(), scope);
                List<AssociationPropertyMapping> props = resolveAssocPropMappingList(
                        r.propertyMappings(), scope);
                if (name.equals(r.associationName()) && props == r.propertyMappings()) yield r;
                yield new AssociationMapping.Relational(name, props);
            }
            case AssociationMapping.ModelJoin mj -> {
                String name = resolveName(mj.associationName(), scope);
                ValueSpecification lam = resolveVs(mj.lambda(), scope);
                yield name.equals(mj.associationName()) && lam == mj.lambda()
                        ? mj : new AssociationMapping.ModelJoin(name,
                                (com.legend.model.spec.LambdaFunction) lam);
            }
            case AssociationMapping.Cross x -> {
                String name = resolveName(x.associationName(), scope);
                List<AssociationMapping.Cross.XStoreProperty> props =
                        resolveList(x.propertyMappings2(), (xp, sc) -> {
                            ValueSpecification e = resolveVs(xp.expression(), sc);
                            return e == xp.expression() ? xp
                                    : new AssociationMapping.Cross.XStoreProperty(
                                            xp.propertyName(), xp.sourceSetId(),
                                            xp.targetSetId(), e);
                        }, scope);
                yield name.equals(x.associationName()) && props == x.propertyMappings2()
                        ? x : new AssociationMapping.Cross(name, props);
            }
        };
    }

    private static AssociationPropertyMapping resolveAssocPropMapping(
            AssociationPropertyMapping apm, Scope scope) {
        PropertyMapping body = resolvePropertyMapping(apm.body(), scope);
        return body == apm.body() ? apm
                : new AssociationPropertyMapping(apm.sourceSetId(), apm.targetSetId(), body);
    }

    private static List<AssociationPropertyMapping> resolveAssocPropMappingList(
            List<AssociationPropertyMapping> list, Scope scope) {
        return resolveList(list, NameResolver::resolveAssocPropMapping, scope);
    }

    // =================================================================
    // Property mappings (9 variants)
    // =================================================================

    private static List<PropertyMapping> resolvePropertyMappingList(
            List<PropertyMapping> list, Scope scope) {
        return resolveList(list, NameResolver::resolvePropertyMapping, scope);
    }

    private static PropertyMapping resolvePropertyMapping(
            PropertyMapping pm, Scope scope) {
        if (pm == null) return null;
        return switch (pm) {
            case PropertyMapping.Column c -> {
                String db = resolveName(c.database(), scope);
                yield db.equals(c.database()) ? c
                        : new PropertyMapping.Column(c.propertyName(), db, c.table(), c.column());
            }
            case PropertyMapping.EnumeratedExpression ee -> ee;
            case PropertyMapping.EnumeratedColumn ec -> {
                String db = resolveName(ec.database(), scope);
                yield db.equals(ec.database()) ? ec
                        : new PropertyMapping.EnumeratedColumn(ec.propertyName(),
                                ec.enumMappingId(), db, ec.table(), ec.column());
            }
            case PropertyMapping.Join j -> {
                String db = resolveName(j.database(), scope);
                List<JoinChainElement> joins = resolveJoinChain(j.joins(), scope);
                yield (db.equals(j.database()) && joins == j.joins()) ? j
                        : new PropertyMapping.Join(j.propertyName(), db, joins,
                                j.targetSetId());
            }
            case PropertyMapping.JoinTerminalColumn jtc -> {
                String db = resolveName(jtc.database(), scope);
                List<JoinChainElement> joins = resolveJoinChain(jtc.joins(), scope);
                RelationalOperation term = resolveRelOp(jtc.terminalColumn(), scope);
                yield (db.equals(jtc.database()) && joins == jtc.joins()
                        && term == jtc.terminalColumn()) ? jtc
                        : new PropertyMapping.JoinTerminalColumn(jtc.propertyName(),
                                db, joins, term, jtc.enumMappingId(), jtc.enumMapped());
            }
            case PropertyMapping.Expression e -> {
                RelationalOperation expr = resolveRelOp(e.expression(), scope);
                yield expr == e.expression() ? e
                        : new PropertyMapping.Expression(e.propertyName(), expr);
            }
            case PropertyMapping.Embedded e -> {
                List<PropertyMapping> subs = resolvePropertyMappingList(
                        e.propertyMappings(), scope);
                yield subs == e.propertyMappings() ? e
                        : new PropertyMapping.Embedded(e.propertyName(), subs);
            }
            case PropertyMapping.InlineEmbedded ie -> ie; // setId is local; no FQN inside
            case PropertyMapping.OtherwiseEmbedded oe -> {
                List<PropertyMapping> emb = resolvePropertyMappingList(
                        oe.embedded(), scope);
                PropertyMapping fb = resolvePropertyMapping(oe.fallback(), scope);
                yield (emb == oe.embedded() && fb == oe.fallback()) ? oe
                        : new PropertyMapping.OtherwiseEmbedded(oe.propertyName(),
                                emb, oe.fallbackSetId(), fb);
            }
            case PropertyMapping.LocalProperty lp -> {
                TypeExpression type = resolveType(lp.type(), scope);
                PropertyMapping body = resolvePropertyMapping(lp.body(), scope);
                yield (type == lp.type() && body == lp.body()) ? lp
                        : new PropertyMapping.LocalProperty(lp.propertyName(), type,
                                lp.multiplicity(), body);
            }
        };
    }

    // =================================================================
    // Database
    // =================================================================

    private static DatabaseDefinition resolveDatabase(
            DatabaseDefinition db, Scope scope) {
        List<String> includes = resolveFqnList(db.includes(), scope);
        List<SchemaDefinition> schemas = resolveSchemas(db.schemas(), scope);
        List<DatabaseDefinition.TableDefinition> tables = db.tables(); // column data types only; no FQN
        List<ViewDefinition> views = resolveViews(db.views(), scope);
        List<JoinDefinition> joins = resolveJoins(db.joins(), scope);
        List<FilterDefinition> filters = resolveFilters(db.filters(), scope);
        List<FilterDefinition> multiGrain = resolveFilters(db.multiGrainFilters(), scope);
        if (includes == db.includes() && schemas == db.schemas() && views == db.views()
                && joins == db.joins() && filters == db.filters() && multiGrain == db.multiGrainFilters()) {
            return db;
        }
        return new DatabaseDefinition(db.qualifiedName(), includes, schemas,
                tables, views, joins, filters, multiGrain);
    }

    private static SchemaDefinition resolveSchema(SchemaDefinition s, Scope scope) {
        List<ViewDefinition> views = resolveViews(s.views(), scope);
        return views == s.views() ? s : new SchemaDefinition(s.name(), s.tables(), views);
    }

    private static List<SchemaDefinition> resolveSchemas(
            List<SchemaDefinition> schemas, Scope scope) {
        return resolveList(schemas, NameResolver::resolveSchema, scope);
    }

    private static ViewDefinition resolveView(ViewDefinition v, Scope scope) {
        FilterMapping filter = resolveFilterMapping(v.filter(), scope);
        List<RelationalOperation> groupBy = resolveRelOpList(v.groupByColumns(), scope);
        List<ViewColumnMapping> cols = resolveViewColumns(v.columnMappings(), scope);
        return (filter == v.filter() && groupBy == v.groupByColumns()
                && cols == v.columnMappings()) ? v
                : new ViewDefinition(v.name(), filter, groupBy, v.distinct(), cols);
    }

    private static List<ViewDefinition> resolveViews(
            List<ViewDefinition> views, Scope scope) {
        return resolveList(views, NameResolver::resolveView, scope);
    }

    private static ViewColumnMapping resolveViewColumn(ViewColumnMapping c, Scope scope) {
        RelationalOperation expr = resolveRelOp(c.expression(), scope);
        return expr == c.expression() ? c
                : new ViewColumnMapping(c.name(), c.targetSetId(), expr, c.primaryKey());
    }

    private static List<ViewColumnMapping> resolveViewColumns(
            List<ViewColumnMapping> cols, Scope scope) {
        return resolveList(cols, NameResolver::resolveViewColumn, scope);
    }

    private static JoinDefinition resolveJoin(JoinDefinition j, Scope scope) {
        RelationalOperation op = resolveRelOp(j.operation(), scope);
        return op == j.operation() ? j : new JoinDefinition(j.name(), op);
    }

    private static List<JoinDefinition> resolveJoins(
            List<JoinDefinition> joins, Scope scope) {
        return resolveList(joins, NameResolver::resolveJoin, scope);
    }

    private static FilterDefinition resolveFilter(FilterDefinition f, Scope scope) {
        RelationalOperation cond = resolveRelOp(f.condition(), scope);
        return cond == f.condition() ? f : new FilterDefinition(f.name(), cond);
    }

    private static List<FilterDefinition> resolveFilters(
            List<FilterDefinition> filters, Scope scope) {
        return resolveList(filters, NameResolver::resolveFilter, scope);
    }

    // =================================================================
    // Runtime / Service / Connection
    // =================================================================

    private static RuntimeDefinition resolveRuntime(
            RuntimeDefinition rd, Scope scope) {
        List<String> mappings = resolveFqnList(rd.mappings(), scope);
        Map<String, String> bindings = resolveFqnMapKeysAndValues(
                rd.connectionBindings(), scope);
        // jsonConnections (List<JsonModelConnection>) carry only literal
        // JSON payloads; no element FQN inside the public record shape.
        if (mappings == rd.mappings() && bindings == rd.connectionBindings()) return rd;
        return new RuntimeDefinition(rd.qualifiedName(), mappings, bindings, rd.jsonConnections());
    }

    private static ServiceDefinition resolveService(
            ServiceDefinition sd, Scope scope) {
        ValueSpecification body = resolveVs(sd.functionBody(), scope);
        String mappingRef = sd.mappingRef() == null
                ? null : resolveName(sd.mappingRef(), scope);
        String runtimeRef = sd.runtimeRef() == null
                ? null : resolveName(sd.runtimeRef(), scope);
        if (body == sd.functionBody()
                && Objects.equals(mappingRef, sd.mappingRef())
                && Objects.equals(runtimeRef, sd.runtimeRef())) {
            return sd;
        }
        return new ServiceDefinition(sd.qualifiedName(), sd.pattern(), body,
                sd.documentation(), mappingRef, runtimeRef, sd.testSuitesSource());
    }

    private static ConnectionDefinition resolveConnection(
            ConnectionDefinition cd, Scope scope) {
        String store = cd.storeName() == null
                ? null : resolveName(cd.storeName(), scope);
        if (Objects.equals(store, cd.storeName())) return cd;
        return new ConnectionDefinition(cd.qualifiedName(), store, cd.databaseType(),
                cd.specification(), cd.authentication());
    }

    // =================================================================
    // Shared nested-AST walkers
    // =================================================================

    private static TableReference resolveTableReference(
            TableReference t, Scope scope) {
        if (t == null) return null;
        String db = resolveName(t.database(), scope);
        return db.equals(t.database()) ? t : new TableReference(db, t.table());
    }

    private static FilterMapping resolveFilterMapping(
            FilterMapping fm, Scope scope) {
        if (fm == null) return null;
        return switch (fm) {
            case FilterMapping.Direct d -> {
                FilterPointer fp = resolveFilterPointer(d.filter(), scope);
                yield fp == d.filter() ? d : new FilterMapping.Direct(fp);
            }
            case FilterMapping.JoinMediated jm -> {
                String src = resolveName(jm.sourceDb(), scope);
                List<JoinChainElement> joins = resolveJoinChain(jm.joins(), scope);
                FilterPointer fp = resolveFilterPointer(jm.filter(), scope);
                if (src.equals(jm.sourceDb()) && joins == jm.joins() && fp == jm.filter()) yield jm;
                yield new FilterMapping.JoinMediated(src, joins, fp);
            }
        };
    }

    private static FilterPointer resolveFilterPointer(
            FilterPointer fp, Scope scope) {
        return switch (fp) {
            case FilterPointer.Local l -> l;
            case FilterPointer.Cross c -> {
                String db = resolveName(c.db(), scope);
                yield db.equals(c.db()) ? c : new FilterPointer.Cross(db, c.name());
            }
        };
    }

    private static JoinChainElement resolveJoinChainElement(
            JoinChainElement jce, Scope scope) {
        String db = resolveName(jce.databaseName(), scope);
        return db.equals(jce.databaseName()) ? jce
                : new JoinChainElement(jce.joinName(), jce.joinType(), db, jce.includeSelf());
    }

    private static List<JoinChainElement> resolveJoinChain(
            List<JoinChainElement> chain, Scope scope) {
        return resolveList(chain, NameResolver::resolveJoinChainElement, scope);
    }

    // =================================================================
    // RelationalOperation (10 variants)
    // =================================================================

    private static RelationalOperation resolveRelOp(
            RelationalOperation op, Scope scope) {
        if (op == null) return null;
        return switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                if (cr.databaseName() == null) yield cr;
                String db = resolveName(cr.databaseName(), scope);
                yield db.equals(cr.databaseName()) ? cr
                        : new RelationalOperation.ColumnRef(db, cr.table(), cr.column());
            }
            case RelationalOperation.TargetColumnRef t -> t;
            case RelationalOperation.Literal l -> l;
            // '@Type' dynafunction arguments: prelude primitives resolve bare
            // downstream; user type names resolve against the imports here.
            case RelationalOperation.TypeRef tr -> {
                String resolved = resolveName(tr.typeName(), scope);
                yield resolved.equals(tr.typeName()) ? tr
                        : new RelationalOperation.TypeRef(resolved);
            }
            case RelationalOperation.FunctionCall fc -> {
                // DESIGN CONTRACT: fc.name() is always a DB-side function name
                // (e.g. concat, coalesce, substring) dispatched by the SQL
                // backend, never a Pure FQN. The Pure grammar prevents
                // Pure-function references inside a RelationalOperation \u2014
                // those live in PropertyMapping.Expression or in a
                // ValueSpecification, which route through resolveVs. If the
                // grammar ever admits qualified names here, the contract is
                // violated and this branch silently drops the FQN. The
                // parser guarantees unqualified identifiers.
                List<RelationalOperation> args = resolveRelOpList(fc.args(), scope);
                yield args == fc.args() ? fc
                        : new RelationalOperation.FunctionCall(fc.name(), args);
            }
            case RelationalOperation.Comparison c -> {
                RelationalOperation l = resolveRelOp(c.left(), scope);
                RelationalOperation r = resolveRelOp(c.right(), scope);
                yield (l == c.left() && r == c.right()) ? c
                        : new RelationalOperation.Comparison(l, c.op(), r);
            }
            case RelationalOperation.BooleanOp b -> {
                RelationalOperation l = resolveRelOp(b.left(), scope);
                RelationalOperation r = resolveRelOp(b.right(), scope);
                yield (l == b.left() && r == b.right()) ? b
                        : new RelationalOperation.BooleanOp(l, b.op(), r);
            }
            case RelationalOperation.IsNull n -> {
                RelationalOperation o = resolveRelOp(n.operand(), scope);
                yield o == n.operand() ? n : new RelationalOperation.IsNull(o);
            }
            case RelationalOperation.IsNotNull n -> {
                RelationalOperation o = resolveRelOp(n.operand(), scope);
                yield o == n.operand() ? n : new RelationalOperation.IsNotNull(o);
            }
            case RelationalOperation.Group g -> {
                RelationalOperation inner = resolveRelOp(g.inner(), scope);
                yield inner == g.inner() ? g : new RelationalOperation.Group(inner);
            }
            case RelationalOperation.ArrayLiteral al -> {
                List<RelationalOperation> els = resolveRelOpList(al.elements(), scope);
                yield els == al.elements() ? al : new RelationalOperation.ArrayLiteral(els);
            }
            case RelationalOperation.JoinNavigation jn -> {
                // databaseName is NULL for the LOCAL form (no [DB] prefix —
                // the contextual database binds later, in the normalizer).
                String db = jn.databaseName() == null ? null
                        : resolveName(jn.databaseName(), scope);
                List<JoinChainElement> chain = resolveJoinChain(jn.chain(), scope);
                RelationalOperation term = resolveRelOp(jn.terminal(), scope);
                if (java.util.Objects.equals(db, jn.databaseName()) && chain == jn.chain()
                        && term == jn.terminal()) yield jn;
                yield new RelationalOperation.JoinNavigation(db, chain, term);
            }
        };
    }

    private static List<RelationalOperation> resolveRelOpList(
            List<RelationalOperation> ops, Scope scope) {
        return resolveList(ops, NameResolver::resolveRelOp, scope);
    }

    // =================================================================
    // Stereotype / TaggedValue profile-name resolution
    // =================================================================

    private static StereotypeApplication resolveStereotype(
            StereotypeApplication app, Scope scope) {
        String resolved = resolveName(app.profileName(), scope);
        return resolved.equals(app.profileName()) ? app
                : new StereotypeApplication(resolved, app.stereotypeName());
    }

    private static List<StereotypeApplication> resolveStereotypes(
            List<StereotypeApplication> apps, Scope scope) {
        return resolveList(apps, NameResolver::resolveStereotype, scope);
    }

    private static TaggedValue resolveTaggedValue(TaggedValue tv, Scope scope) {
        String resolved = resolveName(tv.profileName(), scope);
        return resolved.equals(tv.profileName()) ? tv
                : new TaggedValue(resolved, tv.tagName(), tv.value());
    }

    private static List<TaggedValue> resolveTaggedValues(
            List<TaggedValue> tvs, Scope scope) {
        return resolveList(tvs, NameResolver::resolveTaggedValue, scope);
    }

    // =================================================================
    // ValueSpecification
    // =================================================================

    private static ValueSpecification resolveVs(
            ValueSpecification vs, Scope scope) {
        if (vs == null) return null;
        return switch (vs) {
            case PackageableElementPtr ptr -> {
                String r = resolveName(ptr.fullPath(), scope);
                yield r.equals(ptr.fullPath()) ? ptr : new PackageableElementPtr(r);
            }
            case EnumValue ev -> {
                String r = resolveName(ev.fullPath(), scope);
                yield r.equals(ev.fullPath()) ? ev : new EnumValue(r, ev.value());
            }
            case AppliedFunction af -> {
                String fn = normalizePlatformFunction(resolveName(af.function(), scope));
                List<ValueSpecification> params = resolveVsList(af.parameters(), scope);
                yield (fn.equals(af.function()) && params == af.parameters()) ? af
                        : new AppliedFunction(fn, params);
            }
            case AppliedProperty ap -> {
                ValueSpecification receiver = resolveVs(ap.receiver(), scope);
                yield receiver == ap.receiver() ? ap
                        : new AppliedProperty(receiver, ap.property());
            }
            case LambdaFunction lf -> resolveLambda(lf, scope);
            case Variable v -> resolveVariable(v, scope);
            case NewInstance ni -> {
                String className = resolveName(ni.className(), scope);
                List<TypeExpression> typeArgs = resolveTypeList(ni.typeArguments(), scope);
                Map<String, KeyExpression> props = resolveKeyExpressionMap(
                        ni.properties(), scope);
                if (className.equals(ni.className())
                        && typeArgs == ni.typeArguments()
                        && props == ni.properties()) {
                    yield ni;
                }
                yield new NewInstance(className, typeArgs, props);
            }
            case NewInstanceCast nic -> {
                // Cast form ^Class($src): rewrite className (FQN
                // resolution against the import scope) and recurse
                // into the source expression. Type arguments rewritten
                // via the same path as NewInstance.
                String className = resolveName(nic.className(), scope);
                List<TypeExpression> typeArgs = resolveTypeList(nic.typeArguments(), scope);
                ValueSpecification src = resolveVs(nic.src(), scope);
                if (className.equals(nic.className())
                        && typeArgs == nic.typeArguments()
                        && src == nic.src()) {
                    yield nic;
                }
                yield new NewInstanceCast(className, typeArgs, src);
            }
            case PureCollection coll -> {
                List<ValueSpecification> values = resolveVsList(coll.values(), scope);
                yield values == coll.values() ? coll : new PureCollection(values);
            }
            case ColSpec cs -> resolveColSpec(cs, scope);
            case ColSpecArray csa -> {
                List<ColSpec> out = resolveList(csa.colSpecs(), NameResolver::resolveColSpec, scope);
                yield out == csa.colSpecs() ? csa : new ColSpecArray(out);
            }
            case TypeAnnotation ta -> resolveTypeAnnotation(ta, scope);
            // Explicit leaf pass-through. Listing every C-literal variant
            // means adding a new ValueSpecification variant fails the build
            // here until it is consciously handled, rather than silently
            // skipping resolution.
            case CBoolean cb -> cb;
            case CDate cd -> cd;
            case CDecimal cd -> cd;
            case CFloat cf -> cf;
            case CInteger ci -> ci;
            case CLatestDate cld -> cld;
            case CString cs -> cs;
            case CTime ct -> ct;
        };
    }

    private static List<ValueSpecification> resolveVsList(
            List<ValueSpecification> list, Scope scope) {
        return resolveList(list, NameResolver::resolveVs, scope);
    }

    private static Variable resolveVariable(Variable v, Scope scope) {
        TypeExpression t = resolveType(v.type(), scope);
        return t == v.type() ? v : new Variable(v.name(), t, v.multiplicity());
    }

    private static List<Variable> resolveVariableList(
            List<Variable> vars, Scope scope) {
        return resolveList(vars, NameResolver::resolveVariable, scope);
    }

    /**
     * Resolve a lambda by its concrete type. Shared between the
     * {@code LambdaFunction} arm of {@link #resolveVs} and
     * {@link #resolveColSpec}, which stores {@link LambdaFunction}
     * fields directly and would otherwise need an unchecked
     * downcast.
     */
    private static LambdaFunction resolveLambda(LambdaFunction lf, Scope scope) {
        List<Variable> params = resolveVariableList(lf.parameters(), scope);
        List<ValueSpecification> body = resolveVsList(lf.body(), scope);
        return (params == lf.parameters() && body == lf.body()) ? lf
                : new LambdaFunction(params, body);
    }

    private static Map<String, KeyExpression> resolveKeyExpressionMap(
            Map<String, KeyExpression> props, Scope scope) {
        if (props.isEmpty()) return props;
        boolean changed = false;
        Map<String, KeyExpression> out = new LinkedHashMap<>(props.size());
        for (Map.Entry<String, KeyExpression> e : props.entrySet()) {
            KeyExpression ke = e.getValue();
            ValueSpecification r = resolveVs(ke.value(), scope);
            if (r == ke.value()) {
                out.put(e.getKey(), ke);
            } else {
                out.put(e.getKey(), new KeyExpression(r, ke.isAdd()));
                changed = true;
            }
        }
        return changed ? Map.copyOf(out) : props;
    }

    private static TypeAnnotation.RelationShape.Column resolveRelationShapeColumn(
            TypeAnnotation.RelationShape.Column c, Scope scope) {
        TypeAnnotation inner = resolveTypeAnnotation(c.type(), scope);
        return inner == c.type() ? c
                : new TypeAnnotation.RelationShape.Column(c.name(), inner, c.multiplicity());
    }

    private static ColSpec resolveColSpec(ColSpec cs, Scope scope) {
        LambdaFunction fn1 = cs.function1();
        LambdaFunction fn2 = cs.function2();
        LambdaFunction r1 = fn1 == null ? null : resolveLambda(fn1, scope);
        LambdaFunction r2 = fn2 == null ? null : resolveLambda(fn2, scope);
        return (r1 == fn1 && r2 == fn2) ? cs : new ColSpec(cs.name(), r1, r2);
    }

    private static TypeAnnotation resolveTypeAnnotation(
            TypeAnnotation ta, Scope scope) {
        return switch (ta) {
            case TypeAnnotation.Named named -> {
                TypeExpression t = resolveType(named.type(), scope);
                yield t == named.type() ? named : new TypeAnnotation.Named(t);
            }
            case TypeAnnotation.Wildcard ignored -> ta;
            case TypeAnnotation.RelationShape shape -> {
                List<TypeAnnotation.RelationShape.Column> out = resolveList(
                        shape.columns(), NameResolver::resolveRelationShapeColumn, scope);
                yield out == shape.columns() ? shape
                        : new TypeAnnotation.RelationShape(out);
            }
        };
    }

    // =================================================================
    // Generic list / map helpers
    // =================================================================

    /**
     * Resolver function shape for use with {@link #resolveList}: a
     * pure node-to-node rewrite that preserves reference equality
     * ({@code ==}) when nothing changes.
     */
    @FunctionalInterface
    private interface Resolver<T> {
        T apply(T node, Scope scope);
    }

    /**
     * Resolve every element of {@code in} via {@code fn}. Returns the
     * input list ({@code ==}) when no element changed; otherwise an
     * immutable copy. This is the canonical shape for every list
     * walker in this file: empty fast-path, identity-preserving
     * iteration, allocate-on-write.
     */
    private static <T> List<T> resolveList(
            List<T> in, Resolver<T> fn, Scope scope) {
        if (in == null || in.isEmpty()) return in;
        boolean changed = false;
        List<T> out = new ArrayList<>(in.size());
        for (T t : in) {
            T r = fn.apply(t, scope);
            if (r != t) changed = true;
            out.add(r);
        }
        return changed ? List.copyOf(out) : in;
    }

    private static List<PackageableElement> resolveElementList(List<PackageableElement> els, Scope scope) {
        return resolveList(els, NameResolver::resolveElement, scope);
    }

    private static List<TypeExpression> resolveTypeList(List<TypeExpression> types, Scope scope) {
        return resolveList(types, NameResolver::resolveType, scope);
    }

    private static List<PropertyDefinition> resolvePropertyList(List<PropertyDefinition> props, Scope scope) {
        return resolveList(props, NameResolver::resolveProperty, scope);
    }

    private static List<DerivedPropertyDefinition> resolveDerivedList(List<DerivedPropertyDefinition> derived, Scope scope) {
        return resolveList(derived, NameResolver::resolveDerivedProperty, scope);
    }

    private static List<ParameterDefinition> resolveParameterList(List<ParameterDefinition> params, Scope scope) {
        return resolveList(params, NameResolver::resolveParameter, scope);
    }

    private static List<ConstraintDefinition> resolveConstraintList(List<ConstraintDefinition> constraints, Scope scope) {
        return resolveList(constraints, NameResolver::resolveConstraint, scope);
    }

    private static List<FunctionDefinition.ParameterDefinition> resolveFunctionParams(
            List<FunctionDefinition.ParameterDefinition> params, Scope scope) {
        return resolveList(params, NameResolver::resolveFunctionParam, scope);
    }

    /** Resolve every entry in a list of FQN strings (e.g. db includes). */
    private static List<String> resolveFqnList(List<String> fqns, Scope scope) {
        return resolveList(fqns, NameResolver::resolveName, scope);
    }

    private static Map<String, String> resolveFqnMapKeysAndValues(
            Map<String, String> map, Scope scope) {
        if (map.isEmpty()) return map;
        boolean changed = false;
        Map<String, String> out = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, String> e : map.entrySet()) {
            String k = resolveName(e.getKey(), scope);
            String v = resolveName(e.getValue(), scope);
            if (!k.equals(e.getKey()) || !v.equals(e.getValue())) changed = true;
            out.put(k, v);
        }
        return changed ? Map.copyOf(out) : map;
    }

    // =================================================================
    // Scope
    // =================================================================

    /**
     * Threaded context for resolution: the user's
     * {@link ImportScope}, the universe of known FQNs, and the set of
     * type-parameter names currently in scope.
     *
     * <p>Threading these together as one parameter keeps every helper
     * method to a {@code (Node, Scope)} signature. The
     * {@code typeParams} field models class- and function-level
     * type-parameter shadowing: when a name matches a type parameter
     * currently in scope (e.g. {@code T} inside {@code Class Foo<T>}),
     * resolution is suppressed even if some FQN matches.
     *
     * <p><strong>Multiplicity parameters are intentionally not tracked
     * here.</strong> Names like {@code m} / {@code n} declared via
     * {@code <|m,n>} live in the {@link Multiplicity} AST, never in a
     * position routed through {@link #resolveName}, so they cannot
     * collide with imports. If the grammar ever lets multiplicity
     * names appear in a {@code String}-bearing FQN position, this
     * record will need a {@code multiplicityParams} field with
     * symmetric shadowing.
     */
    public record Scope(
            ImportScope imports,
            Set<String> knownFqns,
            Set<String> typeParams) {

        public Scope {
            Objects.requireNonNull(imports, "imports");
            Objects.requireNonNull(knownFqns, "knownFqns");
            Objects.requireNonNull(typeParams, "typeParams");
        }

        /** Empty type-param scope. Most call sites construct via this. */
        public static Scope of(ImportScope imports, Set<String> knownFqns) {
            return new Scope(imports, knownFqns, Set.of());
        }

        /**
         * Return a child scope with {@code params} added to
         * {@link #typeParams}. Used at class- and function-resolver
         * entry. Returns {@code this} when {@code params} is empty so
         * the common no-type-param case is allocation-free.
         */
        public Scope withTypeParams(List<String> params) {
            if (params == null || params.isEmpty()) return this;
            HashSet<String> merged = new HashSet<>(typeParams);
            merged.addAll(params);
            return new Scope(imports, knownFqns, Set.copyOf(merged));
        }
    }
}
