package com.legend.compiler;

import com.legend.builtin.Pure;
import com.legend.model.ImportScope;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationDefinition.AssociationEndDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.protocol.ConstraintDefinition;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.ParameterDefinition;
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
import com.legend.model.CleanSheetMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.protocol.Realization;
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
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.CTime;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PathLiteral;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.HashMap;
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
        return resolve(parsed, (java.util.Map<String, String>) null);
    }

    /**
     * TOLERANT variant (module compile): a non-null {@code wallSink}
     * collects per-element resolution failures (element FQN &rarr; first
     * error line) and EXCLUDES those elements from the output instead of
     * throwing — one pass walls them all. Null = strict (throw on first).
     */
    public static ParsedModel resolve(ParsedModel parsed,
            java.util.@com.legend.Nullable Map<String, String> wallSink) {
        Objects.requireNonNull(parsed, "parsed");
        // USER scopes stay pure — the platform prelude is a FALLBACK
        // consulted inside resolveNameMulti, never merged into the
        // element's own imports (merging let prelude names OVERWRITE
        // explicit user type-imports: Builder.add is last-wins)
        return resolve(parsed, knownFqns(parsed.elements()), wallSink, true);
    }

    /** {@link #resolve(ParsedModel, java.util.Map)} with {@code alsoKnown}
     * FQNs in the universe — the BOOT LAYER's elements, which a graph's own
     * elements reference by import exactly like each other's (they are not
     * in the graph's parsed elements: they are prepared once per process). */
    public static ParsedModel resolveAlongside(ParsedModel parsed, Set<String> alsoKnown,
            java.util.@com.legend.Nullable Map<String, String> wallSink) {
        Objects.requireNonNull(parsed, "parsed");
        Set<String> known = new java.util.HashSet<>(knownFqns(parsed.elements()));
        known.addAll(alsoKnown);
        return resolve(parsed, known, wallSink, true);
    }

    /**
     * Resolve every simple name reference in {@code model} to its FQN
     * using {@code model.imports()} and {@code knownFqns}. Lower-level entry:
     * the caller supplies the full import scope and FQN universe (the
     * prelude-aware {@link #resolve(ParsedModel)} is the usual entry).
     */
    public static ParsedModel resolve(ParsedModel model, Set<String> knownFqns) {
        return resolve(model, knownFqns, null);
    }

    /** {@link #resolve(ParsedModel, Set)} with an optional tolerant wall sink. */
    public static ParsedModel resolve(ParsedModel model, Set<String> knownFqns,
            java.util.@com.legend.Nullable Map<String, String> wallSink) {
        return resolve(model, knownFqns, wallSink, false);
    }

    /** The implicit import group every element resolves through, walked
     * FIRST-MATCH — the ENGINE's sequence ({@code CompileContext.META_IMPORTS}:
     * legend-pure's {@code system::imports::coreImport} 29 plus
     * {@code metamodel::variant}, {@code metamodel::relation} and
     * {@code precisePrimitives} at the engine's positions). GENERATED from the
     * pinned checkout and held as a sequence by {@code CoreImportsParityTest}
     * ({@code -Dimports.generate=1}). */
    public static final List<String> CORE_IMPORTS = List.of(
            "meta::pure::metamodel",
            "meta::pure::metamodel::type",
            "meta::pure::metamodel::type::generics",
            "meta::pure::metamodel::relationship",
            "meta::pure::metamodel::valuespecification",
            "meta::pure::metamodel::multiplicity",
            "meta::pure::metamodel::function",
            "meta::pure::metamodel::function::property",
            "meta::pure::metamodel::extension",
            "meta::pure::metamodel::import",
            "meta::pure::metamodel::variant",
            "meta::pure::functions::date",
            "meta::pure::functions::string",
            "meta::pure::functions::collection",
            "meta::pure::functions::meta",
            "meta::pure::functions::constraints",
            "meta::pure::functions::lang",
            "meta::pure::functions::boolean",
            "meta::pure::functions::tools",
            "meta::pure::functions::io",
            "meta::pure::functions::math",
            "meta::pure::functions::asserts",
            "meta::pure::functions::test",
            "meta::pure::functions::multiplicity",
            "meta::pure::functions::relation",
            "meta::pure::metamodel::relation",
            "meta::pure::router",
            "meta::pure::service",
            "meta::pure::tds",
            "meta::pure::tools",
            "meta::pure::profiles",
            "meta::pure::precisePrimitives");

    private static ParsedModel resolve(ParsedModel model, Set<String> knownFqns,
            java.util.@com.legend.Nullable Map<String, String> wallSink,
            boolean preludeOn) {
        // SECTION-scoped resolution (real pure): each element resolves in
        // ITS OWN section's imports when recorded; the union scope is the
        // fallback (single-source models, synthesized elements). The
        // element's OWN PACKAGE is always visible bare (real pure's
        // implicit same-package import — §2.4b of the resolution audit).
        List<PackageableElement> resolved = new ArrayList<>(model.elements().size());
        boolean changed = false;
        for (PackageableElement el : model.elements()) {
            ImportScope own = model.elementImports().get(el.qualifiedName());
            String fqn0 = el.qualifiedName();
            int cut0 = fqn0.lastIndexOf("::");
            String ownPkg = cut0 > 0 ? fqn0.substring(0, cut0) : null;
            Scope scope = new Scope(own == null ? model.imports() : own,
                    knownFqns, Set.of(), ownPkg, preludeOn);
            PackageableElement r;
            try {
                r = resolveElement(el, scope);
            } catch (com.legend.error.ResolutionException e) {
                // ELEMENT-ATTRIBUTED: a module compile (many sources) must
                // know WHICH element failed to resolve — the caller's
                // drop-and-wall works on structured identities, never on
                // message text
                if (wallSink != null) {
                    // POISON-NOT-DROP: the element stays, UNRESOLVED as
                    // parsed — downstream lazy compilation fails loudly if
                    // anything actually uses it; mere references survive
                    wallSink.putIfAbsent(el.qualifiedName(),
                            String.valueOf(e.getMessage()).split("\n")[0]);
                    resolved.add(el);
                    continue;
                }
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.RESOLVE,
                        e.getMessage(), el.qualifiedName());
            }
            resolved.add(r);
            changed |= r != el;
        }
        return !changed ? model
                : new ParsedModel(resolved, model.imports(),
                        model.source(), model.elementOffsets(), model.elementImports(),
                        model.elementSources());
    }

    /** The platform's TYPE universe: the catalog's hand shapes and enums and
     * the generated prelude module's classes and enums — what an import, an
     * own-package or a core-import candidate may resolve TO. Never a
     * bare-name fallback (batch 153): a name no import makes visible is
     * unresolved, as in the engine. */
    private static List<String> platformTypeFqns() {
        List<String> all = new ArrayList<>();
        Pure.allNativeClasses().forEach(c -> all.add(c.qualifiedName()));
        Pure.allNativeEnums().forEach(e -> all.add(e.qualifiedName()));
        all.addAll(com.legend.builtin.Prelude.classFqns());
        all.addAll(com.legend.builtin.Prelude.enumFqns());
        return all;
    }

    /** Declared element FQNs + platform FQNs: the wildcard-disambiguation universe. */
    private static Set<String> knownFqns(List<PackageableElement> elements) {
        Set<String> known = new HashSet<>();
        for (PackageableElement el : elements) {
            known.add(el.qualifiedName());
        }
        known.addAll(platformTypeFqns());
        return known;
    }

    private static PackageableElement resolveElement(
            PackageableElement element, Scope scope) {
        return switch (element) {
            // opaque overlay elements carry EXTENSION-owned payloads — core
            // routes them by FQN and never opens them, so there are no names
            // to resolve (Phase M step 3)
            case com.legend.model.OpaqueElementDefinition oe -> oe;
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
            case com.legend.model.ExecutionEnvironmentDefinition ee -> {
                java.util.List<ServiceDefinition.KeyedExecution> ks =
                        new java.util.ArrayList<>(ee.executions().size());
                boolean changed = false;
                for (ServiceDefinition.KeyedExecution k : ee.executions()) {
                    String m = k.mapping() == null ? null
                            : resolveName(k.mapping(), scope);
                    String r = k.runtime() == null ? null
                            : resolveName(k.runtime(), scope);
                    changed |= !Objects.equals(m, k.mapping())
                            || !Objects.equals(r, k.runtime());
                    ks.add(new ServiceDefinition.KeyedExecution(
                            k.keyValue(), m, r));
                }
                yield changed ? new com.legend.model
                        .ExecutionEnvironmentDefinition(ee.qualifiedName(), ks)
                        : ee;
            }
            case ConnectionDefinition cd -> resolveConnection(cd, scope);
            case com.legend.model.DataSpaceDefinition ds ->
                    resolveDataSpace(ds, scope);
            case com.legend.model.ModelConnectionDefinition mc -> {
                String cls = resolveName(mc.className(), scope);
                yield cls.equals(mc.className()) ? mc
                        : new com.legend.model.ModelConnectionDefinition(
                                mc.qualifiedName(), mc.kind(), cls, mc.url());
            }
            case com.legend.model.ModelChainConnectionDefinition mcc -> {
                java.util.List<String> resolved = new java.util.ArrayList<>();
                boolean changed = false;
                for (String m : mcc.mappings()) {
                    String r = resolveName(m, scope);
                    changed |= !r.equals(m);
                    resolved.add(r);
                }
                yield changed ? new com.legend.model
                        .ModelChainConnectionDefinition(mcc.qualifiedName(),
                                resolved)
                        : mcc;
            }
            // Explicit pass-through so adding a new PackageableElement variant
            // surfaces as an unhandled-case compile error rather than silently
            // skipping resolution.
            case EnumDefinition ed -> ed;
            case ProfileDefinition pd -> pd;
            // unit conversion lambdas are numeric expressions over their own
            // parameter — no cross-element names to resolve
            case com.legend.model.MeasureDefinition me -> me;
            case com.legend.model.PersistenceDefinition pe -> {
                String svc = pe.service() == null ? null
                        : resolveName(pe.service(), scope);
                yield Objects.equals(svc, pe.service()) ? pe
                        : new com.legend.model.PersistenceDefinition(
                                pe.qualifiedName(), pe.doc(),
                                pe.triggerSource(), svc, pe.persisterSource(),
                                pe.serviceOutputTargetsSource(),
                                pe.notifierSource(), pe.testsSource());
            }
            case com.legend.model.PersistenceContextDefinition pce -> {
                String p = resolveName(pce.persistence(), scope);
                yield p.equals(pce.persistence()) ? pce
                        : new com.legend.model.PersistenceContextDefinition(
                                pce.qualifiedName(), p, pce.platformSource(),
                                pce.serviceParametersSource(),
                                pce.sinkConnectionSource());
            }
            // activator fields carry function-pointer signatures and raw
            // ownership blocks — nothing safely resolvable by name
            case com.legend.model.SnowflakeActivatorDefinition sa -> sa;
            // generic keyed elements carry field values as written
            case com.legend.model.GenericSectionElementDefinition ge -> ge;
            // The canonical (binding-table) MappingDefinition is produced
            // directly by Door 1 (clean-sheet text), so it reaches the resolver
            // and its binding FQNs must be resolved like any other element's.
            case CleanSheetMappingDefinition md -> resolveCanonicalMapping(md, scope);
            // a COMPILED mapping never re-resolves (post-E artifacts carry
            // resolved FQNs only; resolution runs on parsed surfaces)
            case MappingDefinition md -> md;
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
        return Objects.requireNonNull(resolveVs(vs, scope), "resolveVs(vs, scope)");
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
        return Objects.requireNonNull(resolveVs(query, QUERY_SCOPE));
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
        Set<String> known = new HashSet<>(platformTypeFqns());
        known.addAll(modelFqns);
        return Objects.requireNonNull(
                resolveVs(query, Scope.preludeOf(imports, Set.copyOf(known))));
    }

    /** The sectionless-query scope: prelude imports only; the native FQN universe. */
    private static final Scope QUERY_SCOPE = querycope();

    private static Scope querycope() {
        Set<String> known = new HashSet<>(platformTypeFqns());
        return Scope.preludeOf(new ImportScope.Builder().build(), Set.copyOf(known));
    }

    private static @com.legend.Nullable TypeExpression resolveType(
            @com.legend.Nullable TypeExpression t, Scope scope) {
        if (t == null) return null;
        return switch (t) {
            case TypeExpression.NameRef nr -> {
                String r = resolveName(nr.name(), scope);
                yield r.equals(nr.name()) ? nr : new TypeExpression.NameRef(r);
            }
            case TypeExpression.Generic g -> {
                String r = resolveName(g.name(), scope);
                List<TypeExpression> args = resolveTypeList(g.arguments(), scope);
                // the rebuild keeps the MULTIPLICITY arguments and the type
                // variable values (Result<TabularDataSet|1>: dropping the |1
                // typed every .values read of a user function's Result [*])
                yield (r.equals(g.name()) && args == g.arguments()) ? g
                        : new TypeExpression.Generic(r, args, g.multiplicityArguments(),
                                g.typeVariableValues(), g.pos());
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
                        : new TypeExpression.SchemaAlgebra(nn(l), sa.op(), nn(r));
            }
        };
    }

    private static TypeExpression.TypedParameter resolveTypedParameter(
            TypeExpression.TypedParameter p, Scope scope) {
        TypeExpression t = resolveType(p.type(), scope);
        return t == p.type() ? p
                : new TypeExpression.TypedParameter(nn(t), p.multiplicity());
    }

    private static TypeExpression.Column resolveColumn(
            TypeExpression.Column c, Scope scope) {
        TypeExpression t = resolveType(c.type(), scope);
        return t == c.type() ? c
                : new TypeExpression.Column(c.name(), nn(t), c.multiplicity());
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
        List<String> matches = resolveNameMulti(name, scope);
        if (matches.size() > 1) {
            throw new com.legend.error.ResolutionException(
                    "ambiguous reference '" + name + "' \u2014 matches via imports: "
                    + matches + ". Use a fully qualified name.");
        }
        return matches.get(0);
    }

    /**
     * The multi-referent core: 1 element = resolved (or the name itself
     * when nothing matched — FQNs, type params, unimported names); N
     * elements = several imported packages define the name. TYPE positions
     * error on N (real pure); FUNCTION-CALL positions carry all N as
     * overload candidates and the Typer picks by signature (real pure's
     * function matching collects across imports).
     */
    /** The M3 primitive type names (meta::pure::metamodel::type::*, the
     * platform's {@code Type.Primitive} roster). */
    private static final Set<String> PRIMITIVE_TYPE_NAMES = Set.of(
            "Number", "Integer", "Float", "Decimal", "String", "Boolean",
            "Byte", "Date", "StrictDate", "DateTime", "LatestDate", "StrictTime");

    private static List<String> resolveNameMulti(String name, Scope scope) {
        if (name == null || name.isEmpty()) return java.util.Collections.singletonList(name);
        // Type-parameter shadowing: a NameRef matching an in-scope type
        // parameter (e.g. T inside Class Foo<T>) is a parameter
        // reference, not a Pure FQN. Skip import resolution.
        if (scope.typeParams().contains(name)) return List.of(name);
        if (name.contains("::")) return List.of(name);
        // M3 PRIMITIVES are reserved bare names (real pure): a class that
        // happens to be called Integer / Date / Binary (the relational
        // datatype metamodel, meta::relational::metamodel::datatype::*) is
        // reachable ONLY by its qualified name — never through a wildcard
        // import or the prelude fallback. The engine's own sources spell
        // those classes qualified for exactly this reason.
        if (PRIMITIVE_TYPE_NAMES.contains(name)) {
            // the prelude tier spells the m3 FQN (as it always did for the
            // primitive metaclasses); without it the bare name passes
            // through for the primitive fallback, unchanged
            return scope.prelude()
                    ? List.of("meta::pure::metamodel::type::" + name) : List.of(name);
        }
        // PRECEDENCE (real pure; NAME_RESOLUTION_BUG.md remediation):
        // 1. the file's wildcards + the element's OWN package (implicit
        //    same-package import) over the declared+platform universe
        // 2. the platform prelude — a FALLBACK, shadowed by anything the
        //    user made visible (this tier subsumes the retired
        //    mapping-set-target special case)
        // (the old explicit-type-import tier died with the specific-import
        // invention — both references are wildcard-only)
        // DISTINCT candidates: a package listed twice (harness-built
        // scopes, own-package duplicating an explicit import) is ONE
        // referent, never an ambiguity
        List<String> matches = new ArrayList<>(0);
        for (String pkg : scope.imports().wildcards()) {
            String candidate = pkg + "::" + name;
            if (scope.knownFqns().contains(candidate)
                    && !matches.contains(candidate)) {
                matches.add(candidate);
            }
        }
        if (!matches.isEmpty()) return matches;
        // OWN PACKAGE is a tier BELOW imports, never a peer: the corpus's
        // own testUnionPartial.pure resolves bare 'Address' to the
        // IMPORTED simple::Address in an import-bearing section and to
        // the same-package partial::Address in an import-less one — the
        // engine compiles both, so an import match must never turn
        // same-package visibility into a fake ambiguity
        if (scope.ownPackage() != null) {
            String candidate = scope.ownPackage() + "::" + name;
            if (scope.knownFqns().contains(candidate)) {
                return List.of(candidate);
            }
        }
        // CORE IMPORTS: real pure resolves every file against the
        // `system::imports::coreImport` group (legend-pure m3.pure:175 —
        // the m3 packages, meta::pure::functions::*, router/service/tds/
        // tools/profiles) BELOW the file's own imports; this is how
        // `ExtendedRoutedValueSpecification extends RoutedValueSpecification`
        // finds meta::pure::router::RoutedValueSpecification without an
        // import line. FIRST match in the group's order: this resolver is
        // kind-blind, and real pure's kinds keep a function and a profile
        // of the same bare name apart (functionType — functions::meta vs
        // profiles); the group's order is the spec's.
        for (String pkg : CORE_IMPORTS) {
            String candidate = pkg + "::" + name;
            if (scope.knownFqns().contains(candidate)) {
                return List.of(candidate);
            }
        }
        // NO FALLBACK TIER (USER RULING 2026-09-08, PRELUDE_MODULE_HOMEWORK
        // §9.12 / §6a item 2: "bare names must fail like pure/engine"): a
        // bare name is its section's imports, its own package, or the core
        // import group — or it is unresolved and fails downstream exactly as
        // the engine's `Can't find type`. The platform-prelude fallback that
        // stood here (a simple-name index over every catalog and module
        // class, its collision winner first HashMap order, then declaration
        // order) resolved names no import made visible; deleted.
        return List.of(name);
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
        return new ClassDefinition(cd.qualifiedName(), cd.typeParams(), cd.typeVariables(),
                superClasses, properties, derived, constraints,
                stereotypes, taggedValues, cd.isNative());
    }

    private static PropertyDefinition resolveProperty(
            PropertyDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        List<StereotypeApplication> stereotypes = resolveStereotypes(p.stereotypes(), scope);
        List<TaggedValue> taggedValues = resolveTaggedValues(p.taggedValues(), scope);
        // the declared default resolves in the class's own scope (an enum
        // value, a constructor) — the new-instance checker synthesizes it later
        ValueSpecification dflt = p.defaultValue() == null ? null : resolveVs(p.defaultValue(), scope);
        if (type == p.type() && stereotypes == p.stereotypes() && taggedValues == p.taggedValues()
                && dflt == p.defaultValue()) {
            return p;
        }
        return new PropertyDefinition(p.name(), nn(type),
                p.multiplicity(), stereotypes, taggedValues, p.hasDefault(), dflt);
    }

    private static DerivedPropertyDefinition resolveDerivedProperty(
            DerivedPropertyDefinition dp, Scope scope) {
        List<ParameterDefinition> params = resolveParameterList(dp.parameters(), scope);
        Realization realization = resolveRealization(dp.realization(), scope);
        TypeExpression type = resolveType(dp.type(), scope);
        if (params == dp.parameters() && realization == dp.realization() && type == dp.type()) {
            return dp;
        }
        return new DerivedPropertyDefinition(dp.name(), params, realization,
                nn(type), dp.multiplicity());
    }

    private static ParameterDefinition resolveParameter(
            ParameterDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        return type == p.type() ? p
                : new ParameterDefinition(p.name(), nn(type), p.multiplicity());
    }

    private static ConstraintDefinition resolveConstraint(
            ConstraintDefinition c, Scope scope) {
        Realization realization = resolveRealization(c.realization(), scope);
        // ~message is an EXPRESSION over $this — resolve it like the body;
        // the rebuild carries message + enforcementLevel (the 2-arg
        // convenience ctor silently DROPPED both whenever resolution
        // changed the body — validate()'s MESSAGE column went empty)
        ValueSpecification message = c.message() == null ? null
                : resolveVs(c.message(), scope);
        if (realization == c.realization() && message == c.message()) {
            return c;
        }
        return new ConstraintDefinition(c.name(), realization, message,
                c.enforcementLevel());
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
                : new AssociationEndDefinition(end.propertyName(), nn(target),
                        end.multiplicity());
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
                fd.multiplicityParameters(), params, nn(returnType),
                fd.returnMultiplicity(),
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
                nfd.multiplicityParameters(), params, nn(returnType),
                nfd.returnMultiplicity(),
                stereotypes, taggedValues);
    }

    private static FunctionDefinition.ParameterDefinition resolveFunctionParam(
            FunctionDefinition.ParameterDefinition p, Scope scope) {
        TypeExpression type = resolveType(p.type(), scope);
        return type == p.type() ? p
                : new FunctionDefinition.ParameterDefinition(p.name(), nn(type),
                        p.multiplicity());
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
    private static CleanSheetMappingDefinition resolveCanonicalMapping(
            CleanSheetMappingDefinition md, Scope scope) {
        List<MappingInclude> includes = resolveMappingIncludes(md.includes(), scope);
        List<CleanSheetMappingDefinition.ClassBinding> classBindings =
                resolveList(md.classBindings(), NameResolver::resolveClassBinding, scope);
        List<CleanSheetMappingDefinition.AssociationBinding> assocBindings =
                resolveList(md.associationBindings(), NameResolver::resolveAssociationBinding, scope);
        List<EnumerationMapping> enumMappings = resolveEnumerationMappings(
                md.enumerationMappings(), scope);
        if (includes == md.includes() && classBindings == md.classBindings()
                && assocBindings == md.associationBindings()
                && enumMappings == md.enumerationMappings()) {
            return md;
        }
        return new CleanSheetMappingDefinition(md.qualifiedName(), includes, classBindings,
                assocBindings, enumMappings, md.testSuitesSource());
    }

    private static CleanSheetMappingDefinition.ClassBinding resolveClassBinding(
            CleanSheetMappingDefinition.ClassBinding b, Scope scope) {
        String classFqn = resolveName(b.classFqn(), scope);
        Realization realization = resolveRealization(b.realization(), scope);
        if (classFqn.equals(b.classFqn()) && realization == b.realization()) return b;
        return new CleanSheetMappingDefinition.ClassBinding(
                classFqn, b.kind(), b.setId(), b.extendsSetId(), b.root(),
                realization, b.primaryKeyColumns());
    }

    private static CleanSheetMappingDefinition.AssociationBinding resolveAssociationBinding(
            CleanSheetMappingDefinition.AssociationBinding b, Scope scope) {
        String assocFqn = resolveName(b.associationFqn(), scope);
        Realization realization = resolveRealization(b.realization(), scope);
        if (assocFqn.equals(b.associationFqn()) && realization == b.realization()) return b;
        return new CleanSheetMappingDefinition.AssociationBinding(assocFqn, realization);
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
                List<ValueSpecification> body = resolveList(inl.body(),
                        (x, sc) -> Objects.requireNonNull(resolveVs(x, sc), "resolveVs(x, sc)"),
                        scope);
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

    /** The AggregationAware node's views resolve with the node: each view
     * set like any class mapping, its specification lambdas like any
     * mapping expression. */
    private static ClassMapping.@com.legend.Nullable AggregationAware resolveAggregation(
            ClassMapping.@com.legend.Nullable AggregationAware agg, Scope scope) {
        if (agg == null) {
            return null;
        }
        List<ClassMapping.AggregateView> out = new java.util.ArrayList<>();
        for (ClassMapping.AggregateView v : agg.views()) {
            List<ValueSpecification> gbs = v.groupByFunctions().stream()
                    .map(g -> java.util.Objects.requireNonNull(resolveVs(g, scope))).toList();
            List<ClassMapping.AggregateValue> avs = v.aggregateValues().stream()
                    .map(a -> new ClassMapping.AggregateValue(
                            java.util.Objects.requireNonNull(resolveVs(a.mapFn(), scope)),
                            java.util.Objects.requireNonNull(resolveVs(a.aggregateFn(), scope))))
                    .toList();
            out.add(new ClassMapping.AggregateView(v.index(), v.canAggregate(), gbs, avs,
                    (ClassMapping.Relational) resolveClassMapping(v.set(), scope)));
        }
        return new ClassMapping.AggregationAware(out);
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
                ClassMapping.AggregationAware agg = resolveAggregation(r.aggregation(), scope);
                if (className.equals(r.className()) && mainTable == r.mainTable()
                        && filter == r.filter() && groupBy == r.groupBy()
                        && primaryKey == r.primaryKey() && props == r.propertyMappings()
                        && agg == r.aggregation()) {
                    yield r;
                }
                yield new ClassMapping.Relational(className, r.setId(), r.extendsSetId(),
                        r.root(), mainTable, filter, r.distinct(),
                        groupBy, primaryKey, props, r.sourceUrl(),
                        r.propertyTargetSets(), agg);
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
                if (rf.funcRef() == null) {
                    // ~src inline expression: the SOURCE resolves like any
                    // mapping expression; there is no name to resolve
                    ValueSpecification src = resolveVs(rf.inlineSource(), scope);
                    yield className.equals(rf.className())
                            && src == rf.inlineSource()
                            ? rf
                            : new ClassMapping.RelationFunction(className,
                                    rf.setId(), rf.extendsSetId(), rf.root(),
                                    null, src, rf.columns(), rf.primaryKey());
                }
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
                        p.root(), nn(sourceClass), filter, bindings);
            }
        };
    }

    private static ClassMapping.Pure.PropertyBinding resolvePropertyBinding(
            ClassMapping.Pure.PropertyBinding b, Scope scope) {
        ValueSpecification expr = resolveVs(b.expression(), scope);
        return expr == b.expression() ? b : b.withExpression(nn(expr));
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
                                (com.legend.protocol.spec.LambdaFunction) nn(lam));
            }
            case AssociationMapping.Cross x -> {
                String name = resolveName(x.associationName(), scope);
                List<AssociationMapping.Cross.XStoreProperty> props =
                        resolveList(x.propertyMappings2(), (xp, sc) -> {
                            ValueSpecification e = resolveVs(xp.expression(), sc);
                            return e == xp.expression() ? xp
                                    : new AssociationMapping.Cross.XStoreProperty(
                                            xp.propertyName(), xp.sourceSetId(),
                                            xp.targetSetId(), nn(e));
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
                : new AssociationPropertyMapping(apm.sourceSetId(),
                        apm.targetSetId(), nn(body));
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
        return resolveList(list,
                (x, sc) -> Objects.requireNonNull(
                        resolvePropertyMapping(x, sc)), scope);
    }

    private static @com.legend.Nullable PropertyMapping resolvePropertyMapping(
            @com.legend.Nullable PropertyMapping pm, Scope scope) {
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
                                db, joins, nn(term), jtc.enumMappingId(), jtc.enumMapped());
            }
            case PropertyMapping.Expression e -> {
                RelationalOperation expr = resolveRelOp(e.expression(), scope);
                yield expr == e.expression() ? e
                        : new PropertyMapping.Expression(e.propertyName(), nn(expr));
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
                                emb, oe.fallbackSetId(), nn(fb));
            }
            case PropertyMapping.LocalProperty lp -> {
                TypeExpression type = resolveType(lp.type(), scope);
                PropertyMapping body = resolvePropertyMapping(lp.body(), scope);
                yield (type == lp.type() && body == lp.body()) ? lp
                        : new PropertyMapping.LocalProperty(lp.propertyName(), nn(type),
                                lp.multiplicity(), nn(body));
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
                : new ViewColumnMapping(c.name(), c.targetSetId(), nn(expr),
                        c.primaryKey());
    }

    private static List<ViewColumnMapping> resolveViewColumns(
            List<ViewColumnMapping> cols, Scope scope) {
        return resolveList(cols, NameResolver::resolveViewColumn, scope);
    }

    private static JoinDefinition resolveJoin(JoinDefinition j, Scope scope) {
        RelationalOperation op = resolveRelOp(j.operation(), scope);
        return op == j.operation() ? j : new JoinDefinition(j.name(), nn(op));
    }

    private static List<JoinDefinition> resolveJoins(
            List<JoinDefinition> joins, Scope scope) {
        return resolveList(joins, NameResolver::resolveJoin, scope);
    }

    private static FilterDefinition resolveFilter(FilterDefinition f, Scope scope) {
        RelationalOperation cond = resolveRelOp(f.condition(), scope);
        return cond == f.condition() ? f
                : new FilterDefinition(f.name(), nn(cond));
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
        // jsonConnections (List<JsonModelConnection>) carry only literal
        // JSON payloads; no element FQN inside the public record shape.
        Map<String, List<String>> bindings = new java.util.LinkedHashMap<>();
        boolean bindingsChanged = false;
        for (Map.Entry<String, List<String>> e
                : rd.connectionBindings().entrySet()) {
            String store = resolveName(e.getKey(), scope);
            List<String> conns = resolveFqnList(e.getValue(), scope);
            bindingsChanged |= !store.equals(e.getKey())
                    || conns != e.getValue();
            bindings.put(store, conns);
        }
        List<PackageableElement> inline = new java.util.ArrayList<>();
        boolean inlineChanged = false;
        for (PackageableElement el : rd.inlineConnections()) {
            PackageableElement r = resolveElement(el, scope);
            inlineChanged |= r != el;
            inline.add(r);
        }
        if (mappings == rd.mappings() && !bindingsChanged && !inlineChanged) {
            return rd;
        }
        return new RuntimeDefinition(rd.qualifiedName(), mappings, bindings,
                rd.jsonConnections(), inline);
    }

    private static ServiceDefinition resolveService(
            ServiceDefinition sd, Scope scope) {
        ValueSpecification body = resolveVs(sd.functionBody(), scope);
        String mappingRef = sd.mappingRef() == null
                ? null : resolveName(sd.mappingRef(), scope);
        String runtimeRef = sd.runtimeRef() == null
                ? null : resolveName(sd.runtimeRef(), scope);
        ServiceDefinition.MultiExecution multi = sd.multiExecution();
        if (multi != null) {
            java.util.List<ServiceDefinition.KeyedExecution> ks =
                    new java.util.ArrayList<>(multi.executions().size());
            boolean changed = false;
            for (ServiceDefinition.KeyedExecution k : multi.executions()) {
                String m = k.mapping() == null ? null
                        : resolveName(k.mapping(), scope);
                String r = k.runtime() == null ? null
                        : resolveName(k.runtime(), scope);
                changed |= !Objects.equals(m, k.mapping())
                        || !Objects.equals(r, k.runtime());
                ks.add(new ServiceDefinition.KeyedExecution(k.keyValue(), m, r));
            }
            if (changed) {
                multi = new ServiceDefinition.MultiExecution(multi.key(), ks);
            }
        }
        if (body == sd.functionBody()
                && Objects.equals(mappingRef, sd.mappingRef())
                && Objects.equals(runtimeRef, sd.runtimeRef())
                && multi == sd.multiExecution()) {
            return sd;
        }
        return new ServiceDefinition(sd.qualifiedName(), sd.pattern(), nn(body),
                sd.documentation(), mappingRef, runtimeRef,
                sd.testSuitesSource(), sd.owners(), sd.autoActivateUpdates(),
                multi, sd.testSource());
    }

    private static com.legend.model.DataSpaceDefinition resolveDataSpace(
            com.legend.model.DataSpaceDefinition ds, Scope scope) {
        var contexts = new java.util.ArrayList<com.legend.model
                .DataSpaceDefinition.ExecutionContext>(
                        ds.executionContexts().size());
        boolean changed = false;
        for (var ctx : ds.executionContexts()) {
            // 4.145.0: a context may name a mapping PROVIDER instead of a
            // mapping, and its default runtime is optional
            String m = ctx.mapping() == null ? null : resolveName(ctx.mapping(), scope);
            String r = ctx.defaultRuntime() == null ? null
                    : resolveName(ctx.defaultRuntime(), scope);
            changed |= !java.util.Objects.equals(m, ctx.mapping())
                    || !java.util.Objects.equals(r, ctx.defaultRuntime());
            contexts.add(new com.legend.model.DataSpaceDefinition
                    .ExecutionContext(ctx.name(), ctx.title(),
                            ctx.description(), m, r, ctx.testDataSource()));
        }
        var executables = new java.util.ArrayList<com.legend.model
                .DataSpaceDefinition.Executable>(ds.executables().size());
        for (var e : ds.executables()) {
            // a function-pointer executable keeps its full signature text
            // and is not a resolvable FQN
            String ex = e.executable() == null
                    || e.executable().indexOf('(') >= 0 ? e.executable()
                    : resolveName(e.executable(), scope);
            changed |= !Objects.equals(ex, e.executable());
            executables.add(new com.legend.model.DataSpaceDefinition
                    .Executable(e.id(), e.title(), e.description(), ex,
                            e.querySource(), e.executionContextKey()));
        }
        var diagrams = new java.util.ArrayList<com.legend.model
                .DataSpaceDefinition.Diagram>(ds.diagrams().size());
        for (var g : ds.diagrams()) {
            String d = resolveName(g.diagram(), scope);
            changed |= !d.equals(g.diagram());
            diagrams.add(new com.legend.model.DataSpaceDefinition
                    .Diagram(g.title(), g.description(), d));
        }
        return changed ? new com.legend.model.DataSpaceDefinition(
                ds.qualifiedName(), contexts, ds.defaultExecutionContext(),
                ds.title(), ds.description(), executables, diagrams,
                ds.supportInfoSource(), ds.elements()) : ds;
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

    private static @com.legend.Nullable TableReference resolveTableReference(
            @com.legend.Nullable TableReference t, Scope scope) {
        if (t == null) return null;
        String db = resolveName(t.database(), scope);
        return db.equals(t.database()) ? t : new TableReference(db, t.table());
    }

    private static @com.legend.Nullable FilterMapping resolveFilterMapping(
            @com.legend.Nullable FilterMapping fm, Scope scope) {
        if (fm == null) return null;
        return switch (fm) {
            case FilterMapping.Direct d -> {
                FilterPointer fp = resolveFilterPointer(d.filter(), scope);
                yield fp == d.filter() ? d : new FilterMapping.Direct(fp);
            }
            case FilterMapping.JoinMediated jm -> {
                String sdb = jm.sourceDb();
                String src = sdb == null ? null : resolveName(sdb, scope);
                List<JoinChainElement> joins = resolveJoinChain(jm.joins(), scope);
                FilterPointer fp = resolveFilterPointer(jm.filter(), scope);
                if (Objects.equals(src, jm.sourceDb()) && joins == jm.joins()
                        && fp == jm.filter()) yield jm;
                // joinType MUST ride the rebuild (the compat-ctor
                // field-wipe family: this exact line silently dropped the
                // (INNER) annotation and un-walled wrong rows)
                yield new FilterMapping.JoinMediated(src, joins, fp,
                        jm.joinType());
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
        String jdb = jce.databaseName();
        if (jdb == null) {
            return jce;
        }
        String db = resolveName(jdb, scope);
        return db.equals(jdb) ? jce
                : new JoinChainElement(jce.joinName(), jce.joinType(), db, jce.includeSelf());
    }

    private static List<JoinChainElement> resolveJoinChain(
            List<JoinChainElement> chain, Scope scope) {
        return resolveList(chain, NameResolver::resolveJoinChainElement, scope);
    }

    // =================================================================
    // RelationalOperation (10 variants)
    // =================================================================

    private static @com.legend.Nullable RelationalOperation resolveRelOp(
            @com.legend.Nullable RelationalOperation op, Scope scope) {
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
            // 4.145.0 relational lambdas: names resolve inside the body
            case RelationalOperation.Lambda lam -> new RelationalOperation.Lambda(
                    lam.parameters(), java.util.Objects.requireNonNull(resolveRelOp(lam.body(), scope)));
            case RelationalOperation.LambdaParam p -> p;
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
                        : new RelationalOperation.Comparison(nn(l), c.op(), nn(r));
            }
            case RelationalOperation.BooleanOp b -> {
                RelationalOperation l = resolveRelOp(b.left(), scope);
                RelationalOperation r = resolveRelOp(b.right(), scope);
                yield (l == b.left() && r == b.right()) ? b
                        : new RelationalOperation.BooleanOp(nn(l), b.op(), nn(r));
            }
            case RelationalOperation.IsNull n -> {
                RelationalOperation o = resolveRelOp(n.operand(), scope);
                yield o == n.operand() ? n : new RelationalOperation.IsNull(nn(o));
            }
            case RelationalOperation.IsNotNull n -> {
                RelationalOperation o = resolveRelOp(n.operand(), scope);
                yield o == n.operand() ? n
                        : new RelationalOperation.IsNotNull(nn(o));
            }
            case RelationalOperation.Group g -> {
                RelationalOperation inner = resolveRelOp(g.inner(), scope);
                yield inner == g.inner() ? g
                        : new RelationalOperation.Group(nn(inner));
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
        return resolveList(ops,
                (x, sc) -> Objects.requireNonNull(resolveRelOp(x, sc)), scope);
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

    private static @com.legend.Nullable ValueSpecification resolveVs(
            @com.legend.Nullable ValueSpecification vs, Scope scope) {
        if (vs == null) return null;
        return switch (vs) {
            // A path literal KEEPS its node through resolution (its navigation
            // lambda resolves inside it): the alias of #/A/b!alias# is the node's
            // own field (real pure Path.name) and the project/sort checkers read
            // it there — until the 2026-09-11 audit the node dissolved here and
            // an invented `pathWithAlias` carrier smuggled the alias past this
            // line. The Typer types the node as its lambda.
            case PathLiteral pl -> pl.withChildren(java.util.List.of(
                    java.util.Objects.requireNonNull(resolveVs(pl.desugared(), scope))));
            case com.legend.protocol.spec.CByteArray b -> b;   // literal
            // an inline SQL island has no names to resolve; the typer refuses it
            case com.legend.protocol.spec.SqlIsland si -> si;
            case com.legend.protocol.spec.GqlIsland gi -> gi;
            // a TDS literal dissolves into its desugared tds(...) call
            case com.legend.protocol.spec.TdsLiteral tl ->
                    resolveVs(tl.desugared(), scope);
            case com.legend.protocol.spec.GraphFetchLiteral gf ->
                    resolveVs(gf.desugared(), scope);
            case com.legend.protocol.spec.QuotedTreeCall q ->
                    new com.legend.protocol.spec.QuotedTreeCall(
                            (com.legend.protocol.spec.AppliedFunction)
                                    java.util.Objects.requireNonNull(
                                            resolveVs(q.original(), scope)),
                            q.tree(), q.pos());
            // the grammar carrier's lambdas resolve in the ENCLOSING scope
            // (the payload spells its elements fully qualified; the
            // engine's native compiles it against the same model)
            case com.legend.protocol.spec.QuotedGrammarCall q ->
                    new com.legend.protocol.spec.QuotedGrammarCall(
                            (com.legend.protocol.spec.AppliedFunction)
                                    java.util.Objects.requireNonNull(
                                            resolveVs(q.original(), scope)),
                            q.functions().stream()
                                    .map(f -> (com.legend.protocol.spec.LambdaFunction)
                                            java.util.Objects.requireNonNull(
                                                    resolveVs(f, scope)))
                                    .toList(),
                            q.pos());
            case PackageableElementPtr ptr -> {
                // a UNIT reference Measure~unit (m3): the MEASURE resolves
                // through the import tiers, the unit part rides along
                int tilde = ptr.fullPath().indexOf('~');
                if (tilde > 0) {
                    String base = resolveName(ptr.fullPath().substring(0, tilde), scope);
                    String q = base + ptr.fullPath().substring(tilde);
                    yield q.equals(ptr.fullPath()) ? ptr : new PackageableElementPtr(q);
                }
                String r = resolveName(ptr.fullPath(), scope);
                // A BARE MANGLED function id in value position (leg 4:
                // contains(x, comparator_A_1__A_1__Boolean_1_) — the
                // reference tests pass same-package functions BY REFERENCE
                // via their signature id): the id itself is never a known
                // FQN (the catalog keys base names), so resolve the BASE
                // through the same import/own-package tiers and re-attach
                // the tail; the Typer's function-reference eta-expansion
                // consumes the qualified id.
                // The base is found by CUTTING, not by decoding the tail:
                // every "_" is a candidate cut, the longest base the
                // import tiers resolve wins (the Typer then spells each
                // declaration's engine id and keeps the exact match —
                // SignatureMangle; Phase 5 batch 147)
                if (r.equals(ptr.fullPath()) && !r.contains("::")) {
                    for (int i = r.length() - 1; i > 0; i--) {
                        if (r.charAt(i) != '_') {
                            continue;
                        }
                        String base = r.substring(0, i);
                        String rb = resolveName(base, scope);
                        if (!rb.equals(base)) {
                            r = rb + r.substring(i);
                            break;
                        }
                    }
                }
                yield r.equals(ptr.fullPath()) ? ptr : new PackageableElementPtr(r);
            }
            case EnumValue ev -> {
                String r = resolveName(ev.fullPath(), scope);
                yield r.equals(ev.fullPath()) ? ev : new EnumValue(r, ev.value());
            }
            case AppliedFunction af -> {
                List<String> matches = resolveNameMulti(af.function(), scope);
                // CALL position: several imported packages defining the name
                // is NOT an error — the candidates travel on the node and
                // the Typer unions their overloads (real pure's function
                // matching collects across imports; signature picks).
                // The platform prelude JOINS the union rather than being
                // shadowed: real pure has no user/platform tiering for
                // function matching — legend-pure's platform schema(db,
                // name) coexists with core_relational's relation::
                // schema(rel) and the call's shape picks (the global
                // corpus compile made both visible at once).
                boolean captured = !(matches.size() == 1
                        && matches.get(0).equals(af.function()));
                if (captured && scope.prelude()
                        && !af.function().contains("::")) {
                    List<String> merged = null;
                    for (var nf : com.legend.builtin.Pure
                            .nativeFunctionsAt(af.function())) {
                        String nfq = nf.qualifiedName();
                        if (!matches.contains(nfq)
                                && (merged == null || !merged.contains(nfq))) {
                            if (merged == null) {
                                merged = new ArrayList<>(matches);
                            }
                            merged.add(nfq);
                        }
                    }
                    if (merged != null) {
                        matches = merged;
                    }
                }
                String fn = matches.size() == 1
                        ? normalizePlatformFunction(matches.get(0))
                        : af.function();
                List<String> candidates = matches.size() > 1 ? matches : List.of();
                List<ValueSpecification> params = resolveVsList(af.parameters(), scope);
                yield (fn.equals(af.function()) && params == af.parameters()
                        && candidates.isEmpty()) ? af
                        // preserve pos + the spelling markers: infix is
                        // load-bearing downstream (the emitter's
                        // key-expression rule)
                        : new AppliedFunction(fn, params, candidates, af.pos(),
                                af.propertyCall(), af.grouped(), af.infix());
            }
            case AppliedProperty ap -> {
                ValueSpecification receiver = resolveVs(ap.receiver(), scope);
                yield receiver == ap.receiver() ? ap
                        : new AppliedProperty(nn(receiver), ap.property());
            }
            case LambdaFunction lf -> resolveLambda(lf, scope);
            case Variable v -> resolveVariable(v, scope);
            case NewInstance ni -> {
                String className = resolveName(ni.className(), scope);
                List<TypeExpression> typeArgs = resolveTypeList(ni.typeArguments(), scope);
                java.util.List<NewInstance.KeyBinding> props =
                        resolveKeyBindings(ni.properties(), scope);
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
                yield new NewInstanceCast(className, typeArgs, nn(src),
                        nic.targetSetId());
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
        return resolveList(list,
                (x, sc) -> Objects.requireNonNull(resolveVs(x, sc), "resolveVs(x, sc)"), scope);
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

    private static java.util.List<NewInstance.KeyBinding> resolveKeyBindings(
            java.util.List<NewInstance.KeyBinding> props, Scope scope) {
        if (props.isEmpty()) return props;
        boolean changed = false;
        java.util.List<NewInstance.KeyBinding> out =
                new java.util.ArrayList<>(props.size());
        for (NewInstance.KeyBinding e : props) {
            KeyExpression ke = e.expression();
            ValueSpecification r = resolveVs(ke.value(), scope);
            if (r == ke.value()) {
                out.add(e);
            } else {
                out.add(new NewInstance.KeyBinding(e.key(),
                        ke.withValue(nn(r))));
                changed = true;
            }
        }
        // NOT Map.copyOf: its iteration order is randomized by a per-JVM-run
        // salt (java.util.ImmutableCollections.SALT, seeded from nanoTime), so
        // copying here THREW AWAY the LinkedHashMap order built two lines up.
        // ^Class(...) property checks report the FIRST failing property, so the
        // wall text for an ill-typed instantiation changed between runs and the
        // corpus scoreboard was not byte-reproducible. NewInstance's own compact
        // constructor documents this exact hazard; the damage was done before it.
        return changed ? java.util.List.copyOf(out) : props;
    }

    private static TypeAnnotation.RelationShape.Column resolveRelationShapeColumn(
            TypeAnnotation.RelationShape.Column c, Scope scope) {
        TypeAnnotation inner = resolveTypeAnnotation(c.type(), scope);
        return inner == c.type() ? c
                : new TypeAnnotation.RelationShape.Column(c.name(), inner, c.multiplicity(),
                        c.pos());
    }

    private static ColSpec resolveColSpec(ColSpec cs, Scope scope) {
        LambdaFunction fn1 = cs.function1();
        LambdaFunction fn2 = cs.function2();
        LambdaFunction r1 = fn1 == null ? null : resolveLambda(fn1, scope);
        LambdaFunction r2 = fn2 == null ? null : resolveLambda(fn2, scope);
        // qualifier CALL args (graph-tree synonymByType(ProductSynonymType
        // .CUSIP)) carry names too — un-resolved they reach the checker bare
        List<ValueSpecification> ra = resolveList(cs.args(),
                (x, sc) -> Objects.requireNonNull(resolveVs(x, sc), "resolveVs(x, sc)"), scope);
        return (r1 == fn1 && r2 == fn2 && ra == cs.args()) ? cs
                : new ColSpec(cs.name(), r1, r2, cs.alias(), ra,
                        cs.qualified());
    }

    private static TypeAnnotation resolveTypeAnnotation(
            TypeAnnotation ta, Scope scope) {
        return switch (ta) {
            case TypeAnnotation.Named named -> {
                TypeExpression t = resolveType(named.type(), scope);
                yield t == named.type() ? named : new TypeAnnotation.Named(nn(t));
            }
            case TypeAnnotation.Wildcard ignored -> ta;
            case TypeAnnotation.MultiplicityRef ignored -> ta;   // no names to resolve
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

    /** The passthrough invariant, asserted: a non-null input resolved
     * a non-null output (every resolve* is null-in-null-out). */
    private static <T> T nn(@com.legend.Nullable T v) {
        return Objects.requireNonNull(v, "resolver passthrough");
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

    private static List<TypeExpression> resolveTypeList(List<TypeExpression> types, Scope scope) {
        return resolveList(types,
                (x, sc) -> Objects.requireNonNull(resolveType(x, sc), "resolveType(x, sc)"), scope);
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
            Set<String> typeParams,
            @com.legend.Nullable String ownPackage,
            boolean prelude) {

        public Scope {
            Objects.requireNonNull(imports, "imports");
            Objects.requireNonNull(knownFqns, "knownFqns");
            Objects.requireNonNull(typeParams, "typeParams");
        }

        /** Empty type-param scope, no own package, NO prelude — the raw
         * {@code resolve(model, knownFqns)} entry's contract (bare
         * primitives pass through for the downstream primitive
         * machinery; prelude fires only at the prelude-aware entries). */
        public static Scope of(ImportScope imports, Set<String> knownFqns) {
            return new Scope(imports, knownFqns, Set.of(), null, false);
        }

        /** Prelude-aware scope — the {@code resolve(ParsedModel)} and
         * query entries (the old withPrelude wiring, as a fallback tier). */
        public static Scope preludeOf(ImportScope imports, Set<String> knownFqns) {
            return new Scope(imports, knownFqns, Set.of(), null, true);
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
            return new Scope(imports, knownFqns, Set.copyOf(merged), ownPackage,
                    prelude);
        }
    }
}
