// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.lite.pct.extension;

import org.finos.legend.pure.m3.navigation.Instance;
import org.finos.legend.pure.m3.navigation.M3Properties;
import org.finos.legend.pure.m3.navigation.PrimitiveUtilities;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.runtime.java.interpreted.FunctionExecutionInterpreted;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE INBOUND PACKER (the transport-contingent piece of the derived
 * minimum, ADAPTER_NECESSITY_CENSUS §5b): the SEMANTIC dependency
 * roots the pure-side collectRoots walk supplies resolve in the M3
 * graph and expand — M3-recursive class extraction, enum rendering,
 * source-registry function slicing — into the model text our compiler
 * receives. No text pattern-matching anywhere (the five discovery
 * regexes died by differential, R1). This WHOLE FILE dies at the
 * Gap A structural hand-off; keeping it separate makes that future
 * deletion a file delete.
 */
final class ModelPacker {

    private final FunctionExecutionInterpreted functionExecution;

    ModelPacker(FunctionExecutionInterpreted functionExecution) {
        this.functionExecution = functionExecution;
    }
    // ===== Class metadata extraction =====

    /**
     * Extracts class definitions from the Pure interpreter for ^className() patterns
     * in the expression. Returns a map of qualified name → PureClass.
     */
    /**
     * TEST-MODEL ENUM definitions referenced by the expression (My::Enum.VALUE
     * or @My::Enum) — platform enums are registered natively in core and
     * skipped; unknown FQNs resolve against the interpreter's graph.
     */
    /** F5.7: CONCRETE test-support functions the expression references
     *  extract from the interpreter's OWN source registry (the five
     *  verbatim copies were an unversioned fork of engine test source —
     *  if upstream changed a body we silently kept testing the old one).
     *  Definition text is sliced by the function's source span;
     *  stereotype/tagged-value decorations ({@code <<...>>}, {@code
     *  {doc...}}) strip — they are PCT-harness metadata, not semantics,
     *  and their profiles are not part of the lite compile. */
    /** Slices every ConcreteFunctionDefinition matching {@code path}
     * (a {@code pkg::bareName} or a graph-mangled id) from the
     * interpreter's source registry into {@code out}, keyed
     * {@code pkg::bareName} — the mangling-free identity both discovery
     * pipelines share (F5.7's registry slice; stereotype/doc decorations
     * strip, they are PCT-harness metadata). */
    private void sliceFunctionsNamed(String path,
            java.util.Map<String, String> out, ProcessorSupport ps) {
        // a graph-mangled id (pkg::name_A_1__B_1_) resolves DIRECTLY;
        // a bare name scans the package's children by functionName
        CoreInstance direct = ps.package_getByUserPath(path);
        if (direct != null && Instance.instanceOf(direct,
                "meta::pure::metamodel::function::ConcreteFunctionDefinition", ps)) {
            sliceFunction(direct, out, ps);
            return;
        }
        int cut = path.lastIndexOf("::");
        if (cut < 0) {
            return;
        }
        CoreInstance pkg = ps.package_getByUserPath(path.substring(0, cut));
        if (pkg == null) {
            return;
        }
        String name = path.substring(cut + 2);
        for (CoreInstance child : Instance.getValueForMetaPropertyToManyResolved(
                pkg, M3Properties.children, ps)) {
            if (!Instance.instanceOf(child,
                    "meta::pure::metamodel::function::ConcreteFunctionDefinition", ps)) {
                continue;
            }
            CoreInstance fn = child.getValueForMetaPropertyToOne(
                    M3Properties.functionName);
            if (fn != null && name.equals(fn.getName())) {
                sliceFunction(child, out, ps);
            }
        }
    }

    private void sliceFunction(CoreInstance fnDef,
            java.util.Map<String, String> out, ProcessorSupport ps) {
        var si = fnDef.getSourceInformation();
        if (si == null) {
            return;
        }
        var src = functionExecution.getRuntime().getSourceById(si.getSourceId());
        if (src == null) {
            return;
        }
        // key = pkg::<graph-mangled id> — unique per OVERLOAD (a bare
        // functionName key would collapse overload siblings)
        String pkgPath = getQualifiedName(
                fnDef.getValueForMetaPropertyToOne("package"));
        String key = (pkgPath != null ? pkgPath : "") + "::" + fnDef.getName();
        if (out.containsKey(key)) {
            return;
        }
        // B4: a fixture function may NEVER shadow platform surface — if
        // the bare name resolves in lite's native catalog, injecting the
        // reference's definition would silently replace OUR semantics
        CoreInstance bareName = fnDef.getValueForMetaPropertyToOne(
                M3Properties.functionName);
        String bareFqn = (pkgPath != null ? pkgPath : "") + "::"
                + (bareName != null ? bareName.getName() : fnDef.getName());
        if (!com.legend.builtin.Pure.nativeFunctionsAt(bareFqn).isEmpty()) {
            throw new IllegalStateException("test-support function '"
                    + bareFqn + "' collides with a platform-native"
                    + " function — refusing to shadow");
        }
        String[] lines = src.getContent().split("\n", -1);
        StringBuilder def = new StringBuilder();
        for (int ln = si.getStartLine(); ln <= si.getEndLine()
                && ln <= lines.length; ln++) {
            def.append(lines[ln - 1]).append('\n');
        }
        // B3 (truthfulness burn): sliced source injects BYTE-VERBATIM —
        // the compiler accepts stereotypes and tagged values on injected
        // functions (probed: <<PCT.test>>, the fully-qualified spelling,
        // and {doc.doc=...} all compile and execute). The two
        // decoration-strip regexes (and their corrupt-a-'>>'-in-a-
        // string-literal failure class) are DELETED.
        out.put(key, def.toString());
    }

    /** Renders {@code Enum fqn { A, B }} into {@code out} when the fqn
     * is a real, non-native, non-metamodel Enumeration. */
    private static void renderEnumDef(String fqn,
            java.util.Map<String, String> out, ProcessorSupport ps) {
        if (fqn.startsWith("meta::pure::metamodel") || out.containsKey(fqn)) {
            return;
        }
        CoreInstance enumCls = ps.package_getByUserPath(fqn);
        if (enumCls == null
                || !Instance.instanceOf(enumCls, "meta::pure::metamodel::type::Enumeration", ps)
                || com.legend.builtin.Pure.findNativeEnum(fqn).isPresent()) {
            return;
        }
        StringBuilder def = new StringBuilder("Enum ").append(fqn).append(" { ");
        boolean first = true;
        for (CoreInstance v : Instance.getValueForMetaPropertyToManyResolved(
                enumCls, M3Properties.values, ps)) {
            if (!first) {
                def.append(", ");
            }
            def.append(v.getName());
            first = false;
        }
        def.append(" }");
        out.put(fqn, def.toString());
    }

    // ===== R1 — the canonical injection map (differential seam) =====

    /** One comparable identity for an injection, whichever discovery
     * produced it: {@code E:fqn} / {@code C:fqn} / {@code F:pkg::id}
     * → definition text. */
    private static java.util.TreeMap<String, String> canonicalInjection(
            java.util.Map<String, String> enums,
            java.util.Map<String, String> classes,
            java.util.Map<String, String> fns) {
        var out = new java.util.TreeMap<String, String>();
        enums.forEach((k, v) -> out.put("E:" + k, v));
        classes.forEach((k, v) -> out.put("C:" + k, v));
        fns.forEach((k, v) -> out.put("F:" + k, v));
        return out;
    }

    /** The injection built from the SEMANTIC roots the pure-side
     * collectRoots walk supplied — no text pattern-matching anywhere:
     * each root resolves in the M3 graph and dispatches on what it IS;
     * classes expand through the same M3-recursive extraction. */
    java.util.TreeMap<String, String> injectionFromRoots(
            java.util.List<String> roots, ProcessorSupport ps) {
        Map<String, String> classes = new HashMap<>();
        Set<String> visited = new HashSet<>();
        java.util.Set<String> enums = new java.util.LinkedHashSet<>();
        var enumDefs = new java.util.LinkedHashMap<String, String>();
        var fnDefs = new java.util.LinkedHashMap<String, String>();
        for (String fqn : roots) {
            if (fqn.startsWith("meta::pure::metamodel")
                    || fqn.startsWith("meta::pure::precisePrimitives")) {
                continue;
            }
            CoreInstance el = ps.package_getByUserPath(fqn);
            if (el != null && Instance.instanceOf(el,
                    "meta::pure::metamodel::type::PrimitiveType", ps)) {
                // a PRIMITIVE is the platform's, never injected (2026-10-21: a bare 'Integer' root passed the
                // metamodel prefix and was injected as 'Class Integer extends Number', a user class the
                // compiler now refuses -- an M3 primitive type is an instance of Class)
                continue;
            }
            if (el != null && Instance.instanceOf(el,
                    "meta::pure::metamodel::type::Enumeration", ps)) {
                enums.add(fqn);
            } else if (el != null && Instance.instanceOf(el,
                    "meta::pure::metamodel::type::Class", ps)) {
                // the PLATFORM's classes are never extracted from the M3
                // graph: the catalog's hand shapes AND the generated prelude
                // module (prelude.pure — the boot layer, SYSTEM_PRELUDE_DESIGN
                // §10); a test's own classes are
                if (com.legend.builtin.Pure.findNativeClass(fqn).isEmpty() && !com.legend.builtin.Prelude.classFqns().contains(fqn)) {
                    extractClassRecursive(fqn, classes, visited, enums, ps);
                }
            } else if (fqn.contains("::tests::")) {
                sliceFunctionsNamed(fqn, fnDefs, ps);
            }
        }
        for (String fqn : enums) {
            renderEnumDef(fqn, enumDefs, ps);
        }
        return canonicalInjection(enumDefs, classes, fnDefs);
    }

    // (extractClassMetadata — the regex-driven root discovery — is
    // DELETED: R1 differential 2026-08-27. injectionFromRoots consumes
    // the pure walk's roots; extractClassRecursive below remains the
    // M3-recursive expansion it always was.)

    private void extractClassRecursive(String className, Map<String, String> classes,
                                       Set<String> visited, java.util.Set<String> discoveredEnums,
                                       ProcessorSupport ps) {
        if (visited.contains(className)) return;
        visited.add(className);

        CoreInstance cls = ps.package_getByUserPath(className);
        if (cls == null) return;
        // An ENUMERATION is a class in M3 (it has a 'name' property) but must
        // inject as an Enum — injecting both a Class and an Enum under one
        // FQN split the type in two ("expected X, got X" with identical names).
        if (Instance.instanceOf(cls, "meta::pure::metamodel::type::Enumeration", ps)) {
            discoveredEnums.add(className);
            return;
        }
        // B11 (truthfulness burn): a GENERIC user class cannot extract
        // faithfully without emitting its type parameters — refusing
        // LOUDLY beats silently degrading them (the old shape dropped
        // parameter-typed properties without a trace). Add parameter
        // emission when a witness demands it. (Platform generics —
        // Pair/List/Map — never reach here: the native filter refuses
        // them upstream.)
        if (!cls.getValueForMetaPropertyToMany("typeParameters").isEmpty()) {
            throw new UnsupportedOperationException("generic user class '"
                    + className + "' in test-model extraction — no silent"
                    + " property degradation");
        }

        List<String> propertyLines = new ArrayList<>();
        for (CoreInstance prop : ps.class_getSimpleProperties(cls)) {
            CoreInstance nameInstance = prop.getValueForMetaPropertyToOne(M3Properties.name);
            if (nameInstance == null) continue;
            String propName = PrimitiveUtilities.getStringValue(nameInstance);
            if (propName == null) continue;

            CoreInstance mult = prop.getValueForMetaPropertyToOne(M3Properties.multiplicity);
            if (mult == null) continue;
            int upper = org.finos.legend.pure.m3.navigation.multiplicity.Multiplicity
                    .multiplicityUpperBoundToInt(mult);
            int lower = org.finos.legend.pure.m3.navigation.multiplicity.Multiplicity
                    .multiplicityLowerBoundToInt(mult);
            String multiplicity = multText(lower, upper < 0 ? null : upper);

            CoreInstance genericType = prop.getValueForMetaPropertyToOne(M3Properties.genericType);
            CoreInstance rawType = (genericType != null)
                    ? genericType.getValueForMetaPropertyToOne(M3Properties.rawType) : null;
            if (rawType != null) {
                rawType = org.finos.legend.pure.m3.navigation.importstub.ImportStub
                        .withImportStubByPass(rawType, ps);
            }
            // Resolve the property type via FQN. Primitives print as their
            // Pure simple names ("Integer"); classes and enums print as FQNs
            // (the engine PureClass.toString convention, preserved verbatim).
            // rawType==null means "no declared type" → Any.
            String typeRef;
            if (rawType == null) {
                typeRef = "Any";
            } else {
                String qualifiedTypeName = getQualifiedName(rawType);
                // a primitive is judged by its M3 kind (2026-10-21: matched by lite's full names, a primitive's
                // bare M3 path -- 'Integer' -- missed, and the primitive was injected as a class)
                if (qualifiedTypeName != null && Instance.instanceOf(rawType,
                        "meta::pure::metamodel::type::PrimitiveType", ps)) {
                    typeRef = qualifiedTypeName;
                } else if (qualifiedTypeName == null) {
                    // B11: no silent shape change — a property whose type
                    // has no packageable path names itself and refuses
                    throw new UnsupportedOperationException("property '"
                            + propName + "' of '" + className + "' has a"
                            + " type with no packageable path — refusing"
                            + " to silently drop it");
                } else if (qualifiedTypeName.startsWith("meta::pure::metamodel")) {
                    // metamodel-typed properties are reflection surface,
                    // not model shape lite can host — the ONE documented
                    // skip (a test USING such a property fails loudly at
                    // its own compile)
                    continue;
                } else if (Instance.instanceOf(rawType,
                        "meta::pure::metamodel::type::Enumeration", ps)) {
                    // enum-typed property: reference by name; the DEFINITION
                    // is injected as an Enum, never as a shadow Class
                    discoveredEnums.add(qualifiedTypeName);
                    typeRef = qualifiedTypeName;
                } else {
                    extractClassRecursive(qualifiedTypeName, classes, visited, discoveredEnums, ps);
                    if (!classes.containsKey(qualifiedTypeName)) continue;
                    typeRef = qualifiedTypeName;
                }
            }
            propertyLines.add(propName + ": " + typeRef + multiplicity);
        }

        // SUPERCLASSES ride along (concatenate's LUB is the common
        // supertype; without the extends chain everything collapses to Any)
        List<String> superFqns = new ArrayList<>();
        for (CoreInstance gen : Instance.getValueForMetaPropertyToManyResolved(
                cls, M3Properties.generalizations, ps)) {
            CoreInstance general = gen.getValueForMetaPropertyToOne(M3Properties.general);
            CoreInstance raw = general == null ? null
                    : general.getValueForMetaPropertyToOne(M3Properties.rawType);
            if (raw != null) {
                raw = org.finos.legend.pure.m3.navigation.importstub.ImportStub
                        .withImportStubByPass(raw, ps);
            }
            String superFqn = raw == null ? null : getQualifiedName(raw);
            if (superFqn != null && !superFqn.startsWith("meta::pure::metamodel")) {
                extractClassRecursive(superFqn, classes, visited, discoveredEnums, ps);
                if (classes.containsKey(superFqn)) {
                    superFqns.add(superFqn);
                }
            }
        }

        String qualifiedName = getQualifiedName(cls);
        String fqn = qualifiedName != null ? qualifiedName : className;
        var sb = new StringBuilder("Class ").append(fqn);
        if (!superFqns.isEmpty()) {
            sb.append(" extends ").append(String.join(", ", superFqns));
        }
        sb.append(" {\n");
        for (String line : propertyLines) {
            sb.append("    ").append(line).append(";\n");
        }
        sb.append("}");
        classes.put(fqn, sb.toString());
    }

    /** The Pure multiplicity print form: [1], [0..1], [*], [2..*]. */
    private static String multText(int lower, Integer upper) {
        if (upper == null) {
            return lower == 0 ? "[*]" : "[" + lower + "..*]";
        }
        if (lower == upper) {
            return "[" + upper + "]";
        }
        return "[" + lower + ".." + upper + "]";
    }

    private String getQualifiedName(CoreInstance element) {
        try {
            return org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement
                    .getUserPathForPackageableElement(element);
        } catch (Exception e) {
            return null;
        }
    }
}
