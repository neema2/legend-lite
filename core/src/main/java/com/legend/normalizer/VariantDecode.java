package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.spec.Calls;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.ClassDefinition;
import com.legend.platform.CoreFn;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A class instance decoded from a semi-structured value: a relational property's {@code Binding} transformer
 * ({@code firm: Binding B : [DB] T.FIRM_JSON}, engine {@code HelperRelationalBuilder.buildSemiStructuredPropertyMapping}
 * and its {@code SemiStructuredPropertyAccess} navigation).
 *
 * <p>The decode is the instance's properties read from the value by key -- {@code ^Firm(legalName =
 * to(get($v, 'legalName'), @String), address = ^Address(...), ...)} -- the shape the JSON source's mapping synthesizes
 * ({@code synthJsonSourceMapping}), so a navigation {@code $p.firm.legalName} reads one key of the column, as the
 * engine's navigation does. A class-typed value is guarded by its own emptiness ({@code if(isEmpty($v), |[],
 * |^C(...))}): an absent object is empty, never an object of nulls.
 *
 * <p>Two kinds of refusal, as the normalizer's own line draws them ({@code MappingNormalizer}, "§6's line"): what
 * legend-engine rejects (no such binding, a non-class property, a class outside the binding's model unit) is a
 * {@link ModelException}, a strict build's error; what lite cannot do yet -- a to-many class-typed property (a nested
 * array of objects: lite serves a nested object as an embedded instance only), a class reachable from itself (its
 * decode would not end) -- is DEFERRED key by key ({@link #deferred}): that key's value raises its reason when a
 * query reads it, and every other key, the class that owns the bound property and the rest of the model stay
 * readable, as legend-engine's key-by-key navigation leaves them (the stress corpus's 8118-semi.pure binds such a
 * class). Only a property whose type is no named type is a {@link NotImplementedException}, which poisons the class
 * mapping: there is no type to give its deferred value.
 */
final class VariantDecode {

    private VariantDecode() {
    }

    /**
     * The decode of a bound property: {@code binding}'s checks as the engine makes them (the binding exists; the
     * property is class-typed, "Binding transformer can be used with complex properties only"; its class is in the
     * binding's model unit), then the guarded instance.
     */
    static ValueSpecification bound(String binding, String ownerFqn, String propertyName,
            ValueSpecification value, String mappingFqn, ModelBuilder model) {
        com.legend.model.GenericSectionElementDefinition b = model.findBinding(binding).orElseThrow(() ->
                new ModelException(LegendCompileException.Phase.NORMALIZE, "Can't find binding '" + binding
                        + "' (the Binding transformer of property '" + propertyName + "'); mapping=" + mappingFqn));
        ClassDefinition owner = model.knowledge().hierarchyClass(ownerFqn).orElseThrow(() ->
                new ModelException(LegendCompileException.Phase.NORMALIZE, "Binding transformer on property '"
                        + propertyName + "' of unknown class '" + ownerFqn + "'; mapping=" + mappingFqn));
        ClassDefinition.PropertyDefinition prop = model.knowledge().propertyDef(owner, propertyName);
        if (prop == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, "Binding transformer on property '"
                    + propertyName + "', which class '" + ownerFqn + "' does not declare; mapping=" + mappingFqn);
        }
        String classFqn = prop.type() instanceof TypeExpression.NameRef nr
                && model.knowledge().hierarchyClass(nr.name()).isPresent() ? nr.name() : null;
        if (classFqn == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Binding transformer can be used with complex properties only. Property '" + propertyName
                    + "' return type is '" + typeName(prop.type()) + "'; mapping=" + mappingFqn);
        }
        if (!inModelUnit(b, classFqn)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, "Class: " + classFqn
                    + " should be included in modelUnit for binding: " + binding + "; mapping=" + mappingFqn);
        }
        if (isMany(prop.multiplicity())) {
            return deferred("property '" + propertyName + "' of '"
                    + ownerFqn + "' is to-many and bound by Binding " + binding + ": lite does not decode an array"
                    + " of objects from a semi-structured value yet; mapping=" + mappingFqn, classFqn);
        }
        return guarded(value, instance(classFqn, value, model, mappingFqn, new ArrayDeque<>()));
    }

    /**
     * The value of a key lite cannot decode yet, DEFERRED: {@code cast(fail('reason'), @C)}, a value that raises its
     * reason when it is read and costs nothing when it is not (the shape the M2M enum decode's unmapped arm has). The
     * rest of the instance, and the class that owns the bound property, stay readable; a navigation through the key
     * is refused with the reason where the query is resolved ({@code Pipelines.embeddedView}), any other read of it
     * raises the reason from the database.
     */
    private static ValueSpecification deferred(String reason, String classFqn) {
        return Calls.form(CoreFn.CAST, List.of(
                Calls.exact(Pure.FAIL__STRING_1.qualifiedName(), List.of(new CString(reason))),
                new TypeAnnotation.Named(new TypeExpression.NameRef(classFqn))));
    }

    /** {@code ^C(...)} with each of {@code classFqn}'s properties read from {@code v} by key. */
    static ValueSpecification instance(String classFqn, ValueSpecification v, ModelBuilder model,
            String mappingFqn, Deque<String> path) {
        path.addLast(classFqn);
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (ClassDefinition cd : model.knowledge().lineage(classFqn)) {
            for (ClassDefinition.PropertyDefinition p : cd.properties()) {
                if (fields.containsKey(p.name())) {
                    continue;   // nearest declaration first
                }
                fields.put(p.name(), new KeyExpression(read(classFqn, p, v, model, mappingFqn, path), false, false));
            }
        }
        path.removeLast();
        return MappingNormalizer.buildNewInstanceToOne(classFqn, fields, model);
    }

    /** One property's read from {@code v}: a value by its declared type ({@code to}, {@code toMany}), an object as
     *  its guarded instance. */
    private static ValueSpecification read(String ownerFqn, ClassDefinition.PropertyDefinition p,
            ValueSpecification v, ModelBuilder model, String mappingFqn, Deque<String> path) {
        ValueSpecification get = Calls.exact(Pure.GET__VARIANT_0_1__STRING_1.qualifiedName(),
                List.of(v, new CString(p.name())));
        if (!(p.type() instanceof TypeExpression.NameRef nr)) {
            throw new NotImplementedException("property '" + p.name() + "' of '"
                    + ownerFqn + "' has a type a semi-structured value is not decoded to: " + typeName(p.type())
                    + "; mapping=" + mappingFqn);
        }
        boolean many = isMany(p.multiplicity());
        if (model.knowledge().hierarchyClass(nr.name()).isPresent()) {
            if (many) {
                return deferred("property '" + p.name() + "' of '"
                        + ownerFqn + "' is a to-many class-typed property (" + nr.name() + "): lite does not decode"
                        + " an array of objects from a semi-structured value yet; mapping=" + mappingFqn, nr.name());
            }
            if (path.contains(nr.name())) {
                return deferred("class '" + nr.name() + "' is reachable from itself ("
                        + String.join(" > ", path) + " > " + nr.name() + "): its decode from a semi-structured value"
                        + " would not end; mapping=" + mappingFqn, nr.name());
            }
            return guarded(get, instance(nr.name(), get, model, mappingFqn, path));
        }
        TypeAnnotation.Named type = new TypeAnnotation.Named(new TypeExpression.NameRef(nr.name()));
        if (!many) {
            return Calls.form(CoreFn.TO, List.of(get, type));
        }
        ValueSpecification all = Calls.form(CoreFn.TO_MANY, List.of(get, type));
        return p.multiplicity() instanceof Multiplicity.Concrete c && c.lowerBound() >= 1
                ? Calls.exact(Pure.TO_ONE_MANY__T_MANY.qualifiedName(), List.of(all)) : all;
    }

    /** {@code if(isEmpty($v), |[], |ctor)}: an absent object is empty. */
    private static ValueSpecification guarded(ValueSpecification v, ValueSpecification ctor) {
        return Calls.form(CoreFn.IF, List.of(
                Calls.exact(Pure.IS_EMPTY__T_MANY.qualifiedName(), List.of(v)),
                new LambdaFunction(List.of(), List.of(new PureCollection(List.of()))),
                new LambdaFunction(List.of(), List.of(ctor))));
    }

    private static boolean isMany(Multiplicity m) {
        return !(m instanceof Multiplicity.Concrete c) || c.upperBound() == null || c.upperBound() > 1;
    }

    /** Whether {@code classFqn} is in the binding's model unit: an include naming it or a package above it, and no
     *  exclude naming it (engine {@code ModelUnit.resolve}). */
    private static boolean inModelUnit(com.legend.model.GenericSectionElementDefinition b, String classFqn) {
        return covers(b.fields().getOrDefault("modelIncludes", ""), classFqn)
                && !covers(b.fields().getOrDefault("modelExcludes", ""), classFqn);
    }

    private static boolean covers(String paths, String classFqn) {
        for (String path : paths.split(",")) {
            String p = path.trim();
            if (!p.isEmpty() && (p.equals(classFqn) || classFqn.startsWith(p + "::"))) {
                return true;
            }
        }
        return false;
    }

    private static String typeName(TypeExpression t) {
        return t instanceof TypeExpression.NameRef nr ? nr.name() : t.toString();
    }
}
