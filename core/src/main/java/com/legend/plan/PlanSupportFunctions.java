// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's freemarker SUPPORT-FUNCTION constants — every relational
 * execution node carries them ({@code supportFunctions =
 * relationalPlanSupportFunctions($connection)},
 * relationalMappingExecution.pure:222/:273), and the plan dedups the union
 * into {@code ExecutionPlan.processingTemplateFunctions}
 * (executionPlan_generation.pure:215). Strings are byte-exact ports of the
 * pure constant functions (relationalMappingExecution.pure) — the corpus
 * asserts them literally (templateFunctionsList()).
 */
public final class PlanSupportFunctions {

    private PlanSupportFunctions() {
    }

    /** collectionRenderFunction(). */
    public static final String RENDER_COLLECTION =
            "<#function renderCollection collection separator prefix suffix"
            + " replacementMap defaultValue><#if collection?size == 0>"
            + "<#return defaultValue></#if>"
            + "<#assign newCollection = collection>"
            + "<#list replacementMap as oldValue, newValue>"
            + "   <#assign newCollection = collection?map(ele ->"
            + " ele?replace(oldValue, newValue))></#list>"
            + "<#return prefix + newCollection?join(suffix + separator +"
            + " prefix) + suffix></#function>";

    /** collectionSizeFunction(). */
    public static final String COLLECTION_SIZE =
            "<#function collectionSize collection> <#return"
            + " collection?size?c> </#function>";

    /** optionalVarPlaceHolderOperationSelectorFunction(). */
    public static final String OPTIONAL_VAR_SELECTOR =
            "<#function optionalVarPlaceHolderOperationSelector"
            + " optionalParameter trueClause falseClause>"
            + "<#if optionalParameter?has_content ||"
            + " optionalParameter?is_string><#return trueClause>"
            + "<#else><#return falseClause></#if></#function>";

    /** varPlaceHolderToStringFunction(). */
    public static final String VAR_PLACE_HOLDER_TO_STRING =
            "<#function varPlaceHolderToString optionalParameter prefix"
            + " suffix replacementMap defaultValue>"
            + "<#if optionalParameter?is_enumerable &&"
            + " !optionalParameter?has_content><#return defaultValue>"
            + "<#else><#assign newParam = optionalParameter>"
            + "<#list replacementMap as oldValue, newValue>"
            + "   <#assign newParam = newParam?replace(oldValue,"
            + " newValue)></#list>"
            + "<#return prefix + newParam + suffix></#if></#function>";

    /** equalEnumOperationSelectorFunction(). */
    public static final String EQUAL_ENUM_SELECTOR =
            "<#function equalEnumOperationSelector enumVal inDyna"
            + " equalDyna><#assign enumList = enumVal?split(\",\")>"
            + "<#if enumList?size = 1><#return equalDyna>"
            + "<#else><#return inDyna></#if></#function>";

    /** utcToTimeZoneSupportFunction() — prepended only for a connection
     * with a non-default timeZone. */
    public static final String UTC_TO_TIME_ZONE =
            "<#function GMTtoTZ tz paramDate>"
            + "<#if paramDate?is_enumerable && !paramDate?has_content>"
            + "<#return paramDate><#else>"
            + "<#return (tz+\" \"+paramDate)?date.@alloyDate></#if>"
            + "</#function>"
            + "<#function renderCollectionWithTz collection timeZone"
            + " separator prefix suffix defaultValue>"
            + "<#assign result = [] />"
            + "<#list collection as c>"
            + "<#assign result = [prefix + (timeZone+\" \"+c)?date"
            + ".@alloyDate + suffix] + result></#list>"
            + "<#return result?reverse?join(separator, defaultValue)>"
            + "</#function>";

    /** The DYNAMIC freemarker enum-map function an enum-typed plan
     * parameter adds to processingTemplateFunctions — enum value to
     * source values (strings quoted, integers bare), the engine's
     * relationalMappingExecution enum-template emission. */
    public static String enumMapTemplateFunction(String fnName,
            com.legend.model.EnumerationMapping em) {
        StringBuilder map = new StringBuilder();
        for (var vm : em.valueMappings()) {
            if (map.length() > 0) {
                map.append(", ");
            }
            StringBuilder src = new StringBuilder();
            for (var sv : vm.sourceValues()) {
                if (src.length() > 0) {
                    src.append(", ");
                }
                src.append(switch (sv) {
                    case com.legend.model.EnumerationMapping.SourceValue
                            .StringValue st -> "'" + st.value() + "'";
                    case com.legend.model.EnumerationMapping.SourceValue
                            .IntegerValue iv -> String.valueOf(iv.value());
                    default -> throw new com.legend.error
                            .NotImplementedException("plan: enum-map"
                            + " template for source value " + sv);
                });
            }
            map.append('"').append(vm.enumValue()).append("\":\"")
                    .append(src).append('"');
        }
        return "<#function " + fnName + " inputVal><#assign enumMap = {"
                + map + "}><#if inputVal?has_content>"
                + "<#if inputVal?is_sequence><#assign results = []>"
                + "<#list inputVal as item>"
                + "<#assign results += [enumMap[item]!\"\"]></#list>"
                + "<#return results?join(\", \")>"
                + "<#else><#return enumMap[inputVal]!\"\"></#if>"
                + "<#else><#return \"\"> </#if> </#function>";
    }

    /** {@code relationalPlanSupportFunctions(connection)} — population
     * order preserved; {@code timeZone} null/GMT omits the TZ pair. */
    public static List<String> relationalPlanSupportFunctions(
            @com.legend.base.Nullable String timeZone) {
        List<String> out = new ArrayList<>();
        if (timeZone != null && !timeZone.equals("GMT")
                && !timeZone.equals("UTC")) {
            out.add(UTC_TO_TIME_ZONE);
        }
        out.add(RENDER_COLLECTION);
        out.add(COLLECTION_SIZE);
        out.add(OPTIONAL_VAR_SELECTOR);
        out.add(VAR_PLACE_HOLDER_TO_STRING);
        out.add(EQUAL_ENUM_SELECTOR);
        return out;
    }
}
