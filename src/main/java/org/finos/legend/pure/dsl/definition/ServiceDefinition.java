package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Pure Service definition for hosting as an HTTP endpoint.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Service package::ServiceName
 * {
 *     pattern: '/api/person/{lastName}';
 *     function: |Person.all()->filter({p | $p.lastName == $lastName});
 *     documentation: 'Returns people by last name';
 * }
 * </pre>
 * 
 * The service pattern can contain path parameters in curly braces (e.g., {id}).
 * These parameters are extracted and made available as variables in the
 * function body.
 * 
 * @param qualifiedName The fully qualified service name
 * @param pattern       The URL pattern (e.g., "/api/person/{id}")
 * @param functionBody  The Pure query expression
 * @param pathParams    List of path parameter names extracted from pattern
 * @param documentation Optional documentation string
 */
public record ServiceDefinition(
        String qualifiedName,
        String pattern,
        String functionBody,
        List<String> pathParams,
        String documentation) implements PureDefinition {

    private static final Pattern PATH_PARAM_PATTERN = Pattern.compile("\\{(\\w+)\\}");

    public ServiceDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(pattern, "Pattern cannot be null");
        Objects.requireNonNull(functionBody, "Function body cannot be null");
        pathParams = pathParams != null ? List.copyOf(pathParams) : List.of();
    }

    /**
     * @return The simple service name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * @return The package path (without simple name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }

    /**
     * Extracts path parameter names from a URL pattern.
     * 
     * @param pattern The URL pattern (e.g., "/api/person/{id}/address/{addressId}")
     * @return List of parameter names (e.g., ["id", "addressId"])
     */
    public static List<String> extractPathParams(String pattern) {
        Matcher matcher = PATH_PARAM_PATTERN.matcher(pattern);
        var params = new java.util.ArrayList<String>();
        while (matcher.find()) {
            params.add(matcher.group(1));
        }
        return List.copyOf(params);
    }

    /**
     * Converts the pattern to a regex for matching incoming requests.
     * 
     * @return Regex pattern with named groups for parameters
     */
    public Pattern toRegexPattern() {
        String regex = pattern
                .replaceAll("\\{(\\w+)\\}", "(?<$1>[^/]+)")
                .replace("/", "\\/");
        return Pattern.compile("^" + regex + "$");
    }

    /**
     * Creates a ServiceDefinition with automatic path parameter extraction.
     */
    public static ServiceDefinition of(String qualifiedName, String pattern,
            String functionBody, String documentation) {
        return new ServiceDefinition(
                qualifiedName,
                pattern,
                functionBody,
                extractPathParams(pattern),
                documentation);
    }
}
