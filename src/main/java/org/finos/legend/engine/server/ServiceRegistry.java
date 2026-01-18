package org.finos.legend.engine.server;

import org.finos.legend.pure.dsl.definition.ServiceDefinition;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Registry for hosted services.
 * 
 * Maintains a list of registered services and provides matching based on URL
 * patterns.
 * Supports path parameters (e.g., /api/person/{id}).
 */
public final class ServiceRegistry {

    private final List<RegisteredService> services = new ArrayList<>();

    /**
     * Registers a service.
     * 
     * @param definition The service definition
     * @param executor   The executor that handles this service's requests
     */
    public void register(ServiceDefinition definition, ServiceExecutor executor) {
        services.add(new RegisteredService(definition, executor));
    }

    /**
     * Finds a service matching the given path.
     * 
     * @param path The request path (e.g., "/api/person/Smith")
     * @return Optional containing the match result with extracted parameters
     */
    public Optional<ServiceMatch> findService(String path) {
        for (RegisteredService service : services) {
            var match = service.match(path);
            if (match.isPresent()) {
                return match;
            }
        }
        return Optional.empty();
    }

    /**
     * @return All registered services
     */
    public List<RegisteredService> getServices() {
        return Collections.unmodifiableList(services);
    }

    /**
     * A registered service with its compiled pattern.
     */
    public static final class RegisteredService {
        private final ServiceDefinition definition;
        private final ServiceExecutor executor;
        private final Pattern pattern;

        public RegisteredService(ServiceDefinition definition, ServiceExecutor executor) {
            this.definition = definition;
            this.executor = executor;
            this.pattern = definition.toRegexPattern();
        }

        public ServiceDefinition definition() {
            return definition;
        }

        public ServiceExecutor executor() {
            return executor;
        }

        /**
         * Attempts to match the given path against this service's pattern.
         */
        public Optional<ServiceMatch> match(String path) {
            Matcher matcher = pattern.matcher(path);
            if (matcher.matches()) {
                Map<String, String> params = new HashMap<>();
                for (String paramName : definition.pathParams()) {
                    try {
                        params.put(paramName, matcher.group(paramName));
                    } catch (IllegalArgumentException e) {
                        // Parameter not found in pattern
                    }
                }
                return Optional.of(new ServiceMatch(this, params));
            }
            return Optional.empty();
        }
    }

    /**
     * Result of matching a request path to a service.
     * 
     * @param service        The matched service
     * @param pathParameters Extracted path parameters
     */
    public record ServiceMatch(
            RegisteredService service,
            Map<String, String> pathParameters) {
    }
}
