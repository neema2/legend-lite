package com.gs.legend.plan;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.def.*;
import java.util.List;

/**
 * Plan-level specification for graphFetch JSON output.
 *
 * <p>Produced by the compiler from a parsed {@code GraphFetchTree}.
 * PlanGenerator reads this to wrap SQL output in
 * {@code json_group_array(json_object(...))}.
 *
 * <p>Analogous to {@link ProjectionSpec} and {@link SortSpec} —
 * compiler-resolved, no parser types crossing the boundary.
 *
 * @param rootClass  The target class name (e.g., "Person")
 * @param properties The properties to include in the JSON output
 */
public record GraphFetchSpec(
        String rootClass,
        List<PropertySpec> properties) {

    /**
     * A single property in the fetch spec.
     *
     * @param name   JSON key name
     * @param nested Non-null for nested objects (1-to-1 or 1-to-many).
     *               Null for scalar/primitive properties.
     */
    public record PropertySpec(
            String name,
            GraphFetchSpec nested) {

        /** Creates a scalar (non-nested) property spec. */
        public static PropertySpec scalar(String name) {
            return new PropertySpec(name, null);
        }

        /** Creates a nested property spec. */
        public static PropertySpec nested(String name, GraphFetchSpec nestedSpec) {
            return new PropertySpec(name, nestedSpec);
        }

        /** @return true if this property has nested properties */
        public boolean isNested() {
            return nested != null;
        }
    }

    /** Convenience: list of property names (flat). */
    public List<String> propertyNames() {
        return properties.stream().map(PropertySpec::name).toList();
    }
}
