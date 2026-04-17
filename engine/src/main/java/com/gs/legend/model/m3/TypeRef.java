package com.gs.legend.model.m3;

import java.util.Objects;

/**
 * A lightweight reference to a type by its fully qualified name plus its kind
 * (primitive, class, or enum). The FQN-only counterpart to the resolved-object
 * {@link Type} sealed interface.
 *
 * <p>Introduced in Phase A of the Bazel cross-project dependency work; see
 * {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2. {@link Property} carries a
 * {@code TypeRef} instead of a resolved {@link Type} so that cross-project property
 * types don't force their target classes/enums to be loaded eagerly.
 *
 * <p>Relationship to {@link Type}:
 * <ul>
 *   <li>{@link PrimitiveRef} ↔ {@link PrimitiveType}</li>
 *   <li>{@link ClassRef} ↔ {@link PureClass}</li>
 *   <li>{@link EnumRef} ↔ {@link PureEnumType}</li>
 * </ul>
 *
 * <p>Every variant holds only the FQN string — consumers that need the resolved
 * object look it up via a {@code ModelContext} on demand.
 */
public sealed interface TypeRef permits TypeRef.PrimitiveRef, TypeRef.ClassRef, TypeRef.EnumRef {

    String fqn();

    record PrimitiveRef(String fqn) implements TypeRef {
        public PrimitiveRef {
            Objects.requireNonNull(fqn, "PrimitiveRef fqn cannot be null");
        }
    }

    record ClassRef(String fqn) implements TypeRef {
        public ClassRef {
            Objects.requireNonNull(fqn, "ClassRef fqn cannot be null");
        }
    }

    record EnumRef(String fqn) implements TypeRef {
        public EnumRef {
            Objects.requireNonNull(fqn, "EnumRef fqn cannot be null");
        }
    }
}
