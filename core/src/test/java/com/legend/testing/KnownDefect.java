// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;


import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.regex.Pattern;

/**
 * A confirmed defect whose fix belongs to a later plan item (execution plan rule 0b.11, item W0.0; since D14 only a
 * cosmetic or diagnostic defect is pinned: a wrong answer, wrong binding or security defect is fixed now). The test is
 * written as the CORRECT behaviour and fails today; this annotation keeps the chain green while the defect stands
 * and turns it red the moment the defect is fixed, so the pin cannot outlive the defect silently:
 * <ul>
 *   <li>the test body throws {@link #expected} (an assertion failure by default): the defect is still present, the
 *       test passes;</li>
 *   <li>the test body completes normally: the defect is gone, the test FAILS, asking for this annotation to be
 *       removed in the push that fixed it (normally the {@link #owner} item's push);</li>
 *   <li>the body throws anything else: it propagates, because the test no longer measures the defect it names.</li>
 * </ul>
 * {@link #owner} names the plan item that fixes the defect (for example {@code "W2.5"}), so every pin has an owner
 * (standing ruling: every deferral pinned with an owner).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ExtendWith(KnownDefect.Extension.class)
public @interface KnownDefect {

    /** The execution-plan item that fixes the defect, e.g. {@code "W2.5"} or {@code "W4.3"}. */
    String owner();

    /** What is wrong, in one sentence. */
    String reason();

    /** What the test throws while the defect stands. */
    Class<? extends Throwable> expected() default AssertionError.class;

    /** The JUnit extension that inverts the outcome. */
    final class Extension implements InvocationInterceptor {

        private static final Pattern OWNER = Pattern.compile("W[0-7]\\.[0-9]+[a-z]?");

        @Override
        public void interceptTestMethod(Invocation<Void> invocation,
                ReflectiveInvocationContext<Method> invocationContext,
                ExtensionContext extensionContext) throws Throwable {
            KnownDefect pin = invocationContext.getExecutable().getAnnotation(KnownDefect.class);
            if (pin == null) {
                invocation.proceed();
                return;
            }
            Throwable thrown = null;
            try {
                invocation.proceed();
            } catch (Throwable t) {
                thrown = t;
            }
            judge(pin.owner(), pin.reason(), pin.expected(), thrown);
        }

        /**
         * The outcome of a pinned test: returns when the defect is still present, throws otherwise. Separate from
         * the interceptor so its three cases are unit-tested.
         */
        static void judge(String owner, String reason, Class<? extends Throwable> expected,
                Throwable thrown) throws Throwable {
            if (!OWNER.matcher(owner).matches()) {
                throw new AssertionError("@KnownDefect owner '" + owner
                        + "' is not an execution-plan item (W<wave>.<item>)");
            }
            if (thrown == null) {
                throw new AssertionError("KNOWN DEFECT FIXED (owner " + owner + "): " + reason
                        + " -- the test now passes; remove @KnownDefect in the push that fixed it");
            }
            if (!expected.isInstance(thrown)) {
                throw thrown;
            }
            // the defect as observed, in the test log: a pin that holds for the wrong reason is visible
            System.err.println("KNOWN DEFECT (owner " + owner + ") still stands: " + thrown);
        }
    }
}
