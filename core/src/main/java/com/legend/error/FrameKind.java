// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * WHERE AN ERROR PASSED THROUGH, AS DATA (stage B's BE, the one error model, 2026-11-21): the context lite's compiler
 * adds around an error raised inside it -- the function whose body it was, the calls it was inlined through -- each a
 * frame of the error's record ({@link ErrorRecord.Frame}), worded around the inner error by lite's templates, a text
 * before it and a text after it. Until BE these were the wrappers' stated texts (B1's prefix and suffix).
 */
public enum FrameKind implements ErrorCode {
    /** The body of a function: its name. */
    IN_FUNCTION("in function '%1$s': ", ""),
    /** A property's default value: the property, the class. */
    IN_DEFAULT_VALUE("in default value of property '%1$s' of %2$s: ", ""),
    /** A function's pre-condition: the condition's name, the function. */
    IN_PRE_CONDITION("in pre-condition '%1$s' of %2$s: ", ""),
    /** A function's post-condition: the condition's name, the function. */
    IN_POST_CONDITION("in post-condition '%1$s' of %2$s: ", ""),
    /** The calls an error was inlined through, outermost first: each function's name. */
    INLINED_VIA("", " [inlined via %1$s]"),
    /** A relation function's body whose columns are not its declared type's: the function, the body's columns, the
     *  declared type, its columns. */
    BODY_ANSWERS("the body of %1$s answers %2$s where its declared %3$s has %4$s: ", "");

    private final String before;
    private final String after;

    FrameKind(String before, String after) {
        this.before = before;
        this.after = after;
    }

    @Override
    public String family() {
        return "frame";
    }

    /** The frame's own text over {@code values}, with no inner error. */
    @Override
    public String sentence(java.util.List<String> values) {
        return wrap(values, "");
    }

    /** {@code inner} inside this frame over {@code values}: the text before, the inner error's, the text after. */
    public String wrap(java.util.List<String> values, String inner) {
        java.util.List<String> spelled = this == INLINED_VIA ? java.util.List.of(String.join(" -> ", values)) : values;
        Object[] args = spelled.toArray();
        return String.format(java.util.Locale.ROOT, before, args) + inner
                + String.format(java.util.Locale.ROOT, after, args);
    }
}
