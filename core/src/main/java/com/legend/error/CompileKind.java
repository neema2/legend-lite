package com.legend.error;

/**
 * THE KIND OF A COMPILE ERROR, AND LITE'S SENTENCE FOR IT (phase D's D1.6, W7 of docs/records/d-plan-2026-11-16.md,
 * 2026-11-16): a compile error the user's model or query causes, named by its kind and its operands (the names the
 * error is about: an element's, a type's, a count), never by legend-pure's words -- the product says its own sentence
 * ({@link #sentence}); legend-pure's and legend-engine's sentence for each kind is the compatibility edge's
 * ({@code edge.PureMessages.compile}), which spells it where an upstream client reads the error. Each sentence is a
 * format over the operands, positional ({@code %1$s}, ...).
 */
public enum CompileKind {
    /** An element the text names that the model does not have: its kind's noun and its name. */
    MISSING("%1$s '%2$s' is not defined"),
    /** A profile the text names that the model does not have. */
    MISSING_PROFILE("profile '%1$s' is not defined"),
    /** A stereotype or a tag a profile does not define: the member's noun, its name, the profile. */
    MISSING_PROFILE_MEMBER("profile '%3$s' defines no %1$s '%2$s'"),
    /** A class mapping a union names that its mapping does not have: the set id, the mapping. */
    MISSING_CLASS_MAPPING("mapping '%2$s' has no class mapping '%1$s'"),
    /** A class mapping whose properties read several tables and names none as its main one: the class, the tables. */
    MAIN_TABLE_UNNAMED("class '%1$s' maps properties from the tables %2$s: name its main table (~mainTable)"),
    /** A class extending a type that is not a class: the class's name, the supertype. */
    INVALID_SUPERTYPE("'%1$s' cannot extend '%2$s': it is not a class"),
    /** A generic type given another number of arguments than it declares: the arguments' kind (type, multiplicity),
     *  the number given, the type as declared. */
    TYPE_ARGUMENTS_COUNT("%3$s takes another number of %1$s arguments than the %2$s given"),
    /** A type given another number of type variables than it declares: the number given, the type as declared. */
    TYPE_VARIABLES_COUNT("%2$s takes another number of type variables than the %1$s given"),
    /** A row's cell read at an offset the row does not have: the offset, the row's size. */
    ELEMENT_AT_OFFSET("offset %1$s is outside a row of %2$s cells");

    private final String sentence;

    CompileKind(String sentence) {
        this.sentence = sentence;
    }

    /** Lite's sentence of this kind over {@code operands}. */
    public String sentence(java.util.List<String> operands) {
        return String.format(java.util.Locale.ROOT, sentence, operands.toArray());
    }
}
