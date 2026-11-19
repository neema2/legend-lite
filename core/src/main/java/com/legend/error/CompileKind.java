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
    ELEMENT_AT_OFFSET("offset %1$s is outside a row of %2$s cells"),
    /** A window frame whose literal start is after its end (the P2 checkpoint audit's M4, 2026-11-16: legend-pure's
     *  sentence was the product's until then). */
    WINDOW_FRAME_INVERTED("window frame starts after it ends"),
    /** A window frame whose computed bounds, read by how they are written (-(n) before the current row, any other n
     *  after it), start after they end (register S170). */
    WINDOW_FRAME_COMPUTED_INVERTED("window frame starts after it ends, its computed bounds read by how they are written:"
            + " write a bound before the current row as -(n)"),
    /** Class mapping ids two sets of a mapping's include closure share: the ids (a list, each with its mappings), the
     *  mapping (J10 of the edge audit, 2026-11-17; legend-engine's MappingCompilerExtension). */
    DUPLICATE_CLASS_MAPPING_IDS("mapping '%2$s' has more than one class mapping with the ids %1$s"),
    /** A mapping that lists one include twice: the include as written, the mapping (J10 of the edge audit, 2026-11-17;
     *  legend-engine's MappingCompilerExtension). */
    DUPLICATE_MAPPING_INCLUDE("mapping '%2$s' includes '%1$s' more than once"),
    /** A store the text names that the model does not have (J10 of the edge audit, 2026-11-17; legend-engine's
     *  CompileContext.resolveStore). */
    MISSING_STORE("store '%1$s' is not defined"),
    /** An element name the model defines more than once (J10 of the edge audit, 2026-11-17; legend-engine's
     *  PureModelBuilder, "Duplicated element"). */
    DUPLICATE_ELEMENT("'%1$s' is defined more than once"),
    /** A class declaring one stored property twice: the property, the class (J10 of the edge audit, 2026-11-17;
     *  legend-engine's compiler, "Found duplicated property"). */
    DUPLICATE_PROPERTY("class '%2$s' declares property '%1$s' more than once"),
    /** A default value on an optional property: the property (J10 of the edge audit, 2026-11-17; legend-pure's
     *  PropertyValidator). */
    DEFAULT_ON_OPTIONAL("property '%1$s' is optional and cannot have a default value"),
    /** An enumeration declaring one value twice: the value, the enumeration (J10 of the edge audit, 2026-11-17;
     *  legend-engine's compiler, "Found duplicated value"). */
    DUPLICATE_ENUM_VALUE("enumeration '%2$s' declares value '%1$s' more than once"),
    /** A database condition's column its table or view does not have: the column, the relation's noun (table, view),
     *  its name, the condition (join, filter), the database (J10 of the edge audit, 2026-11-17; legend-engine's
     *  "can't be found" form of a store reference). */
    MISSING_STORE_COLUMN("%2$s '%3$s' of database '%5$s' has no column '%1$s' (%4$s)"),
    /** A database condition's table the database does not have: the table, the database, the condition (J10 of the
     *  edge audit, 2026-11-17; legend-engine's "can't be found" form of a store reference). */
    MISSING_STORE_TABLE("database '%2$s' has no table '%1$s' (%3$s)");

    private final String sentence;

    CompileKind(String sentence) {
        this.sentence = sentence;
    }

    /** Lite's sentence of this kind over {@code operands}. */
    public String sentence(java.util.List<String> operands) {
        return String.format(java.util.Locale.ROOT, sentence, operands.toArray());
    }
}
