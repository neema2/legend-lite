package com.legend.error;

/**
 * THE KIND OF A COMPILE ERROR, AND ITS SENTENCE (phase D's D1.6, W7 of docs/records/d-plan-2026-11-16.md, 2026-11-16):
 * a compile error the user's model or query causes, named by its kind and its operands (the names the error is about:
 * an element's, a type's, a count). ONE CATALOG, its words upstream's (the user's ruling of 2026-11-21 ~02:40,
 * replacing Q6 of 2026-11-10's "the product's own wording"): a kind's sentence is legend-pure's or legend-engine's,
 * byte for byte, wherever upstream has one -- the product and every audience say it; lite's own words only for a kind
 * upstream has none for (the S170 window frame). Each sentence is a format over the operands, positional
 * ({@code %1$s}, ...); an operand upstream's sentence does not print stays a fact of the record.
 */
public enum CompileKind implements ErrorCode {
    /** An element the text names that the model does not have: its kind's noun and its name. */
    MISSING("Can't find %1$s '%2$s'", Raised.COMPILING),
    /** A profile the text names that the model does not have. */
    MISSING_PROFILE("Can't find the profile '%1$s'", Raised.COMPILING),
    /** A stereotype or a tag a profile does not define: the member's noun, its name, the profile. */
    MISSING_PROFILE_MEMBER("Can't find %1$s '%2$s' in profile '%3$s'", Raised.COMPILING),
    /** A class mapping a union names that its mapping does not have: the set id, the mapping. */
    MISSING_CLASS_MAPPING("Can't find class mapping '%1$s' in mapping '%2$s'", Raised.COMPILING),
    /** A class mapping whose properties read several tables and names none as its main one: the class, the tables. */
    MAIN_TABLE_UNNAMED("Can't find the main table for class '%1$s'. Please specify a main table using the"
            + " ~mainTable directive.", Raised.COMPILING),
    /** A class extending a type that is not a class: the class's name, the supertype. */
    INVALID_SUPERTYPE("Invalid supertype: '%1$s' cannot extend '%2$s' as it is not a class.", Raised.COMPILING),
    /** A generic type given another number of arguments than it declares: the arguments' kind (type, multiplicity),
     *  the number given, the type as declared. */
    TYPE_ARGUMENTS_COUNT("Wrong %1$s arguments count (%2$s) for type: %3$s", Raised.COMPILING),
    /** A type given another number of type variables than it declares: the number given, the type as declared. */
    TYPE_VARIABLES_COUNT("Wrong type variables count (%1$s) for type: %2$s", Raised.COMPILING),
    /** A row's cell read at an offset the row does not have: the offset, the row's size. legend-pure raises it at run
     *  (its {@code at} native, {@code natives/essentials/collection/index/At.java}). */
    ELEMENT_AT_OFFSET("The system is trying to get an element at offset %1$s where the collection is of size %2$s",
            Raised.RUNNING),
    /** A window frame whose literal start is after its end (the P2 checkpoint audit's M4, 2026-11-16: legend-pure's
     *  sentence was the product's until then). */
    WINDOW_FRAME_INVERTED("Invalid window frame boundary - lower bound of window frame cannot be greater than the upper"
            + " bound!", Raised.RUNNING),
    /** A window frame whose computed bounds, read by how they are written (-(n) before the current row, any other n
     *  after it), start after they end (register S170). */
    WINDOW_FRAME_COMPUTED_INVERTED("Invalid window frame boundary - a computed bound's direction is read from how it"
            + " is written (-(n) is n before the current row, any other n after it), and this frame then starts after"
            + " where it ends: write the bound before the current row as -(n) (docs/SEMANTICS_REGISTER.md S170)",
            Raised.RUNNING),
    /** A relation column, or a value cast to a class, of a type the relation runtime does not hold: the type. legend-
     *  engine raises it at run, in its words (the relation natives, {@code AggregationShared}, {@code
     *  RelationNativeImplementation}; PCT's {@code testVariantColumn_modelOutputNotSupported} asserts it inside
     *  {@code assertError}). */
    TYPE_NOT_SUPPORTED_YET("The type %1$s is not supported yet!", Raised.RUNNING),
    /** A sub-day unit for {@code timeBucket} of a StrictDate. legend-engine raises it at run, in its words
     *  ({@code TimeBucketShared}; PCT's {@code timeBucket::strictDate} tests assert it inside {@code assertError}). */
    STRICT_DATE_BUCKET_UNIT("Unsupported duration unit for StrictDate. Units can only be: [YEARS, DAYS, MONTHS, WEEKS]",
            Raised.RUNNING),
    /** Class mapping ids two sets of a mapping's include closure share: the ids (a list, each with its mappings), the
     *  mapping (J10 of the edge audit, 2026-11-17; legend-engine's MappingCompilerExtension). */
    DUPLICATE_CLASS_MAPPING_IDS("Duplicated class mappings found with ID %1$s in mapping '%2$s'", Raised.COMPILING),
    /** A mapping that lists one include twice: the include as written, the mapping (J10 of the edge audit, 2026-11-17;
     *  legend-engine's MappingCompilerExtension). */
    DUPLICATE_MAPPING_INCLUDE("Duplicated mapping include '%1$s' in mapping '%2$s'", Raised.COMPILING),
    /** A store the text names that the model does not have (J10 of the edge audit, 2026-11-17; legend-engine's
     *  CompileContext.resolveStore). */
    MISSING_STORE("The store '%1$s' can't be found.", Raised.COMPILING),
    /** An element name the model defines more than once (J10 of the edge audit, 2026-11-17; legend-engine's
     *  PureModelBuilder, "Duplicated element"). */
    DUPLICATE_ELEMENT("Duplicated element '%1$s'", Raised.COMPILING),
    /** A class declaring one stored property twice: the property, the class (J10 of the edge audit, 2026-11-17;
     *  legend-engine's compiler, "Found duplicated property"). */
    DUPLICATE_PROPERTY("Found duplicated property '%1$s' in class '%2$s'", Raised.COMPILING),
    /** A default value on an optional property: the property (J10 of the edge audit, 2026-11-17; legend-pure's
     *  PropertyValidator). */
    DEFAULT_ON_OPTIONAL("Default values are supported only for mandatory fields, and property '%1$s' is optional.",
            Raised.COMPILING),
    /** An enumeration declaring one value twice: the value, the enumeration (J10 of the edge audit, 2026-11-17;
     *  legend-engine's compiler, "Found duplicated value"). */
    DUPLICATE_ENUM_VALUE("Found duplicated value '%1$s' in enumeration '%2$s'", Raised.COMPILING),
    /** A database condition's column its table or view does not have: the column, the relation's noun (table, view),
     *  its name, the condition (join, filter), the database (J10 of the edge audit, 2026-11-17; legend-engine's
     *  "can't be found" form of a store reference). */
    MISSING_STORE_COLUMN("The column '%1$s' can't be found in the %2$s '%3$s' (%4$s of database '%5$s')",
            Raised.COMPILING),
    /** A database condition's table the database does not have: the table, the database, the condition (J10 of the
     *  edge audit, 2026-11-17; legend-engine's "can't be found" form of a store reference). */
    MISSING_STORE_TABLE("The table '%1$s' can't be found in the database '%2$s' (%3$s)", Raised.COMPILING);

    private final String sentence;
    private final Raised raised;

    CompileKind(String sentence, Raised raised) {
        this.sentence = sentence;
        this.raised = raised;
    }

    /** WHERE UPSTREAM RAISES A KIND, stated on every kind -- no default (the user's ruling of 2026-11-21, "D", and
     *  the question of ~03:40: "is it just empirical ... or is it some knowable knowledge"): from how Pure works, read
     *  in the pinned sources. {@link #COMPILING}: legend-pure's compiler or legend-engine's (PureModel, its compiler
     *  extensions, legend-pure's validators -- each kind's upstream sentence cited in {@code edge.PureMessages}): the
     *  program does not load. {@link #RUNNING}: a function's evaluation -- a native's code, or an {@code assert} in a
     *  Pure function's body -- each kind citing its source. */
    public enum Raised {
        COMPILING,
        RUNNING
    }

    /** Whether legend-pure (or legend-engine's Pure) raises this error when the expression RUNS, not when the program
     *  compiles -- read from the pinned sources, the kind's javadoc citing them. Lite finds it while planning; in a
     *  test body it is planned as a failure and raised at its turn, where an {@code assertError} catches it as
     *  upstream's does. Any other compile error in a test body refuses the body at planning (the user's ruling of
     *  2026-11-21, "D"; {@code testexecutionplan.Failures#planned}). */
    public boolean atRun() {
        return raised == Raised.RUNNING;
    }

    @Override
    public String family() {
        return "compile";
    }

    /** Lite's sentence of this kind over {@code operands}. */
    @Override
    public String sentence(java.util.List<String> operands) {
        return String.format(java.util.Locale.ROOT, sentence, operands.toArray());
    }
}
