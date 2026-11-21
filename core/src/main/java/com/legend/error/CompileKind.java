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
    /** A class mapping whose properties read several tables and names none as its main one: the class's name, the
     *  tables (legend-pure's RelationalInstanceSetImplementationProcessor, {@code cls._name()}; the BE audit 2's S2,
     *  2026-11-21: until then the class's path). */
    MAIN_TABLE_UNNAMED("Can't find the main table for class '%1$s'. Please specify a main table using the"
            + " ~mainTable directive.", Raised.COMPILING),
    /** A class mapping whose properties read tables of several databases: the class's name, the databases (legend-pure's
     *  RelationalInstanceSetImplementationProcessor; the BE audit 2's S2, 2026-11-21: until then lite's own prose,
     *  "Inconsistent database definitions for the mapping of class"). */
    MAIN_TABLE_DATABASES_INCONSISTENT("Can't find the main table for class '%1$s'. Inconsistent database definitions"
            + " for the mapping", Raised.COMPILING),
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
     *  after it), start after they end (register S170). Lite's own refusal, raised COMPILING: legend-engine computes
     *  the bound and runs {@code rows(1 - $n, 0)} for n = 2, its rows.pure assert firing only on the numbers it
     *  computes (the BE audit's finding 2, 2026-11-21: it was marked RUNNING). */
    WINDOW_FRAME_COMPUTED_INVERTED("Invalid window frame boundary - a computed bound's direction is read from how it"
            + " is written (-(n) is n before the current row, any other n after it), and this frame then starts after"
            + " where it ends: write the bound before the current row as -(n) (docs/SEMANTICS_REGISTER.md S170)",
            Raised.COMPILING),
    /** A relation column, or a value cast to a class, of a type the relation runtime does not hold: the type. legend-
     *  engine raises it at run, in its words (the relation natives, {@code AggregationShared}, {@code
     *  RelationNativeImplementation}; PCT's {@code testVariantColumn_modelOutputNotSupported} asserts it inside
     *  {@code assertError}). */
    TYPE_NOT_SUPPORTED_YET("The type %1$s is not supported yet!", Raised.RUNNING),
    /** A sub-day unit for {@code timeBucket} of a StrictDate. legend-engine raises it at run, in its words
     *  ({@code TimeBucketShared}; PCT's {@code timeBucket::strictDate} tests assert it inside {@code assertError}). */
    STRICT_DATE_BUCKET_UNIT("Unsupported duration unit for StrictDate. Units can only be: [YEARS, DAYS, MONTHS, WEEKS]",
            Raised.RUNNING),
    /** A class mapping id two sets of a mapping's include closure share: the first such id, the mapping (legend-engine's
     *  MappingValidator.validateMappingElementIds; the BE audit 2's M1, 2026-11-21: until then the ids listed with
     *  their mappings, lite's own). */
    DUPLICATE_CLASS_MAPPING_IDS("Duplicated class mappings found with ID '%1$s' in mapping '%2$s'", Raised.COMPILING),
    /** A mapping that lists one include twice: the include as written, the mapping (J10 of the edge audit, 2026-11-17;
     *  legend-engine's MappingCompilerExtension). */
    DUPLICATE_MAPPING_INCLUDE("Duplicated mapping include '%1$s' in mapping '%2$s'", Raised.COMPILING),
    /** A store the text names that the model does not have (J10 of the edge audit, 2026-11-17; legend-engine's
     *  PureModel.getStore, PureModel.java:1241 at the pin; the BE audit 2's S8, 2026-11-21, corrected the citation). */
    MISSING_STORE("The store '%1$s' can't be found.", Raised.COMPILING),
    /** An element name the model defines more than once (J10 of the edge audit, 2026-11-17; legend-engine's
     *  PureModelContextDataValidator, PureModelContextDataValidator.java:53 at the pin; the BE audit 2's S8,
     *  2026-11-21, corrected the citation). */
    DUPLICATE_ELEMENT("Duplicated element '%1$s'", Raised.COMPILING),
    /** A class declaring one stored property twice: the class's name, the property (legend-pure's ClassValidator;
     *  the BE audit 2's S1, 2026-11-21: legend-engine compiles the model with a warning, "Found duplicated property",
     *  which lite said until then). */
    DUPLICATE_PROPERTY("Property conflict on class %1$s: property '%2$s' defined more than once", Raised.COMPILING),
    /** A default value on an optional property: the property (J10 of the edge audit, 2026-11-17; legend-pure's
     *  PropertyValidator). */
    DEFAULT_ON_OPTIONAL("Default values are supported only for mandatory fields, and property '%1$s' is optional.",
            Raised.COMPILING),
    /** An enumeration declaring a value twice: the enumeration's path, its duplicated values sorted and joined by
     *  ", " (legend-pure's EnumerationValidator; the BE audit 2's S1, 2026-11-21: legend-engine warns, "Found duplicated
     *  value", which lite said until then). */
    DUPLICATE_ENUM_VALUE("Enumeration %1$s has duplicate values: %2$s", Raised.COMPILING),
    /** A database condition's column its table or view does not have: the column, the relation's name (legend-pure's
     *  DatabaseProcessor, "in the table" for a view too; the BE audit 2's M1, 2026-11-21: until then lite added the
     *  condition and the database). */
    MISSING_STORE_COLUMN("The column '%1$s' can't be found in the table '%2$s'", Raised.COMPILING),
    /** A database condition's table the database does not have: the table, its schema ("default" where none is
     *  written), the database's name (legend-pure's DatabaseProcessor; the BE audit 2's M1, 2026-11-21: until then
     *  lite gave no schema, the database's path and the condition). */
    MISSING_STORE_TABLE("The table '%1$s' can't be found in the schema '%2$s' in the database '%3$s'",
            Raised.COMPILING),
    /** A database condition's schema the database and its includes do not declare: the schema, the database's name
     *  (legend-pure's DatabaseProcessor.findTableForAlias, checked before the table; the BE audit 3's S1, 2026-11-21:
     *  until then the table's sentence). */
    MISSING_STORE_SCHEMA("The schema '%1$s' can't be found in the database '%2$s'", Raised.COMPILING),
    /** A Data element's table of no rows, not even a header: the element, the table (lite's own words: a project's
     *  data, no upstream sentence; stage B's BE, 2026-11-21: until then the plan carried the sentence). */
    DATA_TABLE_EMPTY("the Data element %1$s fills %2$s with no rows, not even a header", Raised.COMPILING),
    /** A Data element's table no Database of the model declares: the element, the table (lite's own words). */
    DATA_TABLE_UNDECLARED("the Data element %1$s fills %2$s, which no Database of the model declares",
            Raised.COMPILING),
    /** A Data element's table more than one Database declares: the element, the table, the Databases (lite's own). */
    DATA_TABLE_DECLARED_TWICE("the Data element %1$s fills %2$s, which more than one Database declares %3$s",
            Raised.COMPILING);

    private final String sentence;
    private final Raised raised;

    CompileKind(String sentence, Raised raised) {
        this.sentence = sentence;
        this.raised = raised;
    }

    /** WHERE UPSTREAM RAISES A KIND, stated on every kind -- no default (the user's ruling of 2026-11-21, "D", and
     *  the question of ~03:40: "is it just empirical ... or is it some knowable knowledge"): from how Pure works, read
     *  in the pinned sources. {@link #COMPILING}: legend-pure's compiler or legend-engine's (PureModel, its compiler
     *  extensions, legend-pure's validators -- each kind's javadoc names its upstream source): the
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

    /** The number of values this kind's sentence names: its highest {@code %n$s} (the BE audit 5's L6, 2026-11-21: a
     *  reader checks a record's values by it, never by String.format's failure). */
    public int arity() {
        int most = 0;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("%(\\d+)\\$s").matcher(sentence);
        while (m.find()) {
            most = Math.max(most, Integer.parseInt(m.group(1)));
        }
        return most;
    }

    /** Lite's sentence of this kind over {@code operands}. */
    @Override
    public String sentence(java.util.List<String> operands) {
        return String.format(java.util.Locale.ROOT, sentence, operands.toArray());
    }
}
