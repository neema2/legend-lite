package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

import com.legend.base.Nullable;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * DuckDB's C API (duckdb.h, v1.5.5), called through Java's Foreign Function &amp;
 * Memory API: no JDBC, no reflection, no library beyond DuckDB's own
 * (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md). Every signature was read from
 * that header.
 *
 * <p>In a native image a call costs ~2.3 us (on the JVM ~8 ns), so callers make
 * few of them: a whole 2,048-row chunk becomes Arrow buffers in one call.
 *
 * <p>Handles are the raw {@code invokeExact} targets; the few wrappers below
 * are the ones every caller needs. A pointer is a {@link MemorySegment};
 * DuckDB's null pointer is {@link MemorySegment#NULL}.
 */
final class Duck {

    static final Linker LINKER = Linker.nativeLinker();

    /** {@code duckdb_result}: three idx_t and three pointers, passed BY VALUE to duckdb_fetch_chunk. */
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    /** {@code struct ArrowSchema} and {@code struct ArrowArray} (the Arrow C data interface). */
    static final long SCHEMA_SIZE = 72;
    static final long ARRAY_SIZE = 80;
    static final StructLayout INT32 = MemoryLayout.structLayout(JAVA_INT);
    static final StructLayout INT64 = MemoryLayout.structLayout(JAVA_LONG);
    static final StructLayout HUGEINT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);
    static final StructLayout INTERVAL = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, JAVA_LONG);
    static final StructLayout DECIMAL = MemoryLayout.structLayout(JAVA_BYTE, JAVA_BYTE,
            MemoryLayout.paddingLayout(6), JAVA_LONG, JAVA_LONG);

    // duckdb_type (the ids DuckDB reports for a logical type)
    static final int BOOLEAN = 1, TINYINT = 2, SMALLINT = 3, INTEGER = 4, BIGINT = 5, UTINYINT = 6, USMALLINT = 7,
            UINTEGER = 8, UBIGINT = 9, FLOAT = 10, DOUBLE = 11, TIMESTAMP = 12, DATE = 13, TIME = 14, INTERVAL_T = 15,
            HUGEINT_T = 16, VARCHAR = 17, BLOB = 18, DECIMAL_T = 19, TIMESTAMP_S = 20, TIMESTAMP_MS = 21,
            TIMESTAMP_NS = 22, ENUM = 23, LIST = 24, STRUCT = 25, MAP = 26, UUID = 27, UNION = 28, BIT = 29,
            TIME_TZ = 30, TIMESTAMP_TZ = 31, UHUGEINT = 32, ARRAY = 33;

    // duckdb_result_type
    static final int RESULT_CHANGED_ROWS = 1, RESULT_NOTHING = 2, RESULT_QUERY = 3;
    // duckdb_error_type
    static final int ERROR_PARSER = 14, ERROR_CATALOG = 13, ERROR_BINDER = 24, ERROR_INTERRUPT = 29;

    private static volatile @Nullable Duck loaded;

    /** The API, once {@link #load} has run. */
    static Duck api() {
        Duck d = loaded;
        if (d == null) throw new IllegalStateException("DuckDB's library is not loaded");
        return d;
    }

    /** Loads DuckDB's library (once per process); later calls return the first. */
    static synchronized Duck load(Path library) {
        Duck d = loaded;
        if (d == null) {
            d = new Duck(SymbolLookup.libraryLookup(library, Arena.global()));
            loaded = d;
        }
        return d;
    }

    private final SymbolLookup lib;

    /** Every DuckDB function this class calls, by symbol, with its signature (read from duckdb.h): one table that the
     *  handles below are made from and //warehouse:reachability_metadata renders into the native image's FFM metadata, so
     *  a new signature cannot ship without its metadata (Bazel workplan P2-09). */
    static final java.util.Map<String, FunctionDescriptor> DOWNCALLS;
    /** An Arrow struct's release callback, called through its pointer, not by symbol. */
    static final FunctionDescriptor RELEASE = FunctionDescriptor.ofVoid(ADDRESS);

    static {
        java.util.Map<String, FunctionDescriptor> signatures = new java.util.LinkedHashMap<>();
        signatures.put("duckdb_open_ext", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_close", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        signatures.put("duckdb_disconnect", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_interrupt", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_result_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_result_error_type", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        signatures.put("duckdb_result_return_type", FunctionDescriptor.of(JAVA_INT, RESULT));
        signatures.put("duckdb_rows_changed", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_column_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_column_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_column_logical_type", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_free", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_row_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_value_varchar", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_LONG));
        signatures.put("duckdb_value_int64", FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
        signatures.put("duckdb_library_version", FunctionDescriptor.of(ADDRESS));
        signatures.put("duckdb_fetch_chunk", FunctionDescriptor.of(ADDRESS, RESULT));
        signatures.put("duckdb_destroy_data_chunk", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_connection_get_arrow_options", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_arrow_options", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_data_chunk_to_arrow", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_error_data_has_error", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS));
        signatures.put("duckdb_error_data_message", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_error_data", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_prepare", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_prepare_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_prepare", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_prepared_statement_column_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_prepared_statement_column_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_prepared_statement_column_logical_type", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_execute_prepared", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        signatures.put("duckdb_extract_statements", FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_extract_statements_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_prepare_extracted_statement", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
        signatures.put("duckdb_destroy_extracted", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_bind_varchar", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));
        signatures.put("duckdb_bind_int64", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG));
        signatures.put("duckdb_bind_null", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_connection_get_client_context", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_client_context_get_connection_id", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_destroy_client_context", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_get_type_id", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        signatures.put("duckdb_decimal_width", FunctionDescriptor.of(JAVA_BYTE, ADDRESS));
        signatures.put("duckdb_decimal_scale", FunctionDescriptor.of(JAVA_BYTE, ADDRESS));
        signatures.put("duckdb_list_type_child_type", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_array_type_child_type", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_array_type_array_size", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_struct_type_child_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_struct_type_child_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_struct_type_child_type", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_map_type_key_type", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_map_type_value_type", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_logical_type_get_alias", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_logical_type", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_create_logical_type", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        signatures.put("duckdb_enum_internal_type", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        signatures.put("duckdb_enum_dictionary_size", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        signatures.put("duckdb_enum_dictionary_value", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_scalar_function", FunctionDescriptor.of(ADDRESS));
        signatures.put("duckdb_scalar_function_set_name", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_return_type", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_volatile", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_scalar_function_set_function", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_bind", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_register_scalar_function", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_scalar_function", FunctionDescriptor.ofVoid(ADDRESS));
        signatures.put("duckdb_scalar_function_get_client_context", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_bind_set_error", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_bind_data", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_bind_data_copy", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_get_bind_data", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_data_chunk_get_size", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        signatures.put("duckdb_vector_assign_string_element", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG, ADDRESS));
        signatures.put("duckdb_scalar_function_set_extra_info", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_bind_get_extra_info", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_scalar_function_set_error", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        signatures.put("duckdb_create_bool", FunctionDescriptor.of(ADDRESS, JAVA_BOOLEAN));
        signatures.put("duckdb_create_int8", FunctionDescriptor.of(ADDRESS, JAVA_BYTE));
        signatures.put("duckdb_create_uint8", FunctionDescriptor.of(ADDRESS, JAVA_BYTE));
        signatures.put("duckdb_create_int16", FunctionDescriptor.of(ADDRESS, JAVA_SHORT));
        signatures.put("duckdb_create_uint16", FunctionDescriptor.of(ADDRESS, JAVA_SHORT));
        signatures.put("duckdb_create_int32", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        signatures.put("duckdb_create_uint32", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        signatures.put("duckdb_create_int64", FunctionDescriptor.of(ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_uint64", FunctionDescriptor.of(ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_hugeint", FunctionDescriptor.of(ADDRESS, HUGEINT));
        signatures.put("duckdb_create_uhugeint", FunctionDescriptor.of(ADDRESS, HUGEINT));
        signatures.put("duckdb_create_float", FunctionDescriptor.of(ADDRESS, JAVA_FLOAT));
        signatures.put("duckdb_create_double", FunctionDescriptor.of(ADDRESS, JAVA_DOUBLE));
        signatures.put("duckdb_create_decimal", FunctionDescriptor.of(ADDRESS, DECIMAL));
        signatures.put("duckdb_create_varchar_length", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_blob", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_uuid", FunctionDescriptor.of(ADDRESS, HUGEINT));
        signatures.put("duckdb_create_date", FunctionDescriptor.of(ADDRESS, INT32));
        signatures.put("duckdb_create_time", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_timestamp", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_timestamp_s", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_timestamp_ms", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_timestamp_ns", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_timestamp_tz", FunctionDescriptor.of(ADDRESS, INT64));
        signatures.put("duckdb_create_interval", FunctionDescriptor.of(ADDRESS, INTERVAL));
        signatures.put("duckdb_create_enum_value", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_null_value", FunctionDescriptor.of(ADDRESS));
        signatures.put("duckdb_create_list_value", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_array_value", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_create_struct_value", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS));
        signatures.put("duckdb_create_map_value", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG));
        signatures.put("duckdb_get_varchar", FunctionDescriptor.of(ADDRESS, ADDRESS));
        signatures.put("duckdb_destroy_value", FunctionDescriptor.ofVoid(ADDRESS));
        DOWNCALLS = java.util.Collections.unmodifiableMap(signatures);
    }

    private MethodHandle fn(String name) {
        FunctionDescriptor d = DOWNCALLS.get(name);
        if (d == null) throw new IllegalStateException("Duck.DOWNCALLS has no " + name);
        return LINKER.downcallHandle(lib.find(name).orElseThrow(
                () -> new IllegalStateException("DuckDB's library has no " + name)), d);
    }

    // -- database, connection, query -------------------------------------------------------
    final MethodHandle openExt, close, connect, disconnect, interrupt, query, destroyResult, resultError,
            resultErrorType, resultReturnType, rowsChanged, columnCount, columnName, columnLogicalType, free,
            rowCount, valueVarchar, valueInt64, libraryVersion;
    // -- chunks and Arrow ------------------------------------------------------------------
    final MethodHandle fetchChunk, destroyChunk, arrowOptions, destroyArrowOptions, chunkToArrow, errorHas,
            errorMessage, destroyError, release;
    // -- prepared statements and scripts ---------------------------------------------------
    final MethodHandle prepare, prepareError, destroyPrepare, psColumnCount, psColumnName, psColumnLogicalType,
            executePrepared, extract, extractError, prepareExtracted, destroyExtracted, bindVarchar, bindInt64,
            bindNull;
    // -- client context --------------------------------------------------------------------
    final MethodHandle connectionContext, contextConnectionId, destroyContext;
    // -- logical types ---------------------------------------------------------------------
    final MethodHandle typeId, decimalWidth, decimalScale, listChild, arrayChild, arraySize, structCount, structName,
            structType, mapKey, mapValue, alias, destroyLogicalType, createLogicalType, enumInternalType,
            enumSize, enumValue;
    // -- scalar functions (the identity function) ------------------------------------------
    final MethodHandle createScalarFunction, setName, setReturnType, setVolatile, setFunction, setBind,
            registerScalarFunction, destroyScalarFunction, bindContext, bindSetError, setBindData,
            setBindDataCopy, getBindData, chunkSize, assignString, setExtraInfo, bindExtraInfo, setError;
    // -- values (a nested cell's text, rendered by DuckDB) ---------------------------------
    final MethodHandle createBool, createInt8, createUint8, createInt16, createUint16, createInt32, createUint32,
            createInt64, createUint64, createHugeint, createUhugeint, createFloat, createDouble, createDecimal,
            createVarchar, createBlob, createUuid, createDate, createTime, createTimestamp, createTimestampS,
            createTimestampMs, createTimestampNs, createTimestampTz, createInterval, createEnum, createNull,
            createList, createArray, createStruct, createMap, getVarchar, destroyValue;

    private Duck(SymbolLookup lib) {
        this.lib = lib;
        openExt = fn("duckdb_open_ext");
        close = fn("duckdb_close");
        connect = fn("duckdb_connect");
        disconnect = fn("duckdb_disconnect");
        interrupt = fn("duckdb_interrupt");
        query = fn("duckdb_query");
        destroyResult = fn("duckdb_destroy_result");
        resultError = fn("duckdb_result_error");
        resultErrorType = fn("duckdb_result_error_type");
        resultReturnType = fn("duckdb_result_return_type");
        rowsChanged = fn("duckdb_rows_changed");
        columnCount = fn("duckdb_column_count");
        columnName = fn("duckdb_column_name");
        columnLogicalType = fn("duckdb_column_logical_type");
        free = fn("duckdb_free");
        // scalar cells of small internal results (keywords, history counts): DuckDB's older value readers
        rowCount = fn("duckdb_row_count");
        valueVarchar = fn("duckdb_value_varchar");
        valueInt64 = fn("duckdb_value_int64");
        libraryVersion = fn("duckdb_library_version");

        fetchChunk = fn("duckdb_fetch_chunk");
        destroyChunk = fn("duckdb_destroy_data_chunk");
        arrowOptions = fn("duckdb_connection_get_arrow_options");
        destroyArrowOptions = fn("duckdb_destroy_arrow_options");
        chunkToArrow = fn("duckdb_data_chunk_to_arrow");
        errorHas = fn("duckdb_error_data_has_error");
        errorMessage = fn("duckdb_error_data_message");
        destroyError = fn("duckdb_destroy_error_data");
        release = LINKER.downcallHandle(RELEASE);   // through an Arrow struct's release pointer

        prepare = fn("duckdb_prepare");
        prepareError = fn("duckdb_prepare_error");
        destroyPrepare = fn("duckdb_destroy_prepare");
        psColumnCount = fn("duckdb_prepared_statement_column_count");
        psColumnName = fn("duckdb_prepared_statement_column_name");
        psColumnLogicalType = fn("duckdb_prepared_statement_column_logical_type");
        executePrepared = fn("duckdb_execute_prepared");
        extract = fn("duckdb_extract_statements");
        extractError = fn("duckdb_extract_statements_error");
        prepareExtracted = fn("duckdb_prepare_extracted_statement");
        destroyExtracted = fn("duckdb_destroy_extracted");
        bindVarchar = fn("duckdb_bind_varchar");
        bindInt64 = fn("duckdb_bind_int64");
        bindNull = fn("duckdb_bind_null");

        connectionContext = fn("duckdb_connection_get_client_context");
        contextConnectionId = fn("duckdb_client_context_get_connection_id");
        destroyContext = fn("duckdb_destroy_client_context");

        typeId = fn("duckdb_get_type_id");
        decimalWidth = fn("duckdb_decimal_width");
        decimalScale = fn("duckdb_decimal_scale");
        listChild = fn("duckdb_list_type_child_type");
        arrayChild = fn("duckdb_array_type_child_type");
        arraySize = fn("duckdb_array_type_array_size");
        structCount = fn("duckdb_struct_type_child_count");
        structName = fn("duckdb_struct_type_child_name");
        structType = fn("duckdb_struct_type_child_type");
        mapKey = fn("duckdb_map_type_key_type");
        mapValue = fn("duckdb_map_type_value_type");
        alias = fn("duckdb_logical_type_get_alias");
        destroyLogicalType = fn("duckdb_destroy_logical_type");
        createLogicalType = fn("duckdb_create_logical_type");
        enumInternalType = fn("duckdb_enum_internal_type");
        enumSize = fn("duckdb_enum_dictionary_size");
        enumValue = fn("duckdb_enum_dictionary_value");

        createScalarFunction = fn("duckdb_create_scalar_function");
        setName = fn("duckdb_scalar_function_set_name");
        setReturnType = fn("duckdb_scalar_function_set_return_type");
        setVolatile = fn("duckdb_scalar_function_set_volatile");
        setFunction = fn("duckdb_scalar_function_set_function");
        setBind = fn("duckdb_scalar_function_set_bind");
        registerScalarFunction = fn("duckdb_register_scalar_function");
        destroyScalarFunction = fn("duckdb_destroy_scalar_function");
        bindContext = fn("duckdb_scalar_function_get_client_context");
        bindSetError = fn("duckdb_scalar_function_bind_set_error");
        setBindData = fn("duckdb_scalar_function_set_bind_data");
        setBindDataCopy = fn("duckdb_scalar_function_set_bind_data_copy");
        getBindData = fn("duckdb_scalar_function_get_bind_data");
        chunkSize = fn("duckdb_data_chunk_get_size");
        assignString = fn("duckdb_vector_assign_string_element");
        setExtraInfo = fn("duckdb_scalar_function_set_extra_info");
        bindExtraInfo = fn("duckdb_scalar_function_bind_get_extra_info");
        setError = fn("duckdb_scalar_function_set_error");

        createBool = fn("duckdb_create_bool");
        createInt8 = fn("duckdb_create_int8");
        createUint8 = fn("duckdb_create_uint8");
        createInt16 = fn("duckdb_create_int16");
        createUint16 = fn("duckdb_create_uint16");
        createInt32 = fn("duckdb_create_int32");
        createUint32 = fn("duckdb_create_uint32");
        createInt64 = fn("duckdb_create_int64");
        createUint64 = fn("duckdb_create_uint64");
        createHugeint = fn("duckdb_create_hugeint");
        createUhugeint = fn("duckdb_create_uhugeint");
        createFloat = fn("duckdb_create_float");
        createDouble = fn("duckdb_create_double");
        createDecimal = fn("duckdb_create_decimal");
        createVarchar = fn("duckdb_create_varchar_length");
        createBlob = fn("duckdb_create_blob");
        createUuid = fn("duckdb_create_uuid");
        createDate = fn("duckdb_create_date");
        createTime = fn("duckdb_create_time");
        createTimestamp = fn("duckdb_create_timestamp");
        createTimestampS = fn("duckdb_create_timestamp_s");
        createTimestampMs = fn("duckdb_create_timestamp_ms");
        createTimestampNs = fn("duckdb_create_timestamp_ns");
        createTimestampTz = fn("duckdb_create_timestamp_tz");
        createInterval = fn("duckdb_create_interval");
        createEnum = fn("duckdb_create_enum_value");
        createNull = fn("duckdb_create_null_value");
        createList = fn("duckdb_create_list_value");
        createArray = fn("duckdb_create_array_value");
        createStruct = fn("duckdb_create_struct_value");
        createMap = fn("duckdb_create_map_value");
        getVarchar = fn("duckdb_get_varchar");
        destroyValue = fn("duckdb_destroy_value");
    }

    /** DuckDB's version, as its drivers report it ("v1.5.5"). */
    String version() {
        try {
            String v = text((MemorySegment) libraryVersion.invokeExact());
            return v == null ? "" : v;
        } catch (Throwable t) {
            throw fail(t);
        }
    }

    /** A C string DuckDB owns, as Java text; null for NULL. */
    static @Nullable String text(MemorySegment cString) {
        return cString.equals(MemorySegment.NULL) ? null : cString.reinterpret(Long.MAX_VALUE).getString(0);
    }

    /** A C string the caller must free with duckdb_free, as Java text (freed here). */
    String owned(MemorySegment cString) {
        String s = text(cString);
        if (s == null) return "";
        try {
            free.invokeExact(cString);
        } catch (Throwable t) {
            throw fail(t);
        }
        return s;
    }

    /** An FFM call failed in a way no caller handles (a missing symbol, a wrong signature). */
    static RuntimeException fail(Throwable t) {
        if (t instanceof Error e) throw e;
        if (t instanceof RuntimeException r) return r;
        return new IllegalStateException(t);
    }
}
