package com.legend.warehouse.client.jdbc;

import com.legend.base.Nullable;
import com.legend.json.Json;
import com.legend.warehouse.sqlapi.DuckType;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * A result, read forward only, its values decoded into the same Java
 * objects DuckDB's driver returns (see {@link Carriers}).
 */
final class WhResultSet implements java.sql.ResultSet {

    private final WhStatement statement;
    private final List<Column> columns;
    private final List<DuckType> types = new ArrayList<>();
    private final List<List<Json.Node>> rows;
    private int at = -1;
    private boolean lastWasNull;
    private boolean closed;

    WhResultSet(WhStatement statement, ResultMeta meta, List<List<Json.Node>> rows) {
        this.statement = statement;
        this.columns = meta.columns();
        for (Column c : columns) types.add(DuckType.parse(c.type()));
        this.rows = rows;
    }

    List<Column> columns() {
        return columns;
    }

    List<DuckType> types() {
        return types;
    }

    private @Nullable Object cell(int i) throws SQLException {
        if (closed) throw new SQLException("the result set is closed");
        if (at < 0 || at >= rows.size()) throw new SQLException("not on a row");
        if (i < 1 || i > columns.size()) throw new SQLException("no column " + i);
        Object v = Carriers.decode(Carriers.valueOf(rows.get(at).get(i - 1), types.get(i - 1)), types.get(i - 1));
        lastWasNull = v == null;
        return v;
    }

    @Override
    public boolean next() {
        if (at < rows.size()) at++;
        return at < rows.size();
    }

    @Override
    public boolean wasNull() {
        return lastWasNull;
    }

    @Override
    public @Nullable Object getObject(int i) throws SQLException {
        return cell(i);
    }

    @Override
    public @Nullable Object getObject(String label) throws SQLException {
        return cell(findColumn(label));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> @Nullable T getObject(int i, Class<T> type) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        Object out;
        if (type == LocalDateTime.class) out = Carriers.localDateTime(v);
        else if (type == Timestamp.class && v instanceof LocalDateTime ldt) out = Timestamp.valueOf(ldt);
        else if (type == LocalDate.class || type == LocalTime.class || type == OffsetDateTime.class
                || type == Object.class) out = v;
        else if (type == String.class) out = v.toString();
        else if (type == Long.class) out = ((Number) v).longValue();
        else if (type == Integer.class) out = ((Number) v).intValue();
        else if (type == Double.class) out = ((Number) v).doubleValue();
        else if (type == BigDecimal.class) out = v instanceof BigDecimal bd ? bd : new BigDecimal(v.toString());
        else if (type == BigInteger.class) out = v instanceof BigInteger bi ? bi : new BigInteger(v.toString());
        else if (type == Boolean.class) out = v;
        else throw Unsupported.of("getObject(int, " + type.getName() + ")");
        return (T) out;
    }

    @Override
    public <T> @Nullable T getObject(String label, Class<T> type) throws SQLException {
        return getObject(findColumn(label), type);
    }

    @Override
    public @Nullable String getString(int i) throws SQLException {
        Object v = cell(i);
        // A nested value's text is DuckDB's own, sent with it.
        String text = Carriers.textOf(rows.get(at).get(i - 1), types.get(i - 1));
        if (text != null) return text;
        return v == null ? null : v instanceof WhBlob b ? new String(b.bytes(), java.nio.charset.StandardCharsets.UTF_8) : v.toString();
    }

    @Override
    public @Nullable String getString(String label) throws SQLException {
        return getString(findColumn(label));
    }

    private @Nullable Number number(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (v instanceof Number n) return n;
        if (v instanceof Boolean b) return b ? 1 : 0;
        return new BigDecimal(v.toString());
    }

    @Override
    public long getLong(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.longValue();
    }

    @Override
    public long getLong(String label) throws SQLException {
        return getLong(findColumn(label));
    }

    @Override
    public int getInt(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.intValue();
    }

    @Override
    public int getInt(String label) throws SQLException {
        return getInt(findColumn(label));
    }

    @Override
    public short getShort(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.shortValue();
    }

    @Override
    public byte getByte(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.byteValue();
    }

    @Override
    public double getDouble(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.doubleValue();
    }

    @Override
    public double getDouble(String label) throws SQLException {
        return getDouble(findColumn(label));
    }

    @Override
    public float getFloat(int i) throws SQLException {
        Number n = number(i);
        return n == null ? 0 : n.floatValue();
    }

    @Override
    public @Nullable BigDecimal getBigDecimal(int i) throws SQLException {
        Object v = cell(i);
        return v == null ? null : v instanceof BigDecimal bd ? bd : new BigDecimal(v.toString());
    }

    @Override
    public @Nullable BigDecimal getBigDecimal(String label) throws SQLException {
        return getBigDecimal(findColumn(label));
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.doubleValue() != 0;
        return Boolean.parseBoolean(v.toString());
    }

    @Override
    public boolean getBoolean(String label) throws SQLException {
        return getBoolean(findColumn(label));
    }

    @Override
    public byte @Nullable [] getBytes(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (v instanceof WhBlob b) return b.bytes();
        return v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public @Nullable Array getArray(int i) throws SQLException {
        Object v = cell(i);
        return (Array) v;
    }

    @Override
    public @Nullable Blob getBlob(int i) throws SQLException {
        return (Blob) cell(i);
    }

    @Override
    public java.sql.@Nullable Timestamp getTimestamp(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (v instanceof Timestamp ts) return ts;
        if (v instanceof OffsetDateTime odt) return Timestamp.from(odt.toInstant());
        if (v instanceof LocalDate d) return Timestamp.valueOf(d.atStartOfDay());
        throw new SQLException("column " + i + " is not a timestamp");
    }

    @Override
    public java.sql.@Nullable Date getDate(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (v instanceof LocalDate d) return java.sql.Date.valueOf(d);
        if (v instanceof Timestamp ts) return java.sql.Date.valueOf(ts.toLocalDateTime().toLocalDate());
        throw new SQLException("column " + i + " is not a date");
    }

    @Override
    public java.sql.@Nullable Time getTime(int i) throws SQLException {
        Object v = cell(i);
        if (v == null) return null;
        if (v instanceof LocalTime t) return java.sql.Time.valueOf(t);
        throw new SQLException("column " + i + " is not a time");
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new WhResultSetMetaData(columns, types);
    }

    @Override
    public int findColumn(String label) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(label)) return i + 1;
        }
        throw new SQLException("no column '" + label + "'");
    }

    @Override
    public boolean isBeforeFirst() {
        return at < 0 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() {
        return at >= rows.size() && !rows.isEmpty();
    }

    @Override
    public boolean isFirst() {
        return at == 0;
    }

    @Override
    public boolean isLast() {
        return at == rows.size() - 1;
    }

    @Override
    public int getRow() {
        return at >= 0 && at < rows.size() ? at + 1 : 0;
    }

    @Override
    public int getType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public java.sql.Statement getStatement() {
        return statement;
    }

    @Override
    public @com.legend.base.Nullable SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public void setFetchSize(int rows) {
    }

    @Override
    public int getFetchSize() {
        return rows.size();
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Unsupported.of("unwrap: this driver wraps nothing");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    // -- not supported: each says so, loudly --------------------------------

    @Override
    public void updateBytes(int p0, byte[] p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBytes");
    }

    @Override
    public void updateBytes(java.lang.String p0, byte[] p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBytes");
    }

    @Override
    public byte getByte(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getByte");
    }

    @Override
    public short getShort(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getShort");
    }

    @Override
    public float getFloat(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getFloat");
    }

    @Override
    public byte[] getBytes(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBytes");
    }

    @Override
    public boolean last() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.last");
    }

    @Override
    public boolean first() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.first");
    }

    @Override
    public java.sql.Ref getRef(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getRef");
    }

    @Override
    public java.sql.Ref getRef(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getRef");
    }

    @Override
    public boolean previous() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.previous");
    }

    @Override
    public java.sql.Array getArray(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getArray");
    }

    @Override
    public boolean absolute(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.absolute");
    }

    @Override
    public java.math.BigDecimal getBigDecimal(java.lang.String p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBigDecimal");
    }

    @Override
    public java.math.BigDecimal getBigDecimal(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBigDecimal");
    }

    @Override
    public java.sql.Time getTime(java.lang.String p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTime");
    }

    @Override
    public java.sql.Time getTime(int p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTime");
    }

    @Override
    public java.sql.Time getTime(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTime");
    }

    @Override
    public java.lang.Object getObject(int p0, java.util.Map<java.lang.String, java.lang.Class<?>> p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getObject");
    }

    @Override
    public java.lang.Object getObject(java.lang.String p0, java.util.Map<java.lang.String, java.lang.Class<?>> p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getObject");
    }

    @Override
    public void updateTime(java.lang.String p0, java.sql.Time p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateTime");
    }

    @Override
    public void updateTime(int p0, java.sql.Time p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateTime");
    }

    @Override
    public java.sql.Date getDate(java.lang.String p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getDate");
    }

    @Override
    public java.sql.Date getDate(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getDate");
    }

    @Override
    public java.sql.Date getDate(int p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getDate");
    }

    @Override
    public void updateArray(int p0, java.sql.Array p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateArray");
    }

    @Override
    public void updateArray(java.lang.String p0, java.sql.Array p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateArray");
    }

    @Override
    public java.net.URL getURL(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getURL");
    }

    @Override
    public java.net.URL getURL(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getURL");
    }

    @Override
    public java.sql.Timestamp getTimestamp(int p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTimestamp");
    }

    @Override
    public java.sql.Timestamp getTimestamp(java.lang.String p0, java.util.Calendar p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTimestamp");
    }

    @Override
    public java.sql.Timestamp getTimestamp(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getTimestamp");
    }

    @Override
    public boolean relative(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.relative");
    }

    @Override
    public void updateCharacterStream(java.lang.String p0, java.io.Reader p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(int p0, java.io.Reader p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(java.lang.String p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(java.lang.String p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateCharacterStream");
    }

    @Override
    public java.io.Reader getNCharacterStream(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNCharacterStream");
    }

    @Override
    public java.io.Reader getNCharacterStream(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(java.lang.String p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(java.lang.String p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNCharacterStream");
    }

    @Override
    public java.io.InputStream getAsciiStream(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getAsciiStream");
    }

    @Override
    public java.io.InputStream getAsciiStream(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getAsciiStream");
    }

    @Override
    public java.io.InputStream getUnicodeStream(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getUnicodeStream");
    }

    @Override
    public java.io.InputStream getUnicodeStream(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getUnicodeStream");
    }

    @Override
    public java.io.InputStream getBinaryStream(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBinaryStream");
    }

    @Override
    public java.io.InputStream getBinaryStream(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBinaryStream");
    }

    @Override
    public java.lang.String getCursorName() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getCursorName");
    }

    @Override
    public java.io.Reader getCharacterStream(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getCharacterStream");
    }

    @Override
    public java.io.Reader getCharacterStream(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getCharacterStream");
    }

    @Override
    public void beforeFirst() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.beforeFirst");
    }

    @Override
    public void afterLast() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.afterLast");
    }

    @Override
    public void setFetchDirection(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.setFetchDirection");
    }

    @Override
    public boolean rowUpdated() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.rowUpdated");
    }

    @Override
    public boolean rowInserted() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.rowInserted");
    }

    @Override
    public boolean rowDeleted() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.rowDeleted");
    }

    @Override
    public void updateNull(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNull");
    }

    @Override
    public void updateNull(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNull");
    }

    @Override
    public void updateBoolean(int p0, boolean p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBoolean");
    }

    @Override
    public void updateBoolean(java.lang.String p0, boolean p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBoolean");
    }

    @Override
    public void updateByte(int p0, byte p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateByte");
    }

    @Override
    public void updateByte(java.lang.String p0, byte p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateByte");
    }

    @Override
    public void updateShort(int p0, short p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateShort");
    }

    @Override
    public void updateShort(java.lang.String p0, short p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateShort");
    }

    @Override
    public void updateInt(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateInt");
    }

    @Override
    public void updateInt(java.lang.String p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateInt");
    }

    @Override
    public void updateLong(java.lang.String p0, long p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateLong");
    }

    @Override
    public void updateLong(int p0, long p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateLong");
    }

    @Override
    public void updateFloat(int p0, float p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateFloat");
    }

    @Override
    public void updateFloat(java.lang.String p0, float p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateFloat");
    }

    @Override
    public void updateDouble(java.lang.String p0, double p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateDouble");
    }

    @Override
    public void updateDouble(int p0, double p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateDouble");
    }

    @Override
    public void updateBigDecimal(java.lang.String p0, java.math.BigDecimal p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBigDecimal");
    }

    @Override
    public void updateBigDecimal(int p0, java.math.BigDecimal p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBigDecimal");
    }

    @Override
    public void updateString(java.lang.String p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateString");
    }

    @Override
    public void updateString(int p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateString");
    }

    @Override
    public void updateDate(java.lang.String p0, java.sql.Date p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateDate");
    }

    @Override
    public void updateDate(int p0, java.sql.Date p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateDate");
    }

    @Override
    public void updateTimestamp(java.lang.String p0, java.sql.Timestamp p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateTimestamp");
    }

    @Override
    public void updateTimestamp(int p0, java.sql.Timestamp p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(java.lang.String p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(java.lang.String p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(java.lang.String p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(int p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(java.lang.String p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(java.lang.String p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(int p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(java.lang.String p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBinaryStream");
    }

    @Override
    public void updateObject(java.lang.String p0, java.lang.Object p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateObject");
    }

    @Override
    public void updateObject(int p0, java.lang.Object p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateObject");
    }

    @Override
    public void updateObject(int p0, java.lang.Object p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateObject");
    }

    @Override
    public void updateObject(java.lang.String p0, java.lang.Object p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateObject");
    }

    @Override
    public void insertRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.insertRow");
    }

    @Override
    public void updateRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateRow");
    }

    @Override
    public void deleteRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.deleteRow");
    }

    @Override
    public void refreshRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.moveToCurrentRow");
    }

    @Override
    public java.sql.Blob getBlob(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getBlob");
    }

    @Override
    public java.sql.Clob getClob(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getClob");
    }

    @Override
    public java.sql.Clob getClob(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getClob");
    }

    @Override
    public void updateRef(java.lang.String p0, java.sql.Ref p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateRef");
    }

    @Override
    public void updateRef(int p0, java.sql.Ref p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateRef");
    }

    @Override
    public void updateBlob(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateBlob(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateBlob(java.lang.String p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateBlob(java.lang.String p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateBlob(java.lang.String p0, java.sql.Blob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateBlob(int p0, java.sql.Blob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateBlob");
    }

    @Override
    public void updateClob(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public void updateClob(java.lang.String p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public void updateClob(java.lang.String p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public void updateClob(java.lang.String p0, java.sql.Clob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public void updateClob(int p0, java.sql.Clob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public void updateClob(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateClob");
    }

    @Override
    public java.sql.RowId getRowId(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getRowId");
    }

    @Override
    public java.sql.RowId getRowId(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getRowId");
    }

    @Override
    public void updateRowId(java.lang.String p0, java.sql.RowId p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateRowId");
    }

    @Override
    public void updateRowId(int p0, java.sql.RowId p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateRowId");
    }

    @Override
    public void updateNString(int p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNString");
    }

    @Override
    public void updateNString(java.lang.String p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNString");
    }

    @Override
    public void updateNClob(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public void updateNClob(java.lang.String p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public void updateNClob(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public void updateNClob(java.lang.String p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public void updateNClob(int p0, java.sql.NClob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public void updateNClob(java.lang.String p0, java.sql.NClob p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateNClob");
    }

    @Override
    public java.sql.NClob getNClob(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNClob");
    }

    @Override
    public java.sql.NClob getNClob(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNClob");
    }

    @Override
    public java.sql.SQLXML getSQLXML(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getSQLXML");
    }

    @Override
    public java.sql.SQLXML getSQLXML(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getSQLXML");
    }

    @Override
    public void updateSQLXML(int p0, java.sql.SQLXML p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateSQLXML");
    }

    @Override
    public void updateSQLXML(java.lang.String p0, java.sql.SQLXML p1) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.updateSQLXML");
    }

    @Override
    public java.lang.String getNString(int p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNString");
    }

    @Override
    public java.lang.String getNString(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("ResultSet.getNString");
    }
}
