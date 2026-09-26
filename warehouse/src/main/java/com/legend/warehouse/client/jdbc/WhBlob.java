package com.legend.warehouse.client.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

/** A blob value: its bytes. */
final class WhBlob implements Blob {

    private final byte[] bytes;

    WhBlob(byte[] bytes) {
        this.bytes = bytes;
    }

    byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public long length() {
        return bytes.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) {
        int from = (int) pos - 1;
        return Arrays.copyOfRange(bytes, from, Math.min(bytes.length, from + length));
    }

    @Override
    public InputStream getBinaryStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) {
        return new ByteArrayInputStream(bytes, (int) pos - 1, (int) length);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw Unsupported.of("Blob.position");
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw Unsupported.of("Blob.position");
    }

    @Override
    public int setBytes(long pos, byte[] b) throws SQLException {
        throw Unsupported.of("Blob.setBytes: a result's blob is read-only");
    }

    @Override
    public int setBytes(long pos, byte[] b, int offset, int len) throws SQLException {
        throw Unsupported.of("Blob.setBytes: a result's blob is read-only");
    }

    @Override
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        throw Unsupported.of("Blob.setBinaryStream: a result's blob is read-only");
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw Unsupported.of("Blob.truncate: a result's blob is read-only");
    }

    @Override
    public void free() {
    }
}
