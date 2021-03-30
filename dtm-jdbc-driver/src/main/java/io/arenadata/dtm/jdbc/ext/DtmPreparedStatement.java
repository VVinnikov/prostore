/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLXML;
import java.sql.*;
import java.util.Calendar;

import static java.sql.Types.*;

@Slf4j
public class DtmPreparedStatement extends DtmStatement implements PreparedStatement {
    private final String sql;

    public DtmPreparedStatement(BaseConnection c, int rsType, int rsConcurrency, String sql) throws SQLException {
        super(c, rsType, rsConcurrency);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return result.getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {

    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {

    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {

    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {

    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {

    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.setObject(parameterIndex, x, targetSqlType, -1);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            this.setNull(parameterIndex, OTHER);
        } else if (x instanceof String) {
            this.setString(parameterIndex, (String) x);
        } else if (x instanceof BigDecimal) {
            this.setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof Short) {
            this.setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            this.setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            this.setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            this.setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            this.setDouble(parameterIndex, (Double) x);
        } else if (x instanceof byte[]) {
            this.setBytes(parameterIndex, (byte[]) ((byte[]) x));
        } else if (x instanceof Date) {
            this.setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            this.setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            this.setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Boolean) {
            this.setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            this.setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Character) {
            this.setString(parameterIndex, ((Character) x).toString());
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return result.getResultSet().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType, int scaleOrLength) throws SQLException {
        switch (targetSqlType) {
            //TODO implement bigDecimal setting
            case BOOLEAN:
                this.setBoolean(parameterIndex, (boolean) value);
                break;
            case INTEGER:
                this.setInt(parameterIndex, (int) value);
                break;
            case FLOAT:
                this.setFloat(parameterIndex, (float) value);
                break;
            case DOUBLE:
                this.setDouble(parameterIndex, (double) value);
                break;
            case BIGINT:
                this.setLong(parameterIndex, (long) value);
                break;
            case CHAR:
            case VARCHAR:
                this.setString(parameterIndex, value.toString());
                break;
            case DATE:
                this.setDate(parameterIndex, (Date) value);
                break;
            case TIME:
                this.setTime(parameterIndex, (Time) value);
                break;
            case TIMESTAMP:
                this.setTimestamp(parameterIndex, (Timestamp) value);
                break;
            default:
                throw new DtmSqlException(String.format("Type %s does not support", targetSqlType));
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }
}
