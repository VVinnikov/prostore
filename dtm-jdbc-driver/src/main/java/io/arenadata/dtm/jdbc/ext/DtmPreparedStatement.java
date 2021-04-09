package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.ParameterList;
import io.arenadata.dtm.jdbc.core.QueryParameters;
import io.arenadata.dtm.jdbc.core.SimpleParameterList;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLXML;
import java.sql.*;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;

import static java.sql.Types.*;

@Slf4j
public class DtmPreparedStatement extends DtmStatement implements PreparedStatement {
    private final String sql;
    protected final ParameterList parameters;

    public DtmPreparedStatement(BaseConnection c, int rsType, int rsConcurrency, String sql) throws SQLException {
        super(c, rsType, rsConcurrency);
        this.sql = sql;
        this.parameters = new SimpleParameterList();
        super.prepareQuery(sql);
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
        parameters.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {
        parameters.setBoolean(parameterIndex, value, BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException {
        setShort(parameterIndex, (short) value);
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException {
        parameters.setShort(parameterIndex, value, INTEGER);
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {
        parameters.setInt(parameterIndex, value, INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException {
        parameters.setLong(parameterIndex, value, BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException {
        parameters.setFloat(parameterIndex, value, FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException {
        parameters.setDouble(parameterIndex, value, DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
        parameters.setBigDecimal(parameterIndex, value, DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException {
        parameters.setString(parameterIndex, value, VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] value) throws SQLException {
        parameters.setBytes(parameterIndex, value, ARRAY);
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException {
        long epochDay = value.toLocalDate().toEpochDay();
        parameters.setDate(parameterIndex, epochDay, DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException {
        long nanoOfDay = value.toLocalTime().toNanoOfDay();
        parameters.setTime(parameterIndex, nanoOfDay, TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
        long epochMilli = value.toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        parameters.setTimestamp(parameterIndex, epochMilli, TIMESTAMP);
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
        parameters.clear();
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
        return super.execute(sql, new QueryParameters(parameters.getValues(), parameters.getTypes()));
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
        return createParameterMetaData(connection, parameters.getTypes());
    }

    public ParameterMetaData createParameterMetaData(BaseConnection conn, List<ColumnType> paramTypes) throws SQLException {
        return new DtmParameterMetadata(conn, paramTypes);
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
        if (value == null) {
            parameters.setNull(parameterIndex, targetSqlType);
        } else {
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
