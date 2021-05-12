package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.util.ColumnTypeUtil;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;

public class SimpleParameterList implements ParameterList {

    private static final Object NULL_OBJECT = new Object();
    private Object[] paramValues;
    private ColumnType[] paramTypes;

    public SimpleParameterList(int size) {
        this.paramValues = new Object[size];
        this.paramTypes = new ColumnType[size];
    }

    private void bind(int index, Object value, int sqlType) throws DtmSqlException {
        int normalIndex = index - 1;
        try {
            paramValues[normalIndex] = value;
            paramTypes[normalIndex] = ColumnTypeUtil.getColumnType(sqlType);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new DtmSqlException(String.format("The column index is out of range: %d, number of columns: %d",
                index, paramValues.length));
        }
    }

    @Override
    public void setNull(int index, int sqlType) throws SQLException {
        this.bind(index, NULL_OBJECT, sqlType);
    }

    @Override
    public void setBoolean(int index, boolean value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setByte(int index, byte value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setShort(int index, short value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setInt(int index, int value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setLong(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setFloat(int index, float value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDouble(int index, double value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBigDecimal(int index, BigDecimal value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setString(int index, String value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBytes(int index, byte[] value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDate(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTime(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTimestamp(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public ParameterList copy() {
        SimpleParameterList newCopy = new SimpleParameterList(paramTypes.length);
        newCopy.paramTypes = Arrays.copyOf(paramTypes, paramTypes.length);
        newCopy.paramValues = Arrays.copyOf(paramValues, paramValues.length);
        return newCopy;
    }

    @Override
    public void clear() {
        Arrays.fill(this.paramValues, null);
        Arrays.fill(this.paramTypes, null);
    }

    @Override
    public Object[] getValues() {
        return paramValues;
    }

    @Override
    public ColumnType[] getTypes() {
        return paramTypes;
    }

    @Data
    @AllArgsConstructor
    private static final class Param {
        private int index;
        private Object value;
        private ColumnType columnType;
    }
}
