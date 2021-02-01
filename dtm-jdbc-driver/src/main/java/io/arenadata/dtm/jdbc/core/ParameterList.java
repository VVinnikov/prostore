package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public interface ParameterList {

    void setNull(int index, int sqlType) throws SQLException;

    void setBoolean(int index, boolean value, int sqlType) throws SQLException;

    void setByte(int index, byte value, int sqlType) throws SQLException;

    void setShort(int index, short value, int sqlType) throws SQLException;

    void setInt(int index, int value, int sqlType) throws SQLException;

    void setLong(int index, long value, int sqlType) throws SQLException;

    void setFloat(int index, float value, int sqlType) throws SQLException;

    void setDouble(int index, double value, int sqlType) throws SQLException;

    void setBigDecimal(int index, BigDecimal value, int sqlType) throws SQLException;

    void setString(int index, String value, int sqlType) throws SQLException;

    void setBytes(int index, byte[] value, int sqlType) throws SQLException;

    void setDate(int index, Date value, int sqlType) throws SQLException;

    void setTime(int index, Time value, int sqlType) throws SQLException;

    void setTimestamp(int index, Timestamp value, int sqlType) throws SQLException;

    ParameterList copy();

    void clear();

    List<Object> getValues();

    List<ColumnType> getTypes();
}
