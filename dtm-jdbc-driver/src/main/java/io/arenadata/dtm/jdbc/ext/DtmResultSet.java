package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.core.Tuple;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class DtmResultSet extends AbstractResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DtmResultSet.class);
    private Field[] fields;
    protected List<Tuple> rows;
    private int currentRow = -1;
    private Tuple thisRow;
    private BaseConnection connection;
    private ResultSetMetaData rsMetaData;
    private Map<String, Integer> columnNameIndexMap;
    private final ZoneId zoneId;

    public DtmResultSet(BaseConnection connection, Field[] fields, List<Tuple> tuples, ZoneId timeZone) {
        this.connection = connection;
        this.fields = fields;
        this.rows = tuples;
        this.thisRow = (tuples == null || tuples.isEmpty()) ?
                new Tuple(0) : tuples.get(0);
        this.zoneId = timeZone;
    }

    public static DtmResultSet createEmptyResultSet() {
        return new DtmResultSet(null,
                new Field[]{new Field("", ColumnType.VARCHAR)},
                Collections.emptyList(),
                DtmConnectionImpl.DEFAULT_TIME_ZONE);
    }

    @Override
    public boolean next() {
        if (this.currentRow + 1 >= this.rows.size()) {
            return false;
        } else {
            this.currentRow++;
        }
        initRowBuffer();
        return true;
    }

    @Override
    public boolean first() throws SQLException {
        if (this.rows.size() <= 0) {
            return false;
        }

        this.currentRow = 0;
        initRowBuffer();

        return true;
    }

    private void initRowBuffer() {
        this.thisRow = this.rows.get(this.currentRow);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.getString(findColumn(columnLabel));
    }


    @SneakyThrows
    @Override
    public int findColumn(String columnLabel) {
        int col = findColumnIndex(columnLabel);
        if (col == 0) {
            throw new DtmSqlException("Column not found: " + columnLabel);
        }
        return col;
    }

    private int findColumnIndex(String columnName) {
        if (this.columnNameIndexMap == null) {
            this.columnNameIndexMap = createColumnNameIndexMap(this.fields);
        }
        Integer index = this.columnNameIndexMap.get(columnName);
        if (index != null) {
            return index;
        } else {
            return 0;
        }
    }

    private Map<String, Integer> createColumnNameIndexMap(Field[] fields) {
        Map<String, Integer> columnNameIndexMap = new HashMap(fields.length * 2);

        for (int i = fields.length - 1; i >= 0; --i) {
            String columnLabel = fields[i].getColumnLabel();
            columnNameIndexMap.put(columnLabel, i + 1);
        }

        return columnNameIndexMap;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        if (this.rsMetaData == null) {
            this.rsMetaData = createMetaData();
        }
        return this.rsMetaData;
    }

    protected ResultSetMetaData createMetaData() {
        return new DtmResultSetMetaData(this.connection, this.fields);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        final Field field = this.fields[columnIndex - 1];
        if (this.getValue(columnIndex) == null) {
            return null;
        }
        switch (field.getDtmType()) {
            case INT:
            case BIGINT:
                return this.getLong(columnIndex);
            case VARCHAR:
            case ANY:
            case CHAR:
            case UUID:
            case BLOB:
                return this.getString(columnIndex);
            case FLOAT:
                return this.getFloat(columnIndex);
            case DOUBLE:
                return this.getDouble(columnIndex);
            case BOOLEAN:
                return this.getBoolean(columnIndex);
            case DATE:
                return this.getDate(columnIndex);
            case TIME:
                return this.getTime(columnIndex);
            case TIMESTAMP:
                return this.getTimestamp(columnIndex);
            default:
                throw new SQLException(String.format("Column type %s for index %s not found!",
                        field.getDtmType(), columnIndex));
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    private Object getValue(int columnIndex) throws SQLException {
        if (this.thisRow == null) {
            throw new DtmSqlException("ResultSet not positioned properly, perhaps you need to call next.");
        } else {
            return this.thisRow.get(columnIndex - 1);
        }
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value != null && (boolean) value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : Byte.parseByte(value.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : (Short) value;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : (Integer) value;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        //FIXME Dbeaver used this method for received value of INT field
        final Object value = this.getValue(columnIndex);
        if (value == null) {
            return 0L;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? 0 : ((Number) value).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value == null) {
            return 0.0D;
        } else {
            return ((Number) value).doubleValue();
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String string = this.getString(columnIndex);
        if (string == null) {
            return null;
        } else {
            BigDecimal result = new BigDecimal(string);
            return result.setScale(scale, RoundingMode.HALF_UP);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        return value == null ? new byte[0] : value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        if (value != null) {
            return Date.valueOf(LocalDate.ofEpochDay(((Number) value).longValue()));
        } else {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value != null) {
            long longValue = ((Number) value).longValue();
            long epochSeconds = longValue / 1000000;
            return new Time(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds,
                    getNanos(columnIndex, longValue)
            ), zoneId)).getTime());
        } else {
            return null;
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        final Object value = this.getValue(columnIndex);
        if (value == null) {
            return null;
        } else {
            Number numberValue = (Number) value;
            long epochSeconds = numberValue.longValue() / 1000000;
            int nanos = getNanos(columnIndex, numberValue);
            return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanos), zoneId));
        }
    }

    private int getNanos(int columnIndex, Number tsValue) {
        Field field = fields[columnIndex - 1];
        if (field.getSize() != null) {
            int q = (int) Math.pow(10, 6 - field.getSize());
            return (int) (tsValue.longValue() % 1000000 / q * 1000 * q);
        } else {
            return 0;
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(this.findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(this.findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(this.findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return this.getDate(this.findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return this.getTime(this.findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        return value == null ? null : new BigDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Object value = this.getValue(columnIndex);
        if (value != null) {
            return Date.valueOf((LocalDate) value);
        } else {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return this.getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return this.getTime(this.findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return this.getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel), cal);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

}
