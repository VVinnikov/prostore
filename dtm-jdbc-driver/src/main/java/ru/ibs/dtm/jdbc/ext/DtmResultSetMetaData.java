package ru.ibs.dtm.jdbc.ext;

import lombok.SneakyThrows;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.jdbc.util.DtmException;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class DtmResultSetMetaData implements ResultSetMetaData {
    protected final Connection connection;
    protected final List<ColumnMetadata> fields;

    public DtmResultSetMetaData(Connection connection, List<ColumnMetadata> fields) {
        this.connection = connection;
        this.fields = fields;
    }

    @Override
    public int getColumnCount() {
        return fields.size();
    }

    @SneakyThrows
    @Override
    public String getColumnLabel(int column) {
        try {
            return fields.get(column - 1).getName();
        } catch (Exception e) {
            throw new DtmException("Fields size = " + fields.size() + ": " + fields.toString());
        }
    }

    @Override
    public String getColumnName(int column) {
        return getColumnLabel(column);
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 1;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        ColumnType type = fields.get(column - 1).getType();
        if (type == ColumnType.INT) {
            return JDBCType.INTEGER.getVendorTypeNumber();
        } else {
            return JDBCType
                .valueOf(type.name())
                .getVendorTypeNumber();
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return fields.get(column - 1).getType().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "java.lang.String";
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
