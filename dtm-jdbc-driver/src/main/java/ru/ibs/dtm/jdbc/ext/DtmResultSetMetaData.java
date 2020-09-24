package ru.ibs.dtm.jdbc.ext;

import lombok.SneakyThrows;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.jdbc.model.ColumnInfo;
import ru.ibs.dtm.jdbc.util.DtmException;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.*;
import java.util.Collections;
import java.util.List;

public class DtmResultSetMetaData implements ResultSetMetaData {
    protected final Connection connection;
    protected final List<ColumnMetadata> fields;
    protected final DtmResultSet resultSet;

    public DtmResultSetMetaData(Connection connection, List<ColumnMetadata> fields, DtmResultSet resultSet) {
        this.connection = connection;
        this.fields = fields == null ? Collections.emptyList() : fields;
        this.resultSet = resultSet;
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
        return this.getCol(column).getDatamartMnemonic();
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
        return this.getCol(column).getNullable() ? 1 : 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return this.getCol(column).getLength();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return this.getCol(column).getAccuracy();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.getCol(column).getEntityMnemonic();
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
        } if (type == ColumnType.ANY) {
            return JDBCType.VARCHAR.getVendorTypeNumber();
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

    private ColumnInfo getCol(int column) {
        return (ColumnInfo)this.resultSet.getColumns().get(column - 1);
    }
}
