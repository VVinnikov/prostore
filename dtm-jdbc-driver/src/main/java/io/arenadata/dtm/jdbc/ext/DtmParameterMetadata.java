package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

public class DtmParameterMetadata implements ParameterMetaData {

    private final BaseConnection connection;
    private final List<ColumnType> paramTypes;

    public DtmParameterMetadata(BaseConnection connection, List<ColumnType> paramTypes) {
        this.connection = connection;
        this.paramTypes = paramTypes;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return paramTypes.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        this.checkParamIndex(param);
        return 2;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().isSigned(paramTypes.get(param - 1));
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        this.checkParamIndex(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        this.checkParamIndex(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getSqlType(paramTypes.get(param - 1));
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getAlias(paramTypes.get(param - 1));
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getJavaClass(paramTypes.get(param - 1));
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        this.checkParamIndex(param);
        return 1;
    }

    private void checkParamIndex(int param) throws SQLException {
        if (param < 1 || param > this.paramTypes.size()) {
            throw new SQLException(String.format("The parameter index is out of range: %d, number of parameters", this.paramTypes.size()));
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        } else {
            throw new SQLException("Cannot unwrap to " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }
}
