package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class DtmParameterMetadata implements ParameterMetaData {

    private final BaseConnection connection;
    private final ColumnType[] paramTypes;

    public DtmParameterMetadata(BaseConnection connection, ColumnType[] paramTypes) {
        this.connection = connection;
        this.paramTypes = paramTypes;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return paramTypes.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        this.checkParamIndex(param);
        return 2;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().isSigned(paramTypes[param - 1]);
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
        return connection.getTypeInfo().getSqlType(paramTypes[param - 1]);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getAlias(paramTypes[param - 1]);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        this.checkParamIndex(param);
        return connection.getTypeInfo().getJavaClass(paramTypes[param - 1]);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        this.checkParamIndex(param);
        return 1;
    }

    private void checkParamIndex(int param) throws SQLException {
        if (param < 1 || param > this.paramTypes.length) {
            throw new SQLException(String.format("The parameter index is out of range: %d, number of parameters",
                this.paramTypes.length));
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
