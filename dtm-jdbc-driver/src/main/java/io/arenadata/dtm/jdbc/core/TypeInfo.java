package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public interface TypeInfo {

    boolean isSigned(ColumnType type);

    String getJavaClass(ColumnType type);

    Integer getSqlType(ColumnType type);

    String getAlias(ColumnType type);
}
