package io.arenadata.dtm.jdbc.util;

import java.sql.Types;

public enum DataBaseType {
    BIGINT(Types.BIGINT),
    DATETIME(Types.TIMESTAMP),
    INT(Types.INTEGER),
    INTEGER(Types.INTEGER),
    VARCHAR(Types.VARCHAR);

    DataBaseType(int sqlType) {
        this.sqlType = sqlType;
    }

    private int sqlType;

    public int getSqlType() {
        return sqlType;
    }
}
