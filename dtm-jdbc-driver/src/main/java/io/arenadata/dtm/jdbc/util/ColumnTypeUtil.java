package io.arenadata.dtm.jdbc.util;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import static java.sql.Types.*;

public class ColumnTypeUtil {

    public static ColumnType getColumnType(int sqlType) throws DtmSqlException {
        switch (sqlType) {
            case SMALLINT:
            case TINYINT:
            case INTEGER:
                return ColumnType.INT;
            case BIGINT:
                return ColumnType.BIGINT;
            case CHAR:
                return ColumnType.CHAR;
            case VARCHAR:
                return ColumnType.VARCHAR;
            case FLOAT:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case OTHER:
                return ColumnType.ANY;
            default:
                throw new DtmSqlException(String.format("Unknown mapping from sql type [%d]", sqlType));
        }
    }
}
