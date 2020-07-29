package ru.ibs.dtm.query.execution.core.utils;

import org.apache.calcite.sql.type.SqlTypeName;
import ru.ibs.dtm.common.model.ddl.ColumnType;

public class ColumnTypeUtil {
    public static ColumnType valueOf(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case VARCHAR:
                return ColumnType.VARCHAR;
            case CHAR:
                return ColumnType.CHAR;
            case BIGINT:
                return ColumnType.BIGINT;
            case INTEGER:
                return ColumnType.INT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case FLOAT:
                return ColumnType.FLOAT;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            default:
                return ColumnType.ANY;
        }
    }
}
