package ru.ibs.dtm.query.execution.core.utils;

import org.apache.calcite.sql.type.SqlTypeName;
import ru.ibs.dtm.common.model.ddl.ClassTypes;

public class ColumnTypeUtil {
    public static ClassTypes valueOf(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ClassTypes.BOOLEAN;
            case VARCHAR:
                return ClassTypes.VARCHAR;
            case CHAR:
                return ClassTypes.CHAR;
            case BIGINT:
                return ClassTypes.BIGINT;
            case INTEGER:
                return ClassTypes.INT;
            case DOUBLE:
                return ClassTypes.DOUBLE;
            case FLOAT:
                return ClassTypes.FLOAT;
            case DATE:
                return ClassTypes.DATE;
            case TIME:
                return ClassTypes.TIME;
            case TIMESTAMP:
                return ClassTypes.TIMESTAMP;
            default:
                return ClassTypes.ANY;
        }
    }
}
