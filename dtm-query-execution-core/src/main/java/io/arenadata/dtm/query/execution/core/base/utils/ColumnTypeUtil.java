package io.arenadata.dtm.query.execution.core.base.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.apache.calcite.sql.type.SqlTypeName;

import static io.arenadata.dtm.common.model.ddl.ColumnType.*;

public class ColumnTypeUtil {
    public static ColumnType valueOf(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case VARCHAR:
                return VARCHAR;
            case CHAR:
                return CHAR;
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

    public static ColumnType fromTypeString(String typeString) {
        switch (typeString) {
            case "varchar":
                return VARCHAR;
            case "char":
                return CHAR;
            case "bigint":
                return BIGINT;
            case "int":
            case "integer":
                return INT;
            case "double":
                return DOUBLE;
            case "float":
                return FLOAT;
            case "date":
                return DATE;
            case "time":
                return TIME;
            case "timestamp":
                return TIMESTAMP;
            case "boolean":
                return BOOLEAN;
            case "blob":
                return BLOB;
            case "uuid":
                return UUID;
            case "any":
            default:
                return ANY;
        }
    }
}
