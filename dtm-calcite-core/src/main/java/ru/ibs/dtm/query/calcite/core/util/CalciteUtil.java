package ru.ibs.dtm.query.calcite.core.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import ru.ibs.dtm.query.execution.model.metadata.ColumnType;

import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.*;

public class CalciteUtil {
    public static SqlTypeName valueOf(ColumnType type) {
        switch (type) {
            case BOOLEAN:
                return BOOLEAN;
            case INT:
                return INTEGER;
            case BIGINT:
                return BIGINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case VARCHAR:
            case UUID:
                return VARCHAR;
            default:
                return ANY;
        }
    }

    public static ColumnType toColumnType(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case INTEGER:
                return ColumnType.INT;
            case BIGINT:
                return ColumnType.BIGINT;
            case FLOAT:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case VARCHAR:
                return ColumnType.VARCHAR;
            default:
                return ColumnType.ANY;
        }
    }
}
