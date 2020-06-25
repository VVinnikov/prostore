package ru.ibs.dtm.query.execution.plugin.adg.calcite;

import org.apache.calcite.sql.type.SqlTypeName;
import ru.ibs.dtm.query.execution.model.metadata.ColumnType;

import static org.apache.calcite.sql.type.SqlTypeName.*;

public class CalciteUtil {
    public static SqlTypeName valueOf(ColumnType type) {
        switch (type) {
            case BOOLEAN:
                return BOOLEAN;
            case INTEGER:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case BIG_DECIMAL:
                return DECIMAL;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case STRING:
                return VARCHAR;
            default:
                return ANY;
        }
    }
}
