package ru.ibs.dtm.query.execution.plugin.adb.calcite;

import org.apache.calcite.sql.type.SqlTypeName;
import ru.ibs.dtm.query.execution.plugin.adb.model.metadata.ColumnType;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

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
