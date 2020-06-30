package ru.ibs.dtm.query.execution.plugin.adqm.model.metadata;

import org.apache.calcite.sql.type.SqlTypeName;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

public enum ColumnType {
    STRING, LONG, INTEGER, BIG_DECIMAL, DOUBLE, FLOAT, DATE, TIMESTAMP, BOOLEAN, BLOB, UUID, ANY;

//    public ColumnType valueOf(ClickHouseDataType dataType) {
//        switch (dataType) {
//            case BOOL:
//                return BOOLEAN;
//            case INT2:
//            case INT4:
//                return INTEGER;
//            case INT8:
//                return LONG;
//            case FLOAT4:
//                return FLOAT;
//            case FLOAT8:
//                return DOUBLE;
//            case NUMERIC:
//                return BIG_DECIMAL;
//            case CHAR:
//            case VARCHAR:
//            case BPCHAR:
//            case TEXT:
//            case NAME:
//            case JSON:
//            case JSONB:
//            case UUID:
//                return STRING;
//            case DATE:
//                return DATE;
//            case TIMESTAMP:
//            case TIMESTAMPTZ:
//                return TIMESTAMP;
//            default:
//                return ANY;
//        }
//    }

    public static ColumnType valueOf(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return BOOLEAN;
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return INTEGER;
            case BIGINT:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case DATE:
                return DATE;
            case DECIMAL:
                return BIG_DECIMAL;
            case CHAR:
            case VARCHAR:
                return STRING;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TIMESTAMP;
            case ANY:
                return ANY;
        }
        return ANY;
    }

}
