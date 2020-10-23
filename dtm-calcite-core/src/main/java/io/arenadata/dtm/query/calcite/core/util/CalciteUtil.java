package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;
import static org.apache.calcite.sql.type.SqlTypeName.*;

public class CalciteUtil {
    public static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();

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
