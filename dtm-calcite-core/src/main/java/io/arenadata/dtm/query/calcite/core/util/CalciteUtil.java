package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.val;
import org.apache.calcite.sql.type.SqlTypeName;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.calcite.sql.type.SqlTypeName.*;

public class CalciteUtil {
    private static final String LOCAL_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter LOCAL_DATE_TIME = DateTimeFormatter.ofPattern(LOCAL_DATE_TIME_PATTERN);

    public static LocalDateTime parseLocalDateTime(String localDateTime) {
        try {
            return LocalDateTime.parse(localDateTime, CalciteUtil.LOCAL_DATE_TIME);
        } catch (Exception e) {
            throw new DtmException(String.format("Time [%s] is not in format: [%s]",
                    localDateTime,
                    LOCAL_DATE_TIME_PATTERN),
                    e);
        }
    }

    public static String parseSchemaName(String nameWithSchema) {
        int indexComma = nameWithSchema.indexOf(".");
        return indexComma != -1 ? nameWithSchema.substring(0, indexComma) : null;
    }

    public static String parseTableName(String nameWithSchema) {
        int indexComma = nameWithSchema.indexOf(".");
        return nameWithSchema.substring(indexComma + 1);
    }

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
            case TIME:
                return TIME;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case CHAR:
                return CHAR;
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
            case CHAR:
                return ColumnType.CHAR;
            default:
                return ColumnType.ANY;
        }
    }
}
