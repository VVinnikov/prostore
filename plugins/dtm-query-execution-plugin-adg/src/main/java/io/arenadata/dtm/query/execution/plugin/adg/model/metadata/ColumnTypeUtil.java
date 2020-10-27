package io.arenadata.dtm.query.execution.plugin.adg.model.metadata;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class ColumnTypeUtil {

    public static ColumnType columnTypeFromTtColumnType(String columnType) {
        switch (columnType) {
            case "unsigned":
            case "integer":
                return ColumnType.BIGINT;
            case "number":
                return ColumnType.DOUBLE;
            case "boolean":
                return ColumnType.BOOLEAN;
            case "string":
                return ColumnType.VARCHAR;
            default:
                return ColumnType.ANY;
        }
    }
}