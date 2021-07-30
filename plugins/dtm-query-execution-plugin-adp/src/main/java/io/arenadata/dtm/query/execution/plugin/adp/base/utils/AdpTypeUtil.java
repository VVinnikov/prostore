package io.arenadata.dtm.query.execution.plugin.adp.base.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityField;

import java.util.Optional;

public class AdpTypeUtil {

    private AdpTypeUtil() {
    }

    public static String adpTypeFromDtmType(EntityField field) {
        return adpTypeFromDtmType(field.getType(), field.getSize());
    }

    public static String adpTypeFromDtmType(ColumnType type, Integer size) {
        switch (type) {
            case DATE:
                return "date";
            case TIME:
                return "time(6)";
            case TIMESTAMP:
                return "timestamp(6)";
            case FLOAT:
                return "float4";
            case DOUBLE:
                return "float8";
            case BOOLEAN:
                return "bool";
            case INT:
            case BIGINT:
                return "int8";
            case INT32:
                return "int4";
            case CHAR:
            case VARCHAR:
                return "varchar" + getVarcharSize(size);
            case UUID:
                return "varchar(36)";
            case LINK:
                return "varchar";
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", type));
        }
    }

    private static String getVarcharSize(Integer size) {
        return Optional.ofNullable(size)
                .map(sizeVal -> String.format("(%s)", sizeVal))
                .orElse("");
    }

}
