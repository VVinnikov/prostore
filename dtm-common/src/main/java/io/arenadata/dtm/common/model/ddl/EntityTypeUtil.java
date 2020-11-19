package io.arenadata.dtm.common.model.ddl;

import java.util.Optional;

public class EntityTypeUtil {
    public static String pgFromDtmType(EntityField field) {
        return pgFromDtmType(field.getType(), field.getSize(), field.getAccuracy());
    }

    public static String pgFromDtmType(ColumnType type, Integer size, Integer accuracy) {
        switch (type) {
            case DATE:
                return "date";
            case TIME:
                return "time" + getTimePrecision(accuracy);
            case TIMESTAMP:
                return "timestamp" + getTimePrecision(accuracy);
            case FLOAT:
                return "real";
            case DOUBLE:
                return "float8";
            case BOOLEAN:
                return "boolean";
            case INT:
            case BIGINT:
                return "int8";
            case CHAR:
            case VARCHAR:
                return "varchar" + getVarcharSize(size);
            case UUID:
                return "varchar(36)";
            default:
                throw new UnsupportedOperationException(String.format("`%s` not supported", type));
        }
    }

    private static String getTimePrecision(Integer accuracy) {
        return Optional.ofNullable(accuracy)
                .map(accuracyVal -> String.format("(%s)", accuracyVal))
                .orElse("");
    }

    private static String getVarcharSize(Integer size) {
        return Optional.ofNullable(size)
                .map(sizeVal -> String.format("(%s)", sizeVal))
                .orElse("");
    }
}
