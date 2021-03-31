package io.arenadata.dtm.common.model.ddl;

import java.util.Optional;

public class EntityTypeUtil {

    private EntityTypeUtil() {
    }

    public static String pgFromDtmType(EntityField field) {
        return pgFromDtmType(field.getType(), field.getSize());
    }

    public static String pgFromDtmType(ColumnType type, Integer size) {
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
            case CHAR:
            case VARCHAR:
                return "varchar" + getVarcharSize(size);
            case UUID:
                return "varchar(36)";
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
