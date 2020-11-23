package io.arenadata.dtm.common.model.ddl;

public class EntityTypeUtil {
    public static String pgFromDtmType(EntityField field) {
        switch (field.getType()) {
            case DATE:
                return "date";
            case TIME:
                return "time" + getTimePrecision(field);
            case TIMESTAMP:
                return "timestamp" + getTimePrecision(field);
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
                return "varchar" + getVarcharSize(field);
            case UUID:
                return "varchar(36)";
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", field.getType()));
        }
    }

    private static String getTimePrecision(EntityField field) {
        return field.getAccuracy() == null ? "" : "(" + field.getAccuracy() + ")";
    }

    private static String getVarcharSize(EntityField field) {
        return field.getSize() == null ? "" : "(" + field.getSize() + ")";
    }
}
