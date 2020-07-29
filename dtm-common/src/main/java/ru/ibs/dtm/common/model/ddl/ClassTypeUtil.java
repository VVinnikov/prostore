package ru.ibs.dtm.common.model.ddl;

public class ClassTypeUtil {
    public static String pgFromDtmType(ClassField field) {
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
                throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", field.getType()));
        }
    }

    private static String getTimePrecision(ClassField field) {
        return field.getAccuracy() == null ? "" : "(" + field.getAccuracy() + ")";
    }

    private static String getVarcharSize(ClassField field) {
        return field.getSize() == null ? "" : "(" + field.getSize() + ")";
    }
}
