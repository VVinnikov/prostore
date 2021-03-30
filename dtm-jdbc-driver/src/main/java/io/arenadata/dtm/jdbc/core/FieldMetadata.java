package io.arenadata.dtm.jdbc.core;

public class FieldMetadata {

    final String columnName;
    final String tableName;
    final String schemaName;
    final int nullable;
    final boolean autoIncrement;

    public FieldMetadata(String columnName) {
        this(columnName, "", "", 1, false);
    }

    public FieldMetadata(String columnName, String schemaName) {
        this(columnName, "", schemaName, 1, false);
    }

    FieldMetadata(String columnName, String tableName, String schemaName, int nullable, boolean autoIncrement) {
        this.columnName = columnName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public int getNullable() {
        return nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }
}
