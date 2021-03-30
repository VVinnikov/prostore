package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;

/**
 * Field information in ResultSet
 */
public class Field {
    /**
     * Column name
     */
    private String columnLabel;
    /**
     * Column size
     */
    private Integer size;
    /**
     * Column sql type id
     */
    private int sqlType;
    /**
     * Column dtm type
     */
    private ColumnType dtmType;
    /**
     * Field metadata
     */
    private FieldMetadata metadata;

    public Field(String columnLabel, ColumnType dtmType) {
        this.columnLabel = columnLabel;
        this.dtmType = dtmType;
    }

    public Field(String columnLabel, ColumnType dtmType, FieldMetadata metadata) {
        this.columnLabel = columnLabel;
        this.dtmType = dtmType;
        this.metadata = metadata;
    }

    public Field(String columnLabel, Integer size, ColumnType dtmType, FieldMetadata metadata) {
        this.columnLabel = columnLabel;
        this.size = size;
        this.dtmType = dtmType;
        this.metadata = metadata;
    }

    public String getColumnLabel() {
        return columnLabel;
    }

    public void setColumnLabel(String columnLabel) {
        this.columnLabel = columnLabel;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public FieldMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(FieldMetadata metadata) {
        this.metadata = metadata;
    }

    public ColumnType getDtmType() {
        return dtmType;
    }

    public void setDtmType(ColumnType dtmType) {
        this.dtmType = dtmType;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }
}
