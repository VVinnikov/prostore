package io.arenadata.dtm.jdbc.core;

/**
 * Информация о поле в ResultSet
 */
public class Field {

    /**
     * Название колонки
     */
    private String columnLabel;

    /**
     * Значение в данном поле
     */
    private Object value;

    public Field() {
    }

    public Field(String columnLabel, Object value) {
        this.columnLabel = columnLabel;
        this.value = value;
    }

    public String getColumnLabel() {
        return columnLabel;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Field{" +
                "columnLabel='" + columnLabel + '\'' +
                ", value=" + value +
                '}';
    }
}
