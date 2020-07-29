package ru.ibs.dtm.jdbc.model;

/**
 * Информация о колонке таблицы, получаемая из LL-R сервиса
 */
public class ColumnInfo {
    /**
     * Название колонки
     */
    private String mnemonic;
    /**
     * Тип данных, который хранится в данной колонке
     */
    private String dataType;
    /**
     * Ограничение на длину символов в колонке
     */
    private Integer length;
    /**
     * Точность
     */
    private Integer accuracy;
    /**
     * Название таблицы, которой принадлежит колонка
     */
    private String entityMnemonic;
    /**
     * Название схемы, которой принадлежит колонка
     */
    private String datamartMnemonic;

    public ColumnInfo() {
    }

    public String getMnemonic() {
        return mnemonic;
    }

    public String getDataType() {
        return dataType;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getAccuracy() {
        return accuracy;
    }

    public String getEntityMnemonic() {
        return entityMnemonic;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }
}
