package ru.ibs.dtm.jdbc.model;

/**
 * Информация о таблице, получаемая из LL-R сервиса
 */
public class TableInfo {
    /**
     * Название таблицы
     */
    private String mnemonic;
    /**
     * Название схемы, которой принадлежит таблица
     */
    private String datamartMnemonic;

    public TableInfo() {
    }

    public String getMnemonic() {
        return mnemonic;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

}