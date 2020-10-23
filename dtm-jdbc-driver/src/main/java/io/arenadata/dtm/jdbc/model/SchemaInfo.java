package io.arenadata.dtm.jdbc.model;

/**
 * Информация о схеме, получаемая из LL-R сервиса
 */
public class SchemaInfo {
    /**
     * Идентификатор схемы
     */
    private String id;
    /**
     * Имя схемы
     */
    private String mnemonic;

    public SchemaInfo() {
    }

    public String getId() {
        return id;
    }

    public String getMnemonic() {
        return mnemonic;
    }
}
