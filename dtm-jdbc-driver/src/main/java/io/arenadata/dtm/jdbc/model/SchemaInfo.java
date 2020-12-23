package io.arenadata.dtm.jdbc.model;

/**
 * Schema information from LL-R сервиса
 */
public class SchemaInfo {
    /**
     * schema id
     */
    private String id;
    /**
     * schema name
     */
    private String mnemonic;

    public String getId() {
        return id;
    }

    public String getMnemonic() {
        return mnemonic;
    }
}
