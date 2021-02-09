package io.arenadata.dtm.jdbc.model;

/**
 * Table information from LL-R сервиса
 */
public class TableInfo {
    /**
     * table name
     */
    private String mnemonic;
    /**
     * schema name
     */
    private String datamartMnemonic;

    public String getMnemonic() {
        return mnemonic;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "mnemonic='" + mnemonic + '\'' +
                ", datamartMnemonic='" + datamartMnemonic + '\'' +
                '}';
    }
}
