package ru.ibs.dtm.jdbc.model;

/**
 * Information about table column, receiving from LL-R service
 */
public class ColumnInfo {
    /**
     * Column name
     */
    private String mnemonic;
    /**
     * Column type
     */
    private String dataType;
    /**
     * Column length
     */
    private Integer length;
    /**
     * Precision
     */
    private Integer accuracy;
    /**
     * Table name
     */
    private String entityMnemonic;
    /**
     * Schema name
     */
    private String datamartMnemonic;
    /**
     * Order num of primary key
     */
    private Integer primaryKeyOrder;
    /**
     * Order num distributed key
     */
    private Integer distributeKeykOrder;
    /**
     * Column order
     */
    private Integer ordinalPosition;
    /**
     * Nullable
     */
    private Boolean nullable;

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

    public Integer getPrimaryKeyOrder() {
        return primaryKeyOrder;
    }

    public Integer getDistributeKeykOrder() {
        return distributeKeykOrder;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public Boolean getNullable() {
        return nullable;
    }
}
