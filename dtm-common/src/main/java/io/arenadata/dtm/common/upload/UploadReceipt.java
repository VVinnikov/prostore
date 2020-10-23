package io.arenadata.dtm.common.upload;

/*Квитанция об успешной загрузке чанка*/
public class UploadReceipt {
    String requestId;
    String datamartMnemonic;
    Integer sinId;
    Integer streamNumber;
    Integer chunkNumber;
    String tableName;


    public UploadReceipt() {
    }

    public UploadReceipt(String requestId, String datamartMnemonic, Integer sinId, Integer streamNumber, Integer chunkNumber, String tableName) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sinId = sinId;
        this.streamNumber = streamNumber;
        this.chunkNumber = chunkNumber;
        this.tableName = tableName;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public void setDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
    }

    public Integer getSinId() {
        return sinId;
    }

    public void setSinId(Integer sinId) {
        this.sinId = sinId;
    }

    public Integer getStreamNumber() {
        return streamNumber;
    }

    public void setStreamNumber(Integer streamNumber) {
        this.streamNumber = streamNumber;
    }

    public Integer getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(Integer chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}

