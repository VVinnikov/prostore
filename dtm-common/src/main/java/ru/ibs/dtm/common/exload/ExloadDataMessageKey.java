package ru.ibs.dtm.common.exload;

import java.util.Objects;

public class ExloadDataMessageKey {

    private String tableName;       //имя выгружаемой таблицы
    private Integer streamNumber;   //номер потока
    private Integer streamTotal;    //общее кол-во потоков
    private Integer chunkNumber;    //номер NQ предпоследнего запроса
    private Boolean isLastChunk;    //признак последнего чанка

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getStreamNumber() {
        return streamNumber;
    }

    public void setStreamNumber(Integer streamNumber) {
        this.streamNumber = streamNumber;
    }

    public Integer getStreamTotal() {
        return streamTotal;
    }

    public void setStreamTotal(Integer streamTotal) {
        this.streamTotal = streamTotal;
    }

    public Integer getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(Integer chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    public Boolean getIsLastChunk() {
        return isLastChunk;
    }

    public void setIsLastChunk(Boolean islastChunk) {
        isLastChunk = islastChunk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExloadDataMessageKey)) return false;
        ExloadDataMessageKey key = (ExloadDataMessageKey) o;
        return tableName.equals(key.tableName) &&
                streamNumber.equals(key.streamNumber) &&
                streamTotal.equals(key.streamTotal) &&
                chunkNumber.equals(key.chunkNumber) &&
                isLastChunk.equals(key.isLastChunk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, streamNumber, streamTotal, chunkNumber, isLastChunk);
    }

    @Override
    public String toString() {
        return "ExloadDataMessageKey{" +
                "tableName='" + tableName + '\'' +
                ", streamNumber=" + streamNumber +
                ", streamTotal=" + streamTotal +
                ", chunkNumber=" + chunkNumber +
                ", isLastChunk=" + isLastChunk +
                '}';
    }
}
