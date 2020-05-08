package ru.ibs.dtm.common.reader;

import java.util.Objects;
import java.util.UUID;

/**
 * Ключ сообщения с результатами выполнения SQL-запроса
 */
public class QueryResponseKey {

    /**
     * UUID базового запроса, пришедшего в ПОДД
     */
    private UUID requestId;

    /**
     * UUID подзапроса, выделенного из базового, sql-выражение которого передается Витрине
     */
    private String subRequestId;

    /**
     * номер потока выгрузки
     */
    private int streamNumber;

    /**
     * общее количество потоков выгрузки
     */
    private int streamTotal;

    /**
     * номер фрагмента
     */
    private int chunkNumber;

    /**
     * признак последнего фрагмента
     */
    private boolean isLastChunk;


    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public String getSubRequestId() {
        return subRequestId;
    }

    public void setSubRequestId(String subRequestId) {
        this.subRequestId = subRequestId;
    }

    public int getStreamNumber() {
        return streamNumber;
    }

    public void setStreamNumber(int streamNumber) {
        this.streamNumber = streamNumber;
    }

    public int getStreamTotal() {
        return streamTotal;
    }

    public void setStreamTotal(int streamTotal) {
        this.streamTotal = streamTotal;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(int chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    public boolean getIsLastChunk() {
        return isLastChunk;
    }

    public void setLastChunk(boolean lastChunk) {
        isLastChunk = lastChunk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryResponseKey)) return false;
        QueryResponseKey that = (QueryResponseKey) o;
        return streamNumber == that.streamNumber &&
                streamTotal == that.streamTotal &&
                chunkNumber == that.chunkNumber &&
                isLastChunk == that.isLastChunk &&
                requestId.equals(that.requestId) &&
                subRequestId.equals(that.subRequestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, subRequestId, streamNumber, streamTotal, chunkNumber, isLastChunk);
    }

    @Override
    public String toString() {
        return "QueryResponseKey{" +
                "requestId=" + requestId +
                ", subRequestId='" + subRequestId + '\'' +
                ", streamNumber=" + streamNumber +
                ", streamTotal=" + streamTotal +
                ", chunkNumber=" + chunkNumber +
                ", isLastChunk=" + isLastChunk +
                '}';
    }
}
