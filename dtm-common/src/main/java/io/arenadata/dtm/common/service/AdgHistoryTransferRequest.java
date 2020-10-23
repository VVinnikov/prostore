package io.arenadata.dtm.common.service;

import java.util.Objects;
import java.util.UUID;

public class AdgHistoryTransferRequest {
  private UUID requestId;
  private String datamartMnemonic;
  private int sinId;
  private int streamNumber;
  private int chunkNumber;
  private String tableName;

  public UUID getRequestId() {
    return requestId;
  }

  public void setRequestId(UUID requestId) {
    this.requestId = requestId;
  }

  public String getDatamartMnemonic() {
    return datamartMnemonic;
  }

  public void setDatamartMnemonic(String datamartMnemonic) {
    this.datamartMnemonic = datamartMnemonic;
  }

  public int getSinId() {
    return sinId;
  }

  public void setSinId(int sinId) {
    this.sinId = sinId;
  }

  public int getStreamNumber() {
    return streamNumber;
  }

  public void setStreamNumber(int streamNumber) {
    this.streamNumber = streamNumber;
  }

  public int getChunkNumber() {
    return chunkNumber;
  }

  public void setChunkNumber(int chunkNumber) {
    this.chunkNumber = chunkNumber;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AdgHistoryTransferRequest)) return false;
    AdgHistoryTransferRequest that = (AdgHistoryTransferRequest) o;
    return sinId == that.sinId &&
      streamNumber == that.streamNumber &&
      chunkNumber == that.chunkNumber &&
      requestId.equals(that.requestId) &&
      datamartMnemonic.equals(that.datamartMnemonic) &&
      tableName.equals(that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(requestId, datamartMnemonic, sinId, streamNumber, chunkNumber, tableName);
  }

  @Override
  public String toString() {
    return "AdgHistoryTransferRequest{" +
      "requestId=" + requestId +
      ", datamartMnemonic='" + datamartMnemonic + '\'' +
      ", sinId=" + sinId +
      ", streamNumber=" + streamNumber +
      ", chunkNumber=" + chunkNumber +
      ", tableName='" + tableName + '\'' +
      '}';
  }
}
