package io.arenadata.dtm.common.eventbus;

public enum DataHeader {
  DATAMART("datamart"),
  STATUS_EVENT_CODE("statusEventCode");

  private final String value;

  DataHeader(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}