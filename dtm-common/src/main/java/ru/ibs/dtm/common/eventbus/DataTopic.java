package ru.ibs.dtm.common.eventbus;

public enum DataTopic {
  STATUS_EVENT_PUBLISH("status.event.publish");

  private final String value;

  DataTopic(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
