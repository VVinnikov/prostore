package ru.ibs.dtm.common.configuration.kafka;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerProperty {
  Map<String, String> property = new HashMap<>();

  public Map<String, String> getProperty() {
    return property;
  }

  public void setProperty(Map<String, String> property) {
    this.property = property;
  }
}
