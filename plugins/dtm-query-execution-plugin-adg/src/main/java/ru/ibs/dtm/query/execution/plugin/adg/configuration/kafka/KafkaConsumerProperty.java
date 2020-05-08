package ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class KafkaConsumerProperty {
  Map<String, String> property = new HashMap<>();
}
