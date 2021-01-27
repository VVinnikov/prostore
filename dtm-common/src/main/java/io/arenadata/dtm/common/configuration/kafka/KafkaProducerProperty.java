package io.arenadata.dtm.common.configuration.kafka;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class KafkaProducerProperty {
  Map<String, String> property = new HashMap<>();
}
