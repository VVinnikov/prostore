package ru.ibs.dtm.common.configuration.kafka;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class KafkaConsumerProperty {
  Map<String, String> adb = new HashMap<>();
  Map<String, String> adg = new HashMap<>();
  Map<String, String> adqm = new HashMap<>();
  Map<String, String> core = new HashMap<>();
}
