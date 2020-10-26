package io.arenadata.dtm.common.configuration.kafka;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class KafkaUploadProperty {
    Map<String, String> requestTopic = new HashMap<>();
    Map<String, String> responseTopic = new HashMap<>();
    Map<String, String> errorTopic = new HashMap<>();
}
