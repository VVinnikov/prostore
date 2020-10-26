package io.arenadata.dtm.common.plugin.status.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaGroupTopic {
    private String consumerGroup;
    private String topic;
}
