package ru.ibs.dtm.common.plugin.status.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaTopicOffset {
    private Long start;
    private Long end;
}
