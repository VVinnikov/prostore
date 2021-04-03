package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RestMppwKafkaStopRequest implements Serializable {
    private String requestId;
    private String kafkaTopic;
}
