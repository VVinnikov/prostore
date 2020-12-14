package io.arenadata.dtm.query.execution.plugin.adqm.dto.mppw;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class RestMppwKafkaStopRequest implements Serializable {
    private String requestId;
    private String kafkaTopic;
}
