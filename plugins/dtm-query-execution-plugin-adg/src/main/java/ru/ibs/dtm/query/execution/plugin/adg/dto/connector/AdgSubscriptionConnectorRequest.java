package ru.ibs.dtm.query.execution.plugin.adg.dto.connector;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdgSubscriptionConnectorRequest {
    private long maxNumberOfMessagesPerPartition;
    private JsonObject avroSchema;
    private String topicName;
}
