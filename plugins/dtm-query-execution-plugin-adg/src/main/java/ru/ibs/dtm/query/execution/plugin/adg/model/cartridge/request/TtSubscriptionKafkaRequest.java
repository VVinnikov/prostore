package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TtSubscriptionKafkaRequest {
    private long maxNumberOfMessagesPerPartition;
    private JsonObject avroSchema;
    private String topicName;
}
