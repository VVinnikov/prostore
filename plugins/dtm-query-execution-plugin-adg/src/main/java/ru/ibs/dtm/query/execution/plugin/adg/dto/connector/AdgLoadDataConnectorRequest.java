package ru.ibs.dtm.query.execution.plugin.adg.dto.connector;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdgLoadDataConnectorRequest {
    private long maxNumberOfMessagesPerPartition;
    private Collection<String> spaces;
    private JsonObject avroSchema;
    private String topicName;
}
