package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TtUploadDataKafkaRequest {
    private String query;
    private String topicName;
    private long maxNumberOfRowsPerMessage;
    private JsonObject avroSchema;
}
