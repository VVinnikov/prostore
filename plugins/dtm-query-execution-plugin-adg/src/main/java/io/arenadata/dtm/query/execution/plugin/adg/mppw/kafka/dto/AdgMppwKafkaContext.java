package io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.dto;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.AdgHelperTableNames;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdgMppwKafkaContext {
    private final String topicName;
    private final long hotDelta;
    private final String consumerTableName;
    private final AdgHelperTableNames helperTableNames;
    private final JsonObject schema;
}
