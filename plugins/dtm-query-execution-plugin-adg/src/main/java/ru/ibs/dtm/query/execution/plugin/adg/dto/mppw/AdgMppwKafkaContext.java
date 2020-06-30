package ru.ibs.dtm.query.execution.plugin.adg.dto.mppw;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;

@Data
@AllArgsConstructor
public class AdgMppwKafkaContext {
    private final String topicName;
    private final long hotDelta;
    private final String consumerTableName;
    private final AdgHelperTableNames helperTableNames;
    private final JsonObject schema;
}
