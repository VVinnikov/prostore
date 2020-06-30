package ru.ibs.dtm.query.execution.plugin.adg.factory;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;

public interface AdgMppwKafkaContextFactory {
    AdgMppwKafkaContext create(String datamartMnemonic,
                               String topicName,
                               String tableName,
                               JsonObject schema,
                               long hotDelta);
}
