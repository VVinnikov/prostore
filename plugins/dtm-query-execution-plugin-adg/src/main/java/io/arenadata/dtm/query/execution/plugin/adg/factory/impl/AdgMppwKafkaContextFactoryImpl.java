package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AdgMppwKafkaContextFactoryImpl implements AdgMppwKafkaContextFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Override
    public AdgMppwKafkaContext create(MppwKafkaRequest request) {
        val tableName = request.getDestinationTableName();
        val helperTableNames = helperTableNamesFactory.create(
                request.getEnvName(),
                request.getDatamartMnemonic(),
                tableName);
        return new AdgMppwKafkaContext(
                request.getTopic(),
                request.getSysCn(),
                tableName,
                helperTableNames,
                new JsonObject(request.getUploadMetadata().getExternalSchema())
        );
    }
}
