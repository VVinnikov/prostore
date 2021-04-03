package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

public interface AdqmRestMppwKafkaRequestFactory {

    RestMppwKafkaLoadRequest create(MppwKafkaRequest mppwPluginRequest);
}
