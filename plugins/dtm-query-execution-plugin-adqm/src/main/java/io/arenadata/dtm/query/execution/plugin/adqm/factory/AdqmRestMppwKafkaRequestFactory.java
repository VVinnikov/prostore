package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.mppw.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;

public interface AdqmRestMppwKafkaRequestFactory {

    RestMppwKafkaLoadRequest create(MppwPluginRequest mppwPluginRequest);
}
