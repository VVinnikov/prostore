package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;

public interface AdqmRestMppwKafkaRequestFactory {

    RestMppwKafkaLoadRequest create(MppwRequest mppwRequest);
}
