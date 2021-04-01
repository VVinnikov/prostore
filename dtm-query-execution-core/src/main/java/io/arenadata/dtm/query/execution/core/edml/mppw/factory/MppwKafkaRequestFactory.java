package io.arenadata.dtm.query.execution.core.edml.mppw.factory;

import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;

public interface MppwKafkaRequestFactory {

    Future<MppwKafkaRequest> create(EdmlRequestContext context);
}
