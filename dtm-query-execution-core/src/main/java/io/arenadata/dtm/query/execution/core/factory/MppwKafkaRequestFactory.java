package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.Future;

public interface MppwKafkaRequestFactory {

    Future<MppwKafkaRequest> create(EdmlRequestContext context);
}
