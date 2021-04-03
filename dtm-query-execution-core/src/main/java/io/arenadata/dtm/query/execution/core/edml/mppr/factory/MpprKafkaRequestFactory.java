package io.arenadata.dtm.query.execution.core.edml.mppr.factory;

import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;

public interface MpprKafkaRequestFactory {
    Future<MpprKafkaRequest> create(EdmlRequestContext context);
}
