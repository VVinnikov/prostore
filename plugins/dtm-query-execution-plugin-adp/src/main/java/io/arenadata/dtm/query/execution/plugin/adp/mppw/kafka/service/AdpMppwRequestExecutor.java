package io.arenadata.dtm.query.execution.plugin.adp.mppw.kafka.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;

public interface AdpMppwRequestExecutor {
    Future<QueryResult> execute(MppwKafkaRequest request);
}
