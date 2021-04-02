package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;

public interface KafkaMppwRequestHandler {
    Future<QueryResult> execute(MppwKafkaRequest request);
}
