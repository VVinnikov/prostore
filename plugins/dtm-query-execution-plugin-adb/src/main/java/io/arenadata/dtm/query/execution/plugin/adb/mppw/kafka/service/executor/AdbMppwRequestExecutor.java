package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;

public interface AdbMppwRequestExecutor {

    Future<QueryResult> execute(MppwKafkaRequest request);
}
