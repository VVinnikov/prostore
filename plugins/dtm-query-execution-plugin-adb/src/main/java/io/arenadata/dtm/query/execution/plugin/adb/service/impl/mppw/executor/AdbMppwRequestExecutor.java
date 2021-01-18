package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.vertx.core.Future;

public interface AdbMppwRequestExecutor {

    Future<QueryResult> execute(MppwPluginRequest request);
}
