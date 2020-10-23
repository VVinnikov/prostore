package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;

public interface AdbMppwRequestExecutor {

    Future<QueryResult> execute(MppwRequestContext requestContext);
}
