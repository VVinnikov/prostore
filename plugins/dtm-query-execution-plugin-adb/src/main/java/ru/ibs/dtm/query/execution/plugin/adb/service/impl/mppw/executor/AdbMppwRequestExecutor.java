package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.vertx.core.Future;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface AdbMppwRequestExecutor {

    Future<QueryResult> execute(MppwRequestContext requestContext);
}
