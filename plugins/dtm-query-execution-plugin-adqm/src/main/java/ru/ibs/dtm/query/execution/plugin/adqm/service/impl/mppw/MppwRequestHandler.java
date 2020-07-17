package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.Future;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

public interface MppwRequestHandler {
    Future<QueryResult> execute(MppwRequest request);
}
