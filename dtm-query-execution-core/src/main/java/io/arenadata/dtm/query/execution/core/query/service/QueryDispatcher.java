package io.arenadata.dtm.query.execution.core.query.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

public interface QueryDispatcher {

    Future<QueryResult> dispatch(CoreRequestContext<? extends DatamartRequest, ? extends SqlNode> context);
}
