package io.arenadata.dtm.query.execution.core.service.query;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

public interface QueryDispatcher {

    Future<QueryResult> dispatch(CoreRequestContext<? extends DatamartRequest, ? extends SqlNode> context);
}
