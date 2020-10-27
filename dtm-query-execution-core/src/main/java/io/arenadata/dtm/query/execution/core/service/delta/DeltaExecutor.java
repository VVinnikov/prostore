package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Delta request executor
 */
public interface DeltaExecutor {

    /**
     * <p>Execute delta query</p>
     *
     * @param deltaQuery         delta query
     * @param asyncResultHandler asyncResultHandler
     */
    void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Get delta query action
     *
     * @return delta action type
     */
    DeltaAction getAction();
}
