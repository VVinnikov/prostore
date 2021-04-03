package io.arenadata.dtm.query.execution.core.delta.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.vertx.core.Future;

/**
 * Delta request executor
 */
public interface DeltaExecutor {

    /**
     * <p>Execute delta query</p>
     *
     * @param deltaQuery delta query
     * @return future object
     */
    Future<QueryResult> execute(DeltaQuery deltaQuery);

    /**
     * Get delta query action
     *
     * @return delta action type
     */
    DeltaAction getAction();
}
