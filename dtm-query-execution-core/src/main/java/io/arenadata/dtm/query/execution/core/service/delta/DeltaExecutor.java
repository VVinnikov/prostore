package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;

/**
 * Delta request executor
 */
public interface DeltaExecutor {

    /**
     * <p>Execute delta query</p>
     *  @param deltaQuery         delta query
     * @param handler asyncResultHandler
     */
    void execute(DeltaQuery deltaQuery, AsyncHandler<QueryResult> handler);

    /**
     * Get delta query action
     *
     * @return delta action type
     */
    DeltaAction getAction();
}
