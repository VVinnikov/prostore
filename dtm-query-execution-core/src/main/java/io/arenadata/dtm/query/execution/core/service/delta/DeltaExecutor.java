package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.vertx.core.Future;

import java.util.Collections;
import java.util.List;

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

    default List<PostSqlActionType> getPostActions() {
        return Collections.emptyList();
    }
}
