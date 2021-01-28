package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;

public interface DdlPostExecutor {
    Future<Void> execute(DdlRequestContext context);

    PostSqlActionType getPostActionType();

    @Autowired
    default void register(DdlService<QueryResult> service) {
        service.addPostExecutor(this);
    }
}
