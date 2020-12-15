package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

public interface DdlExecutor<T> {
    void execute(DdlRequestContext context, String sqlNodeName, AsyncHandler<T> handler);

    SqlKind getSqlKind();

    default List<PostSqlActionType> getPostActions(){
        return Arrays.asList(PostSqlActionType.PUBLISH_STATUS, PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
    };

    @Autowired
    default void register(DdlService<T> service) {
        service.addExecutor(this);
    }

}
