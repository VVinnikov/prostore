package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;

public interface DdlExecutor<T> {
    void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<T>> handler);

    SqlKind getSqlKind();

    @Autowired
    default void register(DdlService<T> service) {
        service.addExecutor(this);
    }
}
