package io.arenadata.dtm.query.execution.plugin.api.service.dml;

import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;

public interface DmlExecutor<T> {

    void execute(DmlRequestContext context, Handler<AsyncResult<T>> handler);

    SqlKind getSqlKind();

    @Autowired
    default void register(DmlService<T> service) {
        service.addExecutor(this);
    }
}
