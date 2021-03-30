package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;

public interface DdlExecutor<T> {

    Future<T> execute(DdlRequest request);

    SqlKind getSqlKind();

    @Autowired
    default void register(DdlService<T> service) {
        service.addExecutor(this);
    }

}
