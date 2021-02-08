package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;

public interface DmlExecutor<T> {

    Future<T> execute(DmlRequestContext context);

    DmlType getType();

    @Autowired
    default void register(DmlService<T> service) {
        service.addExecutor(this);
    }
}