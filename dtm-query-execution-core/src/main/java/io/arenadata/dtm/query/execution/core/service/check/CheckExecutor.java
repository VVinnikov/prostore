package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;

public interface CheckExecutor {
    Future<QueryResult> execute(CheckContext context);

    CheckType getType();

    @Autowired
    default void register(CheckService service) {
        service.addExecutor(this);
    }
}
