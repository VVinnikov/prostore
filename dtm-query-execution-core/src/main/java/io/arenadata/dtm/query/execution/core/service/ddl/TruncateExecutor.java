package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.ddl.TruncateType;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;

public interface TruncateExecutor {
    Future<String> execute(TruncateContext context);
    TruncateType getType();

    @Autowired
    default void register(TruncateService service) {
        service.addExecutor(this);
    }
}
