package ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Component
public class DropSchemaExecutor implements DdlExecutor<Void> {

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
