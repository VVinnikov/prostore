package ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final QueryExecutorService executorService;
    private final TtCartridgeProvider cartridgeProvider;

    @Autowired
    public DropTableExecutor(QueryExecutorService executorService, TtCartridgeProvider cartridgeProvider) {
        this.executorService = executorService;
        this.cartridgeProvider = cartridgeProvider;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        dropSpacesFromDb(context.getRequest().getClassTable())
                .compose(d -> Future.future((Promise<Void> promise) ->
                        cartridgeProvider.delete(context.getRequest().getClassTable(), promise)))
                .onSuccess(s -> handler.handle(Future.succeededFuture(s)))
                .onFailure(f -> handler.handle(Future.failedFuture(f)));
    }

    private Future<Object> dropSpacesFromDb(final ClassTable classTable) {
        String actualTable = classTable.getName() + ACTUAL_POSTFIX;
        String historyTable = classTable.getName() + HISTORY_POSTFIX;
        String stagingTable = classTable.getName() + STAGING_POSTFIX;
        // TODO It is better to drop all spaces at one, but currently it is not supported by cartridge
        return executorService.executeProcedure(DROP_SPACE, actualTable)
                .compose(f -> executorService.executeProcedure(DROP_SPACE, historyTable))
                .compose(f -> executorService.executeProcedure(DROP_SPACE, stagingTable));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
