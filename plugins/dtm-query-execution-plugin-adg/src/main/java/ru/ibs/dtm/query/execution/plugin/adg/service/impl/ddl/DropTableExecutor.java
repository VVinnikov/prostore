package ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesQueueRequest;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesRequest;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.Arrays;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final QueryExecutorService executorService;
    private final TtCartridgeProvider cartridgeProvider;
    private final TtCartridgeClient cartridgeClient;

    @Autowired
    public DropTableExecutor(QueryExecutorService executorService, TtCartridgeProvider cartridgeProvider, TtCartridgeClient cartridgeClient) {
        this.executorService = executorService;
        this.cartridgeProvider = cartridgeProvider;
        this.cartridgeClient = cartridgeClient;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        val classTable = context.getRequest().getClassTable();
        String actualTable = classTable.getName() + ACTUAL_POSTFIX;
        String historyTable = classTable.getName() + HISTORY_POSTFIX;
        String stagingTable = classTable.getName() + STAGING_POSTFIX;

        val request = new TtDeleteTablesRequest(Arrays.asList(stagingTable,actualTable,historyTable));
        cartridgeClient.addSpacesToDeleteQueue(request, ar -> {
            if(ar.succeeded()) {
                val response = ar.result();
                val deleteRequest = new TtDeleteTablesQueueRequest(response.getBatchId());
                cartridgeClient.executeDeleteQueue(deleteRequest, del_ar -> {
                    if(del_ar.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        handler.handle(Future.failedFuture(del_ar.cause()));
                    }
                });
            }
            else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
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
