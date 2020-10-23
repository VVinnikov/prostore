package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;
    private final DropTableExecutor dropTableExecutor;

    @Autowired
    public CreateTableExecutor(AdbQueryExecutor adbQueryExecutor, MetadataSqlFactory sqlFactory, DropTableExecutor dropTableExecutor) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
        this.dropTableExecutor = dropTableExecutor;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        try {
            SqlNode query = context.getQuery();
            if (!(query instanceof SqlCreateTable)) {
                handler.handle(Future.failedFuture(
                        String.format("Expecting SqlCreateTable in context, receiving: %s", context.getQuery())));
                return;
            }
            DdlRequestContext dropCtx = createDropRequestContext(context);
            dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName, ar -> {
                if (ar.succeeded()) {
                    createTable(context, handler);
                } else {
                    log.error("Error executing drop table query!", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Error executing create table query!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createTable(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        String createSql = sqlFactory.createTableScripts(context.getRequest().getEntity());
        adbQueryExecutor.executeUpdate(createSql, handler);
    }

    private DdlRequestContext createDropRequestContext(DdlRequestContext context) {
        return new DdlRequestContext(new DdlRequest(new QueryRequest(), context.getRequest().getEntity()));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
