package ru.ibs.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Component
@Slf4j
public class DropSchemaExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Autowired
    public DropSchemaExecutor(AdbQueryExecutor adbQueryExecutor, MetadataSqlFactory sqlFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        try {
            String schemaName = context.getDatamartName();
            String dropSchemaSqlQuery = sqlFactory.dropSchemaSqlQuery(schemaName);
            adbQueryExecutor.executeUpdate(dropSchemaSqlQuery, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture());
                } else {
                    log.error("Error deleting schema [{}] !", schemaName, ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Error executing drop schema query!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
