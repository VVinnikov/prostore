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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Component
@Slf4j
public class CreateSchemaExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;
    private final DropSchemaExecutor dropSchemaExecutor;

    @Autowired
    public CreateSchemaExecutor(AdbQueryExecutor adbQueryExecutor, MetadataSqlFactory sqlFactory, DropSchemaExecutor dropSchemaExecutor) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
        this.dropSchemaExecutor = dropSchemaExecutor;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        try {
            SqlNode query = context.getQuery();
            if (!(query instanceof SqlCreateDatabase)) {
                handler.handle(Future.failedFuture(
                        String.format("Expecting SqlCreateDatabase in context, receiving: %s", context.getQuery())));
                return;
            }
            String schemaName = ((SqlCreateDatabase) query).getName().names.get(0);
            DdlRequestContext dropCtx = createDropRequestContext(schemaName);
            dropSchemaExecutor.execute(dropCtx, SqlKind.DROP_SCHEMA.lowerName, ar -> {
                if (ar.succeeded()) {
                    createSchema(schemaName, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Error executing create schema query!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private DdlRequestContext createDropRequestContext(String schemaName) {
        DdlRequestContext dropCtx = new DdlRequestContext(new DdlRequest(new QueryRequest()));
        dropCtx.setDatamartName(schemaName);
        return dropCtx;
    }

    private void createSchema(String schemaName, Handler<AsyncResult<Void>> handler) {
        String createSchemaSql = sqlFactory.createSchemaSqlQuery(schemaName);
        adbQueryExecutor.executeUpdate(createSchemaSql, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                log.error("Error create schema [{}]!", schemaName, ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
