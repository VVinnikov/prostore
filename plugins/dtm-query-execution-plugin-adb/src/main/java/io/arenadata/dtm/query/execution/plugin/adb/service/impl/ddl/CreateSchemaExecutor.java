package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
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
public class CreateSchemaExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Autowired
    public CreateSchemaExecutor(AdbQueryExecutor adbQueryExecutor,
                                MetadataSqlFactory sqlFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
    }

    @Override
    public void execute(DdlRequestContext context,
                        String sqlNodeName,
                        AsyncHandler<Void> handler) {
        try {
            SqlNode query = context.getQuery();
            if (!(query instanceof SqlCreateDatabase)) {
                handler.handleError(new DdlDatasourceException(
                        String.format("Expecting SqlCreateDatabase in context, receiving: %s",
                                context.getQuery())));
                return;
            }
            String schemaName = ((SqlCreateDatabase) query).getName().names.get(0);
            createSchema(schemaName, handler);
        } catch (Exception e) {
            handler.handleError(new DdlDatasourceException("Error generating create schema query", e));
        }
    }

    private void createSchema(String schemaName, AsyncHandler<Void> handler) {
        String createSchemaSql = sqlFactory.createSchemaSqlQuery(schemaName);
        adbQueryExecutor.executeUpdate(createSchemaSql, ar -> {
            if (ar.succeeded()) {
                handler.handleSuccess();
            } else {
                handler.handleError(ar.cause());
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
