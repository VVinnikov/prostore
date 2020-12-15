package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

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
    public void execute(DdlRequestContext context, String sqlNodeName, AsyncHandler<Void> handler) {
        try {
            String schemaName = context.getDatamartName();
            String dropSchemaSqlQuery = sqlFactory.dropSchemaSqlQuery(schemaName);
            adbQueryExecutor.executeUpdate(dropSchemaSqlQuery, ar -> {
                if (ar.succeeded()) {
                    handler.handleSuccess();
                } else {
                    handler.handleError(ar.cause());
                }
            });
        } catch (Exception e) {
            handler.handleError(new DdlDatasourceException("Error generating drop schema query", e));
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
