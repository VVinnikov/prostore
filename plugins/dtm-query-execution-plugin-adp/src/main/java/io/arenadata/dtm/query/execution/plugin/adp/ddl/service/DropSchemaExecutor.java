package io.arenadata.dtm.query.execution.plugin.adp.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.factory.SchemaSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class DropSchemaExecutor implements DdlExecutor<Void> {

    private final DatabaseExecutor queryExecutor;
    private final SchemaSqlFactory sqlFactory;

    @Autowired
    public DropSchemaExecutor(DatabaseExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        this.sqlFactory = new SchemaSqlFactory();
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return createDropQuery(request.getDatamartMnemonic())
                .compose(queryExecutor::executeUpdate);
    }

    private Future<String> createDropQuery(String datamartMnemonic) {
        return Future.future(promise -> {
            String dropSchemaSqlQuery = sqlFactory.dropSchemaSqlQuery(datamartMnemonic);
            promise.complete(dropSchemaSqlQuery);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adpDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

}
