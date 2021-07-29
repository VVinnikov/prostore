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
public class CreateSchemaExecutor implements DdlExecutor<Void> {

    private final DatabaseExecutor queryExecutor;
    private final SchemaSqlFactory sqlFactory;

    public CreateSchemaExecutor(DatabaseExecutor adpQueryExecutor) {
        this.queryExecutor = adpQueryExecutor;
        this.sqlFactory = new SchemaSqlFactory();
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return createQuerySql(request)
                .compose(queryExecutor::executeUpdate);
    }

    private Future<String> createQuerySql(DdlRequest request) {
        return Future.future(promise -> {
            String datamartMnemonic = request.getDatamartMnemonic();
            promise.complete(sqlFactory.createSchemaSqlQuery(datamartMnemonic));
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adpDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

}
