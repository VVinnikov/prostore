package io.arenadata.dtm.query.execution.plugin.adp.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.factory.SchemaSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final DatabaseExecutor AdpQueryExecutor;
    private final SchemaSqlFactory sqlFactory;
    private final DropTableExecutor dropTableExecutor;
    private final CreateTableQueriesFactory<AdpTables<String>> createTableQueriesFactory;

    @Autowired
    public CreateTableExecutor(@Qualifier("AdpQueryExecutor") DatabaseExecutor AdpQueryExecutor,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdpTables<String>> createTableQueriesFactory) {
        this.AdpQueryExecutor = AdpQueryExecutor;
        this.sqlFactory = new SchemaSqlFactory();
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return dropTableExecutor.execute(request)
                .compose(v -> createTableWithIndexes(request));
    }

    private Future<Void> createTableWithIndexes(DdlRequest request) {
        return Future.future(promise -> {
            AdpTables<String> createTableQueries = createTableQueriesFactory.create(request.getEntity(), request.getEnvName());
            String createTablesSql = String.join("; ", createTableQueries.getActual(),
                    createTableQueries.getStaging());
            String createIndexesSql = sqlFactory.createSecondaryIndexSqlQuery(request.getEntity().getSchema(),
                    request.getEntity().getName());
            AdpQueryExecutor.executeUpdate(createTablesSql)
                    .compose(v -> AdpQueryExecutor.executeUpdate(createIndexesSql))
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adpDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

}
