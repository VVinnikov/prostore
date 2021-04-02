package io.arenadata.dtm.query.execution.plugin.adqm.ddl.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {
    private final DatabaseExecutor databaseExecutor;
    private final DropTableExecutor dropTableExecutor;
    private final CreateTableQueriesFactory<AdqmTables<String>> createTableQueriesFactory;

    @Autowired
    public CreateTableExecutor(DatabaseExecutor databaseExecutor,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdqmTables<String>> createTableQueriesFactory) {
        this.databaseExecutor = databaseExecutor;
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            dropTableExecutor.execute(request)
                    .compose(v -> createTable(request.getEntity(), request.getEnvName()))
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createTable(Entity entity, String envName) {
        AdqmTables<String> createTableQueries = createTableQueriesFactory.create(entity, envName);
        return databaseExecutor.executeUpdate(createTableQueries.getShard())
                .compose(v -> databaseExecutor.executeUpdate(createTableQueries.getDistributed()));
    }
}
