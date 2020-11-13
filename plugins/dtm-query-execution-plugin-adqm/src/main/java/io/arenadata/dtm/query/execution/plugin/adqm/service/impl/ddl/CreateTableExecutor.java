package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
    private final CreateTableQueriesFactory<AdqmCreateTableQueries> createTableQueriesFactory;

    public CreateTableExecutor(DatabaseExecutor databaseExecutor,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdqmCreateTableQueries> createTableQueriesFactory) {
        this.databaseExecutor = databaseExecutor;
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        Entity tbl = context.getRequest().getEntity();
        DdlRequestContext dropCtx = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));

        dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName, ar -> createTable(context).onComplete(handler));
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

    private Future<Void> createTable(DdlRequestContext context) {
        AdqmCreateTableQueries createTableQueries = createTableQueriesFactory.create(context);
        return databaseExecutor.executeUpdate(createTableQueries.getShard())
                .compose(v -> databaseExecutor.executeUpdate(createTableQueries.getDistributed()));
    }
}
