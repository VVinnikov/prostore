package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.Future;
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
    private final CreateTableQueriesFactory<AdbTables<String>> createTableQueriesFactory;

    @Autowired
    public CreateTableExecutor(AdbQueryExecutor adbQueryExecutor,
                               MetadataSqlFactory sqlFactory,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdbTables<String>> createTableQueriesFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context, String sqlNodeName) {
        return createDdlContext(context)
                .compose(dropCtx -> dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName))
                .compose(v -> createTableWithIndexes(context));
    }

    private Future<DdlRequestContext> createDdlContext(DdlRequestContext context) {
        return Future.future(promise -> {
            SqlNode query = context.getQuery();
            if (!(query instanceof SqlCreateTable)) {
                promise.fail(new DdlDatasourceException(
                        String.format("Expecting SqlCreateTable in context, receiving: %s",
                                context.getQuery())));
                return;
            }
            DdlRequestContext dropCtx = createDropRequestContext(context);
            promise.complete(dropCtx);
        });
    }

    private Future<Void> createTableWithIndexes(DdlRequestContext context) {
        return Future.future(promise -> {
            AdbTables<String> createTableQueries = createTableQueriesFactory.create(context);
            String createTablesSql = String.join("; ", createTableQueries.getActual(),
                    createTableQueries.getHistory(), createTableQueries.getStaging());
            String createIndexesSql = sqlFactory.createSecondaryIndexSqlQuery(context.getRequest().getEntity().getSchema(),
                    context.getRequest().getEntity().getName());
            adbQueryExecutor.executeUpdate(createTablesSql)
                    .compose(v -> adbQueryExecutor.executeUpdate(createIndexesSql))
                    .onComplete(promise);
        });
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
