package io.arenadata.dtm.query.execution.core.query.executor;

import io.arenadata.dtm.query.execution.core.query.client.SqlClientProvider;
import io.vertx.core.Future;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("itTestQueryExecutor")
public class QueryExecutorImpl implements QueryExecutor {

    private final SqlClientProvider sqlClientProvider;

    @Autowired
    public QueryExecutorImpl(SqlClientProvider sqlClientProvider) {
        this.sqlClientProvider = sqlClientProvider;
    }

    @Override
    public Future<UpdateResult> executeUpdate(String datamartMnemonic, String sql) {
        return Future.future(p -> {
            log.debug("Requesting [{}]: [{}]", datamartMnemonic, sql);
            sqlClientProvider.get(datamartMnemonic).update(sql, (ar) -> {
                if (ar.succeeded()) {
                    log.debug("Updating request completed successfully: {}", sql);
                    p.complete(ar.result());
                } else {
                    val errorMsg = String.format("Request [%s] failed with error: [%s]", sql, ar.cause().getMessage());
                    log.error(errorMsg, ar.cause());
                    p.fail(errorMsg);
                }
            });
        });
    }

    @Override
    public Future<ResultSet> executeQuery(String datamartMnemonic, String sql) {
        return Future.future(p -> {
            log.debug("Requesting [{}]: [{}]", datamartMnemonic, sql);
            sqlClientProvider.get(datamartMnemonic).query(sql, (ar) -> {
                if (ar.succeeded()) {
                    log.debug("Query request completed successfully: {}", sql);
                    p.complete(ar.result());
                } else {
                    val errorMsg = String.format("Request [%s] failed with error: [%s]", sql, ar.cause().getMessage());
                    log.error(errorMsg, ar.cause());
                    p.fail(errorMsg);
                }
            });
        });
    }

    @Override
    public Future<ResultSet> executeQuery(String sql) {
        return executeQuery("", sql);
    }

}
