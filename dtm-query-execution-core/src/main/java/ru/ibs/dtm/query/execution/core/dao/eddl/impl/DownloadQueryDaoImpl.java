package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadQueryDao;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadQueryRecord;

import static org.jooq.generated.dtmservice.Tables.DOWNLOAD_QUERY;

@Repository
public class DownloadQueryDaoImpl implements DownloadQueryDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DownloadQueryDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void insertDownloadQuery(DownloadQueryRecord downloadQueryRecord, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(DOWNLOAD_QUERY)
                .set(DOWNLOAD_QUERY.ID, downloadQueryRecord.getId())
                .set(DOWNLOAD_QUERY.DATAMART_ID, downloadQueryRecord.getDatamartId())
                .set(DOWNLOAD_QUERY.TABLE_NAME_EXT, downloadQueryRecord.getTableNameExt())
                .set(DOWNLOAD_QUERY.SQL_QUERY, downloadQueryRecord.getSqlQuery())
                .set(DOWNLOAD_QUERY.STATUS, downloadQueryRecord.getStatus()))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
