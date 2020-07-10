package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.eddl.UploadQueryDao;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadQueryRecord;

import static org.jooq.generated.dtmservice.Tables.UPLOAD_QUERY;

@Repository
public class UploadQueryDaoImpl implements UploadQueryDao {

    private final AsyncClassicGenericQueryExecutor executor;

    public UploadQueryDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void inserUploadQuery(UploadQueryRecord uploadQueryRecord, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(UPLOAD_QUERY)
                .set(UPLOAD_QUERY.ID, uploadQueryRecord.getId())
                .set(UPLOAD_QUERY.DATAMART_ID, uploadQueryRecord.getDatamartId())
                .set(UPLOAD_QUERY.TABLE_NAME_EXT, uploadQueryRecord.getTableNameExt())
                .set(UPLOAD_QUERY.TABLE_NAME_DST, uploadQueryRecord.getTableNameDst())
                .set(UPLOAD_QUERY.SQL_QUERY, uploadQueryRecord.getSqlQuery())
                .set(UPLOAD_QUERY.STATUS, uploadQueryRecord.getStatus()))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
