package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.dtmservice.Tables.DATAMARTS_REGISTRY;

@Repository
public class DatamartDaoImpl implements DatamartDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DatamartDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void insertDatamart(String name, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(DATAMARTS_REGISTRY)
                .set(DATAMARTS_REGISTRY.DATAMART_MNEMONICS, name))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATAMARTS_REGISTRY.DATAMART_ID, DATAMARTS_REGISTRY.DATAMART_MNEMONICS)
                .from(DATAMARTS_REGISTRY)).setHandler(ar -> {
            if (ar.succeeded()) {
                if (ar.result().unwrap() instanceof ResultSet) {
                    ResultSet rows = ar.result().unwrap();
                    List<DatamartInfo> datamartInfoList = new ArrayList<>();
                    rows.getRows().forEach(it ->
                            datamartInfoList.add(new DatamartInfo(
                                    it.getInteger(DATAMARTS_REGISTRY.DATAMART_ID.getName()),
                                    it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                            ))
                    );
                    resultHandler.handle(Future.succeededFuture(datamartInfoList));
                } else {
                    resultHandler.handle(Future.failedFuture("Unable to get metadata!"));
                }
            } else
                resultHandler.handle(Future.failedFuture(ar.cause()));
        });
    }

    @Override
    public void findDatamart(String name, Handler<AsyncResult<Long>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATAMARTS_REGISTRY.DATAMART_ID)
                .from(DATAMARTS_REGISTRY)
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(name))).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(DATAMARTS_REGISTRY.DATAMART_ID))
                        : Future.failedFuture(String.format("Datamart [%s] not found!", name)));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropDatamart(Long id, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(DATAMARTS_REGISTRY)
                .where(DATAMARTS_REGISTRY.DATAMART_ID.eq(id))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void isDatamartExists(String name, Handler<AsyncResult<Boolean>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATAMARTS_REGISTRY.DATAMART_ID)
                .from(DATAMARTS_REGISTRY)
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.eq(name))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(ar.result().hasResults()));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
