package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dao.servicedb.EntityDao;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.dtmservice.Tables.DATAMARTS_REGISTRY;
import static org.jooq.generated.dtmservice.Tables.ENTITIES_REGISTRY;

@Repository
@Slf4j
public class EntityDaoImpl implements EntityDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public EntityDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        //FIXME заменить на findEntitiesByDatamartAndTableNames
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID, ENTITIES_REGISTRY.ENTITY_MNEMONICS, DATAMARTS_REGISTRY.DATAMART_MNEMONICS)
                .from(ENTITIES_REGISTRY)
                .join(DATAMARTS_REGISTRY)
                .on(ENTITIES_REGISTRY.DATAMART_ID.eq(DATAMARTS_REGISTRY.DATAMART_ID))
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(datamartMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                if (ar.result().unwrap() instanceof ResultSet) {
                    List<DatamartEntity> datamartEntityList = initDatamartEntities(ar);
                    log.info("Found [{}] entities for datamart [{}]", datamartEntityList.size(), datamartMnemonic);
                    resultHandler.handle(Future.succeededFuture(datamartEntityList));
                } else {
                    resultHandler.handle(Future.failedFuture(
                            new RuntimeException(String.format("Unable to get entities for datamart [%s]",
                                    datamartMnemonic))));
                }
            } else
                resultHandler.handle(Future.failedFuture(ar.cause()));
        });
    }

    @Override
    public void insertEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(ENTITIES_REGISTRY)
                .set(ENTITIES_REGISTRY.DATAMART_ID, datamartId)
                .set(ENTITIES_REGISTRY.ENTITY_MNEMONICS, name)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void findEntity(Long datamartId, String name, Handler<AsyncResult<Long>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID)
                .from(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(name))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(ENTITIES_REGISTRY.ENTITY_ID))
                        : Future.failedFuture(new RuntimeException(String.format("Entity [%s] not found!", name))));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void isEntityExists(Long datamartId, String name, Handler<AsyncResult<Boolean>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID)
                .from(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(name))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(ar.result().hasResults()));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropEntity(Long datamartId, String name, Handler<AsyncResult<Integer>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(name)))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result()));
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void findEntitiesByDatamartAndTableNames(DatamartInfo datamartInfo,
                                                    Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID, ENTITIES_REGISTRY.ENTITY_MNEMONICS,
                        DATAMARTS_REGISTRY.DATAMART_MNEMONICS)
                .from(DATAMARTS_REGISTRY)
                .join(ENTITIES_REGISTRY)
                .on(ENTITIES_REGISTRY.DATAMART_ID.eq(DATAMARTS_REGISTRY.DATAMART_ID))
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.eq(datamartInfo.getSchemaName()))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.in(datamartInfo.getTables()))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                if (ar.result().unwrap() instanceof ResultSet) {
                    List<DatamartEntity> datamartEntityList = initDatamartEntities(ar);
                    log.info("Found [{}] entities for datamary [{}]", datamartEntityList.size(),
                            datamartInfo.getSchemaName());
                    resultHandler.handle(Future.succeededFuture(datamartEntityList));
                } else {
                    resultHandler.handle(Future.failedFuture(
                            new RuntimeException(String.format("Unable to get entities for datamart [%s]!",
                                    datamartInfo.getSchemaName()))));
                }
            } else
                resultHandler.handle(Future.failedFuture(ar.cause()));
        });
    }

    @NotNull
    private List<DatamartEntity> initDatamartEntities(AsyncResult<QueryResult> ar) {
        List<DatamartEntity> datamartEntityList = new ArrayList<>();
        ResultSet rows = ar.result().unwrap();
        rows.getRows().forEach(it ->
                datamartEntityList.add(new DatamartEntity(
                        it.getLong(ENTITIES_REGISTRY.ENTITY_ID.getName()),
                        it.getString(ENTITIES_REGISTRY.ENTITY_MNEMONICS.getName()),
                        it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                ))
        );
        return datamartEntityList;
    }

    @Override
    public void dropByDatamartId(Long datamartId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId)))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
