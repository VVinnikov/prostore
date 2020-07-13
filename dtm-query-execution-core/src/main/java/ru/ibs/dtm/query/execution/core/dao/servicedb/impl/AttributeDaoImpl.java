package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.query.execution.core.dao.servicedb.AttributeDao;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.dtmservice.Tables.*;

@Repository
@Slf4j
public class AttributeDaoImpl implements AttributeDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public AttributeDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ATTRIBUTES_REGISTRY.ATTR_ID,
                        ATTRIBUTES_REGISTRY.ATTR_MNEMONICS,
                        ATTRIBUTES_REGISTRY.LENGTH,
                        ATTRIBUTES_REGISTRY.ACCURACY,
                        DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS,
                        DATAMARTS_REGISTRY.DATAMART_MNEMONICS,
                        ENTITIES_REGISTRY.ENTITY_MNEMONICS
                )
                .from(ATTRIBUTES_REGISTRY)
                .join(DATA_TYPES_REGISTRY).on(ATTRIBUTES_REGISTRY.DATA_TYPE_ID.eq(DATA_TYPES_REGISTRY.DATA_TYPE_ID))
                .join(ENTITIES_REGISTRY).on(ENTITIES_REGISTRY.ENTITY_ID.eq(ATTRIBUTES_REGISTRY.ENTITY_ID))
                .join(DATAMARTS_REGISTRY).on(DATAMARTS_REGISTRY.DATAMART_ID.eq(ENTITIES_REGISTRY.DATAMART_ID))
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(datamartMnemonic)).and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(entityMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded() && ar.result().unwrap() instanceof ResultSet) {
                List<EntityAttribute> res = new ArrayList<>();
                ResultSet rows;
                rows = ar.result().unwrap();
                rows.getRows().forEach(it ->
                        res.add(new EntityAttribute(
                                it.getInteger(ATTRIBUTES_REGISTRY.ATTR_ID.getName()),
                                it.getString(ATTRIBUTES_REGISTRY.ATTR_MNEMONICS.getName()),
                                it.getString(DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS.getName()),
                                it.getInteger(ATTRIBUTES_REGISTRY.LENGTH.getName()),
                                it.getInteger(ATTRIBUTES_REGISTRY.ACCURACY.getName()),
                                it.getString(ENTITIES_REGISTRY.ENTITY_MNEMONICS.getName()),
                                it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                        ))
                );
                log.info("Найдено {} атрибутов для сущности: '{}' схемы: '{}'.", res.size(), entityMnemonic, datamartMnemonic);
                resultHandler.handle(Future.succeededFuture(res));
            } else {
                log.error("Невозможно получить атрибуты метаданных: {}", ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertAttribute(Long entityId, ClassField field, Integer typeId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(ATTRIBUTES_REGISTRY)
                .set(ATTRIBUTES_REGISTRY.ENTITY_ID, entityId)
                .set(ATTRIBUTES_REGISTRY.ATTR_MNEMONICS, field.getName())
                .set(ATTRIBUTES_REGISTRY.LENGTH, field.getSize())
                .set(ATTRIBUTES_REGISTRY.DATA_TYPE_ID, typeId)
                .set(ATTRIBUTES_REGISTRY.PRIMARY_KEY_ORDER, field.getPrimaryOrder())
                .set(ATTRIBUTES_REGISTRY.DISTRIBUTE_KEY_ORDER, field.getShardingOrder())
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropAttribute(Long entityId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(ATTRIBUTES_REGISTRY)
                .where(ATTRIBUTES_REGISTRY.ENTITY_ID.eq(entityId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
