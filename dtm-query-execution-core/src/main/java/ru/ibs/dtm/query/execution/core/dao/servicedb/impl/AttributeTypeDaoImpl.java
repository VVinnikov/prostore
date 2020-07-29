package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.servicedb.AttributeTypeDao;

import static org.jooq.generated.dtmservice.Tables.DATA_TYPES_REGISTRY;

@Repository
public class AttributeTypeDaoImpl implements AttributeTypeDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public AttributeTypeDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void findTypeIdByTypeMnemonic(String typeMnemonic, Handler<AsyncResult<Integer>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATA_TYPES_REGISTRY.DATA_TYPE_ID)
                .from(DATA_TYPES_REGISTRY)
                .where(DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS.equalIgnoreCase(typeMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded())
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(DATA_TYPES_REGISTRY.DATA_TYPE_ID))
                        : Future.failedFuture(new RuntimeException(String.format("Type [%s] not found!", datamartMnemonic))));
            else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
