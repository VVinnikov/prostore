package io.arenadata.dtm.query.execution.core.service.metadata.impl;

import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.dto.metadata.EntityAttribute;
import io.arenadata.dtm.query.execution.core.service.metadata.DatamartMetaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class DatamartMetaServiceImpl implements DatamartMetaService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatamartMetaServiceImpl.class);

    private DatamartDao datamartDao;
    private EntityDao entityDao;

    public DatamartMetaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        datamartDao.getDatamartMeta(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(ar.result()));
            } else {
                LOGGER.error("Error getting metadata", ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        entityDao.getEntitiesMeta(datamartMnemonic, resultHandler);
    }

    @Override
    public void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler) {
        entityDao.getEntity(datamartMnemonic, entityMnemonic)
            .onFailure(error -> resultHandler.handle(Future.failedFuture(error)))
            .onSuccess(entity -> {
                resultHandler.handle(Future.succeededFuture(entity.getFields().stream()
                    .map(ef -> EntityAttribute.builder()
                        .accuracy(ef.getAccuracy())
                        .distributeKeykOrder(ef.getShardingOrder())
                        .primaryKeyOrder(ef.getPrimaryOrder())
                        .dataType(ef.getType())
                        .length(ef.getSize())
                        .mnemonic(ef.getName())
                        .ordinalPosition(ef.getOrdinalPosition())
                        .nullable(ef.getNullable())
                        .accuracy(ef.getAccuracy())
                        .build())
                    .collect(Collectors.toList())));
            });
    }
}