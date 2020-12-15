package io.arenadata.dtm.query.execution.core.service.metadata.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.dto.metadata.EntityAttribute;
import io.arenadata.dtm.query.execution.core.service.metadata.DatamartMetaService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DatamartMetaServiceImpl implements DatamartMetaService {

    private DatamartDao datamartDao;
    private EntityDao entityDao;

    public DatamartMetaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void getDatamartMeta(AsyncHandler<List<DatamartInfo>> handler) {
        datamartDao.getDatamartMeta(ar -> {
            if (ar.succeeded()) {
                handler.handleSuccess(ar.result());
            } else {
                handler.handleError(ar.cause());
            }
        });
    }

    @Override
    public void getEntitiesMeta(String datamartMnemonic, AsyncHandler<List<DatamartEntity>> handler) {
        entityDao.getEntitiesMeta(datamartMnemonic, handler);
    }

    @Override
    public void getAttributesMeta(String datamartMnemonic,
                                  String entityMnemonic,
                                  AsyncHandler<List<EntityAttribute>> handler) {
        entityDao.getEntity(datamartMnemonic, entityMnemonic)
                .onFailure(error -> handler.handle(Future.failedFuture(error)))
                .onSuccess(entity -> {
                    handler.handleSuccess(entity.getFields().stream()
                            .map(ef -> EntityAttribute.builder()
                                    .datamartMnemonic(datamartMnemonic)
                                    .entityMnemonic(entityMnemonic)
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
                            .collect(Collectors.toList()));
                });
    }
}
