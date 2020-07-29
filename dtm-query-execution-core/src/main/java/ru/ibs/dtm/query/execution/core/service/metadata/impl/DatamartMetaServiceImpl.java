package ru.ibs.dtm.query.execution.core.service.metadata.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;
import ru.ibs.dtm.query.execution.core.service.metadata.DatamartMetaService;

import java.util.List;

@Service
public class DatamartMetaServiceImpl implements DatamartMetaService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatamartMetaServiceImpl.class);

    private ServiceDbFacade serviceDbFacade;

    public DatamartMetaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().getDatamartMeta(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(ar.result()));
            } else {
                LOGGER.error("Ошибка получения метаданных", ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        serviceDbFacade.getServiceDbDao().getEntityDao().getEntitiesMeta(datamartMnemonic, resultHandler::handle);
    }

    @Override
    public void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler) {
        serviceDbFacade.getServiceDbDao().getAttributeDao().getAttributesMeta(datamartMnemonic, entityMnemonic, resultHandler::handle);
    }
}
