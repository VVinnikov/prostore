package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.EntityAttribute;
import ru.ibs.dtm.query.execution.core.service.DatamartMetaService;

import java.util.List;

@Service
public class DatamartMetaServiceImpl implements DatamartMetaService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatamartMetaServiceImpl.class);

  private ServiceDao serviceDao;

  public DatamartMetaServiceImpl(ServiceDao serviceDao) {
    this.serviceDao = serviceDao;
  }

  @Override
  public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
    serviceDao.getDatamartMeta(ar -> {
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
    serviceDao.getEntitiesMeta(datamartMnemonic, resultHandler::handle);
  }

  @Override
  public void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler) {
    serviceDao.getAttributesMeta(datamartMnemonic, entityMnemonic, resultHandler::handle);
  }
}
