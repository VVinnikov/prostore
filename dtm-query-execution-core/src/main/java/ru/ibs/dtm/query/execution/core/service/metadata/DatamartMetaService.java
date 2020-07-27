package ru.ibs.dtm.query.execution.core.service.metadata;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;

import java.util.List;

/**
 * Сервис информации о метаданных витрин
 */
public interface DatamartMetaService {

  /**
   * Получение метаданных о всех витринах
   */
  void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

  /**
   * Получение метаданных о всех сущностях витрины
   */
  void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

  /**
   * Получение метаданных о всех атрибутах сущности
   */
  void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler);
}
