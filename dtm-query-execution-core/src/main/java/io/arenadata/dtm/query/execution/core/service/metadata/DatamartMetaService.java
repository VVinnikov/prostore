package io.arenadata.dtm.query.execution.core.service.metadata;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.dto.metadata.EntityAttribute;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Сервис информации о метаданных витрин
 */
public interface DatamartMetaService {

  /**
   * Получение метаданных о всех витринах
   */
  void getDatamartMeta(AsyncHandler<List<DatamartInfo>> handler);

  /**
   * Получение метаданных о всех сущностях витрины
   */
  void getEntitiesMeta(String datamartMnemonic, AsyncHandler<List<DatamartEntity>> handler);

  /**
   * Получение метаданных о всех атрибутах сущности
   */
  void getAttributesMeta(String datamartMnemonic, String entityMnemonic, AsyncHandler<List<EntityAttribute>> handler);
}
