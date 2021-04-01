package io.arenadata.dtm.query.execution.core.metadata.service;

import io.arenadata.dtm.query.execution.core.metadata.dto.DatamartEntity;
import io.arenadata.dtm.query.execution.core.metadata.dto.DatamartInfo;
import io.arenadata.dtm.query.execution.core.metadata.dto.EntityAttribute;
import io.vertx.core.Future;

import java.util.List;

/**
 * Сервис информации о метаданных витрин
 */
public interface DatamartMetaService {

  /**
   * Получение метаданных о всех витринах
   * @return list of datamarts
   */
  Future<List<DatamartInfo>> getDatamartMeta();

  /**
   * Получение метаданных о всех сущностях витрины
   * @return list of entities
   */
  Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic);

  /**
   * Получение метаданных о всех атрибутах сущности
   * @return list of entities attributes
   */
  Future<List<EntityAttribute>> getAttributesMeta(String datamartMnemonic, String entityMnemonic);
}
