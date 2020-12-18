package io.arenadata.dtm.query.execution.core.service.metadata;

import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.dto.metadata.EntityAttribute;
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
