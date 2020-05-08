package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;

import java.util.List;

/**
 * Преобразователи DML запроса
 */
public interface QueryGenerator {
  /**
   * Преобразовать запрос
   */
  void mutateQuery(RelRoot sqlNode, List<Long> selectOn, SchemaDescription schemaDescription, CalciteContext calciteContext, Handler<AsyncResult<String>> handler);
}
