package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;

/**
 * Преобразователи DML запроса
 */
public interface QueryGenerator {
  /**
   * Преобразовать запрос
   */
  void mutateQuery(RelRoot sqlNode,
                   List<DeltaInformation> deltaInformations,
                   SchemaDescription schemaDescription,
                   CalciteContext calciteContext,
                   QueryRequest queryRequest,
                   Handler<AsyncResult<String>> handler);
}
