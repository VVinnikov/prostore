package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryGenerator;

import java.util.List;

@Service("adgQueryGenerator")
@Slf4j
public class AdgQueryGeneratorImpl implements QueryGenerator {
  private QueryExtendService queryExtendService;

  public AdgQueryGeneratorImpl(@Qualifier("adgCalciteDmlQueryExtendService") QueryExtendService queryExtendService) {
    this.queryExtendService = queryExtendService;
  }

  @Override
  public void mutateQuery(RelRoot relNode, List<Long> selectOn, SchemaDescription schemaDescription, CalciteContext calciteContext, Handler<AsyncResult<String>> handler) {
    if (schemaDescription.getLogicalSchema() == null) {
      handler.handle(Future.failedFuture(String.format("Ошибка определения схемы для запроса %s", relNode.toString())));
      return;
    }

    queryExtendService.setRequestBuilder(calciteContext.getRelBuilder(), true);
    for (Long selectOnDelta : selectOn) {
      queryExtendService.addOption(selectOnDelta == null ? -1 : selectOnDelta);
    }
    try {
      RelNode extendedQuery = queryExtendService.extendQuery(relNode.rel);
      RelNode planAfter = calciteContext.getPlanner().transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE), extendedQuery);
      SqlNode sqlNodeResult = new RelToSqlConverter(DtmConvention.getDialect()).visitChild(0, planAfter).asStatement();
      String queryResult = Util.toLinux(sqlNodeResult.toSqlString(DtmConvention.getDialect()).getSql()).replaceAll("\n", " ");
      log.debug("sql = " + queryResult);
      handler.handle(Future.succeededFuture(queryResult));
    } catch (Exception e) {
      log.error("Ошибка исполнения преобразования запроса", e);
      handler.handle(Future.failedFuture(e));
    }
  }
}
