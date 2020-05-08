package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

@Service("adgCalciteDmlQueryParserService")
@Slf4j
public class AdgCalciteDmlQueryParserServiceImpl implements QueryParserService {
  private Vertx vertx;
  private CalciteSchemaFactory schemaFactory;
  private SchemaExtender schemaExtender;

  public AdgCalciteDmlQueryParserServiceImpl(@Qualifier("adgSchemaExtender") SchemaExtender schemaExtender,
                                             @Qualifier("adgVertx") Vertx vertx,
                                             CalciteSchemaFactory schemaFactory) {
    this.vertx = vertx;
    this.schemaFactory = schemaFactory;
    this.schemaExtender = schemaExtender;
  }

  @Override
  public void parse(QueryRequest querySourceRequest, SchemaDescription schemaDescription, CalciteContext context, Handler<AsyncResult<RelRoot>> asyncResultHandler) {
    vertx.executeBlocking(it -> {
      try {
        if (querySourceRequest == null || StringUtils.isEmpty(querySourceRequest.getSql())) {
          log.error("Неопределен запрос {}", querySourceRequest);
          asyncResultHandler.handle(Future.failedFuture(String.format("Неопределен запрос %s", querySourceRequest)));
          return;
        }
        String sqlRequest = querySourceRequest.getSql();
        RelRoot relQuery;
        try {
          Datamart datamartLogicalSchema = schemaDescription.getLogicalSchema();
          schemaFactory.addSubSchema(context.getSchema(), datamartLogicalSchema);
          Datamart datamartPhysicalSchema = schemaExtender.generatePhysicalSchema(datamartLogicalSchema);
          schemaDescription.setPhysicalSchema(datamartPhysicalSchema);

          schemaFactory.addRootSchema(context.getSchema(), datamartPhysicalSchema);

          SqlNode parse = context.getPlanner().parse(sqlRequest);
          SqlNode validatedQuery = context.getPlanner().validate(parse);
          relQuery = context.getPlanner().rel(validatedQuery);
        } catch (Exception e) {
          log.error("Ошибка разбора запроса", e);
          asyncResultHandler.handle(Future.failedFuture(e));
          return;
        }
        it.complete(relQuery);
      } catch (Exception e) {
        log.error("Ошибка парсинга запроса", e);
        it.fail(e);
      }
    }, ar -> {
      if (ar.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture((RelRoot)ar.result()));
      } else {
        log.debug("Ошибка при исполнении метода parse", ar.cause());
        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
