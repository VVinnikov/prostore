package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.calcite.eddl.*;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;

import java.util.List;

@Component
public class EddlQueryParamExtractorImpl implements EddlQueryParamExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(EddlQueryParamExtractorImpl.class);

  private final DefinitionService<SqlNode> definitionService;
  private final Vertx vertx;

  @Autowired
  public EddlQueryParamExtractorImpl(DefinitionService<SqlNode> definitionService,
                                     @Qualifier("coreVertx") Vertx vertx) {
    this.definitionService = definitionService;
    this.vertx = vertx;
  }

  @Override
  public void extract(QueryRequest request, Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
    vertx.executeBlocking(it -> {
      try {
        SqlNode node = definitionService.processingQuery(request.getSql());
        it.complete(node);
      } catch (Exception e) {
        LOGGER.error("Ошибка парсинга запроса", e);
        it.fail(e);
      }
    }, ar -> {
      if (ar.succeeded()) {
        SqlNode sqlNode = (SqlNode) ar.result();
        extract(sqlNode, request.getDatamartMnemonic(), asyncResultHandler);
      } else {
        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void extract(SqlNode sqlNode,
                       String defaultSchema,
                       Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
    if (sqlNode instanceof SqlDdl) {
      if (sqlNode instanceof SqlDropDownloadExternalTable) {
        extractDropDownloadExternalTable(
          (SqlDropDownloadExternalTable) sqlNode,
          defaultSchema,
          asyncResultHandler);
      } else if (sqlNode instanceof SqlCreateDownloadExternalTable) {
        extractCreateDownloadExternalTable(
          (SqlCreateDownloadExternalTable) sqlNode,
          defaultSchema,
          asyncResultHandler);
      } else {
        asyncResultHandler.handle(Future.failedFuture("Запрос [" + sqlNode + "] не является EDDL оператором."));
      }
    } else {
      asyncResultHandler.handle(Future.failedFuture("Запрос [" + sqlNode + "] не является EDDL оператором."));
    }
  }

  private void extractDropDownloadExternalTable(SqlDropDownloadExternalTable ddl,
                                                String defaultSchema,
                                                Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
    try {
      List<String> names = SqlNodeUtils.getTableNames(ddl);
      asyncResultHandler.handle(Future.succeededFuture(
        new DropDownloadExternalTableQuery(
          getSchema(names, defaultSchema),
          getTableName(names)
        )));
    } catch (RuntimeException e) {
      LOGGER.error("Ошибка парсинга запроса", e);
      asyncResultHandler.handle(Future.failedFuture(e.getMessage()));
    }
  }

  private void extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                  String defaultSchema,
                                                  Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
    try {
      List<String> names = SqlNodeUtils.getTableNames(ddl);
      LocationOperator locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
      ChunkSizeOperator chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);

      asyncResultHandler.handle(Future.succeededFuture(
        new CreateDownloadExternalTableQuery(
          getSchema(names, defaultSchema),
          getTableName(names),
          locationOperator.getType(),
          locationOperator.getLocation(),
          SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat(),
          chunkSizeOperator.getChunkSize())));
    } catch (RuntimeException e) {
      LOGGER.error("Ошибка парсинга запроса", e);
      asyncResultHandler.handle(Future.failedFuture(e.getMessage()));
    }
  }

  private String getTableName(List<String> names) {
    return names.get(names.size() - 1);
  }

  private String getSchema(List<String> names, String defaultSchema) {
    return names.size() > 1 ? names.get(names.size() - 2) : defaultSchema;
  }
}
