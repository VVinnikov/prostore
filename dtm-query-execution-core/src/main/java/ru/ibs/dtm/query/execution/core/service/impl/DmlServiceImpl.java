package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.service.*;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;

@Service("coreDmlService")
public class DmlServiceImpl implements DmlService {

  private final DataSourcePluginService dataSourcePluginService;
  private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
  private final SchemaStorageProvider schemaStorageProvider;
  private final MetadataService metadataService;

  @Autowired
  public DmlServiceImpl(DataSourcePluginService dataSourcePluginService,
                        TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                        SchemaStorageProvider schemaStorageProvider,
                        MetadataService metadataService) {
    this.dataSourcePluginService = dataSourcePluginService;
    this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
    this.schemaStorageProvider = schemaStorageProvider;
    this.metadataService = metadataService;
  }

  @Override
  public void execute(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    targetDatabaseDefinitionService.getTargetSource(parsedQueryRequest.getQueryRequest(), ar -> {
      if (ar.succeeded()) {
        QuerySourceRequest querySourceRequest = ar.result();
        if (querySourceRequest.getSourceType() == SourceType.INFORMATION_SCHEMA) {
          metadataService.executeQuery(parsedQueryRequest.getQueryRequest(), asyncResultHandler);
        } else {
          pluginExecute(querySourceRequest, asyncResultHandler);
        }
      } else {
        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void pluginExecute(QuerySourceRequest request,
                             Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    schemaStorageProvider.getLogicalSchema(schemaAr -> {
      if (schemaAr.succeeded()) {
        JsonObject schema = schemaAr.result();
        dataSourcePluginService.llr(
          request.getSourceType(),
          new LlrRequest(request.getQueryRequest(), schema),
          asyncResultHandler);
      } else {
        asyncResultHandler.handle(Future.failedFuture(schemaAr.cause()));
      }
    });
  }
}
