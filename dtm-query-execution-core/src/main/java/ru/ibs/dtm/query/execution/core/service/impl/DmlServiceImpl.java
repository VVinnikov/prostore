package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.MetadataService;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.service.dml.LogicViewReplacer;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DmlService;

import java.util.List;

@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final LogicViewReplacer logicViewReplacer;
    private final MetadataService metadataService;
    private final HintExtractor hintExtractor;

    @Autowired
    public DmlServiceImpl(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor, LogicalSchemaProvider logicalSchemaProvider,
                          LogicViewReplacer logicViewReplacer, MetadataService metadataService, HintExtractor hintExtractor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.logicViewReplacer = logicViewReplacer;
        this.metadataService = metadataService;
        this.hintExtractor = hintExtractor;
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            val sourceRequest = hintExtractor.extractHint(context.getRequest().getQueryRequest());
            logicViewReplace(sourceRequest.getQueryRequest())
                    .compose(deltaQueryPreprocessor::process)
                    .compose(this::initLogicalDatamartSchema)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            sourceRequest.setQueryRequest(ar.result().getQueryRequest());
                            setTargetSourceAndExecute(sourceRequest, asyncResultHandler);
                        } else {
                            asyncResultHandler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
        } catch (Exception e) {
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<QueryRequest> logicViewReplace(QueryRequest request) {
        return Future.future(p -> {
            logicViewReplacer.replace(request.getSql(), request.getDatamartMnemonic(), ar -> {
                if (ar.succeeded()) {
                    QueryRequest withoutViewsRequest = request.copy();
                    withoutViewsRequest.setSql(ar.result());
                    p.complete(withoutViewsRequest);
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }

    private Future<QuerySourceRequest> initLogicalDatamartSchema(QueryRequest request) {
        return Future.future((Promise<QuerySourceRequest> promise) -> {
            QuerySourceRequest sourceRequest = new QuerySourceRequest(request, null);
            logicalSchemaProvider.getSchema(sourceRequest.getQueryRequest(), ar -> {
                if (ar.succeeded()) {
                    List<Datamart> datamarts = ar.result();
                    JsonObject jsonSchema = new JsonObject(Json.encode(datamarts));//TODO проверить
                    sourceRequest.setLogicalSchema(jsonSchema);
                    promise.complete(sourceRequest);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private void setTargetSourceAndExecute(QuerySourceRequest request,
                                           Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        targetDatabaseDefinitionService.getTargetSource(request, ar -> {
            if (ar.succeeded()) {
                QuerySourceRequest querySourceRequest = ar.result();
                if (querySourceRequest.getSourceType() == SourceType.INFORMATION_SCHEMA) {
                    metadataService.executeQuery(querySourceRequest.getQueryRequest(), asyncResultHandler);
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
        dataSourcePluginService.llr(
                request.getSourceType(),
                new LlrRequestContext(new LlrRequest(request.getQueryRequest(), request.getLogicalSchema())),
                asyncResultHandler);
    }
}
