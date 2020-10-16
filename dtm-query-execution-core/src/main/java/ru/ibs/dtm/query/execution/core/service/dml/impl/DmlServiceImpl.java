package ru.ibs.dtm.query.execution.core.service.dml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.service.dml.ColumnMetadataService;
import ru.ibs.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import ru.ibs.dtm.query.execution.core.service.dml.LogicViewReplacer;
import ru.ibs.dtm.query.execution.core.service.schema.SystemDatamartViewsProvider;
import ru.ibs.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DmlService;

@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicViewReplacer logicViewReplacer;
    private final ColumnMetadataService columnMetadataService;
    private final InformationSchemaExecutor informationSchemaExecutor;

    @Autowired
    public DmlServiceImpl(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor, LogicViewReplacer logicViewReplacer,
                          ColumnMetadataService columnMetadataService,
                          InformationSchemaExecutor informationSchemaExecutor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.informationSchemaExecutor = informationSchemaExecutor;
        this.columnMetadataService = columnMetadataService;
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            val queryRequest = context.getRequest().getQueryRequest();
            val sourceRequest = new QuerySourceRequest(queryRequest, queryRequest.getSourceType());
            logicViewReplace(sourceRequest.getQueryRequest())
                    .compose(deltaQueryPreprocessor::process)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            sourceRequest.setQueryRequest(ar.result());
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

    private void setTargetSourceAndExecute(QuerySourceRequest request,
                                           Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        targetDatabaseDefinitionService.getTargetSource(request, ar -> {
            if (ar.succeeded()) {
                QuerySourceRequest querySourceRequest = ar.result();
                if (querySourceRequest.getQueryRequest().getSourceType() == SourceType.INFORMATION_SCHEMA) {
                    //FIXME add receiving logical schema from system views
                    //querySourceRequest.setLogicalSchema(systemDatamartViewsProvider.getLogicalSchemaFromSystemViews());
                    initColumnMetaData(querySourceRequest)
                            .compose(v -> informationSchemaExecute(querySourceRequest))
                            .onComplete(asyncResultHandler);
                } else {
                    initColumnMetaData(querySourceRequest)
                            .compose(v -> pluginExecute(querySourceRequest))
                            .onComplete(asyncResultHandler);
                }
            } else {
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return Future.future(p -> informationSchemaExecutor.execute(querySourceRequest, p));
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest request) {
        return Future.future(p -> dataSourcePluginService.llr(
                request.getQueryRequest().getSourceType(),
                new LlrRequestContext(
                        new LlrRequest(request.getQueryRequest(), request.getLogicalSchema(), request.getMetadata())),
                p));
    }

    private Future<Void> initColumnMetaData(QuerySourceRequest request) {
        return Future.future(p -> {
            val parserRequest = new QueryParserRequest(request.getQueryRequest(), request.getLogicalSchema());
            columnMetadataService.getColumnMetadata(parserRequest, ar -> {
                if (ar.succeeded()) {
                    request.setMetadata(ar.result());
                    p.complete();
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }
}
