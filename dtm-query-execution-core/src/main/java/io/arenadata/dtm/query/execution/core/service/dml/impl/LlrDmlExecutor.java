package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicViewReplacer logicViewReplacer;
    private final ColumnMetadataService columnMetadataService;
    private final InformationSchemaExecutor informationSchemaExecutor;
    private final LogicalSchemaProvider logicalSchemaProvider;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          LogicViewReplacer logicViewReplacer,
                          ColumnMetadataService columnMetadataService,
                          InformationSchemaExecutor informationSchemaExecutor,
                          LogicalSchemaProvider logicalSchemaProvider) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.informationSchemaExecutor = informationSchemaExecutor;
        this.columnMetadataService = columnMetadataService;
        this.logicalSchemaProvider = logicalSchemaProvider;
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            val queryRequest = context.getRequest().getQueryRequest();
            val sourceRequest = new QuerySourceRequest(queryRequest, queryRequest.getSourceType());
            logicViewReplace(sourceRequest.getQueryRequest())
                    .compose(deltaQueryPreprocessor::process)
                    .map(request -> {
                        sourceRequest.setQueryRequest(request);
                        return request;
                    })
                    .compose(v -> initLogicalSchema(sourceRequest))
                    .compose(this::initColumnMetaData)
                    .compose(this::executeRequest)
                    .onComplete(asyncResultHandler);
        } catch (Exception e) {
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<QueryRequest> logicViewReplace(QueryRequest request) {
        return Future.future(p -> logicViewReplacer.replace(request.getSql(), request.getDatamartMnemonic(), ar -> {
            if (ar.succeeded()) {
                QueryRequest withoutViewsRequest = request.copy();
                withoutViewsRequest.setSql(ar.result());
                p.complete(withoutViewsRequest);
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<QuerySourceRequest> initLogicalSchema(QuerySourceRequest sourceRequest) {
        return getLogicalSchema(sourceRequest)
                .map(logicalSchema -> {
                    sourceRequest.setLogicalSchema(logicalSchema);
                    return sourceRequest;
                });
    }

    private Future<List<Datamart>> getLogicalSchema(QuerySourceRequest sourceRequest) {
        return Future.future((Promise<List<Datamart>> promise) -> {
            final QueryRequest queryRequest = sourceRequest.getQueryRequest();
            logicalSchemaProvider.getSchema(queryRequest, promise);
        });
    }

    private Future<QuerySourceRequest> initColumnMetaData(QuerySourceRequest request) {
        return Future.future(p -> {
            val parserRequest = new QueryParserRequest(request.getQueryRequest(), request.getLogicalSchema());
            columnMetadataService.getColumnMetadata(parserRequest, ar -> {
                if (ar.succeeded()) {
                    request.setMetadata(ar.result());
                    p.complete(request);
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> executeRequest(QuerySourceRequest sourceRequest) {
        return Future.future(promise -> {
            if (isInformationSchemaRequest(sourceRequest)) {
                informationSchemaExecute(sourceRequest)
                        .onComplete(promise);
            } else {
                defineTargetSourceType(sourceRequest)
                        .compose(this::pluginExecute)
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return Future.future(p -> informationSchemaExecutor.execute(querySourceRequest, p));
    }

    private Future<QuerySourceRequest> defineTargetSourceType(QuerySourceRequest sourceRequest) {
        return Future.future(promise -> targetDatabaseDefinitionService.getTargetSource(sourceRequest, promise));
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest request) {
        return Future.future(p -> dataSourcePluginService.llr(
                request.getQueryRequest().getSourceType(),
                new LlrRequestContext(
                        new LlrRequest(
                                request.getQueryRequest(),
                                request.getLogicalSchema(),
                                request.getMetadata())
                ),
                p));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.SELECT;
    }

    private boolean isInformationSchemaRequest(QuerySourceRequest request) {
        Set<String> unicSchemes = request.getQueryRequest().getDeltaInformations().stream()
                .map(DeltaInformation::getSchemaName)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        boolean informationSchemaExists = unicSchemes.contains(InformationSchemaView.SCHEMA_NAME);

        if (unicSchemes.size() > 1 && informationSchemaExists) {
            throw new IllegalArgumentException("Simultaneous query to the information schema and user schema isn't supported");
        } else {
            return informationSchemaExists;
        }
    }
}
