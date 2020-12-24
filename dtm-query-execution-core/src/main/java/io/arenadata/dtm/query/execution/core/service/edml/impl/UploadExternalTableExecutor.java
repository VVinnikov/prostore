package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final EdmlUploadFailedExecutor uploadFailedExecutor;
    private final DataSourcePluginService pluginService;
    private final LogicalSchemaProvider logicalSchemaProvider;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       EdmlUploadFailedExecutor uploadFailedExecutor,
                                       List<EdmlUploadExecutor> uploadExecutors,
                                       DataSourcePluginService pluginService,
                                       LogicalSchemaProvider logicalSchemaProvider) {
        this.deltaServiceDao = deltaServiceDao;
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return isEntitySourceTypesExistsInConfiguration(context)
                .compose(v -> writeNewOperationIfNeeded(context, context.getSourceEntity()))
                .compose(v -> executeAndWriteOp(context))
                .compose(queryResult -> writeOpSuccess(context.getSourceEntity().getSchema(),
                        context.getSysCn(),
                        queryResult));
    }

    private Future<Void> isEntitySourceTypesExistsInConfiguration(EdmlRequestContext context) {
        final Set<SourceType> nonExistDestionationTypes = context.getDestinationEntity().getDestination().stream()
                .filter(type -> !pluginService.getSourceTypes().contains(type))
                .collect(Collectors.toSet());
        if (!nonExistDestionationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestionationTypes,
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema());
            return Future.failedFuture(new DtmException(failureMessage));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Long> writeNewOperationIfNeeded(EdmlRequestContext context, Entity entity) {
        if (context.getSysCn() != null) {
            return Future.succeededFuture();
        } else {
            return Future.future(writePromise -> deltaServiceDao.writeNewOperation(createDeltaOp(context, entity))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            long sysCn = ar.result();
                            context.setSysCn(sysCn);
                            writePromise.complete();
                        } else {
                            writePromise.fail(ar.cause());
                        }
                    }));
        }
    }

    private DeltaWriteOpRequest createDeltaOp(EdmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(context.getDestinationEntity().getName())
                .tableNameExt(entity.getName())
                .query(context.getSqlNode().toSqlString(SQL_DIALECT).toString())
                .build();
    }

    private Future<QueryResult> executeAndWriteOp(EdmlRequestContext context) {
        return Future.future(promise ->
                initLogicalSchema(context)
                        .compose(ctx -> executeInternal(context))
                        .onSuccess(promise::complete)
                        .onFailure(error -> {
                            deltaServiceDao.writeOperationError(context.getSourceEntity().getSchema(), context.getSysCn())
                                    .compose(v -> uploadFailedExecutor.execute(context))
                                    .onComplete(writeErrorOpAr -> {
                                        if (writeErrorOpAr.succeeded()) {
                                            promise.fail(error);
                                        } else {
                                            promise.fail(new DtmException("Can't write operation error", writeErrorOpAr.cause()));
                                        }
                                    });
                        }));
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            if (ExternalTableLocationType.KAFKA == context.getSourceEntity().getExternalTableLocationType()) {
                executors.get(context.getSourceEntity().getExternalTableLocationType())
                        .execute(context)
                        .onComplete(promise);
            } else {
                promise.fail(new DtmException("Other download types are not yet implemented"));
            }
        });
    }

    private Future<QueryResult> writeOpSuccess(String datamartName, Long sysCn, QueryResult result) {
        return Future.future(promise ->
                deltaServiceDao.writeOperationSuccess(datamartName, sysCn)
                        .onSuccess(v -> promise.complete(result))
                        .onFailure(promise::fail));
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise -> logicalSchemaProvider.getSchemaFromQuery(context.getRequest().getQueryRequest())
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        final List<Datamart> logicalSchema = ar.result();
                        context.setLogicalSchema(logicalSchema);
                        promise.complete();
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public EdmlAction getAction() {
        return UPLOAD;
    }
}
