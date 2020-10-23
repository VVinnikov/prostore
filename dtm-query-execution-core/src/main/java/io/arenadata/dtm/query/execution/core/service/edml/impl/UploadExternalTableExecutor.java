package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final RollbackRequestContextFactory rollbackRequestContextFactory;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       RollbackRequestContextFactory rollbackRequestContextFactory,
                                       DataSourcePluginService dataSourcePluginService,
                                       List<EdmlUploadExecutor> uploadExecutors) {
        this.deltaServiceDao = deltaServiceDao;
        this.rollbackRequestContextFactory = rollbackRequestContextFactory;
        this.dataSourcePluginService = dataSourcePluginService;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        writeNewOperation(context, context.getEntity())
                .compose(sysCn -> executeAndWriteOp(context))
                .compose(queryResult -> writeOpSuccess(context.getSourceTable().getSchemaName(), context.getSysCn(), queryResult))
                .onComplete(resultHandler);
    }

    private Future<Long> writeNewOperation(EdmlRequestContext context, Entity entity) {
        return Future.future(writePromise -> {
            deltaServiceDao.writeNewOperation(createDeltaOp(context, entity))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            long sysCn = ar.result();
                            context.setSysCn(sysCn);
                            writePromise.complete(sysCn);
                        } else {
                            writePromise.fail(ar.cause());
                        }
                    });
        });
    }

    private DeltaWriteOpRequest createDeltaOp(EdmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(context.getTargetTable().getTableName())
                .tableNameExt(entity.getName())
                .query(context.getSqlNode().toSqlString(SQL_DIALECT).toString())
                .build();
    }

    private Future<QueryResult> executeAndWriteOp(EdmlRequestContext context) {
        return Future.future(promise ->
                execute(context)
                        .onSuccess(promise::complete)
                        .onFailure(error -> {
                            log.error("Edml write operation error!", error);
                            writeErrorOp(context)
                                    .onComplete(writeErrorOpAr -> {
                                        if (writeErrorOpAr.succeeded()) {
                                            promise.fail(error);
                                        } else {
                                            log.error("Can't write operation error!", writeErrorOpAr.cause());
                                            promise.fail(writeErrorOpAr.cause());
                                        }
                                    });
                        }));
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            if (ExternalTableLocationType.KAFKA == context.getEntity().getExternalTableLocationType()) {
                executors.get(context.getEntity().getExternalTableLocationType()).execute(context, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });
            } else {
                log.error("Loading type {} not implemented", context.getEntity().getExternalTableLocationType());
                promise.fail(new RuntimeException("Other download types are not yet implemented!"));
            }
        });
    }

    private Future<Void> writeErrorOp(EdmlRequestContext context) {
        return Future.future(promise -> {
            val datamartName = context.getSourceTable().getSchemaName();
            deltaServiceDao.writeOperationError(datamartName, context.getSysCn())
                    .compose(v -> eraseWriteOp(context))
                    .compose(v -> deltaServiceDao.deleteWriteOperation(datamartName, context.getSysCn()))
                    .setHandler(promise);
        });
    }

    private Future<Void> eraseWriteOp(EdmlRequestContext context) {
        return Future.future(rbPromise -> {
            List<Future> futures = new ArrayList<>();
            final RollbackRequestContext rollbackRequestContext =
                    rollbackRequestContextFactory.create(context);
            dataSourcePluginService.getSourceTypes().forEach(sourceType ->
                    futures.add(Future.future(p -> dataSourcePluginService.rollback(
                            sourceType,
                            rollbackRequestContext,
                            ar -> {
                                if (ar.succeeded()) {
                                    log.debug("Rollback data in plugin [{}], datamart [{}], " +
                                                    "table [{}], sysCn [{}] finished successfully",
                                            sourceType,
                                            context.getEntity().getSchema(),
                                            context.getTargetTable().getTableName(),
                                            context.getSysCn());
                                    p.complete();
                                } else {
                                    log.error("Error rollback data in plugin [{}], " +
                                                    "datamart [{}], table [{}], sysCn [{}]",
                                            sourceType,
                                            context.getEntity().getSchema(),
                                            context.getTargetTable().getTableName(),
                                            context.getSysCn(),
                                            ar.cause());
                                    p.fail(ar.cause());
                                }
                            }))));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    rbPromise.complete();
                } else {
                    rbPromise.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> writeOpSuccess(String datamartName, Long sysCn, QueryResult result) {
        return Future.future(promise ->
                deltaServiceDao.writeOperationSuccess(datamartName, sysCn)
                        .onSuccess(v -> promise.complete(result))
                        .onFailure(promise::fail));
    }

    @Override
    public EdmlAction getAction() {
        return UPLOAD;
    }
}
