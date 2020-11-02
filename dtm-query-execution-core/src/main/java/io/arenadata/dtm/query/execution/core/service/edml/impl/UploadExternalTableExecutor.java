package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final EdmlUploadFailedExecutor uploadFailedExecutor;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       EdmlUploadFailedExecutor uploadFailedExecutor,
                                       List<EdmlUploadExecutor> uploadExecutors) {
        this.deltaServiceDao = deltaServiceDao;
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        writeNewOperationIfNeeded(context, context.getSourceEntity())
                .compose(v -> executeAndWriteOp(context))
                .compose(queryResult -> writeOpSuccess(context.getSourceTable().getSchemaName(), context.getSysCn(), queryResult))
                .onComplete(resultHandler);
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
                .tableName(context.getDestinationTable().getTableName())
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
                            deltaServiceDao.writeOperationError(context.getSourceTable().getSchemaName(), context.getSysCn())
                                    .compose(v -> uploadFailedExecutor.execute(context))
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
            if (ExternalTableLocationType.KAFKA == context.getSourceEntity().getExternalTableLocationType()) {
                executors.get(context.getSourceEntity().getExternalTableLocationType()).execute(context, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });
            } else {
                log.error("Loading type {} not implemented", context.getSourceEntity().getExternalTableLocationType());
                promise.fail(new RuntimeException("Other download types are not yet implemented!"));
            }
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
