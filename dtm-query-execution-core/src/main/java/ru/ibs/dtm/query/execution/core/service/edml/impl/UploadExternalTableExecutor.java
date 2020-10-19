package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       List<EdmlUploadExecutor> uploadExecutors) {
        this.deltaServiceDao = deltaServiceDao;
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
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        writeErrorOp(context, ar.cause())
                            .onFailure(promise::fail);
                    }
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

    private Future<Void> writeErrorOp(EdmlRequestContext context, Throwable error) {
        return Future.future(promise -> {
            val datamartName = context.getSourceTable().getSchemaName();
            deltaServiceDao.writeOperationError(datamartName, context.getSysCn())
                .compose(v -> eraseWriteOp())
                .compose(v -> deltaServiceDao.deleteWriteOperation(datamartName, context.getSysCn()))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.error("Edml write operation error!", error);
                        promise.fail(error);
                    } else {
                        log.error("Can't write operation error!", ar.cause());
                        promise.fail(ar.cause());
                    }
                });
        });
    }

    private Future<Void> eraseWriteOp() {
        //FIXME will be implemented after writing description in confluence
        return Future.succeededFuture();
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
