package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadQueryRecord;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final ServiceDao serviceDao;
    private final EdmlProperties edmlProperties;
    private final Map<Type, EdmlUploadExecutor> executors;

    @Autowired
    public UploadExternalTableExecutor(ServiceDao serviceDao, EdmlProperties edmlProperties, List<EdmlUploadExecutor> uploadExecutors) {
        this.serviceDao = serviceDao;
        this.edmlProperties = edmlProperties;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getDeltaHotInProcess(context)
                .compose(deltaRecord -> insertUploadQuery(context, edmlQuery, deltaRecord))
                .compose(uploadRecord -> executeUpload(context, edmlQuery, asyncResultHandler))
                .setHandler(asyncResultHandler);
    }

    private Future<DeltaRecord> getDeltaHotInProcess(EdmlRequestContext context) {
        return Future.future((Promise<DeltaRecord> promise) ->
                serviceDao.getDeltaHotByDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), ar -> {
                    if (ar.succeeded()) {
                        DeltaRecord deltaRecord = ar.result();
                        if (deltaRecord.getStatus() != DeltaLoadStatus.IN_PROCESS) {
                            promise.fail(new RuntimeException("Не найдена открытая дельта!"));
                        }
                        log.debug("Найдена последняя открытая дельта {}", deltaRecord);
                        promise.complete(deltaRecord);
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<UploadQueryRecord> insertUploadQuery(EdmlRequestContext context, EdmlQuery edmlQuery, DeltaRecord deltaRecord) {
        return Future.future((Promise<UploadQueryRecord> promise) -> {
            UploadQueryRecord uploadQueryRecord = createUploadQueryRecord(context, edmlQuery);
            QueryLoadParam queryLoadParam = createQueryLoadParam(context, (UploadExtTableRecord) edmlQuery.getRecord(), deltaRecord);
            context.setLoadParam(queryLoadParam);
            serviceDao.inserUploadQuery(uploadQueryRecord, ar -> {
                if (ar.succeeded()) {
                    log.debug("Добавлен uploadQuery {}", uploadQueryRecord);
                    promise.complete(uploadQueryRecord);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> executeUpload(EdmlRequestContext context, EdmlQuery edmlQuery,
                                              Handler<AsyncResult<QueryResult>> resultHandler) {
        return Future.future((Promise<QueryResult> promise) -> {
            if (Type.KAFKA_TOPIC.equals(edmlQuery.getRecord().getLocationType())) {
                executors.get(edmlQuery.getRecord().getLocationType()).execute(context, resultHandler);
            } else {
                log.error("Тип загрузки {} не реализован", context.getExloadParam().getLocationType());
                promise.fail(new RuntimeException("Другие типы загрузки ещё не реализованы!"));
            }
        });
    }

    @NotNull
    private QueryLoadParam createQueryLoadParam(EdmlRequestContext context, UploadExtTableRecord uplRecord, DeltaRecord deltaRecord) {
        final QueryLoadParam loadParam = new QueryLoadParam();
        loadParam.setId(UUID.randomUUID());
        loadParam.setIsLoadStart(true);
        loadParam.setDatamart(context.getSourceTable().getSchemaName());
        loadParam.setTableName(context.getTargetTable().getTableName());
        loadParam.setSqlQuery(context.getSqlNode().toSqlString(SQL_DIALECT).toString());
        loadParam.setLocationType(uplRecord.getLocationType());
        loadParam.setLocationPath(uplRecord.getLocationPath());
        loadParam.setFormat(uplRecord.getFormat());
        loadParam.setAvroSchema(uplRecord.getTableSchema());
        loadParam.setDeltaHot(deltaRecord.getSinId());
        loadParam.setMessageLimit(uplRecord.getMessageLimit() != null ?
                uplRecord.getMessageLimit() : edmlProperties.getDefaultMessageLimit());
        return loadParam;
    }

    private UploadQueryRecord createUploadQueryRecord(EdmlRequestContext context, EdmlQuery edmlQuery) {
        return new UploadQueryRecord(
                UUID.randomUUID().toString(),
                edmlQuery.getRecord().getDatamartId(),
                edmlQuery.getRecord().getTableName(),
                context.getTargetTable().getTableName(),
                context.getSqlNode().toSqlString(SQL_DIALECT).toString(),
                0
        );
    }

    @Override
    public EdmlAction getAction() {
        return EdmlAction.UPLOAD;
    }
}
