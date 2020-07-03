package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.plugin.exload.TableAttribute;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.transformer.Transformer;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.edml.*;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction.*;

@Service
@Slf4j
public class DownloadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final Transformer<DownloadExternalTableAttribute, TableAttribute> tableAttributeTransformer;
    private final EdmlProperties edmlProperties;
    private final ServiceDao serviceDao;
    private final Map<Type, EdmlDownloadExecutor> executors;

    @Autowired
    public DownloadExternalTableExecutor(Transformer<DownloadExternalTableAttribute, TableAttribute> tableAttributeTransformer,
                                         EdmlProperties edmlProperties, ServiceDao serviceDao, List<EdmlDownloadExecutor> downloadExecutors) {
        this.tableAttributeTransformer = tableAttributeTransformer;
        this.edmlProperties = edmlProperties;
        this.serviceDao = serviceDao;
        this.executors = downloadExecutors.stream().collect(Collectors.toMap(EdmlDownloadExecutor::getDownloadType, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> resultHandler) {
        insertDownloadQuery(context, (DownloadExtTableRecord) edmlQuery.getRecord())
                .compose(this::getDownloadExternalAttributes)
                .compose(attributes -> executePluginService(context, attributes, resultHandler))
                .setHandler(resultHandler);
    }

    private Future<DownloadExtTableRecord> insertDownloadQuery(EdmlRequestContext context, DownloadExtTableRecord extTableRecord) {
        return Future.future((Promise<DownloadExtTableRecord> promise) -> {
            log.debug("Внешняя таблица {} найдена", context.getTargetTable().getTableName());
            context.getRequest().getQueryRequest().setSql(context.getSqlNode().getSource().toSqlString(SQL_DIALECT).toString());
            log.debug("От запроса оставили: {}", context.getRequest().getQueryRequest().getSql());
            context.setExloadParam(createQueryExloadParam(context, extTableRecord));
            DownloadQueryRecord downloadQueryRecord = createDownloadQueryRecord(context, extTableRecord);
            serviceDao.insertDownloadQuery(downloadQueryRecord, ar -> {
                if (ar.succeeded()) {
                    log.debug("Добавлен downloadQuery {}", downloadQueryRecord);
                    promise.complete(extTableRecord);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<List<DownloadExternalTableAttribute>> getDownloadExternalAttributes(DownloadExtTableRecord extTableRecord) {
        return Future.future((Promise<List<DownloadExternalTableAttribute>> promise) ->
                serviceDao.findDownloadExternalTableAttributes(extTableRecord.getId(), promise));
    }

    private Future<QueryResult> executePluginService(EdmlRequestContext context, List<DownloadExternalTableAttribute> attributes,
                                                     Handler<AsyncResult<QueryResult>> resultHandler) {
        return Future.future((Promise<QueryResult> promise) -> {
            val tableAttributes = attributes.stream()
                    .map(tableAttributeTransformer::transform)
                    .collect(Collectors.toList());
            context.getExloadParam().setTableAttributes(tableAttributes);
            if (Type.KAFKA_TOPIC.equals(context.getExloadParam().getLocationType())) {
                log.debug("Перед обращением к plugin.mmprKafka");
                executors.get(context.getExloadParam().getLocationType()).execute(context, resultHandler);
            } else {
                log.error("Тип выгрузки {} не реализован", context.getExloadParam().getLocationType());
                promise.fail(new RuntimeException("Другие типы выгрузки ещё не реализованы!"));
            }
        });
    }

    @NotNull
    private QueryExloadParam createQueryExloadParam(EdmlRequestContext context, DownloadExtTableRecord detRecord) {
        final QueryExloadParam exloadParam = new QueryExloadParam();
        exloadParam.setId(UUID.randomUUID());
        exloadParam.setDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic());
        exloadParam.setTableName(context.getTargetTable().getTableName());
        exloadParam.setSqlQuery(context.getRequest().getQueryRequest().getSql());
        exloadParam.setLocationType(detRecord.getLocationType());
        exloadParam.setLocationPath(detRecord.getLocationPath());
        exloadParam.setFormat(detRecord.getFormat());
        exloadParam.setChunkSize(detRecord.getChunkSize() != null ?
                detRecord.getChunkSize() : edmlProperties.getDefaultChunkSize());
        return exloadParam;
    }

    @NotNull
    private DownloadQueryRecord createDownloadQueryRecord(EdmlRequestContext context, DownloadExtTableRecord extTableRecord) {
        return new DownloadQueryRecord(UUID.randomUUID().toString(), extTableRecord.getDatamartId(),
                extTableRecord.getTableName(), context.getRequest().getQueryRequest().getSql(), 0);
    }

    @Override
    public EdmlAction getAction() {
        return DOWNLOAD;
    }
}
