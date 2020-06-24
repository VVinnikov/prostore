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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.transformer.Transformer;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EdmlDownloadExternalExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final Transformer<DownloadExternalTableAttribute, TableAttribute> tableAttributeTransformer;
    private final DataSourcePluginService pluginService;
    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final EdmlProperties edmlProperties;
    private final ServiceDao serviceDao;

    @Autowired
    public EdmlDownloadExternalExecutor(Transformer<DownloadExternalTableAttribute, TableAttribute> tableAttributeTransformer,
                                        DataSourcePluginService pluginService, MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                        EdmlProperties edmlProperties, ServiceDao serviceDao) {
        this.tableAttributeTransformer = tableAttributeTransformer;
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.serviceDao = serviceDao;
    }

    @Override
    public void execute(EdmlRequestContext context, EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        //TODO
        executeWithSchemaRef(context, asyncResultHandler);
    }

    public void executeWithSchemaRef(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        //TODO проверить и заменить
        getDownloadExternalTable(context)
                .compose(extTableRecord -> insertDownloadQuery(context, extTableRecord))
                .compose(this::getDownloadExternalAttributes)
                .compose(attributes -> executePluginService(context, attributes, resultHandler))
                .setHandler(resultHandler);
    }

    private Future<DownloadExtTableRecord> getDownloadExternalTable(EdmlRequestContext context) {
        return Future.future((Promise<DownloadExtTableRecord> promiseExtTable) ->
                serviceDao.findDownloadExternalTable(context.getRequest().getQueryRequest().getDatamartMnemonic(),
                        context.getSqlNode().getTargetTable().toString(), promiseExtTable));
    }

    private Future<DownloadExtTableRecord> insertDownloadQuery(EdmlRequestContext context, DownloadExtTableRecord extTableRecord) {
        log.debug("Внешняя таблица {} найдена", context.getSqlNode().getTargetTable().toString());
        context.getRequest().getQueryRequest().setSql(context.getSqlNode().getSource().toSqlString(SQL_DIALECT).toString());
        log.debug("От запроса оставили: {}", context.getRequest().getQueryRequest().getSql());
        context.setExloadParam(createQueryExloadParam(context.getSqlNode().getTargetTable().toString(),
                context.getRequest().getQueryRequest(), extTableRecord));
        return Future.future((Promise<DownloadExtTableRecord> promise) ->
                serviceDao.insertDownloadQuery(context.getExloadParam().getId(), extTableRecord.getId(),
                        context.getRequest().getQueryRequest().getSql(), ar -> {
                            if (ar.succeeded()) {
                                promise.complete(extTableRecord);
                            } else {
                                promise.fail(ar.cause());
                            }
                        }));
    }

    private Future<List<DownloadExternalTableAttribute>> getDownloadExternalAttributes(DownloadExtTableRecord extTableRecord) {
        return Future.future((Promise<List<DownloadExternalTableAttribute>> promise) -> serviceDao.findDownloadExternalTableAttributes(extTableRecord.getId(), promise));
    }

    private Future<QueryResult> executePluginService(EdmlRequestContext context, List<DownloadExternalTableAttribute> attributes, Handler<AsyncResult<QueryResult>> resultHandler) {
        val tableAttributes = attributes.stream()
                .map(tableAttributeTransformer::transform)
                .collect(Collectors.toList());
        context.getExloadParam().setTableAttributes(tableAttributes);
        return Future.future((Promise<QueryResult> promise) -> {
            if (Type.KAFKA_TOPIC == context.getExloadParam().getLocationType()) {
                log.debug("Перед обращением к plugin.mmprKafka");
                pluginService.mpprKafka(edmlProperties.getSourceType(), mpprKafkaRequestFactory.create(context.getRequest().getQueryRequest(),
                        context.getExloadParam(), context.getSchema()), resultHandler);
            } else {
                log.error("Другие типы ещё не реализованы");
                resultHandler.handle(Future.failedFuture("Другие типы ещё не реализованы"));
            }
        });
    }

    public void executeWithSchema(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        final QueryRequest queryRequest = context.getRequest().getQueryRequest();
        log.debug("Начало обработки EDML-запроса. execute(type: {}, queryRequest: {})",
                context.getProcessingType(), queryRequest);

        final String externalTable = context.getSqlNode().getTargetTable().toString();
        String onlySelect = context.getSqlNode().getSource().toSqlString(SQL_DIALECT).toString();
        final QueryRequest qrOnlySelect = queryRequest.copy();
        qrOnlySelect.setSql(onlySelect);
        log.debug("От запроса оставили: {}", onlySelect);

        serviceDao.findDownloadExternalTable(qrOnlySelect.getDatamartMnemonic(), externalTable, ar -> {
            if (ar.succeeded()) {
                final DownloadExtTableRecord detRecord = ar.result();
                log.debug("Внешняя таблица {} найдена", externalTable);
                final QueryExloadParam exloadParam = createQueryExloadParam(externalTable, qrOnlySelect, detRecord);
                serviceDao.insertDownloadQuery(exloadParam.getId(), detRecord.getId(), qrOnlySelect.getSql(), idqHandler -> {
                    if (idqHandler.succeeded()) {
                        serviceDao.findDownloadExternalTableAttributes(detRecord.getId(), attrsHandler -> {
                            if (attrsHandler.succeeded()) {
                                val tableAttributes = attrsHandler.result().stream()
                                        .map(tableAttributeTransformer::transform)
                                        .collect(Collectors.toList());
                                exloadParam.setTableAttributes(tableAttributes);
                                if (Type.KAFKA_TOPIC == exloadParam.getLocationType()) {
                                    log.debug("Перед обращением к plugin.mmprKafka");
                                    pluginService.mpprKafka(
                                            edmlProperties.getSourceType(),
                                            mpprKafkaRequestFactory.create(qrOnlySelect, exloadParam, context.getSchema()),
                                            resultHandler);
                                } else {
                                    log.error("Другие типы ещё не реализованы");
                                    resultHandler.handle(Future.failedFuture("Другие типы ещё не реализованы"));
                                }
                            } else {
                                resultHandler.handle(Future.failedFuture(attrsHandler.cause()));
                            }
                        });
                    } else {
                        resultHandler.handle(Future.failedFuture(idqHandler.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @NotNull
    private QueryExloadParam createQueryExloadParam(String externalTable,
                                                    QueryRequest queryRequest,
                                                    DownloadExtTableRecord detRecord) {
        final QueryExloadParam exloadParam = new QueryExloadParam();
        exloadParam.setId(UUID.randomUUID());
        exloadParam.setDatamart(queryRequest.getDatamartMnemonic());
        exloadParam.setTableName(externalTable);
        exloadParam.setSqlQuery(queryRequest.getSql());
        exloadParam.setLocationType(detRecord.getLocationType());
        exloadParam.setLocationPath(detRecord.getLocationPath());
        exloadParam.setFormat(detRecord.getFormat());
        exloadParam.setChunkSize(detRecord.getChunkSize() != null ?
                detRecord.getChunkSize() : edmlProperties.getDefaultChunkSize());
        return exloadParam;
    }

    @Override
    public EdmlAction getAction() {
        return EdmlAction.DOWNLOAD;
    }
}
