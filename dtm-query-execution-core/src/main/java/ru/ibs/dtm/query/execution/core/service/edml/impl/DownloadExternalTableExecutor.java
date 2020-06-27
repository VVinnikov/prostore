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
import ru.ibs.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
        this.executors = downloadExecutors.stream()
                .collect(Collectors.toMap(EdmlDownloadExecutor::getDownloadType, it -> it));
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
            context.setExloadParam(createQueryExloadParam(context.getTargetTable().getTableName(),
                    context.getRequest().getQueryRequest(), extTableRecord));
            serviceDao.insertDownloadQuery(context.getExloadParam().getId(), extTableRecord.getId(),
                    context.getRequest().getQueryRequest().getSql(), ar -> {
                        if (ar.succeeded()) {
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
    private QueryExloadParam createQueryExloadParam(String externalTable, QueryRequest queryRequest, DownloadExtTableRecord detRecord) {
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
