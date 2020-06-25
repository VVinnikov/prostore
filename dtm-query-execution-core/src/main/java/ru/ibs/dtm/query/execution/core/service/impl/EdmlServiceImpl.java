package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.calcite.eddl.SqlNodeUtils;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.EdmlService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Обработка EDML-запроса перед передачей правильному плагину
 */
@Slf4j
@Service("coreEdmlService")
public class EdmlServiceImpl implements EdmlService<QueryResult> {

    private final SchemaStorageProvider schemaStorageProvider;
    private final ServiceDao serviceDao;
    private final Map<EdmlAction, EdmlExecutor> executors;

    public EdmlServiceImpl(ServiceDao serviceDao, SchemaStorageProvider schemaStorageProvider, List<EdmlExecutor> edmlExecutors) {
        this.serviceDao = serviceDao;
        this.schemaStorageProvider = schemaStorageProvider;
        this.executors = edmlExecutors.stream()
                .collect(Collectors.toMap(EdmlExecutor::getAction, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        schemaStorageProvider.getLogicalSchema(context.getRequest().getQueryRequest().getDatamartMnemonic(), schemaAr -> {
            if (schemaAr.succeeded()) {
                JsonObject schema = schemaAr.result();
                context.setSchema(schema);
                executeRequest(context, resultHandler);
            } else {
                resultHandler.handle(Future.failedFuture(schemaAr.cause()));
            }
        });
    }

    private void executeRequest(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        /*
         * 1. для заданного datamart, в таблице DownloadExternalTables найдена запись, где table_name = название таблицы-источника -> throw Exception
         * 2. для заданного datamart, в таблице UploadExternalTables найдена запись, где table_name = название таблицы-приёмника -> throw Exception
         * 3. для заданного datamart, в таблице DownloadExternalTables найдена запись, где table_name = название таблицы-приёмника -> EDML Download
         * 4. для заданного datamart, в таблице UploadExternalTables найдена запись, где table_name = название таблицы-источника -> EDML Upload
         */
        checkDownloadExtSourceTableExists(context)
                .compose(result -> checkUploadExtTargetTableExists(context))
                .compose(dwnExtRecord -> findTargetDownloadExtTable(context))
                .compose(edmlQuery -> execute(context, edmlQuery, resultHandler))
                .setHandler(resultHandler);
    }

    private Future<Void> checkDownloadExtSourceTableExists(EdmlRequestContext context) {
        return Future.future((Promise<Void> promise) -> {
                    initSourceAndTargetTables(context);
                    serviceDao.findDownloadExternalTable(context.getSourceTable().getSchemaName(),
                            context.getSourceTable().getTableName(), ar -> {
                                if (ar.succeeded()) {
                                    promise.fail(new RuntimeException("Невозможно выбрать данные из внешней таблицы выгрузки: "
                                            + context.getSourceTable()));
                                } else {
                                    promise.complete();
                                }
                            });
                }
        );
    }

    private void initSourceAndTargetTables(EdmlRequestContext context) {
        TableInfo sourceTable = SqlNodeUtils.getTableInfo((SqlSelect) context.getSqlNode().getSource(),
                context.getRequest().getQueryRequest().getDatamartMnemonic());
        TableInfo targetTable = SqlNodeUtils.getTableInfo((SqlIdentifier) context.getSqlNode().getTargetTable(),
                context.getRequest().getQueryRequest().getDatamartMnemonic());
        context.setSourceTable(sourceTable);
        context.setTargetTable(targetTable);
    }

    private Future<Void> checkUploadExtTargetTableExists(EdmlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDao.findUploadExternalTable(context.getTargetTable().getSchemaName(),
                        context.getTargetTable().getTableName(), ar -> {
                            if (ar.succeeded()) {
                                promise.fail(new RuntimeException("Невозможно записать данные во внешнюю таблицу загрузки: "
                                        + context.getSqlNode().getTargetTable().toString()));
                            } else {
                                promise.complete();
                            }
                        }
                ));
    }

    private Future<EdmlQuery> findTargetDownloadExtTable(EdmlRequestContext context) {
        return Future.future((Promise<EdmlQuery> promise) ->
                serviceDao.findDownloadExternalTable(context.getTargetTable().getSchemaName(),
                        context.getTargetTable().getTableName(), ar -> {
                            if (ar.succeeded()) {
                                DownloadExtTableRecord record = ar.result();
                                log.debug("Найдена запись в downloadExternalTable: {}; для targetTable: {}", record, context.getTargetTable());
                                promise.complete(new EdmlQuery(EdmlAction.DOWNLOAD, record));
                            } else {
                                serviceDao.findUploadExternalTable(context.getSourceTable().getSchemaName(),
                                        context.getSourceTable().getTableName(), arUpl -> {
                                            if (arUpl.succeeded()) {
                                                UploadExtTableRecord record = arUpl.result();
                                                log.debug("Найдена запись в uploadExternalTable: {}; для sourceTable: {}",
                                                        record, context.getSourceTable());
                                                promise.complete(new EdmlQuery(EdmlAction.UPLOAD, record));
                                            } else {
                                                promise.fail(new RuntimeException("Не найдено внешних таблиц загрузки/выгрузки!"));
                                            }
                                        });
                            }
                        })
        );
    }

    private Future<QueryResult> execute(EdmlRequestContext context, EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> resultHandler) {
        return Future.future((Promise<QueryResult> promise) -> executors.get(edmlQuery.getAction()).execute(context, edmlQuery, resultHandler));
    }
}
