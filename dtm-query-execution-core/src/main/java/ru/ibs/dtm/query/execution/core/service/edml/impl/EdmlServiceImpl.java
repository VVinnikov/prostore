package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
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

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ServiceDbFacade serviceDbFacade;
    private final Map<EdmlAction, EdmlExecutor> executors;

    @Autowired
    public EdmlServiceImpl(ServiceDbFacade serviceDbFacade, LogicalSchemaProvider logicalSchemaProvider,
                           List<EdmlExecutor> edmlExecutors) {
        this.serviceDbFacade = serviceDbFacade;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.executors = edmlExecutors.stream().collect(Collectors.toMap(EdmlExecutor::getAction, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        logicalSchemaProvider.getSchema(context.getRequest().getQueryRequest(), schemaAr -> {
            if (schemaAr.succeeded()) {
                context.setLogicalSchema(schemaAr.result());
                executeRequest(context, resultHandler);
            } else {
                resultHandler.handle(Future.failedFuture(schemaAr.cause()));
            }
        });
    }

    private void executeRequest(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        checkDownloadExtSourceTableExists(context)
                .compose(result -> checkUploadExtTargetTableExists(context))
                .compose(dwnExtRecord -> findTargetDownloadExtTable(context))
                .compose(edmlQuery -> execute(context, edmlQuery, resultHandler))
                .setHandler(resultHandler);
    }

    private Future<Void> checkDownloadExtSourceTableExists(EdmlRequestContext context) {
        return Future.future((Promise<Void> promise) -> {
                    initSourceAndTargetTables(context);
                    serviceDbFacade.getEddlServiceDao().getDownloadExtTableDao().findDownloadExternalTable(context.getSourceTable().getSchemaName(),
                            context.getSourceTable().getTableName(), ar -> {
                                if (ar.succeeded()) {
                                    promise.fail(new RuntimeException("Unable to select data from external unload table: "
                                            + context.getSourceTable()));
                                } else {
                                    promise.complete();
                                }
                            });
                }
        );
    }

    private void initSourceAndTargetTables(EdmlRequestContext context) {
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val defDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableInfos = tableAndSnapshots.stream()
                .map(n -> new TableInfo(n.tryGetSchemaName().orElse(defDatamartMnemonic),
                        n.tryGetTableName().orElseThrow(() -> getCantGetTableNameError(context))))
                .collect(Collectors.toList());
        context.setTargetTable(tableInfos.get(0));
        context.setSourceTable(tableInfos.get(1));
    }

    private RuntimeException getCantGetTableNameError(EdmlRequestContext context) {
        val sql = context.getRequest().getQueryRequest().getSql();
        return new RuntimeException("Can't get table name from sql: " + sql);
    }

    private Future<Void> checkUploadExtTargetTableExists(EdmlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getEddlServiceDao().getUploadExtTableDao().findUploadExternalTable(context.getTargetTable().getSchemaName(),
                        context.getTargetTable().getTableName(), ar -> {
                            if (ar.succeeded()) {
                                promise.fail("Unable to write data to external load table: "
                                        + context.getSqlNode().getTargetTable().toString());
                            } else {
                                promise.complete();
                            }
                        }
                ));
    }

    private Future<EdmlQuery> findTargetDownloadExtTable(EdmlRequestContext context) {
        return Future.future((Promise<EdmlQuery> promise) ->
                serviceDbFacade.getEddlServiceDao().getDownloadExtTableDao().findDownloadExternalTable(context.getTargetTable().getSchemaName(),
                        context.getTargetTable().getTableName(), ar -> {
                            if (ar.succeeded()) {
                                DownloadExtTableRecord record = ar.result();
                                log.debug("Found entry in downloadExternalTable: {}; for targetTable: {}", record, context.getTargetTable());
                                promise.complete(new EdmlQuery(EdmlAction.DOWNLOAD, record));
                            } else {
                                serviceDbFacade.getEddlServiceDao().getUploadExtTableDao().findUploadExternalTable(context.getSourceTable().getSchemaName(),
                                        context.getSourceTable().getTableName(), arUpl -> {
                                            if (arUpl.succeeded()) {
                                                UploadExtTableRecord record = arUpl.result();
                                                log.debug("Found entry in uploadExternalTable: {}; for sourceTable: {}",
                                                        record, context.getSourceTable());
                                                promise.complete(new EdmlQuery(EdmlAction.UPLOAD, record));
                                            } else {
                                                promise.fail(new RuntimeException("No external load / unload tables found!"));
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
