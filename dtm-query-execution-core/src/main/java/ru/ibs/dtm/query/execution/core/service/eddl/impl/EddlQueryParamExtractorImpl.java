package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.calcite.eddl.*;
import ru.ibs.dtm.query.execution.core.dto.eddl.*;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;

import java.util.List;

@Component
@Slf4j
public class EddlQueryParamExtractorImpl implements EddlQueryParamExtractor {

    public static final String ERROR_PARSING_EDDL_QUERY = "Ошибка парсинга запроса";
    private final DefinitionService<SqlNode> definitionService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final AvroSchemaGenerator avroSchemaGenerator;
    private final Vertx vertx;

    @Autowired
    public EddlQueryParamExtractorImpl(DefinitionService<SqlNode> definitionService,
                                       MetadataCalciteGenerator metadataCalciteGenerator, AvroSchemaGenerator avroSchemaGenerator, @Qualifier("coreVertx") Vertx vertx) {
        this.definitionService = definitionService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.avroSchemaGenerator = avroSchemaGenerator;
        this.vertx = vertx;
    }

    @Override
    public void extract(QueryRequest request, Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                SqlNode node = definitionService.processingQuery(request.getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                SqlNode sqlNode = (SqlNode) ar.result();
                extract(sqlNode, request.getDatamartMnemonic(), asyncResultHandler);
            } else {
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void extract(SqlNode sqlNode,
                         String defaultSchema,
                         Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        if (sqlNode instanceof SqlDdl) {
            if (sqlNode instanceof SqlDropDownloadExternalTable) {
                extractDropDownloadExternalTable(
                        (SqlDropDownloadExternalTable) sqlNode,
                        defaultSchema,
                        asyncResultHandler);
            } else if (sqlNode instanceof SqlCreateDownloadExternalTable) {
                extractCreateDownloadExternalTable(
                        (SqlCreateDownloadExternalTable) sqlNode,
                        defaultSchema,
                        asyncResultHandler);
            } else if (sqlNode instanceof SqlCreateUploadExternalTable) {
                extractCreateUploadExternalTable((SqlCreateUploadExternalTable) sqlNode, defaultSchema, asyncResultHandler);
            } else if (sqlNode instanceof SqlDropUploadExternalTable) {
                extractDropUploadExternalTable((SqlDropUploadExternalTable) sqlNode, defaultSchema, asyncResultHandler);
            } else {
                asyncResultHandler.handle(Future.failedFuture("Запрос [" + sqlNode + "] не является EDDL оператором."));
            }
        } else {
            asyncResultHandler.handle(Future.failedFuture("Запрос [" + sqlNode + "] не является EDDL оператором."));
        }
    }

    private void extractDropDownloadExternalTable(SqlDropDownloadExternalTable ddl,
                                                  String defaultSchema,
                                                  Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            List<String> names = SqlNodeUtils.getTableNames(ddl);
            asyncResultHandler.handle(Future.succeededFuture(
                    new DropDownloadExternalTableQuery(
                            getSchema(names, defaultSchema),
                            getTableName(names)
                    )));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e.getCause()));
        }
    }

    private void extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                    String defaultSchema,
                                                    Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            List<String> names = SqlNodeUtils.getTableNames(ddl);
            LocationOperator locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            ChunkSizeOperator chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);

            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateDownloadExternalTableQuery(
                            getSchema(names, defaultSchema),
                            getTableName(names),
                            locationOperator.getType(),
                            getLocation(locationOperator),
                            SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat(),
                            chunkSizeOperator.getChunkSize())));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e.getCause()));
        }
    }

    private void extractCreateUploadExternalTable(SqlCreateUploadExternalTable sqlNode, String defaultSchema,
                                                  Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            ClassTable classTable = metadataCalciteGenerator.generateTableMetadata(sqlNode);
            if (classTable.getSchema() == null) {
                classTable.setSchema(defaultSchema);
            }
            Schema avroSchema = avroSchemaGenerator.generateTableSchema(classTable);
            LocationOperator locationOperator = SqlNodeUtils.getOne(sqlNode, LocationOperator.class);
            Format format = SqlNodeUtils.getOne(sqlNode, FormatOperator.class).getFormat();
            MassageLimitOperator messageLimitOperator = SqlNodeUtils.getOne(sqlNode, MassageLimitOperator.class);
            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateUploadExternalTableQuery(classTable.getSchema(), classTable.getName(), locationOperator.getType(),
                            getLocation(locationOperator), format, avroSchema.toString(), messageLimitOperator.getMessageLimit())
            ));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e.getCause()));
        }
    }

    private String getLocation(LocationOperator locationOperator) {
        String startToken = "$";
        String replaceToken = startToken + locationOperator.getType().getName();
        //TODO доделать когда решится вопрос с конфигами
        return locationOperator.getLocation().replace(replaceToken, locationOperator.getType().getName());
    }

    private void extractDropUploadExternalTable(SqlDropUploadExternalTable sqlNode, String defaultSchema, Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            List<String> names = SqlNodeUtils.getTableNames(sqlNode);
            asyncResultHandler.handle(Future.succeededFuture(
                    new DropUploadExternalTableQuery(
                            getSchema(names, defaultSchema),
                            getTableName(names)
                    )));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e.getCause()));
        }
    }

    private String getTableName(List<String> names) {
        return names.get(names.size() - 1);
    }

    private String getSchema(List<String> names, String defaultSchema) {
        return names.size() > 1 ? names.get(names.size() - 2) : defaultSchema;
    }
}
