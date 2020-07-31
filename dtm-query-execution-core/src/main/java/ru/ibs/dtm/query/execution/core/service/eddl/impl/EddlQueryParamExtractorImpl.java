package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.calcite.core.extension.eddl.*;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.dto.eddl.*;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;

@Component
@Slf4j
public class EddlQueryParamExtractorImpl implements EddlQueryParamExtractor {

    public static final String ERROR_PARSING_EDDL_QUERY = "Ошибка парсинга запроса";
    public static final String START_LOCATION_TOKEN = "$";
    private final DefinitionService<SqlNode> definitionService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final AvroSchemaGenerator avroSchemaGenerator;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;

    @Autowired
    public EddlQueryParamExtractorImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            MetadataCalciteGenerator metadataCalciteGenerator,
            AvroSchemaGenerator avroSchemaGenerator,
            @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties,
            @Qualifier("coreVertx") Vertx vertx
    ) {
        this.definitionService = definitionService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.avroSchemaGenerator = avroSchemaGenerator;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
    }

    @Override
    public void extract(QueryRequest request, Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        vertx.executeBlocking(it -> processSqlQuery(request, it), ar -> {
            if (ar.succeeded()) {
                SqlNode sqlNode = (SqlNode) ar.result();
                extract(sqlNode, request.getDatamartMnemonic(), asyncResultHandler);
            } else {
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void processSqlQuery(QueryRequest request, Promise<Object> it) {
        try {
            SqlNode node = definitionService.processingQuery(request.getSql());
            it.complete(node);
        } catch (Exception e) {
            log.error("Ошибка парсинга запроса", e);
            it.fail(e);
        }
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
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            asyncResultHandler.handle(Future.succeededFuture(
                    new DropDownloadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName())));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private void extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                    String defaultSchema,
                                                    Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            LocationOperator locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            ChunkSizeOperator chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);
            ClassTable classTable = metadataCalciteGenerator.generateTableMetadata(ddl);
            Schema avroSchema = avroSchemaGenerator.generateTableSchema(classTable, false);
            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateDownloadExternalTableQuery(
                            tableInfo.getSchemaName(),
                            tableInfo.getTableName(),
                            locationOperator.getType(),
                            getLocation(locationOperator),
                            SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat(),
                            chunkSizeOperator.getChunkSize(),
                            avroSchema.toString())));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private void extractCreateUploadExternalTable(SqlCreateUploadExternalTable sqlNode, String defaultSchema,
                                                  Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(sqlNode, defaultSchema);
            ClassTable classTable = metadataCalciteGenerator.generateTableMetadata(sqlNode);
            Schema avroSchema = avroSchemaGenerator.generateTableSchema(classTable);
            LocationOperator locationOperator = SqlNodeUtils.getOne(sqlNode, LocationOperator.class);
            Format format = SqlNodeUtils.getOne(sqlNode, FormatOperator.class).getFormat();
            MassageLimitOperator messageLimitOperator = SqlNodeUtils.getOne(sqlNode, MassageLimitOperator.class);
            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateUploadExternalTableQuery(
                            tableInfo.getSchemaName(),
                            tableInfo.getTableName(),
                            locationOperator.getType(),
                            getLocation(locationOperator),
                            format,
                            avroSchema.toString(),
                            messageLimitOperator.getMessageLimit())
            ));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private String getLocation(LocationOperator locationOperator) {
        String replaceToken = START_LOCATION_TOKEN + locationOperator.getType().getName();
        return locationOperator.getLocation().replace(replaceToken, getConfigUrl(locationOperator.getType()));
    }

    private String getConfigUrl(Type type) {
        switch (type) {
            case KAFKA_TOPIC:
                return kafkaProperties.getProducer().getProperty().get("bootstrap.servers");
            case CSV_FILE:
            case HDFS_LOCATION:
                throw new IllegalArgumentException("Данный location type: " + type + " не поддерживается!");
            default:
                throw new RuntimeException("Данный тип не поддерживается!");
        }
    }

    private void extractDropUploadExternalTable(SqlDropUploadExternalTable sqlNode, String defaultSchema, Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(sqlNode, defaultSchema);
            asyncResultHandler.handle(Future.succeededFuture(
                    new DropUploadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName())));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }
}
