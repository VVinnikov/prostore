package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.calcite.core.extension.eddl.*;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.eddl.*;
import io.arenadata.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EddlQueryParamExtractorImpl implements EddlQueryParamExtractor {

    public static final String ERROR_PARSING_EDDL_QUERY = "Request parsing error";
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
            log.error("Request parsing error", e);
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
                asyncResultHandler.handle(Future.failedFuture("Query [" + sqlNode + "] is not an EDDL statement."));
            }
        } else {
            asyncResultHandler.handle(Future.failedFuture("Query [" + sqlNode + "] is not an EDDL statement."));
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
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);
            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateDownloadExternalTableQuery(
                            tableInfo.getSchemaName(),
                            tableInfo.getTableName(),
                            entity,
                            locationOperator.getType(),
                            getLocation(locationOperator),
                            format,
                            avroSchema.toString(),
                            chunkSizeOperator.getChunkSize())));
        } catch (RuntimeException e) {
            log.error(ERROR_PARSING_EDDL_QUERY, e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }

    private void extractCreateUploadExternalTable(SqlCreateUploadExternalTable ddl,
                                                  String defaultSchema,
                                                  Handler<AsyncResult<EddlQuery>> asyncResultHandler) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val messageLimitOperator = SqlNodeUtils.getOne(ddl, MassageLimitOperator.class);
            asyncResultHandler.handle(Future.succeededFuture(
                    new CreateUploadExternalTableQuery(
                            tableInfo.getSchemaName(),
                            tableInfo.getTableName(),
                            entity,
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
                return getZookeeperHostPort();
            case CSV_FILE:
            case HDFS_LOCATION:
                throw new IllegalArgumentException("The given location type: " + type + " is not supported!");
            default:
                throw new RuntimeException("This type is not supported!");
        }
    }

    @NotNull
    private String getZookeeperHostPort() {
        return kafkaProperties.getCluster().getZookeeperHosts() + ":" + kafkaProperties.getCluster().getZookeeperPort();
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