package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.query.calcite.core.extension.eddl.*;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.eddl.*;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EddlQueryParamExtractorImpl implements EddlQueryParamExtractor {

    public static final String ERROR_PARSING_EDDL_QUERY = "Eddl request parsing error";
    public static final String START_LOCATION_TOKEN = "$";
    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
    private final DefinitionService<SqlNode> definitionService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final AvroSchemaGenerator avroSchemaGenerator;
    private final KafkaZookeeperProperties kafkaZookeeperProperties;
    private final Vertx vertx;

    @Autowired
    public EddlQueryParamExtractorImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            MetadataCalciteGenerator metadataCalciteGenerator,
            AvroSchemaGenerator avroSchemaGenerator,
            KafkaZookeeperProperties kafkaZookeeperProperties,
            @Qualifier("coreVertx") Vertx vertx) {
        this.definitionService = definitionService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.avroSchemaGenerator = avroSchemaGenerator;
        this.kafkaZookeeperProperties = kafkaZookeeperProperties;
        this.vertx = vertx;
    }

    @Override
    public Future<EddlQuery> extract(QueryRequest request) {
        return processSqlQuery(request)
                .compose(sqlNode -> extract(sqlNode, request.getDatamartMnemonic()));
    }

    private Future<SqlNode> processSqlQuery(QueryRequest request) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            SqlNode node = definitionService.processingQuery(request.getSql());
            it.complete(node);
        }, promise));
    }

    private Future<EddlQuery> extract(SqlNode sqlNode, String defaultSchema) {
        return Future.future(promise -> {
            if (sqlNode instanceof SqlDdl) {
                if (sqlNode instanceof SqlDropDownloadExternalTable) {
                    promise.complete(extractDropDownloadExternalTable(
                            (SqlDropDownloadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlCreateDownloadExternalTable) {
                    promise.complete(extractCreateDownloadExternalTable(
                            (SqlCreateDownloadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlCreateUploadExternalTable) {
                    promise.complete(extractCreateUploadExternalTable((SqlCreateUploadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlDropUploadExternalTable) {
                    promise.complete(extractDropUploadExternalTable((SqlDropUploadExternalTable) sqlNode,
                            defaultSchema));
                } else {
                    promise.fail(new DtmException(String.format("Query [%s] is not an EDDL statement",
                            sqlNode)));
                }
            } else {
                promise.fail(new DtmException(String.format("Query [%s] is not an EDDL statement",
                        sqlNode)));
            }
        });
    }

    private DropDownloadExternalTableQuery extractDropDownloadExternalTable(SqlDropDownloadExternalTable ddl,
                                                                            String defaultSchema) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            return new DropDownloadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop download external table query", e);
        }
    }

    private CreateDownloadExternalTableQuery extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                                                String defaultSchema) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);
            return new CreateDownloadExternalTableQuery(
                    tableInfo.getSchemaName(),
                    tableInfo.getTableName(),
                    entity,
                    locationOperator.getType(),
                    getLocation(locationOperator),
                    format,
                    avroSchema.toString(),
                    chunkSizeOperator.getChunkSize());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create download external table query", e);
        }
    }

    private CreateUploadExternalTableQuery extractCreateUploadExternalTable(SqlCreateUploadExternalTable ddl,
                                                                            String defaultSchema) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val messageLimitOperator = SqlNodeUtils.getOne(ddl, MassageLimitOperator.class);
            return new CreateUploadExternalTableQuery(
                    tableInfo.getSchemaName(),
                    tableInfo.getTableName(),
                    entity,
                    locationOperator.getType(),
                    getLocation(locationOperator),
                    format,
                    avroSchema.toString(),
                    messageLimitOperator.getMessageLimit());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create upload external table query", e);
        }
    }

    private DropUploadExternalTableQuery extractDropUploadExternalTable(SqlDropUploadExternalTable sqlNode,
                                                                        String defaultSchema) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(sqlNode, defaultSchema);
            return new DropUploadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop upload external table query", e);
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
                throw new DtmException("The given location type: " + type + " is not supported!");
            default:
                throw new DtmException("This type is not supported!");
        }
    }

    private String getZookeeperHostPort() {
        return kafkaZookeeperProperties.getConnectionString() + ":" + ZOOKEEPER_DEFAULT_PORT;
    }
}
