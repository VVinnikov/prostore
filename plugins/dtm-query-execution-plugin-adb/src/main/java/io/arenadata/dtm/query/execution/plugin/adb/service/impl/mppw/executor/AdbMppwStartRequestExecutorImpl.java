package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory;
    private final Vertx vertx;
    private final MppwProperties mppwProperties;
    private final String dbName;

    @Autowired
    public AdbMppwStartRequestExecutorImpl(AdbQueryExecutor adbQueryExecutor,
                                           MetadataSqlFactory metadataSqlFactory,
                                           MppwTransferRequestFactory mppwTransferRequestFactory,
                                           MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory,
                                           @Qualifier("coreVertx") Vertx vertx,
                                           MppwProperties mppwProperties,
                                           @Value("${adb.datasource.options.database}") String dbName) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwKafkaLoadRequestFactory = mppwKafkaLoadRequestFactory;
        this.vertx = vertx;
        this.mppwProperties = mppwProperties;
        this.dbName = dbName;
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val format = context.getRequest().getKafkaParameter().getUploadMetadata().getFormat();
            if (!Format.AVRO.equals(format)) {
                promise.fail(new RuntimeException(String.format("Format %s not implemented", format)));
            }
            List<KafkaBrokerInfo> brokers = context.getRequest().getKafkaParameter().getBrokers();
            getOrCreateServer(brokers, dbName)
//                    .compose(server -> createWritableExternalTable(server, context))
                    .compose(server -> createMppwKafkaRequestContext(context, server))
                    .onSuccess(kafkaContext -> {
                        vertx.eventBus().send(MppwTopic.KAFKA_START.getValue(), Json.encode(kafkaContext));
                        log.debug("Mppw started successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> getOrCreateServer(List<KafkaBrokerInfo> brokers, String currentDatabase) {
        return Future.future(promise -> {
            val columnMetadata = Collections.singletonList(new ColumnMetadata("foreign_server_name", ColumnType.VARCHAR));
            val brokersList = brokers.stream().map(KafkaBrokerInfo::getAddress).collect(Collectors.joining(","));
            adbQueryExecutor.execute(metadataSqlFactory.checkServerSqlQuery(currentDatabase, brokersList), columnMetadata, checkServerResult -> {
                if (checkServerResult.succeeded()) {
                    val result = checkServerResult.result();
                    if (result.isEmpty()) {
                        adbQueryExecutor.execute(metadataSqlFactory.createServerSqlQuery(currentDatabase, brokersList), Collections.emptyList(), createServerResult -> {
                            if (createServerResult.succeeded()) {
                                promise.complete(String.format(MetadataSqlFactoryImpl.SERVER_NAME_TEMPLATE, currentDatabase));
                            } else {
                                promise.fail(createServerResult.cause());
                            }
                        });
                    } else {
                        promise.complete(result.get(0).get("foreign_server_name").toString());
                    }
                } else {
                    promise.fail(checkServerResult.cause());
                }
            });
        });
    }

    private Future<Void> createWritableExternalTable(String server, MppwRequestContext context) {
        return Future.future(promise -> {
            val columns = new Schema.Parser().parse(context.getRequest()
                    .getKafkaParameter().getUploadMetadata().getExternalSchema())
                    .getFields().stream()
                    .map(this::avroFieldToString)
                    .filter(column -> !column.contains("sys"))
                    .collect(Collectors.toList());
            adbQueryExecutor.executeUpdate(metadataSqlFactory.createExtTableSqlQuery(server, columns, context, mppwProperties), ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private String avroFieldToString(Schema.Field f) {
        String name = f.name();
        String type = avroTypeToNative(f.schema());
        return String.format("%s %s", name, type);
    }

    private String avroTypeToNative(Schema f) {
        switch (f.getType()) {
            case UNION:
                val fields = f.getTypes();
                val types = fields.stream().map(this::avroTypeToNative).collect(Collectors.toList());
                if (types.size() == 2) { // support only union (null, type)
                    int realTypeIdx = types.get(0).equalsIgnoreCase("NULL") ? 1 : 0;
                    return avroTypeToNative(fields.get(realTypeIdx));
                } else {
                    return "";
                }
            case STRING:
                return "TEXT";
            case INT:
                return "INT";
            case LONG:
                return "INT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case BYTES:
                return "BYTEA";
            case NULL:
                return "NULL";
            default:
                return "";
        }
    }

    private Future<MppwKafkaRequestContext> createMppwKafkaRequestContext(MppwRequestContext context,
                                                                          String server) {
        return Future.future((Promise<MppwKafkaRequestContext> promise) -> {
            final MppwKafkaLoadRequest mppwKafkaLoadRequest = mppwKafkaLoadRequestFactory.create(context, server, mppwProperties);
            final String keyColumnsSqlQuery = metadataSqlFactory.createKeyColumnsSqlQuery(
                    context.getRequest().getKafkaParameter().getDatamart(),
                    context.getRequest().getKafkaParameter().getDestinationTableName());
            final List<ColumnMetadata> metadata = metadataSqlFactory.createKeyColumnQueryMetadata();
            adbQueryExecutor.execute(keyColumnsSqlQuery, metadata, ar -> {
                if (ar.succeeded()) {
                    final MppwTransferDataRequest mppwTransferDataRequest =
                            mppwTransferRequestFactory.create(context, ar.result());
                    MppwKafkaRequestContext kafkaRequestContext =
                            new MppwKafkaRequestContext(mppwKafkaLoadRequest, mppwTransferDataRequest);
                    promise.complete(kafkaRequestContext);
                } else {
                    promise.fail(ar.cause());
                }
            });

        });
    }
}
