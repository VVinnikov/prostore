package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component("adbMppwTransferDataHandler")
@Slf4j
public class AdbMppwTransferDataHandler implements AdbMppwHandler {

    private static final String CREATE_FOREIGN_TABLE_SQL =
        "CREATE FOREIGN TABLE %s.%s (%s)\n" +
            "SERVER %s\n" +
            "OPTIONS (\n" +
            "    format '%s',\n" +
            "    k_topic '%s',\n" +
            "    k_consumer_group '%s',\n" +
            "    k_seg_batch '%s',\n" +
            "    k_timeout_ms '%s',\n" +
            "    k_initial_offset '0'\n" +
            ")";
    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final AdbMppwDataTransferService mppwDataTransferService;

    @Autowired
    public AdbMppwTransferDataHandler(AdbQueryExecutor adbQueryExecutor,
                                      MetadataSqlFactory metadataSqlFactory,
                                      AdbMppwDataTransferService mppwDataTransferService) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwDataTransferService = mppwDataTransferService;
    }

    @Override
    public Future<Void> handle(MppwKafkaRequestContext requestContext) {
        return insertIntoStagingTable(requestContext.getMppwKafkaLoadRequest())
            .compose(v -> commitKafkaMessages(requestContext))
            .compose(s -> Future.future((Promise<Void> p) ->
                mppwDataTransferService.execute(requestContext.getMppwTransferDataRequest(), p)));
    }

    private Future<Void> createExtTable(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val columns = requestContext.getMppwKafkaLoadRequest().getSchema()
                .getFields().stream()
                .map(this::avroFieldToString)
                .filter(column -> !column.contains("sys"))
                .collect(Collectors.toList());
            val server = requestContext.getMppwKafkaLoadRequest().getServer();
            adbQueryExecutor.executeUpdate(createExtTableSqlQuery(server,
                columns,
                requestContext.getMppwKafkaLoadRequest().getDatamart(),
                requestContext.getMppwKafkaLoadRequest().getRequestId(),
                "avro",
                requestContext.getMppwKafkaLoadRequest().getTopic(),
                requestContext.getMppwKafkaLoadRequest().getConsumerGroup(),
                requestContext.getMppwKafkaLoadRequest().getUploadMessageLimit(),
                requestContext.getMppwKafkaLoadRequest().getTimeout()), ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    public String createExtTableSqlQuery(String server, List<String> columnNameTypeList,
                                         String schema,
                                         String reqId,
                                         String format,
                                         String topic,
                                         String consumerGroup,
                                         Long uploadMessageLimit,
                                         Long timeout) {
        val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + reqId.replaceAll("-", "_");
        val columns = String.join(", ", columnNameTypeList);
        val chunkSize = uploadMessageLimit != null ? uploadMessageLimit : 25;
        return String.format(CREATE_FOREIGN_TABLE_SQL,
            schema,
            table,
            columns,
            server,
            format,
            topic,
            consumerGroup,
            chunkSize,
            "1000");
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
                return "BIGINT";
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

    private Future<Void> dropExtTable(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val schema = requestContext.getMppwKafkaLoadRequest().getDatamart();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + requestContext.getMppwKafkaLoadRequest().getRequestId().replaceAll("-", "_");
            adbQueryExecutor.executeUpdate(metadataSqlFactory.dropExtTableSqlQuery(schema, table), promise);
        });
    }

    private Future<Void> commitKafkaMessages(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val schema = requestContext.getMppwKafkaLoadRequest().getDatamart();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + requestContext.getMppwKafkaLoadRequest().getRequestId().replaceAll("-", "_");
            val commitOffsetsSql = String.format(MetadataSqlFactoryImpl.COMMIT_OFFSETS, table);
            adbQueryExecutor.executeUpdate(commitOffsetsSql, promise);
        });
    }

    private Future<Void> insertIntoStagingTable(MppwKafkaLoadRequest request) {
        return Future.future(promise -> {
            val schema = request.getDatamart();
            val columns = String.join(", ", request.getColumns());
            val extTable = request.getWritableExtTableName().replaceAll("-", "_");
            val stagingTable = request.getTableName();
            adbQueryExecutor.executeUpdate(metadataSqlFactory.insertIntoStagingTableSqlQuery(schema, columns, stagingTable, extTable), ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }
}
