package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.query.execution.plugin.adb.factory.KafkaMpprSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service("kafkaMpprSqlFactoryImpl")
public class KafkaMpprSqlFactoryImpl implements KafkaMpprSqlFactory {
    private static final String DELIMITER = ", ";
    String WRITABLE_EXTERNAL_TABLE_PREF = "PXF_EXT_";
    private static final String CREAT_WRITABLE_EXT_TABLE_SQL = "CREATE WRITABLE EXTERNAL TABLE %s.%s ( %s )\n" +
            "    LOCATION ('pxf://%s?PROFILE=kafka&BOOTSTRAP_SERVERS=%s&BATCH_SIZE=%d')\n" +
            "    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')";
    public static final String INSERT_INTO_WRITABLE_EXT_TABLE_SQL = "INSERT INTO %s.%s %s";
    public static final String DROP_WRITABLE_EXT_TABLE_SQL = "DROP EXTERNAL TABLE IF EXISTS %s.%s";

    @Override
    public String createWritableExtTableSqlQuery(MpprKafkaRequest request) {
        val schema =request.getDatamartMnemonic();
        val table = getTableName(request.getRequestId().toString());
        val columns = request.getDestinationEntity().getFields().stream()
                .map(field -> field.getName() + " " + EntityTypeUtil.pgFromDtmType(field)).collect(Collectors.toList());
        val topic = request.getTopic();
        val brokers = request.getBrokers().stream()
                .map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.toList());
        val chunkSize = ((DownloadExternalEntityMetadata) request.getDownloadMetadata()).getChunkSize();
        return String.format(CREAT_WRITABLE_EXT_TABLE_SQL,
                schema,
                table,
                String.join(DELIMITER, columns),
                topic,
                String.join(DELIMITER, brokers),
                chunkSize);
    }

    @Override
    public String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql) {
        return String.format(INSERT_INTO_WRITABLE_EXT_TABLE_SQL, schema, table, enrichedSql);
    }

    @Override
    public String dropWritableExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_WRITABLE_EXT_TABLE_SQL, schema, table);
    }

    public String getTableName(String requestId) {
        return WRITABLE_EXTERNAL_TABLE_PREF + requestId.replace("-", "_");
    }
}
