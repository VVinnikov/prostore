package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class MetadataSqlFactoryImpl implements MetadataSqlFactory {
    /**
     * Name of the table of actual data
     */
    public static final String ACTUAL_TABLE = "actual";
    /**
     * History table name
     */
    public static final String HISTORY_TABLE = "history";
    /**
     * Name staging table
     */
    public static final String STAGING_TABLE = "staging";
    /**
     * Delta Number System Field
     */
    public static final String SYS_FROM_ATTR = "sys_from";
    /**
     * System field of maximum delta number
     */
    public static final String SYS_TO_ATTR = "sys_to";
    /**
     * System field of operation on an object
     */
    public static final String SYS_OP_ATTR = "sys_op";
    /**
     * Request ID system field
     */
    public static final String QUERY_DELIMITER = "; ";
    public static final String TABLE_POSTFIX_DELIMITER = "_";
    public static final String WRITABLE_EXTERNAL_TABLE_PREF = "PXF_EXT_";

    private static final String DELIMITER = ", ";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ";
    private static final String DROP_SCHEMA = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    private static final String KEY_COLUMNS_TEMPLATE_SQL = "SELECT c.column_name, c.data_type\n" +
            "FROM information_schema.table_constraints tc\n" +
            "         JOIN information_schema.KEY_COLUMN_USAGE AS ccu USING (constraint_schema, constraint_name)\n" +
            "         JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n" +
            "    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n" +
            "WHERE constraint_type = 'PRIMARY KEY'\n" +
            "  and c.table_schema = '%s' and tc.table_name = '%s'";
    private static final String CREATE_INDEX_SQL = "CREATE INDEX %s_%s_%s ON %s.%s_%s (%s)";
    private static final String CREAT_WRITABLE_EXT_TABLE_SQL = "CREATE WRITABLE EXTERNAL TABLE %s.%s ( %s )\n" +
            "    LOCATION ('pxf://%s?PROFILE=kafka&BOOTSTRAP_SERVERS=%s&BATCH_SIZE=%d')\n" +
            "    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')";
    public static final String INSERT_INTO_WRITABLE_EXT_TABLE_SQL = "INSERT INTO %s.%s %s";
    public static final String DROP_WRITABLE_EXT_TABLE_SQL = "DROP EXTERNAL TABLE IF EXISTS %s.%s";

    @Override
    public String createDropTableScript(Entity entity) {
        return new StringBuilder()
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(ACTUAL_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(HISTORY_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(STAGING_TABLE)
                .append(QUERY_DELIMITER)
                .toString();
    }

    @Override
    public String createSchemaSqlQuery(String schemaName) {
        return String.format(CREATE_SCHEMA, schemaName);
    }

    @Override
    public String dropSchemaSqlQuery(String schemaName) {
        return String.format(DROP_SCHEMA, schemaName);
    }

    @Override
    public String createKeyColumnsSqlQuery(String schema, String tableName) {
        return String.format(KEY_COLUMNS_TEMPLATE_SQL, schema, tableName + TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE);
    }

    @Override
    public String createSecondaryIndexSqlQuery(String schema, String tableName) {
        StringBuilder sb = new StringBuilder();
        final String idxPostfix = "_idx";
        sb.append(String.format(CREATE_INDEX_SQL, tableName, ACTUAL_TABLE,
                SYS_FROM_ATTR + idxPostfix, schema, tableName, ACTUAL_TABLE,
                String.join(DELIMITER, Collections.singletonList(SYS_FROM_ATTR))));
        sb.append(QUERY_DELIMITER);
        sb.append(String.format(CREATE_INDEX_SQL, tableName, HISTORY_TABLE,
                SYS_TO_ATTR + idxPostfix, schema, tableName, HISTORY_TABLE,
                String.join(DELIMITER, Arrays.asList(SYS_TO_ATTR, SYS_OP_ATTR))));
        return sb.toString();
    }

    @Override
    public List<ColumnMetadata> createKeyColumnQueryMetadata() {
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("column_name", ColumnType.VARCHAR));
        metadata.add(new ColumnMetadata("data_type", ColumnType.VARCHAR));
        return metadata;
    }

    @Override
    public String createWritableExtTableSqlQuery(MpprRequest request) {
        val schema = request.getQueryRequest().getDatamartMnemonic();
        val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF + request.getQueryRequest().getRequestId().toString().replaceAll("-", "_");
        val columns = request.getDestinationEntity().getFields().stream()
                .map(field -> field.getName() + " " + EntityTypeUtil.pgFromDtmType(field)).collect(Collectors.toList());
        val topic = request.getKafkaParameter().getTopic();
        val brokers = request.getKafkaParameter().getBrokers().stream()
                .map(kafkaBrokerInfo -> kafkaBrokerInfo.getAddress()).collect(Collectors.toList());
        val chunkSize = ((DownloadExternalEntityMetadata) request.getKafkaParameter().getDownloadMetadata()).getChunkSize();
        return String.format(CREAT_WRITABLE_EXT_TABLE_SQL, schema, table, String.join(DELIMITER, columns), topic, String.join(DELIMITER, brokers), chunkSize);
    }

    @Override
    public String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql) {
        return String.format(INSERT_INTO_WRITABLE_EXT_TABLE_SQL, schema, table, enrichedSql);
    }

    @Override
    public String dropWritableExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_WRITABLE_EXT_TABLE_SQL, schema, table);
    }
}
