package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;
import ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.*;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils.*;

@Component("adqmMppwStartRequestHandler")
@Slf4j
public class MppwStartRequestHandler implements MppwRequestHandler {
    private static final String QUERY_TABLE_SETTINGS = "select %s from system.tables where database = '%s' and name = '%s'";
    private static final String KAFKA_ENGINE_TEMPLATE = "ENGINE = Kafka()\n" +
            "  SETTINGS\n" +
            "    kafka_broker_list = '%s',\n" +
            "    kafka_topic_list = '%s',\n" +
            "    kafka_group_name = '%s',\n" +
            "    kafka_format = '%s'";
    private static final String EXT_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (\n" +
            "  %s\n" +
            ")\n" +
            "%s\n";
    private static final String BUFFER_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (%s, sys_op_buffer Nullable(Int8)) ENGINE = Join(ANY, INNER, %s)";
    private static final String BUFFER_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s AS %s ENGINE=%s";
    private static final String BUFFER_LOADER_TEMPLATE = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER %s TO %s\n" +
            "  AS SELECT %s FROM %s";
    private static final String ACTUAL_LOADER_TEMPLATE = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER %s TO %s\n" +
            "AS SELECT %s, %d AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op, '9999-12-31 00:00:00' as close_date, 1 AS sign " +
            " FROM %s WHERE sys_op <> 1";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;
    private final MppwProperties mppwProperties;

    public MppwStartRequestHandler(final DatabaseExecutor databaseExecutor,
                                   final DdlProperties ddlProperties,
                                   final AppConfiguration appConfiguration,
                                   final MppwProperties mppwProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        val err = DdlUtils.validateRequest(request);
        if (err.isPresent()) {
            return Future.failedFuture(err.get());
        }

        String fullName = DdlUtils.getQualifiedTableName(request, appConfiguration);

        // 1. Determine table engine (_actual_shard)
        Future<String> engineFull = getTableSetting(fullName + ACTUAL_POSTFIX, "engine_full");
        // 2. Get sorting order (_actual)
        Future<String> sortingKey = getTableSetting(fullName + ACTUAL_SHARD_POSTFIX, "sorting_key");

        // 3. Create _ext_shard based on schema from request
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(request.getSchema().encode());
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        String kafkaSettings = genKafkaEngine(request, fullName);
        Future<Void> extTableF = createExternalTable(fullName + EXT_SHARD_POSTFIX, schema, kafkaSettings);

        // 4. Create _buffer_shard
        Future<Void> buffShardF = sortingKey.compose(keys ->
                createBufferShardTable(fullName + BUFFER_SHARD_POSTFIX, keys, schema));

        // 5. Create _buffer
        Future<Void> buffF = CompositeFuture.all(engineFull, buffShardF).compose(r ->
                createBufferTable(fullName + BUFFER_POSTFIX, r.resultAt(0)));

        // 6. Create _buffer_loader_shard
        Future<Void> buffLoaderF = CompositeFuture.all(sortingKey, extTableF, buffF).compose(r ->
                createBufferLoaderTable(fullName + BUFFER_LOADER_SHARD_POSTFIX, r.resultAt(0)));

        // 7. Create _actual_loader_shard
        Future<Void> actualLoaderF = extTableF.compose(v ->
                createActualLoaderTable(fullName + ACTUAL_LOADER_SHARD_POSTFIX, schema, request.getQueryLoadParam().getDeltaHot()));

        return CompositeFuture.all(extTableF, buffShardF, buffF, buffLoaderF, actualLoaderF)
                .flatMap(v -> Future.succeededFuture(QueryResult.emptyResult()));
    }

    private Future<String> getTableSetting(@NonNull String table, @NonNull String settingKey) {
        val nameParts = splitQualifiedTableName(table);
        if (!nameParts.isPresent()) {
            return Future.failedFuture(format("Cannot parse table name %s", table));
        }

        Promise<String> result = Promise.promise();

        String query = format(QUERY_TABLE_SETTINGS, settingKey, nameParts.get().getLeft(), nameParts.get().getRight());
        databaseExecutor.execute(query, ar -> {
            if (ar.failed()) {
                result.fail(ar.cause());
                return;
            }

            @SuppressWarnings("unchecked")
            List<JsonObject> rows = ar.result().getList();
            if (rows.size() == 0) {
                result.fail(format("Cannot find %s for %s", settingKey, table));
                return;
            }

            result.complete(rows.get(0).getString(settingKey));
        });
        return result.future();
    }

    private Optional<DatamartTable> findTableSchema(@NonNull String table, @NonNull final JsonObject schema) {
        try {
            Datamart dm = schema.mapTo(Datamart.class);
            return dm.getDatamartTables().stream()
                    .filter(c -> c.getMnemonic().equalsIgnoreCase(table))
                    .findFirst();
        } catch (DecodeException e) {
            log.error("Cannot decode schema to Datamart", e);
            return Optional.empty();
        }
    }

    private String genKafkaEngine(@NonNull final MppwRequest request, @NonNull String tableName) {
        String brokers = mppwProperties.getKafkaBrokers();
        String topic = request.getTopic();
        String consumerGroup = mppwProperties.getConsumerGroup() + tableName;
        // FIXME Support other formats (Text, CSV, Json?)
        String format = "Avro";
        return format(KAFKA_ENGINE_TEMPLATE, brokers, topic, consumerGroup, format);
    }

    private Future<Void> createExternalTable(@NonNull String table,
                                             @NonNull Schema schema,
                                             @NonNull String kafkaSettings) {
        String columns = schema.getFields().stream()
                .map(DdlUtils::avroFieldToString)
                .collect(Collectors.joining(", "));
        String query = format(EXT_SHARD_TEMPLATE, table, ddlProperties.getCluster(), columns, kafkaSettings);
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferShardTable(@NonNull String tableName,
                                                @NonNull String columns,
                                                @NonNull Schema schema) {
        String[] cols = columns.split(",\\s*");
        String colString = Arrays.stream(cols)
                .filter(c -> !c.equalsIgnoreCase("sys_from"))
                .map(c -> format("%s %s", c, findTypeForColumn(c, schema)))
                .collect(Collectors.joining(", "));

        String joinString = Arrays.stream(cols)
                .filter(c -> !c.equalsIgnoreCase("sys_from"))
                .collect(Collectors.joining(", "));

        String query = format(BUFFER_SHARD_TEMPLATE, tableName, ddlProperties.getCluster(), colString,
                joinString);
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferTable(@NonNull String tableName, @NonNull String engine) {
        String query = format(BUFFER_TEMPLATE, tableName, ddlProperties.getCluster(),
                tableName.replaceAll(BUFFER_POSTFIX, BUFFER_SHARD_POSTFIX),
                engine.replaceAll(ACTUAL_SHARD_POSTFIX, BUFFER_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferLoaderTable(@NonNull String table, @NonNull String columns) {
        String query = format(BUFFER_LOADER_TEMPLATE, table, ddlProperties.getCluster(),
                table.replaceAll(BUFFER_LOADER_SHARD_POSTFIX, BUFFER_POSTFIX),
                columns.replaceAll("sys_from", "sys_op AS sys_op_buffer"),
                table.replaceAll(BUFFER_LOADER_SHARD_POSTFIX, EXT_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createActualLoaderTable(@NonNull String table,
                                                 @NonNull Schema schema,
                                                 long deltaHot) {
        String columns = schema.getFields().stream().map(Schema.Field::name)
                .filter(c -> !c.equalsIgnoreCase("sys_op")).collect(Collectors.joining(", "));

        String query = format(ACTUAL_LOADER_TEMPLATE, table, ddlProperties.getCluster(),
                table.replaceAll(ACTUAL_LOADER_SHARD_POSTFIX, ACTUAL_POSTFIX),
                columns, deltaHot,
                table.replaceAll(ACTUAL_LOADER_SHARD_POSTFIX, EXT_SHARD_POSTFIX));
        return databaseExecutor.executeUpdate(query);
    }

    private String findTypeForColumn(@NonNull String columnName, @NonNull Schema schema) {
        // Sub-optimal find via full scan of schema
        val field = schema.getFields().stream().filter(a -> a.name().equalsIgnoreCase(columnName)).findFirst();
        return field.map(f -> avroTypeToNative(f.schema())).orElse("Int64");
    }
}
