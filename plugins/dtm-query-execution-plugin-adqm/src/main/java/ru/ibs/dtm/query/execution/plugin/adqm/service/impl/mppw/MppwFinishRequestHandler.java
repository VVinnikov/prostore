package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.common.Constants;
import ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.StatusReporter;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.*;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils.sequenceAll;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils.splitQualifiedTableName;

@Component("adqmMppwFinishRequestHandler")
@Slf4j
public class MppwFinishRequestHandler implements MppwRequestHandler {
    private static final String QUERY_TABLE_SETTINGS = "select %s from system.tables where database = '%s' and name = '%s'";
    private static final String DROP_TEMPLATE = "DROP TABLE IF EXISTS %s ON CLUSTER %s";
    private static final String FLUSH_TEMPLATE = "SYSTEM FLUSH DISTRIBUTED %s";
    private static final String OPTIMIZE_TEMPLATE = "OPTIMIZE TABLE %s ON CLUSTER %s FINAL";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s\n" +
            "  SELECT %s, a.sys_from, %d - 1 AS sys_to, b.sys_op_buffer as sys_op, '%s' AS close_date, arrayJoin([-1, 1]) AS sign\n" +
            "  FROM %s a\n" +
            "  ANY INNER JOIN %s b USING(%s)\n" +
            "  WHERE sys_from < %d\n" +
            "    AND sys_to > %d";
    private static final String SELECT_COLUMNS_QUERY = "select name from system.columns where database = '%s' and table = '%s'";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;
    private final StatusReporter statusReporter;

    public MppwFinishRequestHandler(final DatabaseExecutor databaseExecutor,
                                    final DdlProperties ddlProperties,
                                    final AppConfiguration appConfiguration, StatusReporter statusReporter) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
        this.statusReporter = statusReporter;
    }

    @Override
    public Future<QueryResult> execute(final MppwRequest request) {
        val err = DdlUtils.validateRequest(request);
        if (err.isPresent()) {
            return Future.failedFuture(err.get());
        }

        String fullName = DdlUtils.getQualifiedTableName(request, appConfiguration);
        Long deltaHot = request.getQueryLoadParam().getDeltaHot();

        return sequenceAll(Arrays.asList(  // 1. drop shard tables
                fullName + EXT_SHARD_POSTFIX,
                fullName + ACTUAL_LOADER_SHARD_POSTFIX,
                fullName + BUFFER_LOADER_SHARD_POSTFIX
        ), this::dropTable)
                .compose(v -> sequenceAll(Arrays.asList( // 2. flush distributed tables
                        fullName + BUFFER_POSTFIX,
                        fullName + ACTUAL_POSTFIX), this::flushTable))
                .compose(v -> closeActual(fullName, deltaHot))  // 3. insert refreshed records
                .compose(v -> flushTable(fullName + ACTUAL_POSTFIX))  // 4. flush actual table
                .compose(v -> sequenceAll(Arrays.asList(  // 5. drop buffer tables
                        fullName + BUFFER_POSTFIX,
                        fullName + BUFFER_SHARD_POSTFIX), this::dropTable))
                .compose(v -> optimizeTable(fullName + ACTUAL_SHARD_POSTFIX))  // 6. merge shards
                .compose(v -> {
                    reportFinish(request.getTopic());
                    return Future.succeededFuture(QueryResult.emptyResult());
                }, f -> {
                    reportError(request.getTopic());
                    return Future.failedFuture(f.getCause());
                });
    }

    private Future<Void> dropTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(format(DROP_TEMPLATE, table, ddlProperties.getCluster()));
    }

    private Future<Void> flushTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(format(FLUSH_TEMPLATE, table));
    }

    private Future<Void> closeActual(@NonNull String table, long deltaHot) {
        LocalDateTime ldt = LocalDateTime.now();
        String now = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Future<String> columnNames = fetchColumnNames(table + ACTUAL_POSTFIX);
        Future<String> sortingKey = fetchSortingKey(table + ACTUAL_SHARD_POSTFIX);

        return CompositeFuture.all(columnNames, sortingKey).compose(r ->
                databaseExecutor.executeUpdate(
                        format(INSERT_TEMPLATE,
                                table + ACTUAL_POSTFIX,
                                r.resultAt(0),
                                deltaHot,
                                now,
                                table + ACTUAL_POSTFIX,
                                table + BUFFER_SHARD_POSTFIX,
                                r.resultAt(1),
                                deltaHot,
                                deltaHot)));
    }

    private Future<Void> optimizeTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(format(OPTIMIZE_TEMPLATE, table, ddlProperties.getCluster()));
    }

    private Future<String> fetchColumnNames(@NonNull String table) {
        val parts = splitQualifiedTableName(table);
        if (!parts.isPresent()) {
            return Future.failedFuture(format("Incorrect table name, cannot split to schema.table: %s", table));
        }

        String query = format(SELECT_COLUMNS_QUERY, parts.get().getLeft(), parts.get().getRight());

        Promise<String> promise = Promise.promise();
        databaseExecutor.execute(query, ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }
            promise.complete(getColumnNames(ar.result()));
        });
        return promise.future();
    }

    private Future<String> fetchSortingKey(@NonNull String table) {
        val parts = splitQualifiedTableName(table);
        if (!parts.isPresent()) {
            return Future.failedFuture(format("Incorrect table name, cannot split to schema.table: %s", table));
        }

        String query = format(QUERY_TABLE_SETTINGS, "sorting_key", parts.get().getLeft(), parts.get().getRight());

        Promise<String> promise = Promise.promise();
        databaseExecutor.execute(query, ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }

            @SuppressWarnings("unchecked")
            List<JsonObject> rows = ar.result().getList();
            if (rows.size() == 0) {
                promise.fail(format("Cannot find sorting_key for %s", table));
                return;
            }

            String sortingKey = rows.get(0).getString("sorting_key");
            String withoutSysFrom = Arrays.stream(sortingKey.split(",\\s*"))
                    .filter(c -> !c.equalsIgnoreCase(SYS_FROM_FIELD))
                    .collect(Collectors.joining(", "));

            promise.complete(withoutSysFrom);
        });
        return promise.future();
    }

    private String getColumnNames(@NonNull JsonArray result) {
        @SuppressWarnings("unchecked")
        List<JsonObject> rows = result.getList();
        return rows
                .stream()
                .map(o -> o.getString("name"))
                .filter(f -> !SYSTEM_FIELDS.contains(f))
                .map(n -> "a." + n)
                .collect(Collectors.joining(", "));
    }

    private void reportFinish(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onFinish(start);
    }

    private void reportError(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onError(start);
    }
}
