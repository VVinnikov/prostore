package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.service.impl.Constants.*;

@Component("adqmMppwFinishRequestHandler")
@Slf4j
public class MppwFinishRequestHandler implements MppwRequestHandler {
    private static final String DROP_TEMPLATE = "DROP TABLE IF EXISTS %s ON CLUSTER %s";
    private static final String FLUSH_TEMPLATE = "SYSTEM FLUSH DISTRIBUTED %s";
    private static final String OPTIMIZE_TEMPLATE = "OPTIMIZE TABLE %s ON CLUSTER %s FINAL";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s\n" +
            "  SELECT %s, sys_from, %d - 1 AS sys_to, 0 as sys_op, %s AS close_date, arrayJoin(-1, 1) AS sign\n" +
            "  FROM %s\n" +
            "  WHERE bid in (select bid from %s where sys_op <> 1)\n" +
            "    AND sys_from < %d\n" +
            "    AND sys_to > %d\n" +
            "  UNION ALL\n" +
            "  SELECT %s, sys_from, %d - 1 AS sys_to, 1 as sys_op, %s AS close_date, arrayJoin(-1, 1) AS sign\n" +
            "  FROM %s\n" +
            "  WHERE bid in (select bid from %s where sys_op = 1)\n" +
            "    AND sys_from < %d\n" +
            "    AND sys_to > %d";
    private static final String SELECT_COLUMNS_QUERY = "select name from system.columns where database = '%s' and table = '%s'";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    public MppwFinishRequestHandler(final DatabaseExecutor databaseExecutor,
                                    final DdlProperties ddlProperties,
                                    final AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }

    @Override
    public Future<QueryResult> execute(final MppwRequest request) {
        if (request == null) {
            return Future.failedFuture("MppwRequest should not be null");
        }

        QueryLoadParam loadParam = request.getQueryLoadParam();
        if (loadParam == null) {
            return Future.failedFuture("MppwRequest.QueryLoadParam should not be null");
        }

        String tableName = loadParam.getTableName();
        String schema = loadParam.getDatamart();
        if (StringUtils.isEmpty(schema)) {
            schema = appConfiguration.getDefaultDatamart();
        }
        String env = appConfiguration.getSystemName();
        String fullName = env + "__" + schema + "." + tableName;

        Long deltaHot = loadParam.getDeltaHot();

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
                .flatMap(v -> Future.succeededFuture(QueryResult.emptyResult()));
    }

    private <T, E> Future<T> sequenceAll(@NonNull final List<E> actions,
                                         @NonNull final Function<E, Future<T>> action) {
        Future<T> result = null;
        for (E a: actions) {
            if (result == null) {
                result = action.apply(a);
            } else {
                result = result.compose(v -> action.apply(a));
            }
        }

        return result == null ? Future.succeededFuture() : result;
    }

    private Future<Void> dropTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(String.format(DROP_TEMPLATE, table, ddlProperties.getCluster()));
    }

    private Future<Void> flushTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(String.format(FLUSH_TEMPLATE, table));
    }

    private Future<Void> closeActual(@NonNull String table, long deltaHot) {
        LocalDateTime ldt = LocalDateTime.now();
        String now = ldt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return fetchColumnNames(table + ACTUAL_POSTFIX).compose(columnNames ->
                databaseExecutor.executeUpdate(
                        String.format(INSERT_TEMPLATE,
                                table + ACTUAL_POSTFIX,
                                columnNames,
                                deltaHot,
                                now,
                                table + ACTUAL_POSTFIX,
                                table + BUFFER_SHARD_POSTFIX,
                                deltaHot,
                                deltaHot,
                                columnNames,
                                deltaHot,
                                now,
                                table + ACTUAL_POSTFIX,
                                table + BUFFER_SHARD_POSTFIX,
                                deltaHot,
                                deltaHot)));
    }

    private Future<Void> optimizeTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(String.format(OPTIMIZE_TEMPLATE, table, ddlProperties.getCluster()));
    }

    private Future<List<String>> fetchColumnNames(@NonNull String table) {
        String[] parts = table.split("\\.");
        if (parts.length != 2) {
            return Future.failedFuture(String.format("Incorrect table name, cannot split to schema.table: %s", table));
        }

        String query = String.format(SELECT_COLUMNS_QUERY, parts[0], parts[1]);

        Promise<List<String>> promise = Promise.promise();
        databaseExecutor.execute(query, ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }
            promise.complete(getColumnNames(ar.result()));
        });
        return promise.future();
    }

    private List<String> getColumnNames(@NonNull JsonArray result) {
        @SuppressWarnings("unchecked")
        List<JsonObject> rows = result.getList();
        return rows
                .stream()
                .map(o -> o.getString("name"))
                .filter(f -> !SYSTEM_FIELDS.contains(f))
                .collect(Collectors.toList());
    }
}
