package io.arenadata.dtm.query.execution.plugin.adp.db.verticle;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpProperties;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.AdpQueryExecutor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

import java.util.Map;

public class AdpQueryExecutorTaskVerticle extends AbstractVerticle {
    private final String database;
    private final AdpProperties adpProperties;
    private final SqlTypeConverter fromSqlConverter;
    private final SqlTypeConverter toSqlConverter;
    private final Map<String, AdpExecutorTask> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;
    private AdpQueryExecutor adpQueryExecutor;

    public AdpQueryExecutorTaskVerticle(String database,
                                        AdpProperties adpProperties,
                                        SqlTypeConverter fromSqlConverter,
                                        SqlTypeConverter toSqlConverter,
                                        Map<String, AdpExecutorTask> taskMap,
                                        Map<String, AsyncResult<?>> resultMap) {
        this.database = database;
        this.adpProperties = adpProperties;
        this.fromSqlConverter = fromSqlConverter;
        this.toSqlConverter = toSqlConverter;
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        PgConnectOptions pgConnectOptions = new PgConnectOptions();
        pgConnectOptions.setDatabase(database);
        pgConnectOptions.setHost(adpProperties.getHost());
        pgConnectOptions.setPort(adpProperties.getPort());
        pgConnectOptions.setUser(adpProperties.getUser());
        pgConnectOptions.setPassword(adpProperties.getPassword());
        pgConnectOptions.setPreparedStatementCacheMaxSize(adpProperties.getPreparedStatementsCacheMaxSize());
        pgConnectOptions.setPreparedStatementCacheSqlLimit(adpProperties.getPreparedStatementsCacheSqlLimit());
        pgConnectOptions.setCachePreparedStatements(adpProperties.isPreparedStatementsCache());
        pgConnectOptions.setPipeliningLimit(1);
        PoolOptions poolOptions = new PoolOptions();
        poolOptions.setMaxSize(adpProperties.getPoolSize());
        PgPool pool = PgPool.pool(vertx, pgConnectOptions, poolOptions);
        adpQueryExecutor = new AdpQueryExecutor(pool, adpProperties.getFetchSize(), fromSqlConverter, toSqlConverter);

        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE.getTopic(), this::executeHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_WITH_CURSOR.getTopic(), this::executeWithCursorHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_WITH_PARAMS.getTopic(), this::executeWithParamsHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_UPDATE.getTopic(), this::executeUpdateHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_IN_TRANSACTION.getTopic(), this::executeInTransactionHandler);
    }

    private void executeHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.execute(adpExecutorTask.getSql(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });
    }

    private void executeWithCursorHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeWithCursor(adpExecutorTask.getSql(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeWithParamsHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeWithParams(adpExecutorTask.getSql(), adpExecutorTask.getParams(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeUpdateHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeUpdate(adpExecutorTask.getSql())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeInTransactionHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeInTransaction(adpExecutorTask.getPreparedStatementRequests())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }
}
