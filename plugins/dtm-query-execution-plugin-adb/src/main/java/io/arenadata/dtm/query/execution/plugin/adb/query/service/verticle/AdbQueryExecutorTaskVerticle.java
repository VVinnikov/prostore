package io.arenadata.dtm.query.execution.plugin.adb.query.service.verticle;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;

import java.util.Map;

public class AdbQueryExecutorTaskVerticle extends AbstractVerticle {
    private final String database;
    private final AdbProperties adbProperties;
    private final SqlTypeConverter typeConverter;
    private final SqlTypeConverter sqlTypeConverter;
    private final Map<String, AdbExecutorTask> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;
    private AdbQueryExecutor adbQueryExecutor;

    public AdbQueryExecutorTaskVerticle(String database,
                                        AdbProperties adbProperties,
                                        SqlTypeConverter typeConverter,
                                        SqlTypeConverter sqlTypeConverter,
                                        Map<String, AdbExecutorTask> taskMap,
                                        Map<String, AsyncResult<?>> resultMap) {
        this.database = database;
        this.adbProperties = adbProperties;
        this.typeConverter = typeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        PgPoolOptions poolOptions = new PgPoolOptions();
        poolOptions.setDatabase(database);
        poolOptions.setHost(adbProperties.getHost());
        poolOptions.setPort(adbProperties.getPort());
        poolOptions.setUser(adbProperties.getUser());
        poolOptions.setPassword(adbProperties.getPassword());
        poolOptions.setMaxSize(adbProperties.getPoolSize());
        PgPool pool = PgClient.pool(vertx, poolOptions);
        adbQueryExecutor = new AdbQueryExecutor(pool, adbProperties.getFetchSize(), typeConverter, sqlTypeConverter);

        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE.getTopic(), this::executeHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_WITH_CURSOR.getTopic(), this::executeWithCursorHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_WITH_PARAMS.getTopic(), this::executeWithParamsHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_UPDATE.getTopic(), this::executeUpdateHandler);
        vertx.eventBus().consumer(AdbExecutorTopic.EXECUTE_IN_TRANSACTION.getTopic(), this::executeInTransactionHandler);
    }

    private void executeHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.execute(adbExecutorTask.getSql(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });
    }

    private void executeWithCursorHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeWithCursor(adbExecutorTask.getSql(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeWithParamsHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeWithParams(adbExecutorTask.getSql(), adbExecutorTask.getParams(), adbExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeUpdateHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeUpdate(adbExecutorTask.getSql())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeInTransactionHandler(Message<String> message) {
        String key = message.body();
        AdbExecutorTask adbExecutorTask = taskMap.get(key);
        adbQueryExecutor.executeInTransaction(adbExecutorTask.getPreparedStatementRequests())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }
}
