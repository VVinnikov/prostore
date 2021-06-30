package io.arenadata.dtm.query.execution.plugin.adb.query.service.verticle;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service("adbQueryExecutor")
public class AdbQueryExecutorVerticle extends AbstractVerticle implements DatabaseExecutor {
    private static final DeliveryOptions DEFAULT_DELIVERY_OPTIONS = new DeliveryOptions()
            .setSendTimeout(86400000L);

    private final String database;
    private final AdbProperties adbProperties;
    private final SqlTypeConverter typeConverter;
    private final SqlTypeConverter sqlTypeConverter;

    private final Map<String, AdbExecutorTask> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();

    public AdbQueryExecutorVerticle(@Value("${core.env.name}") String database, // Todo transfer to EnvProperties
                                    AdbProperties adbProperties,
                                    @Qualifier("adbTypeToSqlTypeConverter") SqlTypeConverter typeConverter,
                                    @Qualifier("adbTypeFromSqlTypeConverter") SqlTypeConverter sqlTypeConverter) {
        this.database = database;
        this.adbProperties = adbProperties;
        this.typeConverter = typeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(adbProperties.getExecutorsCount());
        vertx.deployVerticle(() -> new AdbQueryExecutorTaskVerticle(database, adbProperties, typeConverter, sqlTypeConverter, taskMap, resultMap),
                deploymentOptions, ar -> {
                    if (ar.succeeded()) {
                        startPromise.complete();
                    } else {
                        startPromise.fail(ar.cause());
                    }
                });
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE_WITH_CURSOR, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters params, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .params(params)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE_WITH_PARAMS, request);
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .build();
            sendRequestWithoutResult(promise, AdbExecutorTopic.EXECUTE_UPDATE, request);
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .preparedStatementRequests(requests)
                    .build();
            sendRequestWithoutResult(promise, AdbExecutorTopic.EXECUTE_IN_TRANSACTION, request);
        });
    }

    private void sendRequestWithResult(Promise<List<Map<String, Object>>> promise, AdbExecutorTopic topic, AdbExecutorTask request) {
        String key = UUID.randomUUID().toString();
        taskMap.put(key, request);
        vertx.eventBus().request(topic.getTopic(), key, DEFAULT_DELIVERY_OPTIONS, ar -> {
            taskMap.remove(key);
            if (ar.succeeded()) {
                promise.handle((AsyncResult<List<Map<String, Object>>>) resultMap.remove(key));
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    private void sendRequestWithoutResult(Promise<Void> promise, AdbExecutorTopic topic, AdbExecutorTask request) {
        String key = UUID.randomUUID().toString();
        taskMap.put(key, request);
        vertx.eventBus().request(topic.getTopic(), key, DEFAULT_DELIVERY_OPTIONS, ar -> {
            taskMap.remove(key);
            if (ar.succeeded()) {
                promise.handle((AsyncResult<Void>) resultMap.remove(key));
            } else {
                promise.fail(ar.cause());
            }
        });
    }
}
