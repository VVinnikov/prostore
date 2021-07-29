package io.arenadata.dtm.query.execution.plugin.adp.db.verticle;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpProperties;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service("adpQueryExecutor")
public class AdpQueryExecutorVerticle extends AbstractVerticle implements DatabaseExecutor {
    private static final DeliveryOptions DEFAULT_DELIVERY_OPTIONS = new DeliveryOptions()
            .setSendTimeout(86400000L);

    private final String database;
    private final AdpProperties adpProperties;
    private final SqlTypeConverter typeConverter;
    private final SqlTypeConverter sqlTypeConverter;

    private final Map<String, AdpExecutorTask> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();

    public AdpQueryExecutorVerticle(@Value("${core.env.name}") String database,
                                    AdpProperties adpProperties,
                                    @Qualifier("adpFromSqlConverter") SqlTypeConverter typeConverter,
                                    @Qualifier("adpToSqlConverter") SqlTypeConverter sqlTypeConverter) {
        this.database = database;
        this.adpProperties = adpProperties;
        this.typeConverter = typeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(adpProperties.getExecutorsCount());
        vertx.deployVerticle(() -> new AdpQueryExecutorTaskVerticle(database, adpProperties, typeConverter, sqlTypeConverter, taskMap, resultMap),
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
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE_WITH_CURSOR, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters params, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .params(params)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE_WITH_PARAMS, request);
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .build();
            sendRequestWithoutResult(promise, AdpExecutorTopic.EXECUTE_UPDATE, request);
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .preparedStatementRequests(requests)
                    .build();
            sendRequestWithoutResult(promise, AdpExecutorTopic.EXECUTE_IN_TRANSACTION, request);
        });
    }

    private void sendRequestWithResult(Promise<List<Map<String, Object>>> promise, AdpExecutorTopic topic, AdpExecutorTask request) {
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

    private void sendRequestWithoutResult(Promise<Void> promise, AdpExecutorTopic topic, AdpExecutorTask request) {
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
