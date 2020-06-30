package ru.ibs.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgMppwKafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgLoadDataConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgSubscriptionConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgTransferDataConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.AdgConnectorApi;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class AdgMppwKafkaService {
    private final AdgMppwKafkaContextFactory contextFactory;
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwKafkaProperties properties;
    private final AdgConnectorApi connectorApi;

    public AdgMppwKafkaService(AdgMppwKafkaContextFactory contextFactory,
                               AdgConnectorApi connectorApi,
                               AdgMppwKafkaProperties properties) {
        this.contextFactory = contextFactory;
        this.connectorApi = connectorApi;
        this.properties = properties;
        initializedLoadingByTopic = new ConcurrentHashMap<>();
    }

    private void initializeLoading(AdgMppwKafkaContext ctx, Handler<AsyncResult<Void>> handler) {
        if (initializedLoadingByTopic.containsKey(ctx.getTopicName())) {
            val expectedTableName = initializedLoadingByTopic.get(ctx.getTopicName());
            if (expectedTableName.equals(ctx.getConsumerTableName())) {
                loadData(ctx, handler);
            } else {
                val msg = String.format(
                        "Tables must be the same within a single load by topic [%s]. Actual [%s], but expected [%s]"
                        , ctx.getConsumerTableName()
                        , expectedTableName
                        , ctx.getTopicName());
                log.error(msg);
                handler.handle(Future.failedFuture(msg));
            }
        } else {
            val request = new AdgSubscriptionConnectorRequest(
                    properties.getMaxNumberOfMessagesPerPartition(),
                    ctx.getSchema(),
                    ctx.getTopicName()
            );
            connectorApi.subscribe(request, ar -> {
                if (ar.succeeded()) {
                    log.debug("Loading initialize completed by [{}]", request);
                    initializedLoadingByTopic.put(ctx.getTopicName(), ctx.getConsumerTableName());
                    loadData(ctx, handler);
                } else {
                    log.error("Loading initialize error:", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }

    private void loadData(AdgMppwKafkaContext ctx, Handler<AsyncResult<Void>> handler) {
        val request = new AdgLoadDataConnectorRequest(
                properties.getMaxNumberOfMessagesPerPartition(),
                Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                ctx.getSchema(),
                ctx.getTopicName()
        );
        connectorApi.loadData(
                request, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Load Data completed by [{}]", request);
                        transferData(ctx, handler);
                    } else {
                        log.error("Load Data error:", ar.cause());
                        handler.handle(Future.failedFuture(ar.cause()));
                        cancelLoadData(ctx, ar2 -> {
                            if (ar2.failed()) {
                                log.error("Cancel error", ar.cause());
                            }
                        });
                    }
                });
    }

    private void transferData(AdgMppwKafkaContext ctx, Handler<AsyncResult<Void>> handler) {
        val request = new AdgTransferDataConnectorRequest(ctx.getHelperTableNames(), ctx.getHotDelta());
        connectorApi.transferDataToScdTable(
                request, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Transfer Data completed by [{}]", request);
                        handler.handle(Future.succeededFuture());
                    } else {
                        log.error("Transfer Data error: ", ar.cause());
                        handler.handle(Future.failedFuture(ar.cause()));
                        cancelLoadData(ctx, ar2 -> {
                            if (ar2.failed()) {
                                log.error("Cancel error", ar.cause());
                            }
                        });
                    }
                }
        );
    }

    private void cancelLoadData(AdgMppwKafkaContext ctx, Handler<AsyncResult<Void>> handler) {
        val topicName = ctx.getTopicName();
        connectorApi.cancelSubscription(topicName, ar -> {
            if (ar.succeeded()) {
                log.debug("Cancel Load Data completed by [{}]", topicName);
                initializedLoadingByTopic.remove(topicName);
                handler.handle(Future.succeededFuture());
            } else {
                log.error("Cancel Load Data error: ", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
