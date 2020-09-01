package ru.ibs.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgMppwKafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import ru.ibs.dtm.query.execution.plugin.adg.model.callback.function.TtTransferDataScdCallbackFunction;
import ru.ibs.dtm.query.execution.plugin.adg.model.callback.params.TtTransferDataScdCallbackParameter;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.TtLoadDataKafkaRequest;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.TtSubscriptionKafkaRequest;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.TtTransferDataEtlRequest;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service("adgMppwKafkaService")
public class AdgMppwKafkaService implements MppwKafkaService<QueryResult> {

    private final AdgMppwKafkaContextFactory contextFactory;
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwKafkaProperties properties;
    private final TtCartridgeClient cartridgeClient;

    public AdgMppwKafkaService(AdgMppwKafkaContextFactory contextFactory,
                               TtCartridgeClient cartridgeClient,
                               AdgMppwKafkaProperties properties) {
        this.contextFactory = contextFactory;
        this.cartridgeClient = cartridgeClient;
        this.properties = properties;
        initializedLoadingByTopic = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        log.debug("mppw start");
        val mppwKafkaContext = contextFactory.create(context.getRequest());
        if (context.getRequest().getLoadStart()) {
            initializeLoading(mppwKafkaContext, asyncResultHandler);
        } else {
            cancelLoadData(mppwKafkaContext, asyncResultHandler);
        }
    }

    private void initializeLoading(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        if (initializedLoadingByTopic.containsKey(ctx.getTopicName())) {
            val expectedTableName = initializedLoadingByTopic.get(ctx.getTopicName());
            if (expectedTableName.equals(ctx.getConsumerTableName())) {
                //loadData(ctx, handler);
                transferData(ctx, handler);
            } else {
                val msg = String.format(
                        "Tables must be the same within a single load by topic [%s]. Actual [%s], but expected [%s]"
                        , ctx.getTopicName()
                        , ctx.getConsumerTableName()
                        , expectedTableName);
                log.error(msg);
                handler.handle(Future.failedFuture(msg));
            }
        } else {
            val callbackFunctionParameter = new TtTransferDataScdCallbackParameter(
                    ctx.getHelperTableNames().getStaging(),
                    ctx.getHelperTableNames().getStaging(),
                    ctx.getHelperTableNames().getActual(),
                    ctx.getHelperTableNames().getHistory(),
                    ctx.getHotDelta()
            );
            val callbackFunction = new TtTransferDataScdCallbackFunction(
                    properties.getCallbackFunctionName(),
                    callbackFunctionParameter,
                    properties.getMaxNumberOfMessagesPerPartition(),
                    properties.getCallbackFunctionSecIdle()
            );


            val request = new TtSubscriptionKafkaRequest(
                    properties.getMaxNumberOfMessagesPerPartition(),
                    null,
                    ctx.getTopicName(),
                    Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                    callbackFunction
            );
            cartridgeClient.subscribe(request, ar -> {
                if (ar.succeeded()) {
                    log.debug("Loading initialize completed by [{}]", request);
                    initializedLoadingByTopic.put(ctx.getTopicName(), ctx.getConsumerTableName());
                    handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                } else {
                    log.error("Loading initialize error:", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }

    private void loadData(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val request = new TtLoadDataKafkaRequest(
                properties.getMaxNumberOfMessagesPerPartition(),
                Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                null,
                ctx.getTopicName()
        );
        cartridgeClient.loadData(
                request, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Load Data completed by request [{}] with result: [{}]", request, ar.result());
                        transferData(ctx, handler);
                    } else {
                        log.error("Load Data error:", ar.cause());
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void transferData(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val request = new TtTransferDataEtlRequest(ctx.getHelperTableNames(), ctx.getHotDelta());
        cartridgeClient.transferDataToScdTable(
                request, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Transfer Data completed by request [{}]", request);
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        log.error("Transfer Data error: ", ar.cause());
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                }
        );
    }

    private void cancelLoadData(AdgMppwKafkaContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val topicName = ctx.getTopicName();
        transferData(ctx, tr -> {
            cartridgeClient.cancelSubscription(topicName, ar -> {
                initializedLoadingByTopic.remove(topicName);
                if (ar.succeeded()) {
                    log.debug("Cancel Load Data completed by request [{}]", topicName);
                    handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                } else {
                    log.error("Cancel Load Data error: ", ar.cause());
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        });
    }
}
