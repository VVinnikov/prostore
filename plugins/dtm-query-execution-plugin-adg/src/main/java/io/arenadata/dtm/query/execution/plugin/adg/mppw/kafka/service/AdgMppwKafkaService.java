package io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.service;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.configuration.properties.AdgMppwKafkaProperties;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.dto.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.factory.AdgMppwKafkaContextFactory;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.callback.function.TtTransferDataScdCallbackFunction;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.callback.params.TtTransferDataScdCallbackParameter;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request.AdgSubscriptionKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.mppw.AdgMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service("adgMppwKafkaService")
public class AdgMppwKafkaService implements AdgMppwExecutor {

    private final AdgMppwKafkaContextFactory contextFactory;
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwKafkaProperties properties;
    private final AdgCartridgeClient cartridgeClient;

    @Autowired
    public AdgMppwKafkaService(AdgMppwKafkaContextFactory contextFactory,
                               AdgCartridgeClient cartridgeClient,
                               AdgMppwKafkaProperties properties) {
        this.contextFactory = contextFactory;
        this.cartridgeClient = cartridgeClient;
        this.properties = properties;
        initializedLoadingByTopic = new ConcurrentHashMap<>();
    }

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        return Future.future(promise -> {
            log.debug("mppw start");
            if (request.getUploadMetadata().getFormat() != ExternalTableFormat.AVRO) {
                promise.fail(new MppwDatasourceException(String.format("Format %s not implemented",
                        request.getUploadMetadata().getFormat())));
            }
            val mppwKafkaContext = contextFactory.create((MppwKafkaRequest) request);
            if (request.getIsLoadStart()) {
                initializeLoading(mppwKafkaContext, request.getSourceEntity().getExternalTableUploadMessageLimit())
                        .onComplete(promise);
            } else {
                cancelLoadData(mppwKafkaContext)
                        .onComplete(promise);
            }
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private Future<QueryResult> initializeLoading(AdgMppwKafkaContext ctx, Integer externalTableUploadMessageLimit) {
        if (initializedLoadingByTopic.containsKey(ctx.getTopicName())) {
            return transferData(ctx);
        } else {
            Long maxNumberOfMessages = Optional.ofNullable(externalTableUploadMessageLimit)
                    .map(Integer::longValue)
                    .orElse(properties.getMaxNumberOfMessagesPerPartition());
            return Future.future(promise -> {
                val callbackFunctionParameter = new TtTransferDataScdCallbackParameter(
                        ctx.getHelperTableNames().getStaging(),
                        ctx.getHelperTableNames().getStaging(),
                        ctx.getHelperTableNames().getActual(),
                        ctx.getHelperTableNames().getHistory(),
                        ctx.getHotDelta());

                val callbackFunction = new TtTransferDataScdCallbackFunction(
                        properties.getCallbackFunctionName(),
                        callbackFunctionParameter,
                        maxNumberOfMessages,
                        properties.getCallbackFunctionSecIdle());

                val request = new AdgSubscriptionKafkaRequest(
                        maxNumberOfMessages,
                        null,
                        ctx.getTopicName(),
                        Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                        callbackFunction);

                cartridgeClient.subscribe(request)
                        .onSuccess(result -> {
                            log.debug("Loading initialize completed by [{}]", request);
                            initializedLoadingByTopic.put(ctx.getTopicName(), ctx.getConsumerTableName());
                            promise.complete(QueryResult.emptyResult());
                        })
                        .onFailure(promise::fail);
            });
        }
    }

    private Future<QueryResult> cancelLoadData(AdgMppwKafkaContext ctx) {
        return Future.future(promise -> {
            val topicName = ctx.getTopicName();
            transferData(ctx)
                    .compose(result -> cartridgeClient.cancelSubscription(topicName))
                    .onSuccess(result -> {
                        initializedLoadingByTopic.remove(topicName);
                        log.debug("Cancel Load Data completed by request [{}]", topicName);
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<QueryResult> transferData(AdgMppwKafkaContext ctx) {
        return Future.future(promise -> {
            val request = new AdgTransferDataEtlRequest(ctx.getHelperTableNames(), ctx.getHotDelta());
            cartridgeClient.transferDataToScdTable(request)
                    .onSuccess(result -> {
                                log.debug("Transfer Data completed by request [{}]", request);
                                promise.complete(QueryResult.emptyResult());
                            }
                    )
                    .onFailure(promise::fail);
        });
    }
}
