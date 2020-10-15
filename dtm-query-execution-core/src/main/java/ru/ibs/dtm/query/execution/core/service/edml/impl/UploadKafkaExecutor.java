package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.*;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UploadKafkaExecutor implements EdmlUploadExecutor {

    public static final String MPPW_LOAD_ERROR_MESSAGE = "Runtime error mppw download!";
    private final DataSourcePluginService pluginService;
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory;
    private final EdmlProperties edmlProperties;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;

    @Autowired
    public UploadKafkaExecutor(DataSourcePluginService pluginService, MppwKafkaRequestFactory mppwKafkaRequestFactory,
                               EdmlProperties edmlProperties, KafkaProperties kafkaProperties, @Qualifier("coreVertx") Vertx vertx) {
        this.pluginService = pluginService;
        this.mppwKafkaRequestFactory = mppwKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        try {
            final Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap = new HashMap<>();
            pluginService.getSourceTypes().forEach(ds -> {
                final MppwRequestContext mppwRequestContext = mppwKafkaRequestFactory.create(context);
                startMppwFutureMap.put(ds, startMppw(ds, mppwRequestContext, context));
            });
            checkPluginsMppwExecution(startMppwFutureMap, resultHandler);
        } catch (Exception e) {
            log.error("Error starting mppw download!", e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<MppwStopFuture> startMppw(SourceType ds, MppwRequestContext mppwRequestContext, EdmlRequestContext context) {
        return Future.future((Promise<MppwStopFuture> promise) -> pluginService.mppwKafka(ds, mppwRequestContext, ar -> {
            if (ar.succeeded()) {
                log.debug("A request has been sent for the plugin: {} to start mppw download: {}", ds, mppwRequestContext.getRequest());
                val statusRequestContext = createStatusRequestContext(mppwRequestContext, context);
                val mppwLoadStatusResult = MppwLoadStatusResult.builder()
                        .lastOffsetTime(LocalDateTime.now())
                        .lastOffset(0L)
                        .build();
                vertx.setPeriodic(edmlProperties.getPluginStatusCheckPeriodMs(), timerId -> {
                    log.trace("Plugin status request: {} mppw downloads", ds);
                    getMppwLoadingStatus(ds, statusRequestContext)
                            .onComplete(chr -> {
                                if (chr.succeeded()) {
                                    //todo: Add error checking (try catch and so on)
                                    StatusQueryResult result = chr.result();
                                    updateMppwLoadStatus(mppwLoadStatusResult, result);
                                    if (isMppwLoadedSuccess(result)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .offset(result.getPartitionInfo().getOffset())
                                                .stopReason(MppwStopReason.OFFSET_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    } else if (isMppwLoadingInitFailure(mppwLoadStatusResult)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .cause(new RuntimeException(String.format("Plugin %s consumer failed to start", ds)))
                                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    } else if (isLastOffsetNotIncrease(mppwLoadStatusResult)) {
                                        vertx.cancelTimer(timerId);
                                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                                .sourceType(ds)
                                                .future(stopMppw(ds, mppwRequestContext))
                                                .cause(new RuntimeException(String.format("Plugin %s consumer offset stopped dead", ds)))
                                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                                .build();
                                        promise.complete(stopFuture);
                                    }
                                } else {
                                    log.error("Error getting plugin status: {}", ds, chr.cause());
                                    vertx.cancelTimer(timerId);
                                    promise.fail(chr.cause());
                                }
                            });
                });
            } else {
                log.error("Error starting loading mppw for plugin: {}", ds, ar.cause());
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(ds)
                        .future(stopMppw(ds, mppwRequestContext))
                        .cause(ar.cause())
                        .stopReason(MppwStopReason.ERROR_RECEIVED)
                        .build();
                promise.complete(stopFuture);
            }
        }));
    }

    private Future<StatusQueryResult> getMppwLoadingStatus(SourceType ds, StatusRequestContext statusRequestContext) {
        return Future.future((Promise<StatusQueryResult> promise) -> pluginService.status(ds, statusRequestContext, ar -> {
            if (ar.succeeded()) {
                StatusQueryResult queryResult = ar.result();
                log.trace("Plugin status received: {} mppw downloads: {}, on request: {}", ds, queryResult, statusRequestContext);
                promise.complete(queryResult);
            } else {
                promise.fail(ar.cause());
            }
        }));
    }

    private void updateMppwLoadStatus(MppwLoadStatusResult mppwLoadStatusResult, StatusQueryResult result) {
        if (result.getPartitionInfo().getOffset() > mppwLoadStatusResult.getLastOffset()) {
            mppwLoadStatusResult.setLastOffsetTime(LocalDateTime.now());
            mppwLoadStatusResult.setLastOffset(result.getPartitionInfo().getOffset());
        }
    }

    private boolean isMppwLoadedSuccess(StatusQueryResult queryResult) {
        return queryResult.getPartitionInfo().getEnd().equals(queryResult.getPartitionInfo().getOffset())
                && queryResult.getPartitionInfo().getEnd() != 0
                && checkLastMessageTime(queryResult.getPartitionInfo().getLastMessageTime());
    }

    private boolean isMppwLoadingInitFailure(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() == 0L &&
                LocalDateTime.now().isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getFirstOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    private boolean isLastOffsetNotIncrease(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() != 0L &&
                LocalDateTime.now().isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getChangeOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    @NotNull
    private StatusRequestContext createStatusRequestContext(MppwRequestContext mppwRequestContext, EdmlRequestContext context) {
        val statusRequestContext = new StatusRequestContext(new StatusRequest(context.getRequest().getQueryRequest()));
        statusRequestContext.getRequest().setTopic(mppwRequestContext.getRequest().getKafkaParameter().getTopic());
        return statusRequestContext;
    }

    private void checkPluginsMppwExecution(Map<SourceType, Future<MppwStopFuture>> startMppwFuturefMap, Handler<AsyncResult<QueryResult>> resultHandler) {
        final Map<SourceType, MppwStopFuture> mppwStopFutureMap = new HashMap<>();
        CompositeFuture.join(new ArrayList<>(startMppwFuturefMap.values()))
                .onComplete(startComplete -> {
                    if (startComplete.succeeded()) {
                        processStopFutures(mppwStopFutureMap, startComplete.result(), resultHandler);
                    } else {
                        log.error(MPPW_LOAD_ERROR_MESSAGE, startComplete.cause());
                        resultHandler.handle(Future.failedFuture(startComplete.cause()));
                    }
                });
    }

    private void processStopFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                    CompositeFuture startCompositeFuture, Handler<AsyncResult<QueryResult>> resultHandler) {
        List<Future<QueryResult>> stopMppwFutures = getStopMppwFutures(mppwStopFutureMap, startCompositeFuture);
        // This extra copy of futures to satisfy CompositeFuture.join signature, which require untyped Future
        CompositeFuture.join(new ArrayList<>(stopMppwFutures))
                .onComplete(stopComplete -> {
                    if (isAllMppwPluginsHasEqualOffsets(mppwStopFutureMap)) {
                        resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        String stopStatus = collectStatus(mppwStopFutureMap);
                        RuntimeException e = new RuntimeException(
                                String.format("The offset of one of the plugins has changed: \n %s", stopStatus),
                                stopComplete.cause());
                        log.error(MPPW_LOAD_ERROR_MESSAGE, e);
                        resultHandler.handle(Future.failedFuture(e));
                    }
                });
    }

    private Future<QueryResult> stopMppw(SourceType ds, MppwRequestContext mppwRequestContext) {
        return Future.future((Promise<QueryResult> promise) -> {
            mppwRequestContext.getRequest().setIsLoadStart(false);
            log.debug("A request has been sent for the plugin: {} to stop loading mppw: {}", ds, mppwRequestContext.getRequest());
            pluginService.mppwKafka(ds, mppwRequestContext, ar -> {
                if (ar.succeeded()) {
                    log.debug("Completed stopping mppw loading by plugin: {}", ds);
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @NotNull
    private List<Future<QueryResult>> getStopMppwFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap, CompositeFuture startCompositeFuture) {
        startCompositeFuture.list().forEach(r -> {
            MppwStopFuture mppwResult = (MppwStopFuture) r;
            mppwStopFutureMap.putIfAbsent(mppwResult.getSourceType(), mppwResult);
        });
        return mppwStopFutureMap.values().stream().map(MppwStopFuture::getFuture).collect(Collectors.toList());
    }

    private String collectStatus(Map<SourceType, MppwStopFuture> mppwStopFutureMap) {
        return mppwStopFutureMap.values().stream().map(s ->
                String.format("Plugin: %s, status: %s, offset: %d, reason: %s",
                        s.sourceType.name(), s.stopReason.name(),
                        s.offset == null ? -1L : s.offset,
                        s.cause == null ? "" : s.cause.getMessage())
        ).collect(Collectors.joining("\n"));
    }

    private boolean isAllMppwPluginsHasEqualOffsets(Map<SourceType, MppwStopFuture> resultMap) {
        //check that the offset for each plugin has not changed
        if (!resultMap.isEmpty()) {
            Long offset = resultMap.values().stream().map(MppwStopFuture::getOffset).collect(Collectors.toList()).get(0);
            for (MppwStopFuture p : resultMap.values()) {
                if (p.getOffset() == null || !p.getOffset().equals(offset)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkLastMessageTime(LocalDateTime endMessageTime) {
        //todo: Remove this. Create normal checks.
        if (endMessageTime == null) {
            endMessageTime = LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        LocalDateTime endMessageTimeWithTimeout = endMessageTime.plus(kafkaProperties.getAdmin().getInputStreamTimeoutMs(),
                ChronoField.MILLI_OF_DAY.getBaseUnit());
        return endMessageTimeWithTimeout.isBefore(LocalDateTime.now());
    }

    @Override
    public ExternalTableLocationType getUploadType() {
        return ExternalTableLocationType.KAFKA;
    }

    @Data
    @ToString
    @Builder
    private static class MppwStopFuture {
        private SourceType sourceType;
        private Future<QueryResult> future;
        private Long offset;
        private Throwable cause;
        private MppwStopReason stopReason;
    }

    @Data
    @Builder
    private static class MppwLoadStatusResult {
        private Long lastOffset;
        private LocalDateTime lastOffsetTime;
    }

    private enum MppwStopReason {
        OFFSET_RECEIVED, TIMEOUT_RECEIVED, ERROR_RECEIVED;
    }

}
