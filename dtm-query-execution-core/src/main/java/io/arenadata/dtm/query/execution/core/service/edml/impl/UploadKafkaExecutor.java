package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.query.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.service.query.impl.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.*;
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
    private final DtmConfig dtmSettings;
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public UploadKafkaExecutor(DataSourcePluginService pluginService,
                               MppwKafkaRequestFactory mppwKafkaRequestFactory,
                               EdmlProperties edmlProperties,
                               KafkaProperties kafkaProperties,
                               @Qualifier("coreVertx") Vertx vertx,
                               DtmConfig dtmSettings,
                               CheckColumnTypesService checkColumnTypesService) {
        this.pluginService = pluginService;
        this.mppwKafkaRequestFactory = mppwKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
        this.dtmSettings = dtmSettings;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future(promise -> {
            final Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap = new HashMap<>();
            final Set<SourceType> destination = context.getDestinationEntity().getDestination();
            log.debug("Mppw loading into table [{}], datamart [{}], for plugins: {}",
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema(),
                    destination);
            QueryParserRequest queryParserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(),
                    context.getLogicalSchema());
            //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
            checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mppwKafkaRequestFactory.create(context)
                            : Future.failedFuture(new DtmException(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                            context.getDestinationEntity().getName()))))
                    .onSuccess(mppwRequestContext -> {
                        destination.forEach(ds ->
                                startMppwFutureMap.put(ds, startMppw(ds, mppwRequestContext.copy(), context)));
                        checkPluginsMppwExecution(startMppwFutureMap, promise);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<MppwStopFuture> startMppw(SourceType ds, MppwRequestContext mppwRequestContext, EdmlRequestContext context) {
        return Future.future((Promise<MppwStopFuture> promise) -> pluginService.mppw(ds, mppwRequestContext)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.debug("A request has been sent for the plugin: {} to start mppw download: {}", ds, mppwRequestContext.getRequest());
                        val statusRequestContext = createStatusRequestContext(mppwRequestContext, context);
                        val mppwLoadStatusResult = MppwLoadStatusResult.builder()
                                .lastOffsetTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                                .lastOffset(0L)
                                .build();
                        sendStatusPeriodicaly(ds, mppwRequestContext, promise, statusRequestContext, mppwLoadStatusResult);
                    } else {
                        MppwStopFuture stopFuture = MppwStopFuture.builder()
                                .sourceType(ds)
                                .future(stopMppw(ds, mppwRequestContext))
                                .cause(new DtmException(String.format("Error starting loading mppw for plugin: %s", ds),
                                        ar.cause()))
                                .stopReason(MppwStopReason.ERROR_RECEIVED)
                                .build();
                        promise.complete(stopFuture);
                    }
                }));
    }

    private void sendStatusPeriodicaly(SourceType ds,
                                       MppwRequestContext mppwRequestContext,
                                       Promise<MppwStopFuture> promise,
                                       StatusRequestContext statusRequestContext,
                                       MppwLoadStatusResult mppwLoadStatusResult) {
        vertx.setTimer(edmlProperties.getPluginStatusCheckPeriodMs(), timerId -> {
            log.trace("Plugin status request: {} mppw downloads", ds);
            getMppwLoadingStatus(ds, statusRequestContext)
                    .onSuccess(statusQueryResult -> {
                        //todo: Add error checking (try catch and so on)
                        updateMppwLoadStatus(mppwLoadStatusResult, statusQueryResult);
                        if (isMppwLoadedSuccess(statusQueryResult)) {
                            vertx.cancelTimer(timerId);
                            MppwStopFuture stopFuture = MppwStopFuture.builder()
                                    .sourceType(ds)
                                    .future(stopMppw(ds, mppwRequestContext))
                                    .offset(statusQueryResult.getPartitionInfo().getOffset())
                                    .stopReason(MppwStopReason.OFFSET_RECEIVED)
                                    .build();
                            try {
                                promise.complete(stopFuture);
                            } catch (Exception e) {
                                log.error("Error mppw ds {}", ds, e);
                                promise.complete(stopFuture);
                            }
                        } else if (isMppwLoadingInitFailure(mppwLoadStatusResult)) {
                            vertx.cancelTimer(timerId);
                            MppwStopFuture stopFuture = MppwStopFuture.builder()
                                    .sourceType(ds)
                                    .future(stopMppw(ds, mppwRequestContext))
                                    .cause(new DtmException(String.format("Plugin %s consumer failed to start", ds)))
                                    .stopReason(MppwStopReason.ERROR_RECEIVED)
                                    .build();
                            promise.complete(stopFuture);
                        } else if (isLastOffsetNotIncrease(mppwLoadStatusResult)) {
                            vertx.cancelTimer(timerId);
                            MppwStopFuture stopFuture = MppwStopFuture.builder()
                                    .sourceType(ds)
                                    .future(stopMppw(ds, mppwRequestContext))
                                    .cause(new DtmException(String.format("Plugin %s consumer offset stopped dead", ds)))
                                    .stopReason(MppwStopReason.ERROR_RECEIVED)
                                    .build();
                            promise.complete(stopFuture);
                        } else {
                            sendStatusPeriodicaly(ds,
                                    mppwRequestContext,
                                    promise,
                                    statusRequestContext,
                                    mppwLoadStatusResult);
                        }
                    })
                    .onFailure(fail -> {
                        vertx.cancelTimer(timerId);
                        promise.fail(new DtmException(
                                String.format("Error getting plugin status: %s", ds),
                                fail));
                    });
        });
    }

    private Future<StatusQueryResult> getMppwLoadingStatus(SourceType ds, StatusRequestContext statusRequestContext) {
        return Future.future((Promise<StatusQueryResult> promise) ->
                pluginService.status(ds, statusRequestContext)
                        .onSuccess(queryResult -> {
                            log.trace("Plugin status received: {} mppw downloads: {}, on request: {}",
                                    ds,
                                    queryResult,
                                    statusRequestContext);
                            promise.complete(queryResult);
                        })
                        .onFailure(promise::fail));
    }

    private void updateMppwLoadStatus(MppwLoadStatusResult mppwLoadStatusResult, StatusQueryResult result) {
        if (result.getPartitionInfo().getOffset() > mppwLoadStatusResult.getLastOffset()) {
            mppwLoadStatusResult.setLastOffsetTime(LocalDateTime.now(dtmSettings.getTimeZone()));
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
                LocalDateTime.now(dtmSettings.getTimeZone()).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getFirstOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    private boolean isLastOffsetNotIncrease(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() != 0L &&
                LocalDateTime.now(dtmSettings.getTimeZone()).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getChangeOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    @NotNull
    private StatusRequestContext createStatusRequestContext(MppwRequestContext mppwRequestContext,
                                                            EdmlRequestContext context) {
        val statusRequestContext = new StatusRequestContext(RequestMetrics.builder()
                .requestId(mppwRequestContext.getMetrics().getRequestId())
                .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                .sourceType(mppwRequestContext.getMetrics().getSourceType())
                .actionType(SqlProcessingType.STATUS)
                .isActive(true)
                .status(RequestStatus.IN_PROCESS)
                .build(),
                new StatusRequest(context.getRequest().getQueryRequest()));
        statusRequestContext.getRequest().setTopic(mppwRequestContext.getRequest().getKafkaParameter().getTopic());
        return statusRequestContext;
    }

    private void checkPluginsMppwExecution(Map<SourceType, Future<MppwStopFuture>> startMppwFuturefMap,
                                           Handler<AsyncResult<QueryResult>> resultHandler) {
        final Map<SourceType, MppwStopFuture> mppwStopFutureMap = new HashMap<>();
        CompositeFuture.join(new ArrayList<>(startMppwFuturefMap.values()))
                .onComplete(startComplete -> {
                    if (startComplete.succeeded()) {
                        processStopFutures(mppwStopFutureMap, startComplete.result(), resultHandler);
                    } else {
                        resultHandler.handle(Future.failedFuture(startComplete.cause()));
                    }
                });
    }

    private void processStopFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                    CompositeFuture startCompositeFuture,
                                    Handler<AsyncResult<QueryResult>> resultHandler) {
        List<Future<QueryResult>> stopMppwFutures = getStopMppwFutures(mppwStopFutureMap, startCompositeFuture);
        // This extra copy of futures to satisfy CompositeFuture.join signature, which require untyped Future
        CompositeFuture.join(new ArrayList<>(stopMppwFutures))
                .onComplete(stopComplete -> {
                    if (stopComplete.succeeded()) {
                        if (isAllMppwPluginsHasEqualOffsets(mppwStopFutureMap)) {
                            resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            String stopStatus = collectStatus(mppwStopFutureMap);
                            RuntimeException e = new DtmException(
                                    String.format("The offset of one of the plugins has changed: \n %s", stopStatus),
                                    stopComplete.cause());
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        resultHandler.handle(Future.failedFuture(stopComplete.cause()));
                    }
                });
    }

    private Future<QueryResult> stopMppw(SourceType ds, MppwRequestContext mppwRequestContext) {
        return Future.future((Promise<QueryResult> promise) -> {
            mppwRequestContext.getRequest().setIsLoadStart(false);
            log.debug("A request has been sent for the plugin: {} to stop loading mppw: {}",
                    ds,
                    mppwRequestContext.getRequest());
            pluginService.mppw(ds, mppwRequestContext)
                    .onSuccess(queryResult -> {
                        log.debug("Completed stopping mppw loading by plugin: {}", ds);
                        promise.complete(queryResult);
                    })
                    .onFailure(promise::fail);
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
        return endMessageTimeWithTimeout.isBefore(LocalDateTime.now(dtmSettings.getTimeZone()));
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
