/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopFuture;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.factory.impl.MppwErrorMessageFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.Builder;
import lombok.Data;
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

    private final QueryParserService parserService;
    private final DataSourcePluginService pluginService;
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory;
    private final EdmlProperties edmlProperties;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;
    private final MppwErrorMessageFactory errorMessageFactory;
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public UploadKafkaExecutor(@Qualifier("coreCalciteDMLQueryParserService") QueryParserService coreCalciteDMLQueryParserService,
                               DataSourcePluginService pluginService,
                               MppwKafkaRequestFactory mppwKafkaRequestFactory,
                               EdmlProperties edmlProperties,
                               KafkaProperties kafkaProperties,
                               @Qualifier("coreVertx") Vertx vertx,
                               MppwErrorMessageFactory errorMessageFactory,
                               CheckColumnTypesService checkColumnTypesService) {
        this.parserService = coreCalciteDMLQueryParserService;
        this.pluginService = pluginService;
        this.mppwKafkaRequestFactory = mppwKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
        this.errorMessageFactory = errorMessageFactory;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future(promise -> {
            final Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap = new EnumMap<>(SourceType.class);
            final Set<SourceType> destinations = context.getDestinationEntity().getDestination();
            log.debug("Mppw loading into table [{}], datamart [{}], for plugins: {}",
                    context.getDestinationEntity().getName(),
                    context.getDestinationEntity().getSchema(),
                    destinations);
            parserService.parse(new QueryParserRequest(context.getSqlNode(), context.getLogicalSchema()))
                    //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
                    .compose(parserResponse -> {
                        if (!checkColumnTypesService.check(context.getDestinationEntity().getFields(), parserResponse.getRelNode())) {
                            throw new DtmException(String.format(CheckColumnTypesService.FAIL_CHECK_COLUMNS_PATTERN,
                                    context.getDestinationEntity().getName()));
                        }
                        return mppwKafkaRequestFactory.create(context);
                    })
                    .onSuccess(kafkaRequest -> {
                        destinations.forEach(ds -> startMppwFutureMap.put(ds,
                                startMppw(ds, context.getMetrics(), kafkaRequest.toBuilder().build())));
                        checkPluginsMppwExecution(startMppwFutureMap, context.getRequest().getQueryRequest().getRequestId(), promise);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<MppwStopFuture> startMppw(SourceType ds,
                                             RequestMetrics metrics,
                                             MppwKafkaRequest kafkaRequest) {
        return Future.future((Promise<MppwStopFuture> promise) -> pluginService.mppw(ds, metrics, kafkaRequest)
                .onComplete(ar -> {
                    val mppwRequestWrapper = MppwRequestWrapper.builder()
                            .sourceType(ds)
                            .metrics(metrics)
                            .request(kafkaRequest)
                            .topic(kafkaRequest.getTopic())
                            .build();

                    if (ar.succeeded()) {
                        log.debug("A request has been sent for the plugin: {} to start mppw download: {}", ds, kafkaRequest);
                        val mppwLoadStatusResult = MppwLoadStatusResult.builder()
                                .lastOffsetTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                                .lastOffset(0L)
                                .build();
                        mppwRequestWrapper.setLoadStatusResult(mppwLoadStatusResult);
                    } else {
                        log.error("Error starting loading mppw for plugin: {}", ds, ar.cause());
                        BreakMppwContext.requestRollback(kafkaRequest.getDatamartMnemonic(),
                                kafkaRequest.getSysCn(),
                                MppwStopReason.UNABLE_TO_START);
                    }

                    sendStatusPeriodically(mppwRequestWrapper, promise);
                }));
    }

    private void sendStatusPeriodically(MppwRequestWrapper mppwRequestWrapper,
                                        Promise<MppwStopFuture> promise) {
        vertx.setTimer(edmlProperties.getPluginStatusCheckPeriodMs(), timerId -> {
            log.trace("Plugin status request: {} mppw downloads", mppwRequestWrapper.getSourceType());
            getMppwLoadingStatus(mppwRequestWrapper)
                    .onSuccess(statusQueryResult -> processMppwLoad(timerId, promise, mppwRequestWrapper, statusQueryResult))
                    .onFailure(fail -> {
                        vertx.cancelTimer(timerId);
                        promise.fail(new DtmException(
                                String.format("Error getting plugin status: %s", mppwRequestWrapper.getSourceType()),
                                fail));
                    });
        });
    }

    private void processMppwLoad(Long timerId,
                                 Promise<MppwStopFuture> promise,
                                 MppwRequestWrapper mppwRequestWrapper,
                                 StatusQueryResult statusQueryResult) {
        try {
            updateMppwLoadStatus(mppwRequestWrapper.getLoadStatusResult(), statusQueryResult);
            if (isMppwLoadedSuccess(statusQueryResult)) {
                log.debug("Plugin {} MPPW loaded successfully for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                vertx.cancelTimer(timerId);
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(mppwRequestWrapper.getSourceType())
                        .future(stopMppw(mppwRequestWrapper))
                        .offset(statusQueryResult.getPartitionInfo().getOffset())
                        .stopReason(MppwStopReason.OFFSET_RECEIVED)
                        .build();
                promise.complete(stopFuture);
                return;
            } else if (isMppwLoadingInitFailure(mppwRequestWrapper.getLoadStatusResult())) {
                log.error("Plugin {} consumer failed to start for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                BreakMppwContext.requestRollback(mppwRequestWrapper.getRequest().getDatamartMnemonic(),
                        mppwRequestWrapper.getRequest().getSysCn(),
                        MppwStopReason.FIRST_OFFSET_TIMEOUT);
            } else if (isLastOffsetNotIncrease(mppwRequestWrapper.getLoadStatusResult())) {
                log.error("Plugin {} last offset not increased for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId());
                BreakMppwContext.requestRollback(mppwRequestWrapper.getRequest().getDatamartMnemonic(),
                        mppwRequestWrapper.getRequest().getSysCn(),
                        MppwStopReason.CHANGE_OFFSET_TIMEOUT);
            }

            if (BreakMppwContext.rollbackRequested(
                    mppwRequestWrapper.getRequest().getDatamartMnemonic(),
                    mppwRequestWrapper.getRequest().getSysCn())) {
                log.info("Plugin {} got BREAK_MPPW task for request [{}]",
                        mppwRequestWrapper.getSourceType(),
                        mppwRequestWrapper.getRequest().getRequestId());
                vertx.cancelTimer(timerId);

                MppwStopReason reason = BreakMppwContext.getReason(
                        mppwRequestWrapper.getRequest().getDatamartMnemonic(),
                        mppwRequestWrapper.getRequest().getSysCn());
                MppwStopFuture stopFuture = MppwStopFuture.builder()
                        .sourceType(mppwRequestWrapper.getSourceType())
                        .future(stopMppw(mppwRequestWrapper))
                        .cause(new DtmException(String.format("Stopping MPPW for datasource %s. Reason: %s. Request id: %s",
                                mppwRequestWrapper.getSourceType().name(),
                                reason,
                                mppwRequestWrapper.getRequest().getRequestId())))
                        .stopReason(reason)
                        .build();
                promise.complete(stopFuture);
            } else {
                sendStatusPeriodically(mppwRequestWrapper, promise);
            }
        } catch (Exception e) {
            log.error("Plugin {} mppw process failed for request [{}]", mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getRequest().getRequestId(), e);
            vertx.cancelTimer(timerId);
            MppwStopFuture stopFuture = MppwStopFuture.builder()
                    .sourceType(mppwRequestWrapper.getSourceType())
                    .future(stopMppw(mppwRequestWrapper))
                    .cause(new DtmException(String.format("Error in processing mppw by plugin %s",
                            mppwRequestWrapper.getSourceType()), e))
                    .stopReason(MppwStopReason.ERROR_RECEIVED)
                    .build();
            promise.complete(stopFuture);
        }
    }

    private Future<StatusQueryResult> getMppwLoadingStatus(MppwRequestWrapper mppwRequestWrapper) {
        return Future.future((Promise<StatusQueryResult> promise) ->
                pluginService.status(mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getMetrics(), mppwRequestWrapper.getTopic())
                        .onSuccess(queryResult -> {
                            log.trace("Plugin status received: {} mppw downloads: {}, on topic: {}",
                                    mppwRequestWrapper.getSourceType(),
                                    queryResult,
                                    mppwRequestWrapper.getTopic());
                            promise.complete(queryResult);
                        })
                        .onFailure(promise::fail));
    }

    private void updateMppwLoadStatus(MppwLoadStatusResult mppwLoadStatusResult, StatusQueryResult result) {
        if (result.getPartitionInfo().getOffset() > mppwLoadStatusResult.getLastOffset()) {
            mppwLoadStatusResult.setLastOffsetTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID));
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
                LocalDateTime.now(CoreConstants.CORE_ZONE_ID).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getFirstOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    private boolean isLastOffsetNotIncrease(MppwLoadStatusResult mppwLoadStatusResult) {
        return mppwLoadStatusResult.getLastOffset() != 0L &&
                LocalDateTime.now(CoreConstants.CORE_ZONE_ID).isAfter(mppwLoadStatusResult.getLastOffsetTime()
                        .plus(edmlProperties.getChangeOffsetTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit()));
    }

    private void checkPluginsMppwExecution(Map<SourceType, Future<MppwStopFuture>> startMppwFutureMap,
                                           UUID requestId,
                                           Promise<QueryResult> promise) {
        final Map<SourceType, MppwStopFuture> mppwStopFutureMap = new EnumMap<>(SourceType.class);
        CompositeFuture.join(new ArrayList<>(startMppwFutureMap.values()))
                .onSuccess(startResult -> processStopFutures(mppwStopFutureMap, startResult, requestId, promise))
                .onFailure(promise::fail);
    }

    private void processStopFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                    CompositeFuture startCompositeFuture,
                                    UUID requestId,
                                    Promise<QueryResult> promise) {
        List<Future<QueryResult>> stopMppwFutures = getStopMppwFutures(mppwStopFutureMap, startCompositeFuture);
        CompositeFuture.join(new ArrayList<>(stopMppwFutures))
                .onComplete(stopComplete -> {
                    if (stopComplete.succeeded()) {
                        if (isAllMppwPluginsHasEqualOffsets(mppwStopFutureMap)) {
                            log.debug("MPPW load successfully finished for request [{}]", requestId);
                            promise.complete(QueryResult.emptyResult());
                        } else {
                            failMppw(mppwStopFutureMap, requestId, promise, new DtmException("MPPW plugins have not equal offsets"));
                        }
                    } else {
                        failMppw(mppwStopFutureMap, requestId, promise, stopComplete.cause());
                    }
                });
    }

    private void failMppw(Map<SourceType, MppwStopFuture> mppwStopFutureMap, UUID requestId, Promise<QueryResult> promise, Throwable cause) {
        String stopStatus = collectStatus(mppwStopFutureMap);
        RuntimeException e = new DtmException(
                String.format("Mppw load failed: %n %s", stopStatus),
                cause);
        log.error("MPPW load failed for request [{}], cause: {}", requestId, stopStatus);
        promise.fail(e);
    }

    private Future<QueryResult> stopMppw(MppwRequestWrapper mppwRequestWrapper) {
        return Future.future((Promise<QueryResult> promise) -> {
            mppwRequestWrapper.getRequest().setIsLoadStart(false);
            log.debug("A request has been sent for the plugin: {} to stop loading mppw: {}",
                    mppwRequestWrapper.getSourceType(),
                    mppwRequestWrapper.getRequest());
            pluginService.mppw(mppwRequestWrapper.getSourceType(), mppwRequestWrapper.getMetrics(), mppwRequestWrapper.getRequest())
                    .onSuccess(queryResult -> {
                        log.debug("Completed stopping MPPW loading by plugin: {}", mppwRequestWrapper.getSourceType());
                        promise.complete(queryResult);
                    })
                    .onFailure(t -> {
                        log.error("Exception during stopping MPPW by plugin: {}", mppwRequestWrapper.getSourceType(), t);
                        promise.fail(t);
                    });
        });
    }

    @NotNull
    private List<Future<QueryResult>> getStopMppwFutures(Map<SourceType, MppwStopFuture> mppwStopFutureMap,
                                                         CompositeFuture startCompositeFuture) {
        startCompositeFuture.list().forEach(r -> {
            MppwStopFuture mppwResult = (MppwStopFuture) r;
            mppwStopFutureMap.putIfAbsent(mppwResult.getSourceType(), mppwResult);
        });
        return mppwStopFutureMap.values().stream().map(MppwStopFuture::getFuture).collect(Collectors.toList());
    }

    private String collectStatus(Map<SourceType, MppwStopFuture> mppwStopFutureMap) {
        return mppwStopFutureMap.values().stream()
                .map(errorMessageFactory::create)
                .collect(Collectors.joining("\n"));
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
        return endMessageTimeWithTimeout.isBefore(LocalDateTime.now(CoreConstants.CORE_ZONE_ID));
    }

    @Override
    public ExternalTableLocationType getUploadType() {
        return ExternalTableLocationType.KAFKA;
    }


    @Data
    @Builder
    private static class MppwLoadStatusResult {
        private Long lastOffset;
        private LocalDateTime lastOffsetTime;
    }

    @Data
    @Builder
    private static class MppwRequestWrapper {
        private SourceType sourceType;
        private RequestMetrics metrics;
        private MppwKafkaRequest request;
        private String topic;
        private MppwLoadStatusResult loadStatusResult;
    }

}
