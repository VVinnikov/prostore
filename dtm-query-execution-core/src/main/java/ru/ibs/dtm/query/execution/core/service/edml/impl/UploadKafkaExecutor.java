package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlUploadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class UploadKafkaExecutor implements EdmlUploadExecutor {

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
        final Map<SourceType, PluginMppwStatus> offsetMap = new ConcurrentHashMap<>();
        try {
            final Set<SourceType> sourceTypes = pluginService.getSourceTypes();
            Arrays.asList(SourceType.ADB, SourceType.ADG).forEach(ds -> {
                final MppwRequestContext mppwRequestContext = mppwKafkaRequestFactory.create(context);
                pluginService.mppwKafka(ds, mppwRequestContext, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Отпрален запрос {} на старт загрузки mppw для плагина {}", mppwRequestContext.getRequest(), ds);
                    } else {
                        mppwRequestContext.getRequest().getQueryLoadParam().setIsLoadStart(false);
                        log.debug("Отправлен запрос {} на остановку загрузки mppw для плагина {}", mppwRequestContext.getRequest(), ds);
                        pluginService.mppwKafka(ds, mppwRequestContext, er -> {
                            if (er.succeeded()) {
                                //TODO завершение алгоритма т.к. при старте загрузки произошла ошибка
                                resultHandler.handle(Future.failedFuture(new RuntimeException("Загрузка в плагине " + ds + " завершена с ошибкой!")));
                            } else {
                                resultHandler.handle(Future.failedFuture(er.cause()));
                            }
                        });
                    }
                });
                final StatusRequestContext statusRequestContext = new StatusRequestContext(new StatusRequest(context.getRequest().getQueryRequest()));
                vertx.setTimer(edmlProperties.getPluginStatusCheckPeriodMs(), tr -> {
                    pluginService.status(ds, statusRequestContext, ar -> {
                        if (ar.succeeded()) {
                            StatusQueryResult queryResult = ar.result();
                            if (queryResult.getPartitionInfo().getEnd().equals(queryResult.getPartitionInfo().getOffset())
                                    && checkLastCommitTime(queryResult.getPartitionInfo().getLastCommitTime())) {
                                mppwRequestContext.getRequest().getQueryLoadParam().setIsLoadStart(false);
                                log.debug("Отправлен запрос {} на остановку загрузки mppw для плагина {}", mppwRequestContext.getRequest(), ds);
                                pluginService.mppwKafka(ds, mppwRequestContext, par -> {
                                    if (par.succeeded()) {
                                        PluginMppwStatus status = new PluginMppwStatus(ds, true, queryResult.getPartitionInfo().getOffset());
                                        log.debug("Завершена загрузка mppw по плагину {}", status);
                                        offsetMap.putIfAbsent(ds, status);
                                        //проверка загрузки остальных плагинов
                                        if (checkFinishMppwLoading(ds, offsetMap, sourceTypes)) {
                                            //завершение загрузки
                                            resultHandler.handle(Future.succeededFuture(new QueryResult()));//TODO доделать возвращение результата
                                        }
                                    } else {
                                        resultHandler.handle(Future.failedFuture(par.cause()));
                                    }
                                });
                            }
                        } else {
                            resultHandler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
                });
            });
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private boolean checkFinishMppwLoading(SourceType currentPlugin, Map<SourceType, PluginMppwStatus> resultMap, Set<SourceType> sourceTypes) {
        //проверяем, что загрузка произведена по всем плагинам
        if (sourceTypes.containsAll(resultMap.keySet())) {
            final PluginMppwStatus currentPluginStatus = resultMap.get(currentPlugin);
            //проверяем, что offset по каждому плагину не изменился
            for (Map.Entry<SourceType, PluginMppwStatus> entry : resultMap.entrySet()) {
                if (!currentPluginStatus.getOffset().equals(entry.getValue().getOffset())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private boolean checkLastCommitTime(LocalDateTime lastCommitTime) {
        LocalDateTime commitTimeWithTimeout = lastCommitTime.plus(kafkaProperties.getAdmin().getInputStreamTimeoutMs(), ChronoField.MILLI_OF_DAY.getBaseUnit());
        return commitTimeWithTimeout.isAfter(LocalDateTime.now());
    }

    @Override
    public Type getUploadType() {
        return Type.KAFKA_TOPIC;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    private class PluginMppwStatus {
        private SourceType sourceType;
        private boolean isFinished;
        private Long offset;
    }
}
