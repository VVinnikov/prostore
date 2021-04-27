package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("adbMppwStopRequestExecutor")
@Slf4j
public class AdbMppwStopRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final Vertx vertx;
    private final AdbQueryExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdbMppwStopRequestExecutorImpl(@Qualifier("coreVertx") Vertx vertx,
                                          AdbQueryExecutor adbQueryExecutor,
                                          KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                          MppwProperties mppwProperties) {
        this.vertx = vertx;
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<QueryResult> execute(MppwKafkaRequest request) {
        return Future.future((Promise<QueryResult> promise) -> vertx.eventBus().request(
                MppwTopic.KAFKA_STOP.getValue(),
                request.getRequestId().toString(),
                new DeliveryOptions().setSendTimeout(mppwProperties.getStopTimeoutMs()),
                ar -> dropExtTable(request)
                        .onSuccess(v -> {
                            if (ar.succeeded()) {
                                log.debug("Mppw kafka stopped successfully");
                                promise.complete(QueryResult.emptyResult());
                            } else {
                                promise.fail(new MppwDatasourceException("Error stopping mppw kafka", ar.cause()));
                            }
                        })
                        .onFailure(error -> promise.fail(new MppwDatasourceException("Error stopping mppw kafka", error)))));
    }

    private Future<Void> dropExtTable(MppwRequest request) {
        return Future.future(promise ->
                adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.dropExtTableSqlQuery(request.getDatamartMnemonic(),
                        kafkaMppwSqlFactory.getTableName(request.getRequestId().toString())))
                        .onComplete(promise));
    }
}
