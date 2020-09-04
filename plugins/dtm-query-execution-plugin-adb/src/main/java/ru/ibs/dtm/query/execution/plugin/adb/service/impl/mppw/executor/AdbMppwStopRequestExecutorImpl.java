package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

@Component("adbMppwStopRequestExecutor")
@Slf4j
public class AdbMppwStopRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final Vertx vertx;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdbMppwStopRequestExecutorImpl(@Qualifier("coreVertx") Vertx vertx, MppwProperties mppwProperties) {
        this.vertx = vertx;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext requestContext) {
        return Future.future((Promise<QueryResult> promise) -> {
            vertx.eventBus().request(
                    MppwTopic.KAFKA_STOP.getValue(),
                    requestContext.getRequest().getQueryRequest().getRequestId().toString(),
                    new DeliveryOptions().setSendTimeout(mppwProperties.getStopTimeoutMs()),
                    ar -> {
                        if (ar.succeeded()) {
                            log.debug("Mppw kafka stopped successfully");
                            promise.complete(QueryResult.emptyResult());
                        } else {
                            log.error("Error stopping mppw kafka", ar.cause());
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
