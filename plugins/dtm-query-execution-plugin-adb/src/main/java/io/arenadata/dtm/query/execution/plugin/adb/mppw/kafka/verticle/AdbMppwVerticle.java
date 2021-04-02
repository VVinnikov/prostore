package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.verticle;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.handler.AdbMppwHandler;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.verticle.worker.AdbMppwWorker;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AdbMppwVerticle extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap = new ConcurrentHashMap<>();
    private final Map<String, Future> resultMap = new ConcurrentHashMap<>();
    private final MppwProperties mppwProperties;
    private final AdbMppwHandler mppwTransferDataHandler;

    @Autowired
    public AdbMppwVerticle(MppwProperties mppwProperties,
                           @Qualifier("adbMppwTransferDataHandler") AdbMppwHandler mppwTransferDataHandler) {
        this.mppwProperties = mppwProperties;
        this.mppwTransferDataHandler = mppwTransferDataHandler;
    }

    @Override
    public void start() throws Exception {
        val options = new DeploymentOptions()
                .setWorkerPoolSize(this.mppwProperties.getPoolSize())
                .setWorker(true);
        for (int i = 0; i < this.mppwProperties.getPoolSize(); i++) {
            vertx.deployVerticle(new AdbMppwWorker(this.requestMap, this.resultMap,
                    this.mppwTransferDataHandler), options, ar -> {
                if (ar.succeeded()) {
                    log.debug("Mppw workers deployed successfully");
                } else {
                    log.error("Error deploying mppw workers", ar.cause());
                }
            });
        }
    }
}
