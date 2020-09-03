package ru.ibs.dtm.query.execution.plugin.adb.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler.AdbMppwHandler;
import ru.ibs.dtm.query.execution.plugin.adb.verticle.worker.AdbMppwWorker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AdbMppwVerticle extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap = new ConcurrentHashMap<>();
    private final MppwProperties mppwProperties;
    private final AdbMppwHandler mppwTransferDataHandler;

    @Autowired
    public AdbMppwVerticle(MppwProperties mppwProperties,
                           @Qualifier("adbMppwTransferDataHandler") AdbMppwHandler mppwTransferDataHandler,
                           @Qualifier("coreVertx") Vertx vertx) {
        this.mppwProperties = mppwProperties;
        this.mppwTransferDataHandler = mppwTransferDataHandler;
    }

    @Override
    public void start() throws Exception {
        val options = new DeploymentOptions()
                .setWorkerPoolSize(this.mppwProperties.getPoolSize())
                .setWorker(true);
        for (int i = 0; i < this.mppwProperties.getPoolSize(); i++) {
            vertx.deployVerticle(new AdbMppwWorker(this.requestMap, this.mppwTransferDataHandler), options, ar -> {
                if (ar.succeeded()) {
                    log.debug("Mppw workers deployed successfully");
                } else {
                    log.error("Error deploying mppw workers", ar.cause());
                }
            });
        }
    }
}
