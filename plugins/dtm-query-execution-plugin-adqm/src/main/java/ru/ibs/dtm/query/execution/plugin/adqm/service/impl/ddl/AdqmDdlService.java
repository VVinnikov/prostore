package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.KafkaTopicCreatorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.Arrays;
import java.util.List;

@Service("adqmDdlService")
public class AdqmDdlService implements DdlService<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdqmDdlService.class);

    private final MetadataFactory metadataFactory;
    private final DatabaseExecutor adqmDatabaseExecutor;
    private final KafkaTopicCreatorService kafkaTopicService;
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;

    @Autowired
    public AdqmDdlService(MetadataFactory metadataFactory,
                          DatabaseExecutor adqmDatabaseExecutor,
                          KafkaTopicCreatorService kafkaTopicService,
                          KafkaProperties kafkaProperties,
                          @Qualifier("adqmVertx") Vertx vertx
    ) {
        this.metadataFactory = metadataFactory;
        this.adqmDatabaseExecutor = adqmDatabaseExecutor;
        this.kafkaTopicService = kafkaTopicService;
        this.kafkaProperties = kafkaProperties;
        this.vertx = vertx;
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        switch (context.getDdlType()) {
            case CREATE_TABLE:
                createTable(context, handler);
                return;
            case DROP_TABLE:
                dropTable(context, handler);
                return;
            case CREATE_SCHEMA:
            case DROP_SCHEMA:
                applySql(context.getRequest().getQueryRequest().getSql(), handler);
                return;
        }
        handler.handle(Future.failedFuture("DDL не опознан: " + context));
    }

    private void applySql(String sql, Handler<AsyncResult<Void>> handler) {
        adqmDatabaseExecutor.executeUpdate(sql, executeResult -> {
            if (executeResult.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(executeResult.cause()));
            }
        });
    }

    private void createTable(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        metadataFactory.apply(context.getRequest().getClassTable(), ar -> {
            if (ar.succeeded()) {
                if (!context.getDdlType().isCreateTopic()) {
                    handler.handle(Future.succeededFuture());
                    return;
                }
                kafkaTopicService.createOrReplace(getTopics(context.getRequest().getClassTable()), ar2 -> {
                    if (ar2.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error("Ошибка генерации топиков в Kafka", ar2.cause());
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                LOGGER.error("Ошибка исполнения запроса", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void dropTable(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        metadataFactory.purge(context.getRequest().getClassTable(), ar -> {
            if (ar.succeeded()) {
                kafkaTopicService.delete(getTopics(context.getRequest().getClassTable()), ar2 -> {
                    if (ar2.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error("Ошибка удаления топиков в Kafka", ar2.cause());
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                LOGGER.error("Ошибка исполнения запроса", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private List<String> getTopics(ClassTable table) {
        KafkaAdminProperty properties = kafkaProperties.getAdmin();
        String adqmUploadRq = String.format(properties.getAdqmUploadRq(), table.getName(), table.getSchema());
        String adqmUploadRs = String.format(properties.getAdqmUploadRs(), table.getName(), table.getSchema());
        String adqmUploadErr = String.format(properties.getAdqmUploadErr(), table.getName(), table.getSchema());
        return Arrays.asList(adqmUploadRq, adqmUploadRs, adqmUploadErr);
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {

    }
}
