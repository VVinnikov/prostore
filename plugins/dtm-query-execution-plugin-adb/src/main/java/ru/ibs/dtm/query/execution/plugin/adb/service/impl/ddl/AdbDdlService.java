package ru.ibs.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.common.configuration.kafka.KafkaConfig;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.KafkaTopicCreatorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.Arrays;
import java.util.List;

@Service("adbDdlService")
public class AdbDdlService implements DdlService<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdbDdlService.class);

    private final MetadataFactory metadataFactory;
    private final DatabaseExecutor adbDatabaseExecutor;
    private final KafkaTopicCreatorService kafkaTopicService;
    private final KafkaConfig kafkaProperties;

    @Autowired
    public AdbDdlService(MetadataFactory metadataFactory,
                         DatabaseExecutor adbDatabaseExecutor, KafkaTopicCreatorService kafkaTopicService,
                         @Qualifier("coreKafkaProperties") KafkaConfig kafkaProperties
    ) {
        this.metadataFactory = metadataFactory;
        this.adbDatabaseExecutor = adbDatabaseExecutor;
        this.kafkaTopicService = kafkaTopicService;
        this.kafkaProperties = kafkaProperties;
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
        adbDatabaseExecutor.executeUpdate(sql, executeResult -> {
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
        KafkaAdminProperty properties = kafkaProperties.getKafkaAdminProperty();
        String adbUploadRq = String.format(properties.getUpload().getRequestTopic().get(SourceType.ADB.toString().toLowerCase()), table.getName(), table.getSchema());
        String adbUploadRs = String.format(properties.getUpload().getResponseTopic().get(SourceType.ADB.toString().toLowerCase()), table.getName(), table.getSchema());
        String adbUploadErr = String.format(properties.getUpload().getErrorTopic().get(SourceType.ADB.toString().toLowerCase()), table.getName(), table.getSchema());
        return Arrays.asList(adbUploadRq, adbUploadRs, adbUploadErr);
    }

	@Override
	public void addExecutor(DdlExecutor<Void> executor) {
		// TODO implemented
	}
}
