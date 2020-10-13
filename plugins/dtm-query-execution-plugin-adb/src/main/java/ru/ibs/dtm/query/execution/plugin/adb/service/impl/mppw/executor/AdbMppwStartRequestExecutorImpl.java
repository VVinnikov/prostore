package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;

@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwRestLoadRequestFactory mppwRestLoadRequestFactory;
    private final Vertx vertx;

    @Autowired
    public AdbMppwStartRequestExecutorImpl(AdbQueryExecutor adbQueryExecutor,
                                           MetadataSqlFactory metadataSqlFactory,
                                           MppwTransferRequestFactory mppwTransferRequestFactory,
                                           MppwRestLoadRequestFactory mppwRestLoadRequestFactory,
                                           @Qualifier("coreVertx") Vertx vertx) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwRestLoadRequestFactory = mppwRestLoadRequestFactory;
        this.vertx = vertx;
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            final RestLoadRequest restLoadRequest = mppwRestLoadRequestFactory.create(context);
            if (!restLoadRequest.getFormat().equals(Format.AVRO.getName())) {
                promise.fail(new RuntimeException(String.format("Format %s not implemented", restLoadRequest.getFormat())));
            }
            createMppwKafkaRequestContext(context, restLoadRequest)
                    .onSuccess(kafkaContext -> {
                        vertx.eventBus().send(MppwTopic.KAFKA_START.getValue(), Json.encode(kafkaContext));
                        log.debug("Mppw started successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<MppwKafkaRequestContext> createMppwKafkaRequestContext(MppwRequestContext context,
                                                                          RestLoadRequest restLoadRequest) {
        return Future.future((Promise<MppwKafkaRequestContext> promise) -> {
            final String keyColumnsSqlQuery = metadataSqlFactory.createKeyColumnsSqlQuery(
                    context.getRequest().getKafkaParameter().getDatamart(),
                    context.getRequest().getKafkaParameter().getTargetTableName());
            adbQueryExecutor.execute(keyColumnsSqlQuery, ar -> {
                if (ar.succeeded()) {
                    final List<JsonObject> result = ar.result();
                    final MppwTransferDataRequest mppwTransferDataRequest =
                            mppwTransferRequestFactory.create(context, result);
                    MppwKafkaRequestContext kafkaRequestContext =
                            new MppwKafkaRequestContext(restLoadRequest, mppwTransferDataRequest);
                    promise.complete(kafkaRequestContext);
                } else {
                    promise.fail(ar.cause());
                }
            });

        });
    }

}
