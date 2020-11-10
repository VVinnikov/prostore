package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

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
            final RestMppwKafkaLoadRequest restLoadRequest = mppwRestLoadRequestFactory.create(context);
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
                                                                          RestMppwKafkaLoadRequest restLoadRequest) {
        return Future.future((Promise<MppwKafkaRequestContext> promise) -> {
            final String keyColumnsSqlQuery = metadataSqlFactory.createKeyColumnsSqlQuery(
                    context.getRequest().getKafkaParameter().getDatamart(),
                    context.getRequest().getKafkaParameter().getTargetTableName());
            final List<ColumnMetadata> metadata = metadataSqlFactory.createKeyColumnQueryMetadata();
            adbQueryExecutor.execute(keyColumnsSqlQuery, metadata, ar -> {
                if (ar.succeeded()) {
                    final MppwTransferDataRequest mppwTransferDataRequest =
                            mppwTransferRequestFactory.create(context, ar.result());
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
