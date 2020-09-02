package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.eventbus.DataTopic;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

import java.util.List;

@Slf4j
@Component("adbMppwKafkaService")
public class AdbMppwKafkaService implements MppwKafkaService<QueryResult> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwRestLoadRequestFactory mppwRestLoadRequestFactory;
    private final Vertx vertx;

    public AdbMppwKafkaService(AdbQueryExecutor adbQueryExecutor,
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
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
        try {
            final RestLoadRequest restLoadRequest = mppwRestLoadRequestFactory.create(context);
            if (!restLoadRequest.getFormat().equals(Format.AVRO.getName())) {
                asyncHandler.handle(Future.failedFuture(
                        new RuntimeException(String.format("Format %s not implemented", restLoadRequest.getFormat()))));
            }
            final String keyColumnsSqlQuery = metadataSqlFactory.createKeyColumnsSqlQuery(
                    context.getRequest().getQueryLoadParam().getDatamart(),
                    context.getRequest().getQueryLoadParam().getTableName());
            adbQueryExecutor.execute(keyColumnsSqlQuery, ar -> {
                if (ar.succeeded()) {
                    log.debug("Mppw start by request: {}", restLoadRequest);
                    final List<JsonObject> result = ar.result();
                    final MppwTransferDataRequest mppwTransferDataRequest =
                            mppwTransferRequestFactory.create(context, result);
                    MppwKafkaRequestContext kafkaRequestContext =
                            new MppwKafkaRequestContext(restLoadRequest, mppwTransferDataRequest);
                    vertx.eventBus().publish(DataTopic.MPPW_START.getValue(), Json.encode(kafkaRequestContext));
                    asyncHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                } else {
                    asyncHandler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            asyncHandler.handle(Future.failedFuture(e));
        }
    }
}
