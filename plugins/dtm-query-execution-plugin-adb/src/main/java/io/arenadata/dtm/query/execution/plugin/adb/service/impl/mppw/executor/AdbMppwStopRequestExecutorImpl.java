package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("adbMppwStopRequestExecutor")
@Slf4j
public class AdbMppwStopRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final Vertx vertx;
    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdbMppwStopRequestExecutorImpl(@Qualifier("coreVertx") Vertx vertx,
                                          AdbQueryExecutor adbQueryExecutor,
                                          MetadataSqlFactory metadataSqlFactory,
                                          MppwProperties mppwProperties) {
        this.vertx = vertx;
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext requestContext) {
        return dropExtTable(requestContext)
                .compose(v -> Future.future((Promise<QueryResult> promise) -> vertx.eventBus().request(
                        MppwTopic.KAFKA_STOP.getValue(),
                        requestContext.getRequest().getQueryRequest().getRequestId().toString(),
                        new DeliveryOptions().setSendTimeout(mppwProperties.getStopTimeoutMs()),
                        ar -> {
                            if (ar.succeeded()) {
                                log.debug("Mppw kafka stopped successfully");
                                promise.complete(QueryResult.emptyResult());
                            } else {
                                promise.fail(new MppwDatasourceException("Error stopping mppw kafka", ar.cause()));
                            }
                        })));
    }

    private Future<Void> dropExtTable(MppwRequestContext requestContext) {
        return Future.future(promise -> {
            QueryRequest queryRequest = requestContext.getRequest().getQueryRequest();
            val schema = queryRequest.getDatamartMnemonic();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF +
                    queryRequest.getRequestId().toString().replaceAll("-", "_");
            adbQueryExecutor.executeUpdate(metadataSqlFactory.dropExtTableSqlQuery(schema, table))
                    .onComplete(promise);
        });
    }
}
