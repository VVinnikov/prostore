package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.handler;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.query.impl.AdbQueryExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("adbMppwTransferDataHandler")
@Slf4j
public class AdbMppwTransferDataHandler implements AdbMppwHandler {

    private final AdbQueryExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final AdbMppwDataTransferService mppwDataTransferService;

    @Autowired
    public AdbMppwTransferDataHandler(AdbQueryExecutor adbQueryExecutor,
                                      KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                      AdbMppwDataTransferService mppwDataTransferService) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwDataTransferService = mppwDataTransferService;
    }

    @Override
    public Future<Void> handle(MppwKafkaRequestContext requestContext) {
        return insertIntoStagingTable(requestContext.getMppwKafkaLoadRequest())
                .compose(v -> commitKafkaMessages(requestContext))
                .compose(s -> mppwDataTransferService.execute(requestContext.getMppwTransferDataRequest()));
    }

    private Future<Void> commitKafkaMessages(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val schema = requestContext.getMppwKafkaLoadRequest().getDatamart();
            val table = kafkaMppwSqlFactory.getTableName(requestContext.getMppwKafkaLoadRequest().getRequestId());
            val commitOffsetsSql = kafkaMppwSqlFactory.commitOffsetsSqlQuery(schema, table);
            adbQueryExecutor.executeUpdate(commitOffsetsSql)
                    .onComplete(promise);
        });
    }

    private Future<Void> insertIntoStagingTable(MppwKafkaLoadRequest request) {
        return Future.future(promise -> {
            val schema = request.getDatamart();
            val columns = String.join(", ", request.getColumns());
            val extTable = request.getWritableExtTableName().replace("-", "_");
            val stagingTable = request.getTableName();
            adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.insertIntoStagingTableSqlQuery(schema,
                    columns,
                    stagingTable,
                    extTable))
                    .onSuccess(promise::complete)
                    .onFailure(promise::fail);
        });
    }
}
