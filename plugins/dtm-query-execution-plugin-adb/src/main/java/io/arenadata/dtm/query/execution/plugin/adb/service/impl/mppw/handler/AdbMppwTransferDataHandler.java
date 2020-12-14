package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("adbMppwTransferDataHandler")
@Slf4j
public class AdbMppwTransferDataHandler implements AdbMppwHandler {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory metadataSqlFactory;
    private final AdbMppwDataTransferService mppwDataTransferService;

    @Autowired
    public AdbMppwTransferDataHandler(AdbQueryExecutor adbQueryExecutor,
                                      MetadataSqlFactory metadataSqlFactory,
                                      AdbMppwDataTransferService mppwDataTransferService) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.metadataSqlFactory = metadataSqlFactory;
        this.mppwDataTransferService = mppwDataTransferService;
    }

    @Override
    public Future<Void> handle(MppwKafkaRequestContext requestContext) {
        return insertIntoStagingTable(requestContext.getMppwKafkaLoadRequest())
                .compose(v -> commitKafkaMessages(requestContext))
                .compose(s -> Future.future((Promise<Void> p) ->
                        mppwDataTransferService.execute(requestContext.getMppwTransferDataRequest(), p)));
    }

    private Future<Void> commitKafkaMessages(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val schema = requestContext.getMppwKafkaLoadRequest().getDatamart();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF +
                    requestContext.getMppwKafkaLoadRequest().getRequestId().replaceAll("-", "_");
            val commitOffsetsSql = String.format(MetadataSqlFactoryImpl.COMMIT_OFFSETS, schema, table);
            adbQueryExecutor.executeUpdate(commitOffsetsSql, promise);
        });
    }

    private Future<Void> insertIntoStagingTable(MppwKafkaLoadRequest request) {
        return Future.future(promise -> {
            val schema = request.getDatamart();
            val columns = String.join(", ", request.getColumns());
            val extTable = request.getWritableExtTableName().replaceAll("-", "_");
            val stagingTable = request.getTableName();
            adbQueryExecutor.executeUpdate(metadataSqlFactory.insertIntoStagingTableSqlQuery(schema,
                    columns,
                    stagingTable,
                    extTable),
                    ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
