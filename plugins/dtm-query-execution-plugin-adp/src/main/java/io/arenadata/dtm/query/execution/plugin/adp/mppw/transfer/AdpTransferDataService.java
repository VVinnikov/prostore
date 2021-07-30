package io.arenadata.dtm.query.execution.plugin.adp.mppw.transfer;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.factory.AdpTransferDataSqlFactory;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AdpTransferDataService {

    private final AdpTransferDataSqlFactory transferDataSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdpTransferDataService(AdpTransferDataSqlFactory transferDataSqlFactory,
                                  DatabaseExecutor databaseExecutor) {
        this.transferDataSqlFactory = transferDataSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    public Future<Void> transferData(AdpTransferDataRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Start transfer data");

            val sql = String.join("",
                    transferDataSqlFactory.getCloseVersionOfRecordsSql(request.getDatamart(), request.getTableName(), request.getPrimaryKeys(), request.getSysCn()),
                    transferDataSqlFactory.getUploadHotRecordsSql(request.getDatamart(), request.getTableName(), request.getAllFields(), request.getPrimaryKeys(), request.getSysCn()),
                    transferDataSqlFactory.getTruncateSql(request.getDatamart(), request.getTableName())
            );

            databaseExecutor.executeUpdate(sql)
                    .onSuccess(v -> {
                        log.info("[ADP] Transfer data completed successfully");
                        promise.complete();
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Transfer data failed with exception", t);
                        promise.fail(t);
                    });
        });
    }
}
