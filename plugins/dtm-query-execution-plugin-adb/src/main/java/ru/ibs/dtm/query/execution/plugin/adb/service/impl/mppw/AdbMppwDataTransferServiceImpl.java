package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;

import java.util.List;

@Component
@Slf4j
public class AdbMppwDataTransferServiceImpl implements AdbMppwDataTransferService {

    private final MppwRequestFactory mppwRequestFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbMppwDataTransferServiceImpl(MppwRequestFactory mppwRequestFactory,
                                          AdbQueryExecutor adbQueryExecutor) {
        this.mppwRequestFactory = mppwRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public void execute(MppwTransferDataRequest dataRequest, Handler<AsyncResult<Void>> asyncHandler) {
        List<PreparedStatementRequest> mppwScripts = mppwRequestFactory.create(dataRequest);
        adbQueryExecutor.executeInTransaction(mppwScripts, ar -> {
            if (ar.succeeded()) {
                asyncHandler.handle(Future.succeededFuture());
            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
