package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AdbMppwDataTransferServiceImpl implements AdbMppwDataTransferService {

    private final MppwRequestFactory<AdbKafkaMppwTransferRequest> mppwRequestFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbMppwDataTransferServiceImpl(MppwRequestFactory<AdbKafkaMppwTransferRequest> mppwRequestFactory,
                                          AdbQueryExecutor adbQueryExecutor) {
        this.mppwRequestFactory = mppwRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> execute(MppwTransferDataRequest dataRequest) {
        return Future.future(promise -> {
            AdbKafkaMppwTransferRequest transferRequest = mppwRequestFactory.create(dataRequest);
            adbQueryExecutor.executeInTransaction(transferRequest.getFirstTransaction())
                    .compose(v -> adbQueryExecutor.executeInTransaction(transferRequest.getSecondTransaction()))
                    .onComplete(promise);
        });
    }
}
