package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.impl;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
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
