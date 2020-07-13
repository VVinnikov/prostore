package ru.ibs.dtm.query.execution.core.dao.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;

import java.util.List;

public interface DeltaServiceDao {

    void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler);

    void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler);

    void getDeltaHotByDatamart(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void getDeltaActualBySinIdAndDatamart(String datamartMnemonic, Long sinId, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void insertDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);

    void updateDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);
}
