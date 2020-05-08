package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;

import java.util.List;

@Service
public class DeltaServiceImpl implements DeltaService {
  private final ServiceDao executor;

  @Autowired
  public DeltaServiceImpl(ServiceDao executor) {
    this.executor = executor;
  }

  @Override
  public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler) {
    executor.getDeltaOnDateTime(actualDeltaRequest, resultHandler);
  }

  @Override
  public void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler) {
    executor.getDeltasOnDateTimes(actualDeltaRequests, resultHandler);
  }
}
