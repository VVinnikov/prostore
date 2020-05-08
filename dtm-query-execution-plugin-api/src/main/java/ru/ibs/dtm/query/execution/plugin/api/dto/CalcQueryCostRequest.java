package ru.ibs.dtm.query.execution.plugin.api.dto;

import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для выполнения расчета стоимости запроса
 */
public class CalcQueryCostRequest extends BaseRequest {

  public CalcQueryCostRequest() {
  }

  public CalcQueryCostRequest(QueryRequest queryRequest) {
    super(queryRequest);
  }
}
