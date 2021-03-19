package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;

public interface AdqmQueryJoinConditionsCheckService {

    boolean isJoinConditionsCorrect(EnrichQueryRequest enrichQueryRequest);
}
