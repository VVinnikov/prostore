package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.query.AdqmCheckJoinRequest;

public interface AdqmQueryJoinConditionsCheckService {

    boolean isJoinConditionsCorrect(AdqmCheckJoinRequest joinRequest);
}
