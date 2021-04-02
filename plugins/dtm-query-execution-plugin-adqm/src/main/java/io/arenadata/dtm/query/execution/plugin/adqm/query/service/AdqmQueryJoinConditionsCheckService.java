package io.arenadata.dtm.query.execution.plugin.adqm.query.service;

import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;

public interface AdqmQueryJoinConditionsCheckService {

    boolean isJoinConditionsCorrect(AdqmCheckJoinRequest joinRequest);
}
