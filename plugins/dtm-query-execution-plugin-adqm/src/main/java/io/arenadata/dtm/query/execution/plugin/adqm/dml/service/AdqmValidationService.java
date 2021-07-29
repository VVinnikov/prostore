package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import org.springframework.stereotype.Service;

@Service("adqmValidationService")
public class AdqmValidationService implements LlrValidationService {

    private final AdqmQueryJoinConditionsCheckService joinConditionsCheckService;

    public AdqmValidationService(AdqmQueryJoinConditionsCheckService joinConditionsCheckService) {
        this.joinConditionsCheckService = joinConditionsCheckService;
    }

    @Override
    public void validate(QueryParserResponse queryParserResponse) {
        if (!joinConditionsCheckService.isJoinConditionsCorrect(new AdqmCheckJoinRequest(queryParserResponse.getRelNode().rel, queryParserResponse.getSchema()))) {
            throw new DataSourceException("Clickhouse’s global join is restricted");
        }
    }

}
