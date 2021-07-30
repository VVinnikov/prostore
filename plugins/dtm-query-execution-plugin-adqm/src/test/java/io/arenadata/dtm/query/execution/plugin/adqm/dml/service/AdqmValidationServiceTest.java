package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdqmValidationServiceTest {

    private final AdqmQueryJoinConditionsCheckService adqmQueryJoinConditionsCheckService = mock(AdqmQueryJoinConditionsCheckService.class);
    private final QueryParserResponse parserResponse = mock(QueryParserResponse.class);
    private final RelRoot relNode = mock(RelRoot.class);
    private final LlrValidationService validationService = new AdqmValidationService(adqmQueryJoinConditionsCheckService);

    @BeforeEach
    void setUp() {
        when(parserResponse.getRelNode()).thenReturn(relNode);
        when(parserResponse.getSchema()).thenReturn(null);
    }

    @Test
    void validateSuccess() {
        when(adqmQueryJoinConditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        validationService.validate(parserResponse);
    }

    @Test
    void validateFail() {
        when(adqmQueryJoinConditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(false);
        try {
            validationService.validate(parserResponse);
        } catch (Exception e) {
            assertTrue(e instanceof DataSourceException);
            assertTrue(e.getMessage().contains("Clickhouse’s global join is restricted"));
        }
    }
}
