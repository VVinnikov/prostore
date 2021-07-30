package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;

public interface LlrValidationService {

    void validate(QueryParserResponse queryParserResponse);
}
