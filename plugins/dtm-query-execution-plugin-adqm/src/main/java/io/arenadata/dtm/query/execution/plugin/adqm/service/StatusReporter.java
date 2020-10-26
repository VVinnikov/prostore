package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;

public interface StatusReporter {
    void onStart(StatusReportDto payload);
    void onFinish(StatusReportDto payload);
    void onError(StatusReportDto payload);
}
