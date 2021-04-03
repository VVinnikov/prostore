package io.arenadata.dtm.query.execution.plugin.adqm.status.service;

import io.arenadata.dtm.query.execution.plugin.adqm.status.dto.StatusReportDto;

public interface StatusReporter {
    void onStart(StatusReportDto payload);
    void onFinish(StatusReportDto payload);
    void onError(StatusReportDto payload);
}
