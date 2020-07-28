package ru.ibs.dtm.query.execution.plugin.adqm.service;

import ru.ibs.dtm.query.execution.plugin.adqm.dto.StatusReportDto;

public interface StatusReporter {
    void onStart(StatusReportDto payload);
    void onFinish(StatusReportDto payload);
    void onError(StatusReportDto payload);
}
