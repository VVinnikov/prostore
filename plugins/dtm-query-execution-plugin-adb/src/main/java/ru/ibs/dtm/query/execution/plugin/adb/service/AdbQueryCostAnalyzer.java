package ru.ibs.dtm.query.execution.plugin.adb.service;

import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostAnalyzer;

public interface AdbQueryCostAnalyzer<T> extends QueryCostAnalyzer<T, AdbQueryCostService<T>> {
}
