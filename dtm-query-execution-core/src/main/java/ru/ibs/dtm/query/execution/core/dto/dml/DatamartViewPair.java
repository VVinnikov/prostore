package ru.ibs.dtm.query.execution.core.dto.dml;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DatamartViewPair {
    private String datamart;
    private final String viewName;
}
