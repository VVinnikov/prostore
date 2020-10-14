package ru.ibs.dtm.query.execution.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DatamartView {
    private String viewName;
    private String query;
}
