package ru.ibs.dtm.query.execution.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

@Data
@AllArgsConstructor
public class SystemDatamartView {
    private String name;
    private DatamartTable datamartTable;
    private String query;
}
