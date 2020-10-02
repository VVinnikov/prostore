package ru.ibs.dtm.query.execution.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.common.model.ddl.Entity;

@Data
@AllArgsConstructor
public class SystemDatamartView {
    private String name;
    private Entity entity;
    private String query;
}
