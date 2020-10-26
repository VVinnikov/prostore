package io.arenadata.dtm.query.execution.core.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SystemDatamartView {
    private String name;
    private Entity entity;
    private String query;
}
