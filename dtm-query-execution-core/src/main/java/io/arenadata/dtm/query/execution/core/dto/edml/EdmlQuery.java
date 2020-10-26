package io.arenadata.dtm.query.execution.core.dto.edml;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;


@Data
@AllArgsConstructor
@Builder
public class EdmlQuery {
    private EdmlAction action;
    private Entity entity;
    private BaseExtTableRecord record;
}
