package ru.ibs.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import ru.ibs.dtm.common.model.ddl.Entity;


@Data
@AllArgsConstructor
@Builder
public class EdmlQuery {
    private EdmlAction action;
    private Entity entity;
    private BaseExtTableRecord record;
}
