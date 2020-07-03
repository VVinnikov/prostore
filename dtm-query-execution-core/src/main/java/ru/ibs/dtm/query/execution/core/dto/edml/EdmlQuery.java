package ru.ibs.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EdmlQuery {
    private EdmlAction action;
    private BaseExtTableRecord record;
}
