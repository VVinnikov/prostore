package ru.ibs.dtm.query.execution.core.dto.edml;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExtTableRecord extends BaseExtTableRecord {
    private String tableSchema;
    private Integer messageLimit;
}
