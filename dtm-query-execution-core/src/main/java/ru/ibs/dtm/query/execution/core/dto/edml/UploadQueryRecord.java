package ru.ibs.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UploadQueryRecord {
    private String id;
    private Long datamartId;
    private String tableNameExt;
    private String tableNameDst;
    private String sqlQuery;
    private Integer status;
}
