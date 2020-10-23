package io.arenadata.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class UploadQueryRecord {
    private String id;
    private Long datamartId;
    private String tableNameExt;
    private String tableNameDst;
    private String sqlQuery;
    private Integer status;
}
