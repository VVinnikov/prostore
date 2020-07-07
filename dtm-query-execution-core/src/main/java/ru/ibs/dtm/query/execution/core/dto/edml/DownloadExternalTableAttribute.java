package ru.ibs.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DownloadExternalTableAttribute {
    private String columnName;
    private String dataType;
    private int orderNum;
    private long detId;
}
