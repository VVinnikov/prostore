package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MppwTransferDataRequest implements Serializable {
    private long hotDelta;
    private String datamart;
    private String tableName;
    private List<String> columnList;
    private List<String> keyColumnList;

}
