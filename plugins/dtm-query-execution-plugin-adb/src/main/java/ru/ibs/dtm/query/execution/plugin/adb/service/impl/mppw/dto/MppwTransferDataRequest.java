package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class MppwTransferDataRequest implements Serializable {
    private long hotDelta;
    private String datamart;
    private String tableName;
    private List<String> columnList;
    private List<String> keyColumnList;
}
