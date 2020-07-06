package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeltaInformation {
    private String schemaName;
    private String tableName;
    private final String tableAlias;
    private final String deltaTimestamp;
    private final long deltaNum;

    public DeltaInformation withDeltaNum(long deltaNum) {
        return new DeltaInformation(this.schemaName, this.tableName, this.tableAlias, this.deltaTimestamp, deltaNum);
    }

    public static DeltaInformation copy(DeltaInformation s) {
        return new DeltaInformation(s.schemaName, s.tableName, s.tableAlias, s.deltaTimestamp, s.deltaNum);
    }
}