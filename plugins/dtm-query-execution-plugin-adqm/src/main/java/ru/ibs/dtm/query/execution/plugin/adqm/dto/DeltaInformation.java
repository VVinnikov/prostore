package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.Data;

@Data
public class DeltaInformation {
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    private final String deltaTimestamp;
    private final long deltaNum;

    public DeltaInformation withDeltaNum(long deltaNum) {
        return new DeltaInformation(this.schemaName, this.tableName, this.tableAlias, this.deltaTimestamp, deltaNum);
    }
}