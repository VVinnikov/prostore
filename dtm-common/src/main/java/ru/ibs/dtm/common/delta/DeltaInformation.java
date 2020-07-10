package ru.ibs.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeltaInformation {
    private final String tableAlias;
    private final String deltaTimestamp;
    private final long deltaNum;
    private String schemaName;
    private String tableName;

    public static DeltaInformation copy(DeltaInformation s) {
        return new DeltaInformation(
                s.tableAlias,
                s.deltaTimestamp,
                s.deltaNum,
                s.schemaName,
                s.tableName);
    }

    public DeltaInformation withDeltaNum(long deltaNum) {
        return new DeltaInformation(
                tableAlias,
                deltaTimestamp,
                deltaNum,
                schemaName,
                tableName);
    }
}
