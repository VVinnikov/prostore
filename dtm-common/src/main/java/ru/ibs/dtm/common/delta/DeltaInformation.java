package ru.ibs.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.sql.parser.SqlParserPos;

@AllArgsConstructor
@Data
public class DeltaInformation {
    private final String tableAlias;
    private final String deltaTimestamp;
    private final boolean isLatestUncommitedDelta;
    private final long deltaNum;
    private String schemaName;
    private String tableName;
    private SqlParserPos pos;

    public static DeltaInformation copy(DeltaInformation s) {
        return new DeltaInformation(
                s.tableAlias,
                s.deltaTimestamp,
                s.isLatestUncommitedDelta,
                s.deltaNum,
                s.schemaName,
                s.tableName,
                s.pos);
    }

    public DeltaInformation withDeltaNum(long deltaNum) {
        return new DeltaInformation(
                tableAlias,
                deltaTimestamp,
                isLatestUncommitedDelta,
                deltaNum,
                schemaName,
                tableName,
                pos);
    }
}
