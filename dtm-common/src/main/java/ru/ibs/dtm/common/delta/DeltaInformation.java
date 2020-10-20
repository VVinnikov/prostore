package ru.ibs.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.parser.SqlParserPos;

@AllArgsConstructor
@Data
@Builder
public class DeltaInformation {
    private final String tableAlias;
    private final String deltaTimestamp;
    private final boolean isLatestUncommitedDelta;
    private final Long deltaNum;
    private final DeltaInterval deltaInterval;
    private final DeltaType type;
    private Long selectOnNum;
    private SelectOnInterval selectOnInterval;
    private String schemaName;
    private String tableName;
    private SqlParserPos pos;

    public static DeltaInformation copy(DeltaInformation s) {
        return new DeltaInformation(
                s.tableAlias,
                s.deltaTimestamp,
                s.isLatestUncommitedDelta,
                s.deltaNum,
                s.deltaInterval,
                s.type,
                s.selectOnNum,
                s.selectOnInterval,
                s.schemaName,
                s.tableName,
                s.pos);
    }

    public DeltaInformation(String tableAlias, String deltaTimestamp, boolean isLatestUncommitedDelta, Long deltaNum, DeltaInterval deltaInterval, DeltaType type, String schemaName, String tableName, SqlParserPos pos) {
        this.tableAlias = tableAlias;
        this.deltaTimestamp = deltaTimestamp;
        this.isLatestUncommitedDelta = isLatestUncommitedDelta;
        this.deltaNum = deltaNum;
        this.deltaInterval = deltaInterval;
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.pos = pos;
    }
}
