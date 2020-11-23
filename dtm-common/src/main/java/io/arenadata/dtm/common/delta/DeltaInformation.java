package io.arenadata.dtm.common.delta;

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
    private final boolean isLatestUncommittedDelta;
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
            s.isLatestUncommittedDelta,
            s.type,
            s.selectOnNum,
            s.selectOnInterval,
            s.schemaName,
            s.tableName,
            s.pos);
    }
}
