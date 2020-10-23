package ru.ibs.dtm.query.execution.plugin.adg.dto.rollback;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReverseHistoryTransferRequest {
    private int eraseOperationBatchSize;
    private String stagingTableName;
    private String actualTableName;
    private String historyTableName;
    private long sysCn;
}
