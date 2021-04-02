package io.arenadata.dtm.query.execution.core.delta.dto;

import io.arenadata.dtm.query.execution.core.delta.dto.operation.WriteOpFinish;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HotDelta {
    private long deltaNum;
    private long cnFrom;
    private Long cnTo;
    private long cnMax;
    private boolean rollingBack;
    private List<WriteOpFinish> writeOperationsFinished;
}
