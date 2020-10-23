package io.arenadata.dtm.query.execution.core.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.query.execution.core.dto.delta.operation.WriteOpFinish;

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
