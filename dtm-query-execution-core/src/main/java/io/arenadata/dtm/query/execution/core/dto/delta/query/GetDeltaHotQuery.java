package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.query.execution.core.dto.delta.operation.WriteOpFinish;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.*;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaHotQuery extends DeltaQuery {

    private Long cnFrom;
    private Long cnTo;
    private Long cnMax;
    private boolean isRollingBack;
    private List<WriteOpFinish> writeOpFinishList;

    @Builder
    public GetDeltaHotQuery(UUID requestId, String datamart, Long deltaNum, LocalDateTime deltaDate,
                            Long cnFrom, Long cnTo, Long cnMax, boolean isRollingBack,
                            List<WriteOpFinish> writeOpFinishList) {
        super(requestId, datamart, deltaNum, deltaDate);
        this.cnFrom = cnFrom;
        this.cnTo = cnTo;
        this.cnMax = cnMax;
        this.isRollingBack = isRollingBack;
        this.writeOpFinishList = writeOpFinishList;
    }

    @Override
    public DeltaAction getDeltaAction() {
        return GET_DELTA_HOT;
    }
}
