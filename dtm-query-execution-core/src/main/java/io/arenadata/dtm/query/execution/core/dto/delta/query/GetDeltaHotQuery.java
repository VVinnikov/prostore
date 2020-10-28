package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.WriteOpFinish;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.List;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.GET_DELTA_HOT;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaHotQuery extends DeltaQuery {

    private Long cnFrom;
    private Long cnTo;
    private Long cnMax;
    private boolean isRollingBack;
    private List<WriteOpFinish> writeOpFinishList;

    @Builder
    public GetDeltaHotQuery(QueryRequest request,
                            String datamart,
                            Long deltaNum,
                            LocalDateTime deltaDate,
                            Long cnFrom,
                            Long cnTo,
                            Long cnMax,
                            boolean isRollingBack,
                            List<WriteOpFinish> writeOpFinishList) {
        super(request, datamart, deltaNum, deltaDate);
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
