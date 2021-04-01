package io.arenadata.dtm.query.execution.core.delta.dto.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction.GET_DELTA_OK;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaOkQuery extends DeltaQuery {
    private Long cnFrom;
    private Long cnTo;

    @Builder
    public GetDeltaOkQuery(QueryRequest request,
                           String datamart,
                           Long deltaNum,
                           LocalDateTime deltaDate,
                           Long cnFrom,
                           Long cnTo) {
        super(request, datamart, deltaNum, deltaDate);
        this.cnFrom = cnFrom;
        this.cnTo = cnTo;
    }

    @Override
    public DeltaAction getDeltaAction() {
        return GET_DELTA_OK;
    }
}
