package io.arenadata.dtm.query.execution.core.delta.dto.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaByDateTimeQuery extends DeltaQuery {

    @Builder
    public GetDeltaByDateTimeQuery(QueryRequest request,
                                   String datamart,
                                   Long deltaNum,
                                   LocalDateTime deltaDate) {
        super(request, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return DeltaAction.GET_DELTA_BY_DATETIME;
    }
}
