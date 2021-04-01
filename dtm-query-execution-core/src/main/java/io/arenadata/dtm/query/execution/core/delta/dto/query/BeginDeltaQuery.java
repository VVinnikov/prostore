package io.arenadata.dtm.query.execution.core.delta.dto.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction.BEGIN_DELTA;

@EqualsAndHashCode(callSuper = true)
@Data
public class BeginDeltaQuery extends DeltaQuery {

    @Builder
    public BeginDeltaQuery(QueryRequest request,
                           String datamart,
                           Long deltaNum,
                           LocalDateTime deltaDate) {
        super(request, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return BEGIN_DELTA;
    }
}
