package io.arenadata.dtm.query.execution.core.delta.dto.request;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class BreakMppwRequest {

    private String datamart;
    private long sysCn;

    public String asString() {
        return datamart + "." + sysCn;
    }
}
