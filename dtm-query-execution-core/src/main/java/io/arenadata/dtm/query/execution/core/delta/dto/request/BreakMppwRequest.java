package io.arenadata.dtm.query.execution.core.delta.dto.request;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class BreakMppwRequest {

    private String datamart;
    private long sysCn;

}
