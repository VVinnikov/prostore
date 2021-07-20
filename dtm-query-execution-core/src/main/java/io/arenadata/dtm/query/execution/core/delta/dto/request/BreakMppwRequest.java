package io.arenadata.dtm.query.execution.core.delta.dto.request;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;

@Getter
@Builder
public class BreakMppwRequest {

    private String datamart;
    private long sysCn;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BreakMppwRequest that = (BreakMppwRequest) o;
        return sysCn == that.sysCn && Objects.equals(datamart, that.datamart);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datamart, sysCn);
    }
}
