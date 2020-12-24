package io.arenadata.dtm.common.cache;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Objects;

@Data
@Builder
public class QueryTemplateKey {
    private String sourceQueryTemplate;
    private List<Datamart> logicalSchema;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTemplateKey that = (QueryTemplateKey) o;
        return sourceQueryTemplate.equals(that.sourceQueryTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceQueryTemplate);
    }
}
