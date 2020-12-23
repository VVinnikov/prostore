package io.arenadata.dtm.query.execution.core.dto.cache;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Objects;

@Data
@Builder
public class QueryTemplateKey {
    private String queryTemplate;
    private List<Datamart> logicalSchema;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTemplateKey that = (QueryTemplateKey) o;
        return queryTemplate.equals(that.queryTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryTemplate);
    }
}
