package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Empty query result.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class EmptyQueryResult extends QueryResult {
    public EmptyQueryResult() {
        super(null, Collections.emptyList());
    }

    @Override
    public void setResult(List<Map<String, Object>> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadata(List<ColumnMetadata> metadata) {
        throw new UnsupportedOperationException();
    }
}
