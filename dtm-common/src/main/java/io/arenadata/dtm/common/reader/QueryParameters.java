package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class QueryParameters {
    private List<Object> values;
    private List<ColumnType> types;

    public QueryParameters copy() {
        return new QueryParameters(this.values, this.types);
    }
}
