package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.List;

public class QueryParameters {
    private final List<Object> values;
    private final List<ColumnType> types;

    public QueryParameters(List<Object> values, List<ColumnType> types) {
        this.values = values;
        this.types = types;
    }

    public List<Object> getValues() {
        return values;
    }

    public List<ColumnType> getTypes() {
        return types;
    }
}
