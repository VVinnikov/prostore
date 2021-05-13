package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.Data;
import lombok.Getter;

import java.util.List;

@Data
public class QueryParameters {
    private final Object[] values;
    private final ColumnType[] types;
}
