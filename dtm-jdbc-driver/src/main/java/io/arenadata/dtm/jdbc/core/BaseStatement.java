package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public interface BaseStatement extends Statement {

    ResultSet createDriverResultSet(List<Field[]> fields, List<ColumnMetadata> metadata);
}
