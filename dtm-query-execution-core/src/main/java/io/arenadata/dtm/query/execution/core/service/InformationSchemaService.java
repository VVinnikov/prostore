package io.arenadata.dtm.query.execution.core.service;

import org.apache.calcite.sql.SqlCall;

public interface InformationSchemaService {

    void update(SqlCall sql);

    void initialize();
}
