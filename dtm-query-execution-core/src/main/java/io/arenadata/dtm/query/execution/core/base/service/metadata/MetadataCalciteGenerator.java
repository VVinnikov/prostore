package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.common.model.ddl.Entity;
import org.apache.calcite.sql.SqlCreate;

public interface MetadataCalciteGenerator {

    /**
     * Transform nodeList(table's columns) calcite in metadata object
     *
     * @return metadata object, representing table
     */
    Entity generateTableMetadata(SqlCreate sqlCreate);
}
