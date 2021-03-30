package io.arenadata.dtm.query.calcite.core.extension.dml;

import org.apache.calcite.sql.SqlCharStringLiteral;

public interface SqlDataSourceTypeGetter {
    SqlCharStringLiteral getDatasourceType();
}
