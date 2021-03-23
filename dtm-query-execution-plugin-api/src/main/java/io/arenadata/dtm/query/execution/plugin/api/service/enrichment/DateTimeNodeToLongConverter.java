package io.arenadata.dtm.query.execution.plugin.api.service.enrichment;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

public interface DateTimeNodeToLongConverter {

    long convert(String stringValue, SqlTypeName type);
}
