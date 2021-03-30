package io.arenadata.dtm.query.execution.plugin.api.service;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public interface TemplateParameterConverter {
    List<SqlNode> convert(List<SqlNode> params, List<SqlTypeName> parameterTypes);
}
