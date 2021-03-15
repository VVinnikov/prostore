package io.arenadata.dtm.query.execution.plugin.adqm.service;

import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface TemplateParameterConverter {
    List<SqlNode> convert(List<SqlNode> params);
}
