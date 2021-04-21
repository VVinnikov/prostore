package io.arenadata.dtm.query.execution.core.dml.service;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public interface SqlParametersTypeExtractor {
    List<SqlTypeName> extract(RelNode relNode);
}