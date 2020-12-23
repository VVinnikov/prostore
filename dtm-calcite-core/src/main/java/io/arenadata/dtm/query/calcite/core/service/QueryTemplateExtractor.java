package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface QueryTemplateExtractor {
    QueryTemplateResult extract(SqlNode sqlNode);
    QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns);
    QueryTemplateResult extract(String sql);
    SqlNode enrichTemplate(EnrichmentTemplateRequest enrichmentTemplateRequest);
}
