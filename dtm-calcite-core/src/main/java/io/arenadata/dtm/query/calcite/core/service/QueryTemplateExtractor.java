package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.dto.QueryTemplateResult;
import org.apache.calcite.sql.SqlNode;

public interface QueryTemplateExtractor {
    QueryTemplateResult extract(SqlNode sqlNode);
    QueryTemplateResult extract(String sql);
    SqlNode enrichTemplate(EnrichmentTemplateRequest enrichmentTemplateRequest);
}
