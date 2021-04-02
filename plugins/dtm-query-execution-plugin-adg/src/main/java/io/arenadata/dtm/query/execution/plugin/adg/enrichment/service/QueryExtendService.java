package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service;

import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Query extending service
 */
public interface QueryExtendService {

    RelNode extendQuery(QueryGeneratorContext context);

}
