package io.arenadata.dtm.query.execution.plugin.adg.base.service.enrichment;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Query extending service
 */
public interface QueryExtendService {

    RelNode extendQuery(QueryGeneratorContext context);

}
