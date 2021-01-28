package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Query extending service
 */
public interface QueryExtendService {

    RelNode extendQuery(QueryGeneratorContext context);

}
