package io.arenadata.dtm.query.execution.plugin.adb.base.service.enrichment;

import io.arenadata.dtm.query.execution.plugin.adb.base.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Query extender service
 */
public interface QueryExtendService {

    /**
     * Extending query tree
     *
     * @param context context with source query tree
     * @return modified query tree
     */
    RelNode extendQuery(QueryGeneratorContext context);

}
