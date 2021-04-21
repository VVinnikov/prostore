package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service;

import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Service interface to enrich the request
 */
public interface QueryExtendService {

    /**
     * Extending the query tree
     *
     * @param context с исходным деревом объектов
     * @return модифицированное дерево запроса
     */
    RelNode extendQuery(QueryGeneratorContext context);

}