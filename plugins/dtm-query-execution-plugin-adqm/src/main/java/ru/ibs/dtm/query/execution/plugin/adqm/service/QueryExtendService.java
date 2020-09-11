package ru.ibs.dtm.query.execution.plugin.adqm.service;

import org.apache.calcite.rel.RelNode;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;

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
