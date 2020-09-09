package ru.ibs.dtm.query.execution.plugin.adqm.service;

import org.apache.calcite.rel.RelNode;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;

/**
 * Интерфейс сервиса для обогащения запроса
 */
public interface QueryExtendService {

    /**
     * Расширение дерева запроса
     *
     * @param context с исходным деревом объектов
     * @return модифицированное дерево запроса
     */
    RelNode extendQuery(QueryGeneratorContext context);

}
