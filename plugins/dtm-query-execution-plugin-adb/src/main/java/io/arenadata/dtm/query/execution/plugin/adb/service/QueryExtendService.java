package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.query.execution.plugin.adb.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

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
