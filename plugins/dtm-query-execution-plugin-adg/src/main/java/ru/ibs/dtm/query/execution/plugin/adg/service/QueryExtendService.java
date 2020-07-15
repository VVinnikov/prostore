package ru.ibs.dtm.query.execution.plugin.adg.service;

import org.apache.calcite.rel.RelNode;
import ru.ibs.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;

/**
 * Интерфейс сервиса для обогащения запроса
 */
public interface QueryExtendService {

    RelNode extendQuery(QueryGeneratorContext context);

}
