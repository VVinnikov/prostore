package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import org.apache.calcite.rel.RelNode;

/**
 * Интерфейс сервиса для обогащения запроса
 */
public interface QueryExtendService {

    RelNode extendQuery(QueryGeneratorContext context);

}
