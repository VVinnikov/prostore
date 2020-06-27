package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.util.Set;

/**
 * Сервис взаимодействия ядра с плагинами источников данных
 */
public interface DataSourcePluginService {

    /**
     * <p>Получить тип Источника</p>
     *
     * @return пооддерживаемые типы источников
     */
    Set<SourceType> getSourceTypes();

    /**
     * <p>Применение физической модели на БД</p>
     *
     * @param sourceType         тип источника
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void ddl(SourceType sourceType,
             DdlRequestContext context,
             Handler<AsyncResult<Void>> asyncResultHandler);

    /**
     * <p>Выполнить получение данных</p>
     *
     * @param sourceType         тип источника
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void llr(SourceType sourceType,
             LlrRequestContext context,
             Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Выполнить извлечение данных</p>
     *
     * @param sourceType         тип источника
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void mpprKafka(SourceType sourceType,
                   MpprRequestContext context,
                   Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Получить оценку стоимости выполнения запроса</p>
     *
     * @param sourceType         тип источника
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void calcQueryCost(SourceType sourceType,
                       QueryCostRequestContext context,
                       Handler<AsyncResult<Integer>> asyncResultHandler);

    /**
     * <p>Выполнить загрузку данных</p>
     * @param sourceType тип источника
     * @param mppwRequestContext запрос
     * @param resultHandler хэндлер асинхронной обработки результата
     */
    void mppwKafka(SourceType sourceType, MppwRequestContext mppwRequestContext,
                   Handler<AsyncResult<QueryResult>> resultHandler);

    /**
     * <p>Получить статус плагина</p>
     * @param sourceType тип источника
     * @param statusRequestContext запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void status(SourceType sourceType, StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler);
}
