package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.plugin.core.Plugin;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

/**
 * Интерфейс взаимодействия с плагинами источников данных
 */
public interface DtmDataSourcePlugin extends Plugin<SourceType> {

    /**
     * <p>Поддержка типа источника</p>
     *
     * @param sourceType тип источника
     * @return поддерживается или нет
     */
    default boolean supports(SourceType sourceType) {
        return getSourceType() == sourceType;
    }

    /**
     * <p>Получить тип Источника</p>
     *
     * @return тип Источника
     */
    SourceType getSourceType();

    /**
     * <p>Применение DDL по созданию базы дынных</p>
     *
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void ddl(DdlRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler);

    /**
     * <p>Получение данных с помощью выполнения Low Latency запроса</p>
     *
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void llr(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Выполнить извлечение данных</p>
     *
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void mpprKafka(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Выполнить загрузку данных</p>
     *
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void mppwKafka(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Получить оценку стоимости выполнения запроса</p>
     *
     * @param context            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void calcQueryCost(QueryCostRequestContext context, Handler<AsyncResult<Integer>> asyncResultHandler);

    /**
     * <p>Возвращает информацию о состоянии плагина и собранную статистику</p>
     */
    void status(StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler);
}
