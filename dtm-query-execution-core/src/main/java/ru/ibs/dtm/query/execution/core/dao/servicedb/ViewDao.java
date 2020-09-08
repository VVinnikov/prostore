package ru.ibs.dtm.query.execution.core.dao.servicedb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;
import ru.ibs.dtm.query.execution.core.dto.SystemDatamartView;

import java.util.List;

public interface ViewDao {

    void existsView(String viewName, Long datamartId, Handler<AsyncResult<Boolean>> resultHandler);

    void findViewsByDatamart(String datamart, List<String> views, Handler<AsyncResult<List<DatamartView>>> resultHandler);

    void insertView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler);

    void updateView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler);

    void dropView(String viewName, Long datamartId, Handler<AsyncResult<Void>> resultHandler);

    void dropByDatamartId(Long datamartId, Handler<AsyncResult<Void>> resultHandler);

    void findAllSystemViews(Handler<AsyncResult<List<SystemDatamartView>>> resultHandler);
}
