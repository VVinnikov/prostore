package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.async.AsyncUtils;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ViewDao;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;
import ru.ibs.dtm.query.execution.core.dto.SystemDatamartView;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.dtmservice.Tables.*;

@Repository
@Slf4j
public class ViewDaoImpl implements ViewDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public ViewDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void existsView(String viewName, Long datamartId, Handler<AsyncResult<Boolean>> resultHandler) {
        executor.query(dsl -> dsl
            .select(VIEWS_REGISTRY.DATAMART_ID
                , VIEWS_REGISTRY.VIEW_NAME
                , VIEWS_REGISTRY.QUERY
            )
            .from(VIEWS_REGISTRY)
            .where(VIEWS_REGISTRY.DATAMART_ID.eq(datamartId))
            .and(VIEWS_REGISTRY.VIEW_NAME.eq(viewName))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                int viewsSize = resultSet.getNumRows();
                if (viewsSize == 0) {
                    resultHandler.handle(Future.succeededFuture(false));
                } else if (viewsSize == 1) {
                    resultHandler.handle(Future.succeededFuture(true));
                } else {
                    val failureMsg = String.format("Many views exists by viewName [%s] and datamart [%s]: %s"
                        , viewName
                        , datamartId
                        , resultSet.getRows());
                    log.error(failureMsg);
                    resultHandler.handle(Future.failedFuture(new RuntimeException(failureMsg)));
                }
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void findViewsByDatamart(String datamart, List<String> views, Handler<AsyncResult<List<DatamartView>>> resultHandler) {
        executor.query(dsl -> dsl
            .select(VIEWS_REGISTRY.DATAMART_ID
                , VIEWS_REGISTRY.VIEW_NAME
                , VIEWS_REGISTRY.QUERY
            )
            .from(VIEWS_REGISTRY)
            .join(DATAMARTS_REGISTRY).on(DATAMARTS_REGISTRY.DATAMART_ID.eq(VIEWS_REGISTRY.DATAMART_ID))
            .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.eq(datamart))
            .and(VIEWS_REGISTRY.VIEW_NAME.in(views))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                val viewRecords = new ArrayList<DatamartView>();
                resultSet.getRows().forEach(row -> {
                    viewRecords.add(
                        new DatamartView(row.getString(VIEWS_REGISTRY.VIEW_NAME.getName()),
                            row.getString(VIEWS_REGISTRY.QUERY.getName())));
                });
                resultHandler.handle(Future.succeededFuture(viewRecords));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.insertInto(VIEWS_REGISTRY)
            .set(VIEWS_REGISTRY.VIEW_NAME, viewName)
            .set(VIEWS_REGISTRY.DATAMART_ID, datamartId)
            .set(VIEWS_REGISTRY.QUERY, query)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void updateView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.update(VIEWS_REGISTRY)
            .set(VIEWS_REGISTRY.QUERY, query)
            .where(VIEWS_REGISTRY.VIEW_NAME.eq(viewName))
            .and(VIEWS_REGISTRY.DATAMART_ID.eq(datamartId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropView(String viewName, Long datamartId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.deleteFrom(VIEWS_REGISTRY)
            .where(VIEWS_REGISTRY.VIEW_NAME.eq(viewName))
            .and(VIEWS_REGISTRY.DATAMART_ID.eq(datamartId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropByDatamartId(Long datamartId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.deleteFrom(VIEWS_REGISTRY)
            .where(VIEWS_REGISTRY.DATAMART_ID.eq(datamartId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void findAllSystemViews(Handler<AsyncResult<List<SystemDatamartView>>> resultHandler) {
        executor.query(dsl -> dsl
            .select(SYSTEM_VIEWS_REGISTRY.NAME
                , SYSTEM_VIEWS_REGISTRY.QUERY
                , SYSTEM_VIEWS_REGISTRY.SCHEMA
            )
            .from(SYSTEM_VIEWS_REGISTRY)
        ).setHandler(AsyncUtils.succeed(resultHandler, result -> {
            ResultSet resultSet = result.unwrap();
            val viewRecords = new ArrayList<SystemDatamartView>();
            resultSet.getRows().forEach(row -> viewRecords.add(
                new SystemDatamartView(row.getString(SYSTEM_VIEWS_REGISTRY.NAME.getName()),
                    getDatamartTable(row),
                    row.getString(SYSTEM_VIEWS_REGISTRY.QUERY.getName()))));
            resultHandler.handle(Future.succeededFuture(viewRecords));
        }));
    }

    private DatamartTable getDatamartTable(JsonObject row) {
        try {
            return DatabindCodec.mapper().readValue(row.getString(SYSTEM_VIEWS_REGISTRY.SCHEMA.getName()), DatamartTable.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
