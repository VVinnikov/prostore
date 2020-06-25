package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class DropViewDdlExecutor extends QueryResultDdlExecutor {
    @Autowired
    public DropViewDdlExecutor(MetadataFactory<DdlRequestContext> metadataFactory,
                               MariaProperties mariaProperties,
                               ServiceDao serviceDao) {
        super(metadataFactory, mariaProperties, serviceDao);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        val sql = getSql(context, sqlNodeName);
        val viewName = SqlPreparer.getViewName(sql);
        findDatamart(schema)
                .compose(datamartId -> dropView(viewName, datamartId))
                .onComplete(handler);
    }

    private Future<Long> findDatamart(String datamartName) {
        return Future.future(p -> serviceDao.findDatamart(datamartName, ar -> {
            if (ar.succeeded()) {
                p.complete(ar.result());
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<QueryResult> dropView(String viewName, Long datamartId) {
        return Future.future(p -> serviceDao.existsView(viewName, datamartId, existsHandler -> {
            if (existsHandler.succeeded()) {
                if (existsHandler.result()) {
                    serviceDao.dropView(viewName, datamartId, updateHandler -> {
                        if (updateHandler.succeeded()) {
                            p.complete();
                        } else {
                            p.fail(updateHandler.cause());
                        }
                    });
                } else {
                    val failureMsg = String.format("View is not exists [%s] by datamartId [%s]"
                            , viewName
                            , datamartId);
                    log.error(failureMsg);
                    p.fail(failureMsg);
                }
            } else {
                p.fail(existsHandler.cause());
            }
        }));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_VIEW;
    }
}
