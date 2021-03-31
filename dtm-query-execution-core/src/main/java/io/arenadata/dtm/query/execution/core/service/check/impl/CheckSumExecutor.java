package io.arenadata.dtm.query.execution.core.service.check.impl;

import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckSum;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.dto.check.CheckSumRequestContext;
import io.arenadata.dtm.query.execution.core.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.check.CheckExecutor;
import io.arenadata.dtm.query.execution.core.service.check.CheckSumTableService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service("checkSumExecutor")
public class CheckSumExecutor implements CheckExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final CheckSumTableService checkSumTableService;
    private final CheckQueryResultFactory queryResultFactory;

    @Autowired
    public CheckSumExecutor(DeltaServiceDao deltaServiceDao,
                            EntityDao entityDao,
                            CheckSumTableService checkSumTableService,
                            CheckQueryResultFactory queryResultFactory) {
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.checkSumTableService = checkSumTableService;
        this.queryResultFactory = queryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(promise -> {
            SqlCheckSum sqlCheckSum = (SqlCheckSum) context.getSqlNode();
            val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
            val deltaNum = sqlCheckSum.getDeltaNum();
            val table = Optional.ofNullable(sqlCheckSum.getTable());
            val columns = sqlCheckSum.getColumns();
            val checkContext = CheckSumRequestContext.builder()
                    .checkContext(context)
                    .datamart(datamart)
                    .columns(columns)
                    .build();
            deltaServiceDao.getDeltaHot(datamart)
                    .compose(hotDelta -> {
                        if (hotDelta == null || hotDelta.getDeltaNum() != deltaNum) {
                            return deltaServiceDao.getDeltaByNum(datamart, deltaNum)
                                    .compose(okDelta -> calculateCheckSum(table, checkContext, okDelta.getCnFrom(), okDelta.getCnTo()));
                        } else {
                            return calculateCheckSum(table, checkContext, hotDelta.getCnFrom(), hotDelta.getCnTo());
                        }
                    })
                    .onSuccess(sum -> promise.complete(createQueryResult(sum)))
                    .onFailure(promise::fail);
        });
    }

    private Future<Long> calculateCheckSum(Optional<String> table, CheckSumRequestContext checkContext, long cnFrom, long cnTo) {
        checkContext.setCnFrom(cnFrom);
        checkContext.setCnTo(cnTo);
        if (table.isPresent()) {
            return entityDao.getEntity(checkContext.getDatamart(), table.get())
                    .map(e -> {
                        if (e.getEntityType() != EntityType.TABLE) {
                            throw new EntityNotExistsException(e.getName());
                        } else {
                            checkContext.setEntity(e);
                            return e;
                        }
                    })
                    .compose(entity -> checkSumTableService.calcCheckSumTable(checkContext));
        } else {
            return checkSumTableService.calcCheckSumForAllTables(checkContext);
        }
    }

    private QueryResult createQueryResult(Long sum) {
        return queryResultFactory.create(sum == null ? null : sum.toString());
    }

    @Override
    public CheckType getType() {
        return CheckType.SUM;
    }
}
