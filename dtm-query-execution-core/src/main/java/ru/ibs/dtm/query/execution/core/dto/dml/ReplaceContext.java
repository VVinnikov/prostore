package ru.ibs.dtm.query.execution.core.dto.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.*;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
public class ReplaceContext {
    private final Map<DatamartViewPair, DatamartViewWrap> viewMap;
    private final Handler<AsyncResult<String>> resultHandler;
    private final List<ViewReplaceAction> resultActions;
    private final List<ViewReplaceAction> tempActions;
    private final Set<DatamartViewPair> tables;
    private final SqlNode rootSqlNode;
    private final String defaultDatamart;

    public ReplaceContext(SqlNode rootSqlNode,
                          String defaultDatamart,
                          Handler<AsyncResult<String>> resultHandler) {
        this.rootSqlNode = rootSqlNode;
        this.defaultDatamart = defaultDatamart;
        this.resultHandler = resultHandler;
        resultActions = new ArrayList<>();
        tempActions = new ArrayList<>();
        viewMap = new HashMap<>();
        tables = new HashSet<>();
    }
}
