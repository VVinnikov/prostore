package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.ColumnTypeUtil;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service("adgQueryExecutor")
public class AdgQueryExecutorServiceImpl implements QueryExecutorService {

    private TtPool ttPool;
    private final SqlTypeConverter typeConverter;

    @Autowired
    public AdgQueryExecutorServiceImpl(@Qualifier("adgTtPool") TtPool ttPool,
                                       @Qualifier("adgTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
        this.ttPool = ttPool;
        this.typeConverter = typeConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(String sql, List<ColumnMetadata> queryMetadata, Handler<AsyncResult<List<Map<String, Object>>>> handler) {
        TtClient cl = null;
        try {
            cl = ttPool.borrowObject();
            cl.callQuery(ar -> {
                if (ar.succeeded() && ar.result() != null && !ar.result().isEmpty()) {
                    log.debug("ADG. execute query {}", sql);
                    val map = (Map<?, ?>) ar.result().get(0);
                    val metadata = getMetadata((List<Map<String, String>>) map.get("metadata"));
                    val dataSet = (List<List<?>>) map.get("rows");
                    val columnTypeMap =
                            queryMetadata.stream()
                                    .collect(Collectors.toMap(ColumnMetadata::getName, ColumnMetadata::getType));
                    final List<Map<String, Object>> result = new ArrayList<>();
                    try {
                        dataSet.forEach(row -> {
                            val rowMap = createRowMap(metadata, columnTypeMap, row);
                            result.add(rowMap);
                        });
                    } catch (Exception e) {
                        log.error("Error converting ADG values to jdbc types!", e);
                        handler.handle(Future.failedFuture(e));
                        return;
                    }
                    handler.handle(Future.succeededFuture(result));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            }, sql, null);
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        } finally {
            if (cl != null) {
                ttPool.returnObject(cl);
            }
        }
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata,
                                             Map<String, ColumnType> columnTypeMap,
                                             List<?> row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            final String columnName = metadata.get(i).getName();
            rowMap.put(columnName,
                    typeConverter.convert(columnTypeMap.get(columnName), value));
        }
        return rowMap;
    }

    @Override
    public Future<Object> executeProcedure(String procedure, Object... args) {
        return Future.future((Promise<Object> promise) -> {
            TtClient cl = null;
            try {
                cl = ttPool.borrowObject();
            } catch (Exception e) {
                log.error("Error creating Tarantool client", e);
                promise.fail(e);
            }
            try {
                cl.call(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }, procedure, args);
            } finally {
                log.debug("ADG. execute procedure {} {}", procedure, args);
                ttPool.returnObject(cl);
            }
        });
    }

    private List<ColumnMetadata> getMetadata(List<Map<String, String>> columns) {
        return columns.stream().map(it -> {
            if (!it.containsKey("name")) {
                throw new IllegalStateException("name is not specified");
            }
            if (!it.containsKey("type")) {
                throw new IllegalStateException("type is not specified");
            }
            return new ColumnMetadata(it.get("name"), ColumnTypeUtil.columnTypeFromTtColumnType(it.get("type")));
        }).collect(Collectors.toList());
    }

}
