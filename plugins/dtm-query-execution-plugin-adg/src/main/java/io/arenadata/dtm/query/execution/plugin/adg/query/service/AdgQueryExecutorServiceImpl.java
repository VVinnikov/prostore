package io.arenadata.dtm.query.execution.plugin.adg.query.service;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgClientPool;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service("adgQueryExecutor")
public class AdgQueryExecutorServiceImpl implements QueryExecutorService {

    private final AdgClientPool adgClientPool;
    private final SqlTypeConverter adgTypeConverter;
    private final SqlTypeConverter sqlTypeConverter;

    @Autowired
    public AdgQueryExecutorServiceImpl(@Qualifier("adgTtPool") AdgClientPool adgClientPool,
                                       @Qualifier("adgTypeToSqlTypeConverter") SqlTypeConverter adgTypeConverter,
                                       @Qualifier("adgTypeFromSqlTypeConverter") SqlTypeConverter sqlTypeConverter) {
        this.adgClientPool = adgClientPool;
        this.adgTypeConverter = adgTypeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<List<Map<String, Object>>> execute(String sql,
                                                     QueryParameters queryParameters,
                                                     List<ColumnMetadata> queryMetadata) {
        return Future.future(promise -> {
            AdgClient cl = null;
            try {
                cl = adgClientPool.borrowObject();
                List<Object> paramsList = createParamsList(queryParameters);
                log.debug("ADG. Execute query [{}]", sql);
                AsyncUtils.measureMs(cl.callQuery(sql, paramsList.toArray()),
                        duration -> log.debug("ADG. Query completed successfully: [{}] in [{}]ms", sql, duration))
                        .onComplete(ar -> {
                            if (ar.succeeded() && ar.result() != null && !ar.result().isEmpty()) {
                                val map = (Map<?, ?>) ar.result().get(0);
                                val dataSet = (List<List<?>>) map.get("rows");
                                final List<Map<String, Object>> result = new ArrayList<>();
                                try {
                                    dataSet.forEach(row -> {
                                        val rowMap = createRowMap(queryMetadata, row);
                                        result.add(rowMap);
                                    });
                                } catch (Exception e) {
                                    promise.fail(
                                            new DataSourceException("Error converting value to jdbc type", e));
                                    return;
                                }
                                promise.complete(result);
                            } else {
                                promise.fail(ar.cause());
                            }
                        });
            } catch (Exception ex) {
                promise.fail(ex);
            } finally {
                if (cl != null) {
                    adgClientPool.returnObject(cl);
                }
            }
        });
    }

    private List<Object> createParamsList(QueryParameters params) {
        if (params == null) {
            return Collections.emptyList();
        } else {
            return IntStream.range(0, params.getValues().size())
                    .mapToObj(n -> sqlTypeConverter.convert(params.getTypes().get(n),
                            params.getValues().get(n)))
                    .collect(Collectors.toList());
        }
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, List<?> row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < row.size(); i++) {
            final ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(), adgTypeConverter.convert(columnMetadata.getType(), row.get(i)));
        }
        return rowMap;
    }

    @Override
    public Future<Object> executeProcedure(String procedure, Object... args) {
        return Future.future((Promise<Object> promise) -> {
            AdgClient cl = null;
            try {
                cl = adgClientPool.borrowObject();
            } catch (Exception e) {
                promise.fail(new DataSourceException("Error creating Tarantool client", e));
            }
            try {
                log.debug("ADG. Execute procedure [{}] {}", procedure, args);
                AsyncUtils.measureMs(cl.call(procedure, args),
                        duration -> log.debug("ADG. Procedure completed successfully: [{}] in [{}]ms", procedure, duration))
                        .onComplete(ar -> {
                            if (ar.succeeded()) {
                                promise.complete(ar.result());
                            } else {
                                promise.fail(ar.cause());
                            }
                        });
            } finally {
                if (cl != null) {
                    adgClientPool.returnObject(cl);
                }
            }
        });
    }

}
