package io.arenadata.dtm.query.execution.core.service.query.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.exception.query.PreparedStatementNotFoundException;
import io.arenadata.dtm.query.execution.core.service.query.QueryPreparedService;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class QueryPreparedServiceImpl implements QueryPreparedService {

    private final CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService;

    @Autowired
    public QueryPreparedServiceImpl(@Qualifier("corePreparedQueryCacheService")
                                            CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService) {
        this.preparedQueryCacheService = preparedQueryCacheService;
    }

    @Override
    public SqlNode getPreparedQuery(QueryRequest request) {
        PreparedQueryValue preparedQueryValue = preparedQueryCacheService.get(new PreparedQueryKey(request.getSql()));
        if (preparedQueryValue != null) {
            return preparedQueryValue.getSqlNode();
        } else {
            throw new PreparedStatementNotFoundException();
        }
    }
}
