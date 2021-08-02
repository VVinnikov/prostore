package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;

import static io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpCheckDataQueryFactory.createCheckDataByCountQuery;
import static io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpCheckDataQueryFactory.createCheckDataByHashInt32Query;

@Service("adpCheckDataService")
public class AdpCheckDataService implements CheckDataService {

    private final DatabaseExecutor queryExecutor;
    private static final String COUNT_COLUMN_NAME = "cnt";
    private static final String HASH_SUM_COLUMN_NAME = "hash_sum";

    public AdpCheckDataService(@Qualifier("adpQueryExecutor") DatabaseExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        ColumnMetadata metadata = new ColumnMetadata(COUNT_COLUMN_NAME, ColumnType.BIGINT);
        return queryExecutor.execute(createCheckDataByCountQuery(request, COUNT_COLUMN_NAME),
                Collections.singletonList(metadata))
                .map(result -> Long.valueOf(result.get(0).get(COUNT_COLUMN_NAME).toString()));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        return checkDataByHash(request);
    }

    private Future<Long> checkDataByHash(CheckDataByHashInt32Request request) {
        val columnMetadata = new ColumnMetadata(HASH_SUM_COLUMN_NAME, ColumnType.BIGINT);
        return queryExecutor.execute(createCheckDataByHashInt32Query(request, HASH_SUM_COLUMN_NAME),
                Collections.singletonList(columnMetadata))
                .map(result -> {
                    val res = result.get(0).get(HASH_SUM_COLUMN_NAME);
                    if (res == null) {
                        return 0L;
                    } else {
                        return Long.valueOf(res.toString());
                    }
                });
    }
}
